from itertools import product
import json

import pandas as pd


from pytrends import exceptions
import aiohttp
from aiohttp import ClientSession, ClientResponse
from aiohttp_socks import ProxyConnector, SocksVer
from urllib.parse import quote


BASE_TRENDS_URL = 'https://trends.google.com/trends'

country_codes = [
    'cy', 'pe', 'hk', 'co', 'mx', 'br', 'ua', 'ng', 'ph', 'au',
    'tr', 'vn', 'tw', 'ar', 'gi', 'my', 'gh', 'ec', 'na', 'sb',
    'ni', 'gt', 'pr', 'sg', 'bh', 'nf', 'pl', 'bo', 'ag', 'om',
    'sa', 'bn', 'lb', 'pk', 'py', 'qa', 'ru', 'pa', 'cu', 'np',
    'sl', 'do', 'sv', 'mt', 'eg', 'kw', 'et', 'ly', 'fj', 'jm',
    'kh', 'uy', 'bd', 'mm', 'vc', 'af', 'tj', 'ai', 'bz', 'pg',
    'by', 'iq', 'tn', 'ge', 'kz', 'gr', 'jo', 'nr', 've', 'lv',
    'pt', 'dz', 'ht', 'gy', 'bi', 'ps', 'gp', 'am'
]
class TrendReq(object):
    """
    Google Trends API
    """
    GET_METHOD = 'get'
    POST_METHOD = 'post'

    ERROR_CODES = (500, 502, 504, 429)

    async def __init__(self, hl='en-US', base_url=None, tz=360, geo='', timeout=(2, 5), proxies='',
                 retries=0, backoff_factor=0, requests_args=None):
        """
        Initialize default values for params
        """
        self.BASE_TRENDS_URL = f'https://trends.google.com.{base_url}/trends' if base_url is not None and base_url in country_codes else 'https://trends.google.com/trends'

        self.GENERAL_URL = f'{self.BASE_TRENDS_URL}/api/explore'
        self.INTEREST_OVER_TIME_URL = f'{self.BASE_TRENDS_URL}/api/widgetdata/multiline'
        self.MULTIRANGE_INTEREST_OVER_TIME_URL = f'{self.BASE_TRENDS_URL}/api/widgetdata/multirange'
        self.INTEREST_BY_REGION_URL = f'{self.BASE_TRENDS_URL}/api/widgetdata/comparedgeo'
        self.RELATED_QUERIES_URL = f'{self.BASE_TRENDS_URL}/api/widgetdata/relatedsearches'
        self.TRENDING_SEARCHES_URL = f'{self.BASE_TRENDS_URL}/hottrends/visualize/internal/data'
        self.TOP_CHARTS_URL = f'{self.BASE_TRENDS_URL}/api/topcharts'
        self.SUGGESTIONS_URL = f'{self.BASE_TRENDS_URL}/api/autocomplete/'
        self.CATEGORIES_URL = f'{self.BASE_TRENDS_URL}/api/explore/pickers/category'
        self.TODAY_SEARCHES_URL = f'{self.BASE_TRENDS_URL}/api/dailytrends'
        self.REALTIME_TRENDING_SEARCHES_URL = f'{self.BASE_TRENDS_URL}/api/realtimetrends'

        # google rate limit
        self.google_rl = 'You have reached your quota limit. Please try again later.'
        self.results = None
        # set user defined options used globally
        self.tz = tz
        self.hl = hl
        self.geo = geo
        self.kw_list = list()
        self.timeout = timeout
        self.proxies = proxies  # add a proxy option
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.proxy_index = 0
        self.requests_args = requests_args or {}
        self.cookies = self.GetGoogleCookie()
        # intialize widget payloads
        self.token_payload = dict()
        self.interest_over_time_widget = dict()
        self.interest_by_region_widget = dict()
        self.related_topics_widget_list = list()
        self.related_queries_widget_list = list()

        self.headers = {'accept-language': self.hl}
        self.headers.update(self.requests_args.pop('headers', {}))
    async def get_google_cookies(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'{self.BASE_TRENDS_URL}/explore/?geo={self.hl[-2:]}',
                proxy=self.proxies,
                timeout=self.timeout,
                **self.requests_args
            ) as response:
                # aiohttp automatically stores cookies in the session.
                # If you need to access them, you can retrieve them like this:
                cookies = session.cookie_jar.filter_cookies(response.url)
                return cookies.items()
    async def GetGoogleCookie(self):
        """
        Gets google cookie (used for each and every proxy; once on init otherwise)
        Removes proxy from the list on proxy error
        """
        while True:
            if "proxies" in self.requests_args:
                try:
                    cookies= await self.get_google_cookies()
                    return dict(filter(lambda i: i[0] == 'NID', cookies))
                except:
                    continue
            else:
                if len(self.proxies) > 0:
                    proxy = {'https': self.proxies[self.proxy_index]}
                else:
                    proxy = ''
                try:
                    self.proxies=proxy
                    cookies= await self.get_google_cookies()

                    return dict(filter(lambda i: i[0] == 'NID', cookies))
                except Exception:
                    print('Proxy error. Changing IP')
                    if len(self.proxies) > 1:
                        self.proxies.remove(self.proxies[self.proxy_index])
                    else:
                        print('No more proxies available. Bye!')
                        raise
                    continue

    async def GetNewProxy(self):
        """
        Increment proxy INDEX; zero on overflow
        """
        if self.proxy_index < (len(self.proxies) - 1):
            self.proxy_index += 1
        else:
            self.proxy_index = 0


    async def _get_data(self, url: str, method: str, **kwargs):
        """Send a request to Google and return the JSON response as a Python object."""
        async with ClientSession() as session:
            headers = kwargs.get('headers', {})
            headers.update(self.headers)
            params = kwargs.get('params', {})
            payload = kwargs.get('payload', {})

            proxy_url = self.proxies[self.proxy_index] if self.proxies and self.proxy_index < len(self.proxies) else None
            connector = None

            if proxy_url:
                connector = ProxyConnector.from_url(proxy_url, socks_ver=SocksVer.SOCKS5)

            request_options = {
                'headers': headers,
                'timeout': self.timeout,
                'connector': connector,
                **kwargs
            }

            if method.upper() == 'GET':
                response: ClientResponse = await session.get(url, params=params, **request_options)
            elif method.upper() == 'POST':
                response = await session.post(url, json=payload, **request_options)
            else:
                raise ValueError(f"HTTP method {method} is not supported.")

            # Check if the response content type is JSON
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                try:
                    # Read and parse the response content
                    content = await response.text()
                    # Clean up the content if necessary
                    content = content.lstrip(")]}'\n")
                    return json.loads(content)
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON from response: {e}")
                    # You may want to handle or re-raise the exception based on your needs
            else:
                print(f"Unexpected Content-Type: {content_type}")
                # Handle non-JSON responses or raise an error

            # Raise an exception for 4xx/5xx errors
            response.raise_for_status()

    async def build_payload(self, kw_list, cat=0, timeframe='today 5-y', geo='',
                      gprop=''):
        """Create the payload for related queries, interest over time and interest by region"""
        if gprop not in ['', 'images', 'news', 'youtube', 'froogle']:
            raise ValueError('gprop must be empty (to indicate web), images, news, youtube, or froogle')
        self.kw_list = kw_list
        self.geo = geo or self.geo
        self.token_payload = {
            'hl': self.hl,
            'tz': self.tz,
            'req': {'comparisonItem': [], 'category': cat, 'property': gprop}
        }

        if not isinstance(self.geo, list):
            self.geo = [self.geo]

        # Check if timeframe is a list
        if isinstance(timeframe, list):
            for index, (kw, geo) in enumerate(product(self.kw_list, self.geo)):
                keyword_payload = {'keyword': kw, 'time': timeframe[index], 'geo': geo}
                self.token_payload['req']['comparisonItem'].append(keyword_payload)
        else:
            # build out json for each keyword with
            for kw, geo in product(self.kw_list, self.geo):
                keyword_payload = {'keyword': kw, 'time': timeframe, 'geo': geo}
                self.token_payload['req']['comparisonItem'].append(keyword_payload)

        # requests will mangle this if it is not a string
        self.token_payload['req'] = json.dumps(self.token_payload['req'])
        # get tokens
        self._tokens()
        return

    async def _tokens(self):
        """Makes request to Google to get API tokens for interest over time, interest by region and related queries"""
        # make the request and parse the returned json
        widget_dicts = await  self._get_data(
            url=self.GENERAL_URL,
            method=TrendReq.POST_METHOD,
            params=self.token_payload,
            trim_chars=4,
        )['widgets']
        # order of the json matters...
        first_region_token = True
        # clear self.related_queries_widget_list and self.related_topics_widget_list
        # of old keywords'widgets
        self.related_queries_widget_list[:] = []
        self.related_topics_widget_list[:] = []
        # assign requests
        for widget in widget_dicts:
            if widget['id'] == 'TIMESERIES':
                self.interest_over_time_widget = widget
            if widget['id'] == 'GEO_MAP' and first_region_token:
                self.interest_by_region_widget = widget
                first_region_token = False
            # response for each term, put into a list
            if 'RELATED_TOPICS' in widget['id']:
                self.related_topics_widget_list.append(widget)
            if 'RELATED_QUERIES' in widget['id']:
                self.related_queries_widget_list.append(widget)
        return

    async def interest_over_time(self):
        """Request data from Google's Interest Over Time section and return a dataframe"""

        over_time_payload = {
            # convert to string as requests will mangle
            'req': json.dumps(self.interest_over_time_widget['request']),
            'token': self.interest_over_time_widget['token'],
            'tz': self.tz
        }

        # make the request and parse the returned json
        req_json = await self._get_data(
            url=self.INTEREST_OVER_TIME_URL,
            method=TrendReq.GET_METHOD,
            trim_chars=5,
            params=over_time_payload,
        )

        df = pd.DataFrame(req_json['default']['timelineData'])
        if (df.empty):
            return df

        df['date'] = pd.to_datetime(df['time'].astype(dtype='float64'),
                                    unit='s')
        df = df.set_index(['date']).sort_index()
        # split list columns into seperate ones, remove brackets and split on comma
        result_df = df['value'].apply(lambda x: pd.Series(
            str(x).replace('[', '').replace(']', '').split(',')))
        # rename each column with its search term, relying on order that google provides...

        for idx, (kw, g) in enumerate(product(self.kw_list, self.geo)):
            # there is currently a bug with assigning columns that may be
            # parsed as a date in pandas: use explicit insert column method
            name = kw if len(self.geo) == 1 else (kw, g)
            result_df.insert(len(result_df.columns), name,
                             result_df[idx].astype('int'))
            del result_df[idx]

        if 'isPartial' in df:
            # make other dataframe from isPartial key data
            # split list columns into seperate ones, remove brackets and split on comma
            df = df.infer_objects(copy=False)
            # .fillna(False)

            result_df2 = df['isPartial'].apply(lambda x: pd.Series(
                str(x).replace('[', '').replace(']', '').split(',')))
            result_df2.columns = ['isPartial']
            # Change to a bool type.
            result_df2.isPartial = result_df2.isPartial == 'True'
            # concatenate the two dataframes
            final = pd.concat([result_df, result_df2], axis=1)
        else:
            final = result_df
            final['isPartial'] = False

        if len(self.geo) > 1:
            final.columns = pd.MultiIndex.from_tuples(
                [c if isinstance(c, tuple) else (c, ) for c in final],
                names=['keyword', 'region']
            )
        return final

    async def multirange_interest_over_time(self):
        """Request data from Google's Interest Over Time section across different time ranges and return a dataframe"""

        over_time_payload = {
            # convert to string as requests will mangle
            'req': json.dumps(self.interest_over_time_widget['request']),
            'token': self.interest_over_time_widget['token'],
            'tz': self.tz
        }

        # make the request and parse the returned json
        req_json = await self._get_data(
            url=self.MULTIRANGE_INTEREST_OVER_TIME_URL,
            method=TrendReq.GET_METHOD,
            trim_chars=5,
            params=over_time_payload,
        )

        df = pd.DataFrame(req_json['default']['timelineData'])
        if (df.empty):
            return df

        result_df = pd.json_normalize(df['columnData'])

        # Split dictionary columns into seperate ones
        for i, column in enumerate(result_df.columns):
            result_df["[" + str(i) + "] " + str(self.kw_list[i]) + " date"] = result_df[i].apply(pd.Series)["formattedTime"]
            result_df["[" + str(i) + "] " + str(self.kw_list[i]) + " value"] = result_df[i].apply(pd.Series)["value"]
            result_df = result_df.drop([i], axis=1)

        # Adds a row with the averages at the top of the dataframe
        avg_row = {}
        for i, avg in enumerate(req_json['default']['averages']):
            avg_row["[" + str(i) + "] " + str(self.kw_list[i]) + " date"] = "Average"
            avg_row["[" + str(i) + "] " + str(self.kw_list[i]) + " value"] = req_json['default']['averages'][i]

        result_df.loc[-1] = avg_row
        result_df.index = result_df.index + 1
        result_df = result_df.sort_index()

        return result_df


    async def interest_by_region(self, resolution='COUNTRY', inc_low_vol=False,
                           inc_geo_code=False):
        """Request data from Google's Interest by Region section and return a dataframe"""

        # make the request
        region_payload = dict()
        if self.geo == '':
            self.interest_by_region_widget['request'][
                'resolution'] = resolution
        elif self.geo == 'US' and resolution in ['DMA', 'CITY', 'REGION']:
            self.interest_by_region_widget['request'][
                'resolution'] = resolution

        self.interest_by_region_widget['request'][
            'includeLowSearchVolumeGeos'] = inc_low_vol

        # convert to string as requests will mangle
        region_payload['req'] = json.dumps(
            self.interest_by_region_widget['request'])
        region_payload['token'] = self.interest_by_region_widget['token']
        region_payload['tz'] = self.tz

        # parse returned json
        req_json = await self._get_data(
            url=self.INTEREST_BY_REGION_URL,
            method=TrendReq.GET_METHOD,
            trim_chars=5,
            params=region_payload,
        )
        df = pd.DataFrame(req_json['default']['geoMapData'])
        if (df.empty):
            return df

        # rename the column with the search keyword
        geo_column = 'geoCode' if 'geoCode' in df.columns else 'coordinates'
        columns = ['geoName', geo_column, 'value']
        df = df[columns].set_index(['geoName']).sort_index()
        # split list columns into separate ones, remove brackets and split on comma
        result_df = df['value'].apply(lambda x: pd.Series(
            str(x).replace('[', '').replace(']', '').split(',')))
        if inc_geo_code:
            if geo_column in df.columns:
                result_df[geo_column] = df[geo_column]
            else:
                print('Could not find geo_code column; Skipping')

        # rename each column with its search term
        for idx, kw in enumerate(self.kw_list):
            result_df[kw] = result_df[idx].astype('int')
            del result_df[idx]

        return result_df

    async def related_topics(self):
        """Request data from Google's Related Topics section and return a dictionary of dataframes

        If no top and/or rising related topics are found, the value for the key "top" and/or "rising" will be None
        """

        # make the request
        related_payload = dict()
        result_dict = dict()
        for request_json in self.related_topics_widget_list:
            # ensure we know which keyword we are looking at rather than relying on order
            try:
                kw = request_json['request']['restriction'][
                    'complexKeywordsRestriction']['keyword'][0]['value']
            except KeyError:
                kw = ''
            # convert to string as requests will mangle
            related_payload['req'] = json.dumps(request_json['request'])
            related_payload['token'] = request_json['token']
            related_payload['tz'] = self.tz

            # parse the returned json
            req_json = await self._get_data(
                url=self.RELATED_QUERIES_URL,
                method=TrendReq.GET_METHOD,
                trim_chars=5,
                params=related_payload,
            )

            # top topics
            try:
                top_list = req_json['default']['rankedList'][0]['rankedKeyword']
                df_top = pd.json_normalize(top_list, sep='_')
            except KeyError:
                # in case no top topics are found, the lines above will throw a KeyError
                df_top = None

            # rising topics
            try:
                rising_list = req_json['default']['rankedList'][1]['rankedKeyword']
                df_rising = pd.json_normalize(rising_list, sep='_')
            except KeyError:
                # in case no rising topics are found, the lines above will throw a KeyError
                df_rising = None

            result_dict[kw] = {'rising': df_rising, 'top': df_top}
        return result_dict

    async def related_queries(self):
        """Request data from Google's Related Queries section and return a dictionary of dataframes

        If no top and/or rising related queries are found, the value for the key "top" and/or "rising" will be None
        """

        # make the request
        related_payload = dict()
        result_dict = dict()
        for request_json in self.related_queries_widget_list:
            # ensure we know which keyword we are looking at rather than relying on order
            try:
                kw = request_json['request']['restriction'][
                    'complexKeywordsRestriction']['keyword'][0]['value']
            except KeyError:
                kw = ''
            # convert to string as requests will mangle
            related_payload['req'] = json.dumps(request_json['request'])
            related_payload['token'] = request_json['token']
            related_payload['tz'] = self.tz

            # parse the returned json
            req_json = await self._get_data(
                url=self.RELATED_QUERIES_URL,
                method=TrendReq.GET_METHOD,
                trim_chars=5,
                params=related_payload,
            )

            # top queries
            try:
                top_df = pd.DataFrame(
                    req_json['default']['rankedList'][0]['rankedKeyword'])
                top_df = top_df[['query', 'value']]
            except KeyError:
                # in case no top queries are found, the lines above will throw a KeyError
                top_df = None

            # rising queries
            try:
                rising_df = pd.DataFrame(
                    req_json['default']['rankedList'][1]['rankedKeyword'])
                rising_df = rising_df[['query', 'value']]
            except KeyError:
                # in case no rising queries are found, the lines above will throw a KeyError
                rising_df = None

            result_dict[kw] = {'top': top_df, 'rising': rising_df}
        return result_dict

    async def trending_searches(self, pn='united_states'):
        """Request data from Google's Hot Searches section and return a dataframe"""

        # make the request
        # forms become obsolete due to the new TRENDING_SEARCHES_URL
        # forms = {'ajax': 1, 'pn': pn, 'htd': '', 'htv': 'l'}
        req_json = await self._get_data(
            url=self.TRENDING_SEARCHES_URL,
            method=TrendReq.GET_METHOD
        )[pn]
        result_df = pd.DataFrame(req_json)
        return result_df

    async def today_searches(self, pn='US'):
        """Request data from Google Daily Trends section and returns a dataframe"""
        forms = {'ns': 15, 'geo': pn, 'tz': '-180', 'hl': self.hl}
        req_json = await self._get_data(
            url=self.TODAY_SEARCHES_URL,
            method=TrendReq.GET_METHOD,
            trim_chars=5,
            params=forms,
            **self.requests_args
        )['default']['trendingSearchesDays'][0]['trendingSearches']
        # parse the returned json
        result_df = pd.DataFrame(trend['title'] for trend in req_json)
        return result_df.iloc[:, -1]

    async def realtime_trending_searches(self, pn='US', cat='all', count =300):
        """Request data from Google Realtime Search Trends section and returns a dataframe"""
        # Don't know what some of the params mean here, followed the nodejs library
        # https://github.com/pat310/google-trends-api/ 's implemenration


        #sort: api accepts only 0 as the value, optional parameter

        # ri: number of trending stories IDs returned,
        # max value of ri supported is 300, based on emperical evidence

        ri_value = 300
        if count < ri_value:
            ri_value = count

        # rs : don't know what is does but it's max value is never more than the ri_value based on emperical evidence
        # max value of ri supported is 200, based on emperical evidence
        rs_value = 200
        if count < rs_value:
            rs_value = count-1

        forms = {'ns': 15, 'geo': pn, 'tz': '300', 'hl': self.hl, 'cat': cat, 'fi' : '0', 'fs' : '0', 'ri' : ri_value, 'rs' : rs_value, 'sort' : 0}
        req_json = await self._get_data(
            url=self.REALTIME_TRENDING_SEARCHES_URL,
            method=TrendReq.GET_METHOD,
            trim_chars=5,
            params=forms
        )['storySummaries']['trendingStories']

        # parse the returned json
        wanted_keys = ["entityNames", "title"]

        final_json = [{ key: ts[key] for key in ts.keys() if key in wanted_keys} for ts in req_json ]

        result_df = pd.DataFrame(final_json)

        return result_df

    async def top_charts(self, date, hl='en-US', tz=300, geo='GLOBAL'):
        """Request data from Google's Top Charts section and return a dataframe"""

        try:
            date = int(date)
        except:
            raise ValueError(
                'The date must be a year with format YYYY. See https://github.com/GeneralMills/pytrends/issues/355')

        # create the payload
        chart_payload = {'hl': hl, 'tz': tz, 'date': date, 'geo': geo,
                         'isMobile': False}

        # make the request and parse the returned json
        req_json = await self._get_data(
            url=self.TOP_CHARTS_URL,
            method=TrendReq.GET_METHOD,
            trim_chars=5,
            params=chart_payload
        )
        try:
            df = pd.DataFrame(req_json['topCharts'][0]['listItems'])
        except IndexError:
            df = None
        return df

    async def suggestions(self, keyword):
        """Request data from Google's Keyword Suggestion dropdown and return a dictionary"""

        # make the request
        kw_param = quote(keyword)
        parameters = {'hl': self.hl}

        req_json = await self._get_data(
            url=self.SUGGESTIONS_URL + kw_param,
            params=parameters,
            method=TrendReq.GET_METHOD,
            trim_chars=5
        )['default']['topics']
        return req_json

    async def categories(self):
        """Request available categories data from Google's API and return a dictionary"""

        params = {'hl': self.hl}

        req_json = await self._get_data(
            url=self.CATEGORIES_URL,
            params=params,
            method=TrendReq.GET_METHOD,
            trim_chars=5
        )
        return req_json

    async def get_historical_interest(self, *args, **kwargs):
        raise NotImplementedError(
            """This method has been removed for incorrectness. It will be removed completely in v5.
If you'd like similar functionality, please try implementing it yourself and consider submitting a pull request to add it to pytrends.

There is discussion at:
https://github.com/GeneralMills/pytrends/pull/542"""
        )
