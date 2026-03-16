"""
This module contains the middleware that can be applied to any request, to enable query profiler.
It internally calls context manager, and has the same output as the context manager.  It just adds some response
headers, for the chrome plugin to display
"""
import json
from time import time
from typing import Callable, Optional

from django.conf import settings
from django.http import HttpRequest
from django.http.response import HttpResponseBase
from django.urls import reverse

import django_query_profiler.client.urls as query_profiler_url
from django_query_profiler.chrome_plugin_helpers import ChromePluginData, redis_utils
from django_query_profiler.client.context_manager import QueryProfiler
from django_query_profiler.query_profiler_storage import QueryProfiledData, QueryProfilerLevel

DETAILED_VIEW_EXCEPTION_URL = "django_query_profiler_detailed_view_not_setup_check_redis_and_urls.py"
DETAILED_VIEW_EXCEPTION_LINK_TEXT = "redis_or_urls.py_not_setup"


class QueryProfilerMiddleware:

    def __init__(self, get_response: Callable):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponseBase:
        # Check if we have to enable query profiler or not.
        query_profiler_level: Optional[QueryProfilerLevel] = settings.DJANGO_QUERY_PROFILER_LEVEL_FUNC(request)
        if not query_profiler_level:
            return self.get_response(request)

        start_time: float = time()
        '''
        Lets clear all the storage related to this thread.  This is not strictly needed, but just as a safety measure
        As a side effect, this implies that we *CANNOT* use this middleware twice for a request
        '''
        with QueryProfiler(query_profiler_level, clear_thread_local=True) as query_profiler:
            response = self.get_response(request)

        query_profiled_data: QueryProfiledData = query_profiler.query_profiled_data
        try:
            # Pickling the object, and saving to redis
            redis_key: str = redis_utils.store_data(query_profiled_data)
            query_profiled_detail_relative_url: str = reverse(query_profiler_url.GET_QUERY_PROFILED_DATA_NAME,
                                                              args=[redis_key, query_profiler_level.name])
            query_profiled_detail_absolute_url: str = request.build_absolute_uri(query_profiled_detail_relative_url)
            detailed_view_link_text: str = query_profiler_level.name.lower()
        except Exception as ex:
            # The exception can happen because of two reasons:
            # 1. redis throws exception
            # 2. detailed_view_url not setup in urls.py
            if settings.DJANGO_QUERY_PROFILER_IGNORE_DETAILED_VIEW_EXCEPTION:
                query_profiled_detail_absolute_url: str = DETAILED_VIEW_EXCEPTION_URL
                detailed_view_link_text: str = DETAILED_VIEW_EXCEPTION_LINK_TEXT
            else:
                raise ex

        # Setting all headers that the chrome plugin require
        response[ChromePluginData.QUERY_PROFILED_SUMMARY_DATA] = json.dumps(query_profiled_data.summary.as_dict())
        response[ChromePluginData.QUERY_PROFILED_DETAILED_URL] = query_profiled_detail_absolute_url
        response[ChromePluginData.TIME_SPENT_PROFILING_IN_MICROS] = query_profiled_data.time_spent_profiling_in_micros
        response[ChromePluginData.TOTAL_SERVER_TIME_IN_MILLIS] = int((time() - start_time) * 1000)
        response[ChromePluginData.QUERY_PROFILER_DETAILED_VIEW_LINK_TEXT] = detailed_view_link_text

        settings.DJANGO_QUERY_PROFILER_POST_PROCESSOR(query_profiled_data, request, response)
        return response
