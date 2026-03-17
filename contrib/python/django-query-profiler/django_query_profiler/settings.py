from typing import Optional

from django.http import HttpRequest
from django.http.response import HttpResponseBase

from django_query_profiler.query_profiler_storage import QueryProfiledData, QueryProfilerLevel

# Parameters for configuring redis
DJANGO_QUERY_PROFILER_REDIS_HOST: str = 'localhost'
DJANGO_QUERY_PROFILER_REDIS_PORT: int = 6379
DJANGO_QUERY_PROFILER_REDIS_DB: int = 0
DJANGO_QUERY_PROFILER_REDIS_KEYS_EXPIRY_SECONDS: int = 3600

"""
Parameter that controls if we should eat up the exception if redis or urls.py is not configured
redis & urls.py are required for setting up the detailed view - the view that opens up when clicking on the
link in the chrome plugin that says "Details link"
Setting this parameter to True means that we would still get the summary view, and not raise Exception
"""
DJANGO_QUERY_PROFILER_IGNORE_DETAILED_VIEW_EXCEPTION = True

"""
The list of modules which would not be considered as part of application modules, hence the stack-traces originating
from these modules would not be shown in the stack-trace

NB: Not having django.contrib here
"""
DJANGO_QUERY_PROFILER_APP_MODULES_TO_EXCLUDE = (
    'django.apps', 'django.bin', 'django.conf', 'django.core',
    'django.db', 'django.dispatch', 'django.forms', 'django.http',
    'django.template', 'django.templatetags', 'django.utils', 'django.test',
    'IPython', 'django_query_profiler', 'test', 'socketserver', 'threading')


# noinspection PyPep8Naming
def DJANGO_QUERY_PROFILER_LEVEL_FUNC(request) -> Optional[QueryProfilerLevel]:
    """
    The middleware calls this function for every request, and determines what kind of profiling to apply for that
    request.  There are three options:
    1. QUERY:  This does not capture the stack-trace, just the query.  The unit of grouping is the normalized query.
        This would not give us information about N+1 code paths though
    2. QUERY_SIGNATURE:  This captures both the stack-trace, and the query.  The unit of grouping is the tuple of
        (query, stack-trace), and figures out N+1 code paths, and provide recommendation to remove N+1's
    3. None:  This disables profiling for that request

    NB:  Name of the function is all caps because of settings file restriction of all names to be all caps,
        including functions
    """
    return QueryProfilerLevel.QUERY_SIGNATURE


# noinspection PyPep8Naming
def DJANGO_QUERY_PROFILER_POST_PROCESSOR(
        query_profiled_data: QueryProfiledData,
        request: HttpRequest,
        response: HttpResponseBase) -> None:
    """
    This function is called by the QueryProfilerMiddleware after it has profiled the data.  This is a hook for the
    clients to do something with the data collected by profiler.

    One example could be to log the data in a log file, and do some analysis with it later.
    Another example could be to add additional headers on the response, or change the values that are set by the
    QueryProfilerMiddleware, and maybe build your own chrome plugin
    """
    pass
