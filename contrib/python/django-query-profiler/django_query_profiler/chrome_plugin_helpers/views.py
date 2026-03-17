import json
from typing import Dict

from django.http import HttpResponse
from django.shortcuts import render

from django_query_profiler.chrome_plugin_helpers import redis_utils
from django_query_profiler.query_profiler_storage import QueryProfiledData, QueryProfilerLevel

QUERY_PROFILER_LEVEL_TO_TEMPLATE: Dict[str, str] = {
    QueryProfilerLevel.QUERY_SIGNATURE.name: 'django_query_profiler_level_query_signature.html',
    QueryProfilerLevel.QUERY.name: 'django_query_profiler_level_query.html',
}


def get_query_profiled_data(request, redis_key: str, query_profiler_level: str) -> HttpResponse:
    query_profiled_data: QueryProfiledData = redis_utils.retrieve_data(redis_key)
    context = {
        'summary': query_profiled_data.summary,
        'query_signature_to_statistics': query_profiled_data.query_signature_to_query_signature_statistics,
        'flamegraphStack': json.dumps(query_profiled_data.flamegraph_stack),
    }
    return render(request, QUERY_PROFILER_LEVEL_TO_TEMPLATE[query_profiler_level], context)
