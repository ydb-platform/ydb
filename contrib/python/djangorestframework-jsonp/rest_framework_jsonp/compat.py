def get_query_params(request):
    if hasattr(request, 'query_params'):
        params = request.query_params
    else:
        # DRF < 3.2
        params = request.QUERY_PARAMS

    return params
