
def app_maintenance(env, start_response):
    """This will answer requests while in maintenance mode."""
    content_type = [('Content-Type', 'text/html')]

    if env.get('REQUEST_URI', '') == '/ping':
        # This may satisfy readiness probes, used in certain cloud services.
        start_response('200 OK', content_type)
        return b'success'

    start_response('503 Service Unavailable', content_type)
    return b'Service is temporarily unavailable due to a maintenance.<br>Please come back later.'
