from ModelTracker.Tracker import ModelTracker


class ModelTrackerMiddleware(object):
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        ModelTracker.thread.request = request
        response = self.get_response(request)

        return response
