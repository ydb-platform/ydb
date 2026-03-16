"""OpenAPI core validation processors module"""


class OpenAPIProcessor(object):

    def __init__(self, request_validator, response_validator):
        self.request_validator = request_validator
        self.response_validator = response_validator

    def process_request(self, request):
        return self.request_validator.validate(request)

    def process_response(self, request, response):
        return self.response_validator.validate(request, response)
