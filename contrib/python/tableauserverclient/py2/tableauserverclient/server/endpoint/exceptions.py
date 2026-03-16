import xml.etree.ElementTree as ET


class ServerResponseError(Exception):
    def __init__(self, code, summary, detail):
        self.code = code
        self.summary = summary
        self.detail = detail
        super(ServerResponseError, self).__init__(str(self))

    def __str__(self):
        return "\n\n\t{0}: {1}\n\t\t{2}".format(self.code, self.summary, self.detail)

    @classmethod
    def from_response(cls, resp, ns):
        # Check elements exist before .text
        parsed_response = ET.fromstring(resp)
        error_response = cls(parsed_response.find('t:error', namespaces=ns).get('code', ''),
                             parsed_response.find('.//t:summary', namespaces=ns).text,
                             parsed_response.find('.//t:detail', namespaces=ns).text)
        return error_response


class InternalServerError(Exception):
    def __init__(self, server_response):
        self.code = server_response.status_code
        self.content = server_response.content

    def __str__(self):
        return "\n\nError status code: {0}\n{1}".format(self.code, self.content)


class MissingRequiredFieldError(Exception):
    pass


class ServerInfoEndpointNotFoundError(Exception):
    pass


class EndpointUnavailableError(Exception):
    pass


class ItemTypeNotAllowed(Exception):
    pass


class NonXMLResponseError(Exception):
    pass


class InvalidGraphQLQuery(Exception):
    pass


class GraphQLError(Exception):
    def __init__(self, error_payload):
        self.error = error_payload

    def __str__(self):
        from pprint import pformat
        return pformat(self.error)
