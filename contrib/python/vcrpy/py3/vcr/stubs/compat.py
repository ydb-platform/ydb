from io import BytesIO
import http.client


"""
The python3 http.client api moved some stuff around, so this is an abstraction
layer that tries to cope with this move.
"""


def get_header(message, name):
    return message.getallmatchingheaders(name)


def get_header_items(message):
    for (key, values) in get_headers(message):
        for value in values:
            yield key, value


def get_headers(message):
    for key in set(message.keys()):
        yield key, message.get_all(key)


def get_httpmessage(headers):
    return http.client.parse_headers(BytesIO(headers))
