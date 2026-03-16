"""OpenAPI core contrib flask providers module"""
from flask.globals import request


class FlaskRequestProvider(object):

    @classmethod
    def provide(self, *args, **kwargs):
        return request
