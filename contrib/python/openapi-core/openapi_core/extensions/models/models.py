"""OpenAPI X-Model extension models module"""


class BaseModel(object):
    """Base class for OpenAPI X-Model."""

    @property
    def __dict__(self):
        raise NotImplementedError


class Model(BaseModel):
    """Model class for OpenAPI X-Model."""

    def __init__(self, properties=None):
        self.__properties = properties or {}

    @property
    def __dict__(self):
        return self.__properties

    def __getattr__(self, name):
        if name not in self.__properties:
            raise AttributeError

        return self.__properties[name]
