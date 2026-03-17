"""OpenAPI X-Model extension factories module"""
from openapi_core.extensions.models.models import Model


class ModelClassFactory(object):

    base_class = Model

    def create(self, name):
        return type(name, (self.base_class, ), {})


class ModelFactory(object):

    def __init__(self, model_class_factory=None):
        self.model_class_factory = model_class_factory or ModelClassFactory()

    def create(self, properties, name=None):
        name = name or 'Model'

        model_class = self._create_class(name)
        return model_class(properties)

    def _create_class(self, name):
        return self.model_class_factory.create(name)
