from django import VERSION
from django.contrib.postgres.operations import CreateExtension


class VectorExtension(CreateExtension):
    if VERSION[0] >= 6:
        def __init__(self, hints=None):
            super().__init__('vector', hints=hints)
    else:
        def __init__(self):
            self.name = 'vector'
