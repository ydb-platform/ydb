from django.contrib.postgres.indexes import PostgresIndex


class IvfflatIndex(PostgresIndex):
    suffix = 'ivfflat'

    def __init__(self, *expressions, lists=None, **kwargs):
        self.lists = lists
        super().__init__(*expressions, **kwargs)

    def deconstruct(self):
        path, args, kwargs = super().deconstruct()
        if self.lists is not None:
            kwargs['lists'] = self.lists
        return path, args, kwargs

    def get_with_params(self):
        with_params = []
        if self.lists is not None:
            with_params.append('lists = %d' % self.lists)
        return with_params


class HnswIndex(PostgresIndex):
    suffix = 'hnsw'

    def __init__(self, *expressions, m=None, ef_construction=None, **kwargs):
        self.m = m
        self.ef_construction = ef_construction
        super().__init__(*expressions, **kwargs)

    def deconstruct(self):
        path, args, kwargs = super().deconstruct()
        if self.m is not None:
            kwargs['m'] = self.m
        if self.ef_construction is not None:
            kwargs['ef_construction'] = self.ef_construction
        return path, args, kwargs

    def get_with_params(self):
        with_params = []
        if self.m is not None:
            with_params.append('m = %d' % self.m)
        if self.ef_construction is not None:
            with_params.append('ef_construction = %d' % self.ef_construction)
        return with_params
