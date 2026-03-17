from sqlalchemy import text
from sqlalchemy.util import to_list

from .base import Engine, KeysExpressionOrColumn, TableCol
from .util import parse_columns


class MergeTree(Engine):

    __visit_name__ = 'merge_tree'

    def __init__(
            self,
            partition_by=None,
            order_by=None,
            primary_key=None,
            sample_by=None,
            ttl=None,
            **settings
    ):
        self.partition_by = None
        if partition_by is not None:
            self.partition_by = KeysExpressionOrColumn(*to_list(partition_by))

        self.order_by = None
        if order_by is not None:
            self.order_by = KeysExpressionOrColumn(*to_list(order_by))

        self.primary_key = None
        if primary_key is not None:
            self.primary_key = KeysExpressionOrColumn(*to_list(primary_key))

        self.sample_by = None
        if sample_by is not None:
            self.sample_by = KeysExpressionOrColumn(*to_list(sample_by))

        self.ttl = None
        if ttl is not None:
            self.ttl = KeysExpressionOrColumn(*to_list(ttl))

        self.settings = settings
        super(MergeTree, self).__init__()

    def _set_parent(self, table):
        super(MergeTree, self)._set_parent(table)
        if self.partition_by is not None:
            self.partition_by._set_parent(table)
        if self.order_by is not None:
            self.order_by._set_parent(table)
        if self.primary_key is not None:
            self.primary_key._set_parent(table)
        if self.sample_by is not None:
            self.sample_by._set_parent(table)
        if self.ttl is not None:
            self.ttl._set_parent(table)

    @classmethod
    def wrap_with_text(cls, table, cols):
        return [x if x in table.columns else text(x) for x in cols]

    @classmethod
    def _reflect_merge_tree(
            cls, table, partition_key=None, sorting_key=None, primary_key=None,
            sampling_key=None, ttl=None, **kwargs):

        # TODO: reflect settings
        rv = {}
        if partition_key:
            partition_by = parse_columns(partition_key)
            rv['partition_by'] = cls.wrap_with_text(table, partition_by)
        if sorting_key:
            order_by = parse_columns(sorting_key)
            rv['order_by'] = cls.wrap_with_text(table, order_by)
        if primary_key:
            primary_key = parse_columns(primary_key)
            rv['primary_key'] = cls.wrap_with_text(table, primary_key)
        if sampling_key:
            sample_by = parse_columns(sampling_key)
            rv['sample_by'] = cls.wrap_with_text(table, sample_by)
        if ttl:
            rv['ttl'] = cls.wrap_with_text(table, parse_columns(ttl))

        return rv

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        return cls(**cls._reflect_merge_tree(table, **kwargs))


class AggregatingMergeTree(MergeTree):
    pass


class GraphiteMergeTree(MergeTree):

    def __init__(self, config_name, *args, **kwargs):
        super(GraphiteMergeTree, self).__init__(*args, **kwargs)
        self.config_name = config_name

    def get_parameters(self):
        return "'{}'".format(self.config_name)

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        engine = parse_columns(engine_full, delimeter=' ')[0]
        config_name = engine[len(cls.__name__):].strip("()'")

        return cls(
            config_name,
            **cls._reflect_merge_tree(table, **kwargs)
        )


class CollapsingMergeTree(MergeTree):
    def __init__(self, sign_col, *args, **kwargs):
        super(CollapsingMergeTree, self).__init__(*args, **kwargs)
        self.sign_col = TableCol(sign_col)

    def get_parameters(self):
        return self.sign_col.get_column()

    def _set_parent(self, table):
        super(CollapsingMergeTree, self)._set_parent(table)

        self.sign_col._set_parent(table)

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        engine = parse_columns(engine_full, delimeter=' ')[0]
        sign_col = engine[len(cls.__name__):].strip('()')

        return cls(
            sign_col,
            **cls._reflect_merge_tree(table, **kwargs)
        )


class VersionedCollapsingMergeTree(MergeTree):
    def __init__(self, sign_col, version_col, *args, **kwargs):
        super(VersionedCollapsingMergeTree, self).__init__(*args, **kwargs)

        self.sign_col = TableCol(sign_col)
        self.version_col = TableCol(version_col)

    def get_parameters(self):
        return [self.sign_col.get_column(), self.version_col.get_column()]

    def _set_parent(self, table):
        super(VersionedCollapsingMergeTree, self)._set_parent(table)

        self.sign_col._set_parent(table)
        self.version_col._set_parent(table)

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        engine = parse_columns(engine_full, delimeter=' ')[0]
        columns = engine[len(cls.__name__):].strip('()')
        sign_col, version_col = parse_columns(columns)

        return cls(
            sign_col, version_col,
            **cls._reflect_merge_tree(table, **kwargs)
        )


class SummingMergeTree(MergeTree):
    def __init__(self, *args, **kwargs):
        summing_cols = kwargs.pop('columns', None)
        super(SummingMergeTree, self).__init__(*args, **kwargs)

        self.summing_cols = None
        if summing_cols is not None:
            self.summing_cols = KeysExpressionOrColumn(*to_list(summing_cols))

    def _set_parent(self, table):
        super(SummingMergeTree, self)._set_parent(table)

        if self.summing_cols is not None:
            self.summing_cols._set_parent(table)

    def get_parameters(self):
        if self.summing_cols is not None:
            cols = self.summing_cols.get_expressions_or_columns()
            return [cols] if len(cols) > 1 else cols

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        engine = parse_columns(engine_full, delimeter=' ')[0]
        columns = engine[len(cls.__name__):].strip('()')
        columns = parse_columns(columns) or None

        return cls(
            columns=columns,
            **cls._reflect_merge_tree(table, **kwargs)
        )


class ReplacingMergeTree(MergeTree):
    def __init__(self, *args, **kwargs):
        version_col = kwargs.pop('version', None)
        super(ReplacingMergeTree, self).__init__(*args, **kwargs)

        self.version_col = None
        if version_col is not None:
            self.version_col = TableCol(version_col)

    def _set_parent(self, table):
        super(ReplacingMergeTree, self)._set_parent(table)

        if self.version_col is not None:
            self.version_col._set_parent(table)

    def get_parameters(self):
        if self.version_col is not None:
            return self.version_col.get_column()

    @classmethod
    def reflect(cls, table, engine_full, **kwargs):
        engine = parse_columns(engine_full, delimeter=' ')[0]
        version_col = engine[len(cls.__name__):].strip('()') or None

        return cls(
            version=version_col,
            **cls._reflect_merge_tree(table, **kwargs)
        )
