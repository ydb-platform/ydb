from django.db.models.expressions import ExpressionList, F, Func
from django.db.models.indexes import IndexExpression, names_digest, split_identifier
from django.db.models.sql import Query

from clickhouse_backend.models.engines import BaseMergeTree

__all__ = [
    "Index",
    "MinMax",
    "Set",
    "NgrambfV1",
    "TokenbfV1",
    "BloomFilter",
]


class Index:
    suffix = "idx"

    max_name_length = 30

    def __init__(
        self,
        *expressions,
        fields=(),
        name=None,
        type=None,
        granularity=None,
    ):
        if not isinstance(type, Type):
            raise ValueError("Index type must be a Type instance.")
        if not isinstance(granularity, int) and granularity > 0:
            raise ValueError("Index granularity must be a positive integer.")
        if not isinstance(fields, (list, tuple)):
            raise ValueError("Index.fields must be a list or tuple.")
        if not expressions and not fields:
            raise ValueError(
                "At least one field or expression is required to define an index."
            )
        if expressions and fields:
            raise ValueError(
                "Index.fields and expressions are mutually exclusive.",
            )
        if not name:
            raise ValueError("An index must be named.")
        if fields and not all(isinstance(field, str) for field in fields):
            raise ValueError("Index.fields must contain only strings with field names.")
        self.fields = list(fields)
        # A list of 2-tuple with the field name and ordering ("" or "DESC").
        self.fields_orders = [
            (field_name[1:], "DESC") if field_name.startswith("-") else (field_name, "")
            for field_name in self.fields
        ]
        self.include = []
        self.condition = None
        self.name = name or ""
        self.type = type
        self.granularity = granularity
        self.expressions = tuple(
            F(expression) if isinstance(expression, str) else expression
            for expression in expressions
        )

    @property
    def contains_expressions(self):
        return bool(self.expressions)

    def index_sql(self, model, schema_editor):
        return self.create_sql(model, schema_editor, inline=True)

    def create_sql(self, model, schema_editor, **kwargs):
        engine = getattr(model._meta, "engine", None)
        if engine and not isinstance(engine, BaseMergeTree):
            raise TypeError(
                "Only MergeTree family support indexes. Refer https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes"
            )
        if self.expressions:
            index_expressions = []
            for expression in self.expressions:
                index_expression = IndexExpression(expression)
                index_expression.set_wrapper_classes(schema_editor.connection)
                index_expressions.append(index_expression)
            expressions = ExpressionList(*index_expressions).resolve_expression(
                Query(model, alias_cols=False),
            )
            fields = None
            col_suffixes = None
        else:
            fields = [
                model._meta.get_field(field_name)
                for field_name, _ in self.fields_orders
            ]
            col_suffixes = [order[1] for order in self.fields_orders]
            expressions = None
        return schema_editor._create_index_sql(
            model,
            fields=fields,
            name=self.name,
            col_suffixes=col_suffixes,
            type=self.type,
            granularity=self.granularity,
            expressions=expressions,
            **kwargs,
        )

    def remove_sql(self, model, schema_editor, **kwargs):
        engine = getattr(model._meta, "engine", None)
        if engine and not isinstance(engine, BaseMergeTree):
            raise TypeError(
                "Only MergeTree family support indexes. Refer https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes"
            )
        return schema_editor._delete_index_sql(model, self.name, **kwargs)

    def deconstruct(self):
        path = "%s.%s" % (self.__class__.__module__, self.__class__.__name__)
        if path.startswith("clickhouse_backend.models.indexes"):
            path = path.replace(
                "clickhouse_backend.models.indexes", "clickhouse_backend.models"
            )
        kwargs = {"name": self.name}
        if self.fields:
            kwargs["fields"] = self.fields
        kwargs["type"] = self.type
        kwargs["granularity"] = self.granularity
        return path, self.expressions, kwargs

    def clone(self):
        """Create a copy of this Index."""
        _, args, kwargs = self.deconstruct()
        return self.__class__(*args, **kwargs)

    def set_name_with_model(self, model):
        """
        Generate a unique name for the index.

        The name is divided into 3 parts - table name (12 chars), field name
        (8 chars) and unique hash + suffix (10 chars). Each part is made to
        fit its size by truncating the excess length.
        """
        _, table_name = split_identifier(model._meta.db_table)
        column_names = [
            model._meta.get_field(field_name).column for field_name in self.fields
        ]
        # The length of the parts of the name is based on the default max
        # length of 30 characters.
        hash_data = [table_name] + column_names + [self.suffix]
        self.name = "%s_%s_%s" % (
            table_name[:11],
            column_names[0][:7],
            "%s_%s" % (names_digest(*hash_data, length=6), self.suffix),
        )
        assert len(self.name) <= self.max_name_length, (
            "Index too long for multiple database support. Is self.suffix "
            "longer than 3 characters?"
        )
        if self.name[0] == "_" or self.name[0].isdigit():
            self.name = "D%s" % self.name[1:]

    def __repr__(self):
        return "<%s:%s%s%s%s>" % (
            self.__class__.__name__,
            "" if not self.fields else " fields='%s'" % ", ".join(self.fields),
            ""
            if not self.expressions
            else " expressions='%s'"
            % ", ".join([str(expression) for expression in self.expressions]),
            " type=%s" % self.type,
            " granularity=%s" % self.granularity,
        )

    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return self.deconstruct() == other.deconstruct()
        return NotImplemented


class Type(Func):
    def deconstruct(self):
        path, args, kwargs = super().deconstruct()
        if path.startswith("clickhouse_backend.models.indexes"):
            path = path.replace(
                "clickhouse_backend.models.indexes", "clickhouse_backend.models"
            )
        return path, args, kwargs


class MinMax(Type):
    function = "minmax"
    template = "%(function)s"
    arity = 0


class Set(Type):
    function = "set"
    arity = 1


class NgrambfV1(Type):
    function = "ngrambf_v1"
    arity = 4


class TokenbfV1(Type):
    function = "tokenbf_v1"
    arity = 3


class BloomFilter(Type):
    function = "bloom_filter"
