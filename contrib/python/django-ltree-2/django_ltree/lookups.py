from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.lookups import PostgresOperatorLookup
from django.db.models import Value

from .fields import LqueryField, PathField


@PathField.register_lookup
class EqualLookup(PostgresOperatorLookup):
    postgres_operator = "="
    lookup_name = "exact"


@PathField.register_lookup
class AncestorLookup(PostgresOperatorLookup):
    lookup_name = "ancestors"
    postgres_operator = "@>"


@PathField.register_lookup
class DescendantLookup(PostgresOperatorLookup):
    lookup_name = "descendants"
    postgres_operator = "<@"


@PathField.register_lookup
class MatchLookup(PostgresOperatorLookup):
    lookup_name = "match"
    postgres_operator = "~"


@PathField.register_lookup
class ContainsLookup(PostgresOperatorLookup):
    lookup_name = "contains"
    postgres_operator = "?"

    def __init__(self, lhs, rhs):
        if not isinstance(rhs, (tuple, list)):
            raise TypeError("Contains lookup requires a list or tuple of values")

        rhs = Value(rhs, output_field=ArrayField(base_field=LqueryField()))
        super().__init__(lhs, rhs)
