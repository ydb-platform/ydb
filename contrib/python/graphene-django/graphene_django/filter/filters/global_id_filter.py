from django_filters import Filter, MultipleChoiceFilter
from graphql_relay.node.node import from_global_id

from ...forms import GlobalIDFormField, GlobalIDMultipleChoiceField


class GlobalIDFilter(Filter):
    """
    Filter for Relay global ID.
    """

    field_class = GlobalIDFormField

    def filter(self, qs, value):
        """Convert the filter value to a primary key before filtering"""
        _id = None
        if value is not None:
            _, _id = from_global_id(value)
        return super().filter(qs, _id)


class GlobalIDMultipleChoiceFilter(MultipleChoiceFilter):
    field_class = GlobalIDMultipleChoiceField

    def filter(self, qs, value):
        gids = [from_global_id(v)[1] for v in value]
        return super().filter(qs, gids)
