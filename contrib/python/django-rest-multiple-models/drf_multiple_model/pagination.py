from collections import OrderedDict

from rest_framework.pagination import LimitOffsetPagination


class MultipleModelLimitOffsetPagination(LimitOffsetPagination):
    """
    An extentions of Rest Framework's limit/offset pagination
    to work with querylists.  This mostly involves creating a running
    tally of the highest queryset `count`, rather than only referring
    to a single queryset's count
    """
    def paginate_queryset(self, queryset, request, view=None):
        """
        adds `max_count` as a running tally of the largest table size.  Used for calculating
        next/previous links later
        """
        result = super(MultipleModelLimitOffsetPagination, self).paginate_queryset(queryset, request, view)

        try:
            if self.max_count < self.count:
                self.max_count = self.count
        except AttributeError:
            self.max_count = self.count

        try:
            self.total += self.count
        except AttributeError:
            self.total = self.count

        return result

    def format_response(self, data):
        """
        replaces the `count` (the last queryset count) with the running `max_count` variable,
        to ensure accurate link calculation
        """
        self.count = self.max_count

        return OrderedDict([
            ('highest_count', self.max_count),
            ('overall_total', self.total),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('results', data)
        ])
