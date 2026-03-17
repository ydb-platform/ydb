from contextlib import contextmanager

from django.http import QueryDict
from django_filters import compat
from django_filters.rest_framework import backends
from rest_framework.exceptions import ValidationError

from .complex_ops import combine_complex_queryset, decode_complex_ops
from .filterset import FilterSet


class RestFrameworkFilterBackend(backends.DjangoFilterBackend):
    filterset_base = FilterSet

    @property
    def template(self):
        if compat.is_crispy():
            return 'rest_framework_filters/crispy_form.html'
        return 'rest_framework_filters/form.html'

    @contextmanager
    def patch_for_rendering(self, request):
        # Patch ``.get_filterset_class()`` so the resulting filterset does not perform
        # filter expansion during form rendering.
        original = self.get_filterset_class

        def get_filterset_class(view, queryset=None):
            filterset_class = original(view, queryset)

            # Don't break if filterset_class is not provided
            if filterset_class is None:
                return None

            # django-filter compatibility
            if issubclass(filterset_class, FilterSet):
                filterset_class = filterset_class.disable_subset(depth=1)

            return filterset_class

        self.get_filterset_class = get_filterset_class
        try:
            yield
        finally:
            self.get_filterset_class = original

    def to_html(self, request, queryset, view):
        # Patching the behavior of ``.get_filterset_class()`` in this method allows us
        # to avoid maintenance issues with code duplication.
        with self.patch_for_rendering(request):
            return super().to_html(request, queryset, view)


class ComplexFilterBackend(RestFrameworkFilterBackend):
    complex_filter_param = 'filters'
    operators = None
    negation = True

    def filter_queryset(self, request, queryset, view):
        if self.complex_filter_param not in request.query_params:
            return super().filter_queryset(request, queryset, view)

        # Decode the set of complex operations
        encoded_querystring = request.query_params[self.complex_filter_param]
        try:
            complex_ops = decode_complex_ops(
                encoded_querystring,
                self.operators,
                self.negation,
            )
        except ValidationError as exc:
            raise ValidationError({self.complex_filter_param: exc.detail})

        # Collect the individual filtered querysets
        querystrings = [op.querystring for op in complex_ops]
        try:
            querysets = self.get_filtered_querysets(querystrings, request, queryset, view)
        except ValidationError as exc:
            raise ValidationError({self.complex_filter_param: exc.detail})

        return combine_complex_queryset(querysets, complex_ops)

    def get_filtered_querysets(self, querystrings, request, queryset, view):
        original_GET = request._request.GET

        querysets, errors = [], {}
        for qs in querystrings:
            request._request.GET = QueryDict(qs)
            try:
                result = super().filter_queryset(request, queryset, view)
                querysets.append(result)
            except ValidationError as exc:
                errors[qs] = exc.detail
            finally:
                request._request.GET = original_GET

        if errors:
            raise ValidationError(errors)
        return querysets
