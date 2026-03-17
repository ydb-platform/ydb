from django.contrib.admin.views.main import ChangeList
import django

from . import util


class TreeChangeList(ChangeList):
    TREE_IGNORED_PARAMS = ("_", "node", "selected_node")

    def __init__(self, request, model, model_admin, list_filter, node_id, max_level):
        self.node_id = node_id
        self.max_level = max_level

        params = dict(
            request=request,
            model=model,
            list_display=(),
            list_display_links=(),
            list_filter=list_filter,
            date_hierarchy=None,
            search_fields=(),
            list_select_related=(),
            list_per_page=100,
            list_max_show_all=200,
            list_editable=(),
            model_admin=model_admin,
            sortable_by=[],
        )

        if django.VERSION >= (4, 0):
            params["search_help_text"] = ""

        super().__init__(**params)

    def get_filters_params(self, params=None):
        params = super().get_filters_params()

        lookup_params = params.copy()

        for ignored in self.TREE_IGNORED_PARAMS:
            if ignored in lookup_params:
                del lookup_params[ignored]

        return lookup_params

    def get_queryset(self, request, exclude_parameters=None):
        (
            self.filter_specs,
            self.has_filters,
            remaining_lookup_params,
            filters_may_have_duplicates,
            self.has_active_filters,
        ) = self.get_filters(request)

        qs = util.get_tree_queryset(
            model=self.model,
            node_id=self.node_id,
            max_level=self.max_level,
        )

        for filter_spec in self.filter_specs:
            new_qs = filter_spec.queryset(request, qs)
            if new_qs is not None:
                qs = new_qs

        self.clear_all_filters_qs = self.get_query_string(
            new_params=remaining_lookup_params,
            remove=self.get_filters_params(),
        )

        return qs
