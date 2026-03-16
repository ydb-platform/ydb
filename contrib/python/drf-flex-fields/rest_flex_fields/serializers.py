import copy
import importlib
from typing import List, Optional, Tuple

from rest_framework import serializers

from rest_flex_fields import (
    EXPAND_PARAM,
    FIELDS_PARAM,
    OMIT_PARAM,
    WILDCARD_VALUES,
    MAXIMUM_EXPANSION_DEPTH,
    RECURSIVE_EXPANSION_PERMITTED,
    split_levels,
)


class FlexFieldsSerializerMixin(object):
    """
    A ModelSerializer that takes additional arguments for
    "fields", "omit" and "expand" in order to
    control which fields are displayed, and whether to replace simple
    values with complex, nested serializations
    """

    expandable_fields = {}
    maximum_expansion_depth: Optional[int] = None
    recursive_expansion_permitted: Optional[bool] = None

    def __init__(self, *args, **kwargs):
        expand = list(kwargs.pop(EXPAND_PARAM, []))
        fields = list(kwargs.pop(FIELDS_PARAM, []))
        omit = list(kwargs.pop(OMIT_PARAM, []))
        parent = kwargs.pop("parent", None)

        super(FlexFieldsSerializerMixin, self).__init__(*args, **kwargs)

        self.parent = parent
        self.expanded_fields = []
        self._flex_fields_rep_applied = False

        self._flex_options_base = {
            "expand": expand,
            "fields": fields,
            "omit": omit,
        }
        self._flex_options_rep_only = {
            "expand": (
                self._get_permitted_expands_from_query_param(EXPAND_PARAM)
                if not expand
                else []
            ),
            "fields": (self._get_query_param_value(FIELDS_PARAM) if not fields else []),
            "omit": (self._get_query_param_value(OMIT_PARAM) if not omit else []),
        }
        self._flex_options_all = {
            "expand": self._flex_options_base["expand"]
            + self._flex_options_rep_only["expand"],
            "fields": self._flex_options_base["fields"]
            + self._flex_options_rep_only["fields"],
            "omit": self._flex_options_base["omit"]
            + self._flex_options_rep_only["omit"],
        }

    def get_maximum_expansion_depth(self) -> Optional[int]:
        """
        Defined at serializer level or based on MAXIMUM_EXPANSION_DEPTH setting
        """
        return self.maximum_expansion_depth or MAXIMUM_EXPANSION_DEPTH

    def get_recursive_expansion_permitted(self) -> bool:
        """
        Defined at serializer level or based on RECURSIVE_EXPANSION_PERMITTED setting
        """
        if self.recursive_expansion_permitted is not None:
            return self.recursive_expansion_permitted
        else:
            return RECURSIVE_EXPANSION_PERMITTED

    def to_representation(self, instance):
        if not self._flex_fields_rep_applied:
            self.apply_flex_fields(self.fields, self._flex_options_rep_only)
            self._flex_fields_rep_applied = True
        return super().to_representation(instance)

    def get_fields(self):
        fields = super().get_fields()
        self.apply_flex_fields(fields, self._flex_options_base)
        return fields

    def apply_flex_fields(self, fields, flex_options):
        expand_fields, next_expand_fields = split_levels(flex_options["expand"])
        sparse_fields, next_sparse_fields = split_levels(flex_options["fields"])
        omit_fields, next_omit_fields = split_levels(flex_options["omit"])

        for field_name in self._get_fields_names_to_remove(
            fields, omit_fields, sparse_fields, next_omit_fields
        ):
            fields.pop(field_name)

        for name in self._get_expanded_field_names(
            expand_fields, omit_fields, sparse_fields, next_omit_fields
        ):
            self.expanded_fields.append(name)

            fields[name] = self._make_expanded_field_serializer(
                name, next_expand_fields, next_sparse_fields, next_omit_fields
            )

        return fields

    def _make_expanded_field_serializer(
        self, name, nested_expand, nested_fields, nested_omit
    ):
        """
        Returns an instance of the dynamically created nested serializer.
        """
        field_options = self._expandable_fields[name]

        if isinstance(field_options, tuple):
            serializer_class = field_options[0]
            settings = copy.deepcopy(field_options[1]) if len(field_options) > 1 else {}
        else:
            serializer_class = field_options
            settings = {}

        if type(serializer_class) == str:
            serializer_class = self._get_serializer_class_from_lazy_string(
                serializer_class
            )

        if issubclass(serializer_class, serializers.Serializer):
            settings["context"] = self.context

        if issubclass(serializer_class, FlexFieldsSerializerMixin):
            settings["parent"] = self

            if name in nested_expand:
                settings[EXPAND_PARAM] = nested_expand[name]

            if name in nested_fields:
                settings[FIELDS_PARAM] = nested_fields[name]

            if name in nested_omit:
                settings[OMIT_PARAM] = nested_omit[name]

        return serializer_class(**settings)

    def _get_serializer_class_from_lazy_string(self, full_lazy_path: str):
        path_parts = full_lazy_path.split(".")
        class_name = path_parts.pop()
        path = ".".join(path_parts)
        serializer_class, error = self._import_serializer_class(path, class_name)

        if error and not path.endswith(".serializers"):
            serializer_class, error = self._import_serializer_class(
                path + ".serializers", class_name
            )

        if serializer_class:
            return serializer_class

        raise Exception(error)

    def _import_serializer_class(
        self, path: str, class_name: str
    ) -> Tuple[Optional[str], Optional[str]]:
        try:
            module = importlib.import_module(path)
        except ImportError:
            return (
                None,
                "No module found at path: %s when trying to import %s"
                % (path, class_name),
            )

        try:
            return getattr(module, class_name), None
        except AttributeError:
            return None, "No class %s class found in module %s" % (path, class_name)

    def _get_fields_names_to_remove(
        self,
        current_fields: List[str],
        omit_fields: List[str],
        sparse_fields: List[str],
        next_level_omits: List[str],
    ) -> List[str]:
        """
        Remove fields that are found in omit list, and if sparse names
        are passed, remove any fields not found in that list.
        """
        sparse = len(sparse_fields) > 0
        to_remove = []

        if not sparse and len(omit_fields) == 0:
            return to_remove

        for field_name in current_fields:
            should_exist = self._should_field_exist(
                field_name, omit_fields, sparse_fields, next_level_omits
            )

            if not should_exist:
                to_remove.append(field_name)

        return to_remove

    def _should_field_exist(
        self,
        field_name: str,
        omit_fields: List[str],
        sparse_fields: List[str],
        next_level_omits: List[str],
    ) -> bool:
        """
        Next level omits take form of:
        {
            'this_level_field': [field_to_omit_at_next_level]
        }
        We don't want to prematurely omit a field, eg "omit=house.rooms.kitchen"
        should not omit the entire house or all the rooms, just the kitchen.
        """
        if field_name in omit_fields and field_name not in next_level_omits:
            return False
        elif self._contains_wildcard_value(sparse_fields):
            return True
        elif len(sparse_fields) > 0 and field_name not in sparse_fields:
            return False
        else:
            return True

    def _get_expanded_field_names(
        self,
        expand_fields: List[str],
        omit_fields: List[str],
        sparse_fields: List[str],
        next_level_omits: List[str],
    ) -> List[str]:
        if len(expand_fields) == 0:
            return []

        if self._contains_wildcard_value(expand_fields):
            expand_fields = self._expandable_fields.keys()

        accum = []

        for name in expand_fields:
            if name not in self._expandable_fields:
                continue

            if not self._should_field_exist(
                name, omit_fields, sparse_fields, next_level_omits
            ):
                continue

            accum.append(name)

        return accum

    @property
    def _expandable_fields(self) -> dict:
        """It's more consistent with DRF to declare the expandable fields
        on the Meta class, however we need to support both places
        for legacy reasons."""
        if hasattr(self, "Meta") and hasattr(self.Meta, "expandable_fields"):
            return self.Meta.expandable_fields

        return self.expandable_fields

    def _get_query_param_value(self, field: str) -> List[str]:
        """
        Only allowed to examine query params if it's the root serializer.
        """
        if self.parent:
            return []

        if not hasattr(self, "context") or not self.context.get("request"):
            return []

        values = self.context["request"].query_params.getlist(field)

        if not values:
            values = self.context["request"].query_params.getlist(f"{field}[]")

        if values and len(values) == 1:
            values = values[0].split(",")

        for expand_path in values:
            self._validate_recursive_expansion(expand_path)
            self._validate_expansion_depth(expand_path)

        return values or []

    def _split_expand_field(self, expand_path: str) -> List[str]:
        return expand_path.split(".")

    def recursive_expansion_not_permitted(self):
        """
        A customized exception can be raised when recursive expansion is found, default ValidationError
        """
        raise serializers.ValidationError(detail="Recursive expansion found")

    def _validate_recursive_expansion(self, expand_path: str) -> None:
        """
        Given an expand_path, a dotted-separated string,
        an Exception is raised when a recursive
        expansion is detected.
        Only applies when REST_FLEX_FIELDS["RECURSIVE_EXPANSION"] setting is False.
        """
        recursive_expansion_permitted = self.get_recursive_expansion_permitted()
        if recursive_expansion_permitted is True:
            return

        expansion_path = self._split_expand_field(expand_path)
        expansion_length = len(expansion_path)
        expansion_length_unique = len(set(expansion_path))
        if expansion_length != expansion_length_unique:
            self.recursive_expansion_not_permitted()

    def expansion_depth_exceeded(self):
        """
        A customized exception can be raised when expansion depth is found, default ValidationError
        """
        raise serializers.ValidationError(detail="Expansion depth exceeded")

    def _validate_expansion_depth(self, expand_path: str) -> None:
        """
        Given an expand_path, a dotted-separated string,
        an Exception is raised when expansion level is
        greater than the `expansion_depth` property configuration.
        Only applies when REST_FLEX_FIELDS["EXPANSION_DEPTH"] setting is set
        or serializer has its own expansion configuration through default_expansion_depth attribute.
        """
        maximum_expansion_depth = self.get_maximum_expansion_depth()
        if maximum_expansion_depth is None:
            return

        expansion_path = self._split_expand_field(expand_path)
        if len(expansion_path) > maximum_expansion_depth:
            self.expansion_depth_exceeded()

    def _get_permitted_expands_from_query_param(self, expand_param: str) -> List[str]:
        """
        If a list of permitted_expands has been passed to context,
        make sure that the "expand" fields from the query params
        comply.
        """
        expand = self._get_query_param_value(expand_param)

        if "permitted_expands" in self.context:
            permitted_expands = self.context["permitted_expands"]

            if self._contains_wildcard_value(expand):
                return permitted_expands
            else:
                return list(set(expand) & set(permitted_expands))

        return expand

    def _contains_wildcard_value(self, expand_values: List[str]) -> bool:
        if WILDCARD_VALUES is None:
            return False
        intersecting_values = list(set(expand_values) & set(WILDCARD_VALUES))
        return len(intersecting_values) > 0


class FlexFieldsModelSerializer(FlexFieldsSerializerMixin, serializers.ModelSerializer):
    pass
