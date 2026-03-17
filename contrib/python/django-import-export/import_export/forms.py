import os.path
from collections.abc import Iterable
from copy import deepcopy
from itertools import chain

from django import forms
from django.conf import settings
from django.utils.translation import gettext_lazy as _

from .resources import ModelResource


class ImportExportFormBase(forms.Form):
    resource = forms.ChoiceField(
        label=_("Resource"),
        choices=(),
        required=False,
    )
    format = forms.ChoiceField(
        label=_("Format"),
        choices=(),
    )

    def __init__(self, formats, resources, **kwargs):
        super().__init__(**kwargs)
        self._init_resources(resources)
        self._init_formats(formats)

    def _init_resources(self, resources):
        if not resources:
            raise ValueError("no defined resources")
        self.fields["resource"].choices = [
            (i, resource.get_display_name()) for i, resource in enumerate(resources)
        ]
        if len(resources) == 1:
            self.fields["resource"].widget = forms.HiddenInput()
            self.initial["resource"] = "0"

    def _init_formats(self, formats):
        if not formats:
            raise ValueError("invalid formats list")

        choices = [(str(i), f().get_title()) for i, f in enumerate(formats)]
        if len(formats) == 1:
            field = self.fields["format"]
            field.value = formats[0]().get_title()
            field.initial = 0
            field.widget.attrs["readonly"] = True
        if len(formats) > 1:
            choices.insert(0, ("", "---"))

        self.fields["format"].choices = choices


class ImportForm(ImportExportFormBase):
    import_file = forms.FileField(label=_("File to import"))

    # field ordered for usability:
    # ensure that the 'file' select appears before 'format'
    # so that the 'guess_format' js logic makes sense
    field_order = ["resource", "import_file", "format"]

    def __init__(self, formats, resources, **kwargs):
        super().__init__(formats, resources, **kwargs)
        if len(formats) > 1:
            self.fields["import_file"].widget.attrs["class"] = "guess_format"
            self.fields["format"].widget.attrs["class"] = "guess_format"

    @property
    def media(self):
        media = super().media
        extra = "" if settings.DEBUG else ".min"
        return media + forms.Media(
            js=(
                f"admin/js/vendor/jquery/jquery{extra}.js",
                "admin/js/jquery.init.js",
                "import_export/guess_format.js",
            )
        )


class ConfirmImportForm(forms.Form):
    import_file_name = forms.CharField(widget=forms.HiddenInput())
    original_file_name = forms.CharField(widget=forms.HiddenInput())
    format = forms.CharField(widget=forms.HiddenInput())
    resource = forms.CharField(widget=forms.HiddenInput(), required=False)

    def clean_import_file_name(self):
        data = self.cleaned_data["import_file_name"]
        data = os.path.basename(data)
        return data


class ExportForm(ImportExportFormBase):
    export_items = forms.MultipleChoiceField(
        widget=forms.MultipleHiddenInput(), required=False
    )


class SelectableFieldsExportForm(ExportForm):
    def __init__(self, formats, resources, **kwargs):
        super().__init__(formats, resources, **kwargs)
        self._init_selectable_fields(resources)

    @property
    def media(self):
        media = super().media
        return media + forms.Media(
            js=("import_export/export_selectable_fields.js",),
            css={
                "all": ["import_export/export.css"],
            },
        )

    def _init_selectable_fields(self, resources: Iterable[ModelResource]) -> None:
        """
        Create `BooleanField(s)` for resource fields
        """
        self.resources = resources
        self.is_selectable_fields_form = True
        self.resource_fields = {resource.__name__: [] for resource in resources}

        for index, resource in enumerate(resources):
            boolean_fields = self._create_boolean_fields(resource, index)
            self.resource_fields[resource.__name__] = boolean_fields

        # Order fields by resource select then boolean fields
        ordered_fields = [
            "resource",
            # flatten resource fields lists
            *chain(*self.resource_fields.values()),
        ]
        self.order_fields(ordered_fields)

    def _get_field_label(self, resource: ModelResource, field_name: str) -> str:
        title = field_name.replace("_", " ").title()
        field = resource.fields.get(field_name)
        if field and field.column_name != field_name:
            title = f"{title} ({field.column_name})"
        return title

    def _create_boolean_fields(self, resource: ModelResource, index: int) -> None:
        # Initiate resource to get ordered export fields
        fields = resource().get_export_order()
        boolean_fields = []  # will be used for ordering the fields
        is_initial_field = False

        for field in fields:
            field_name = self.create_boolean_field_name(resource, field)
            boolean_field = forms.BooleanField(
                label=self._get_field_label(resource, field),
                label_suffix="",
                initial=True,
                required=False,
            )

            # These attributes will be used for rendering in template
            boolean_field.is_selectable_field = True
            boolean_field.resource_name = resource.__name__
            boolean_field.resource_index = index
            boolean_field.widget.attrs["resource-id"] = index
            if is_initial_field is False:
                boolean_field.initial_field = is_initial_field = True

            self.fields[field_name] = boolean_field
            boolean_fields.append(field_name)

        return boolean_fields

    @staticmethod
    def create_boolean_field_name(resource: ModelResource, field_name: str) -> str:
        """
        Create field name by combining `resource_name` + `field_name` to prevent
        conflict between resource fields with same name

        Example:
            BookResource            +   name -> bookresource_name
            BookResourceWithNames   +   name -> bookresourcewithnames_name
        """
        return resource.__name__.lower() + "_" + field_name

    def clean(self):
        selected_resource = self.get_selected_resource()

        if selected_resource:
            # Remove fields for not selected resources
            self._remove_unselected_resource_fields(selected_resource)
            # Normalize resource field names
            self._normalize_resource_fields(selected_resource)
            # Validate at least one field is selected for selected resource
            self._validate_any_field_selected(selected_resource)

        return self.cleaned_data

    def _remove_unselected_resource_fields(
        self, selected_resource: ModelResource
    ) -> None:
        """
        Remove boolean fields except the fields for selected resource
        """
        _cleaned_data = deepcopy(self.cleaned_data)

        for resource_name, fields in self.resource_fields.items():
            if selected_resource.__name__ == resource_name:
                # Skip selected resource
                continue

            for field in fields:
                del _cleaned_data[field]

        self.cleaned_data = _cleaned_data

    def get_selected_resource(self):
        if not getattr(self, "cleaned_data", None):
            raise forms.ValidationError(
                _("Form is not validated, call `is_valid` first")
            )

        # Return selected resource by index
        resource_index = 0
        if "resource" in self.cleaned_data:
            try:
                resource_index = int(self.cleaned_data["resource"])
            except ValueError:
                pass
        return self.resources[resource_index]

    def _normalize_resource_fields(self, selected_resource: ModelResource) -> None:
        """
        Field names are combination of resource_name + field_name,
        normalize field names by removing resource name
        """
        selected_resource_name = selected_resource.__name__.lower() + "_"
        _cleaned_data = {}
        self._selected_resource_fields = []

        for k, v in self.cleaned_data.items():
            if selected_resource_name in k:
                field_name = k.replace(selected_resource_name, "")
                _cleaned_data[field_name] = v
                if v is True:
                    # Add to _selected_resource_fields to determine what
                    # fields were selected for export
                    self._selected_resource_fields.append(field_name)
                continue
            _cleaned_data[k] = v

        self.cleaned_data = _cleaned_data

    def get_selected_resource_export_fields(self):
        selected_resource = self.get_selected_resource()
        # Initialize resource to use `get_export_order` method
        resource_fields = selected_resource().get_export_order()
        return [
            field
            for field, value in self.cleaned_data.items()
            if value is True and field in resource_fields
        ]

    def _validate_any_field_selected(self, resource) -> None:
        """
        Validate if any field for resource was selected in form data
        """
        resource_fields = list(resource().get_export_order())

        if not any(v for k, v in self.cleaned_data.items() if k in resource_fields):
            raise forms.ValidationError(
                _("""Select at least 1 field for "%(resource_name)s" to export"""),
                code="invalid",
                params={
                    "resource_name": resource.get_display_name(),
                },
            )
