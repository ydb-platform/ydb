import logging
import warnings
from warnings import warn

from django.conf import settings
from django.http import HttpResponse
from django.utils.timezone import now
from django.views.generic.edit import FormView

from .formats import base_formats
from .forms import SelectableFieldsExportForm
from .resources import modelresource_factory
from .signals import post_export

logger = logging.getLogger(__name__)


class BaseImportExportMixin:
    """
    Base mixin for functionality related to importing and exporting via the Admin
    interface.
    """

    resource_class = None
    resource_classes = []

    @property
    def formats(self):
        return getattr(settings, "IMPORT_EXPORT_FORMATS", base_formats.DEFAULT_FORMATS)

    @property
    def export_formats(self):
        return getattr(settings, "EXPORT_FORMATS", self.formats)

    @property
    def import_formats(self):
        return getattr(settings, "IMPORT_FORMATS", self.formats)

    def check_resource_classes(self, resource_classes):
        if resource_classes and not hasattr(resource_classes, "__getitem__"):
            raise Exception(
                "The resource_classes field type must be "
                "subscriptable (list, tuple, ...)"
            )

    def get_resource_classes(self, request):
        """
        Return subscriptable type (list, tuple, ...) containing resource classes
        :param request: The request object.
        :returns: The Resource classes.
        """
        if self.resource_classes and self.resource_class:
            raise Exception(
                "Only one of 'resource_class' and 'resource_classes' can be set"
            )
        if hasattr(self, "get_resource_class"):
            cls = self.__class__
            warnings.warn(
                "The 'get_resource_class()' method has been deprecated. "
                "Please implement the new 'get_resource_classes()' method in "
                f"{cls.__module__}.{cls.__qualname__}",
                DeprecationWarning,
            )
            return [self.get_resource_class()]
        if self.resource_class:
            cls = self.__class__
            warnings.warn(
                "The 'resource_class' field has been deprecated. "
                "Please implement the new 'resource_classes' field in "
                f"{cls.__module__}.{cls.__qualname__}",
                DeprecationWarning,
            )
        if not self.resource_classes and not self.resource_class:
            return [modelresource_factory(self.model)]
        if self.resource_classes:
            return self.resource_classes
        return [self.resource_class]

    def get_resource_kwargs(self, request, *args, **kwargs):
        """
        Return the kwargs which are to be passed to the Resource constructor.
        Can be overridden to provide additional kwarg params.

        :param request: The request object.
        :param kwargs: Keyword arguments.
        :returns: The Resource kwargs (by default, is the kwargs passed).
        """
        return kwargs

    def get_resource_index(self, form):
        """
        Return the index of the resource class defined in the form.

        :param form: The form object.
        :returns: The index of the resource as an int.
        """
        resource_index = 0
        if form and "resource" in form.cleaned_data:
            try:
                resource_index = int(form.cleaned_data["resource"])
            except ValueError:
                pass
        return resource_index


class BaseImportMixin(BaseImportExportMixin):
    #: If enabled, the import workflow skips the import confirm page
    #: and imports the data directly.
    #: See :ref:`import_export_skip_admin_confirm`.
    skip_import_confirm = False

    def get_import_resource_classes(self, request):
        """
        :param request: The request object.
        Returns ResourceClass subscriptable (list, tuple, ...) to use for import.
        """
        if hasattr(self, "get_import_resource_class"):
            cls = self.__class__
            warnings.warn(
                "The 'get_import_resource_class()' method has been deprecated. "
                "Please implement the new 'get_import_resource_classes()' method in"
                f"{cls.__module__}.{cls.__qualname__}",
                DeprecationWarning,
            )
            return [self.get_import_resource_class()]
        resource_classes = self.get_resource_classes(request)
        self.check_resource_classes(resource_classes)
        return resource_classes

    def get_import_formats(self):
        """
        Returns available import formats.
        """
        return [f for f in self.import_formats if f().can_import()]

    def get_import_resource_kwargs(self, request, **kwargs):
        """
        Returns kwargs which will be passed to the Resource constructor.
        :param request: The request object.
        :param kwargs: Keyword arguments.
        :returns: The kwargs (dict)
        """
        return self.get_resource_kwargs(request, **kwargs)

    def choose_import_resource_class(self, form, request):
        """
        Identify which class should be used for import
        :param form: The form object.
        :param request: The request object.
        :returns: The import Resource class.
        """
        resource_index = self.get_resource_index(form)
        return self.get_import_resource_classes(request)[resource_index]

    def is_skip_import_confirm_enabled(self):
        return (
            getattr(settings, "IMPORT_EXPORT_SKIP_ADMIN_CONFIRM", False)
            or self.skip_import_confirm is True
        )


class BaseExportMixin(BaseImportExportMixin):
    model = None

    #: If enabled, the export workflow skips the export form and
    #: exports the data directly.
    #: See :ref:`import_export_skip_admin_export_ui`.
    skip_export_form = False

    #: If enabled, the export workflow from Admin UI action menu
    #: skips the export form and exports the data directly.
    #: See :ref:`import_export_skip_admin_action_export_ui`.
    skip_export_form_from_action = False

    def get_export_formats(self):
        """
        Returns available export formats.
        """
        return [f for f in self.export_formats if f().can_export()]

    def get_export_resource_classes(self, request):
        """
        Returns ResourceClass subscriptable (list, tuple, ...) to use for export.
        :param request: The request object.
        :returns: The Resource classes.
        """
        if hasattr(self, "get_export_resource_class"):
            cls = self.__class__
            warnings.warn(
                "The 'get_export_resource_class()' method has been deprecated. "
                "Please implement the new 'get_export_resource_classes()' method "
                f"in {cls.__module__}.{cls.__qualname__}",
                DeprecationWarning,
            )
            return [self.get_export_resource_class()]
        resource_classes = self.get_resource_classes(request)
        self.check_resource_classes(resource_classes)
        return resource_classes

    def choose_export_resource_class(self, form, request):
        """
        Identify which class should be used for export
        :param request: The request object.
        :param form: The form object.
        :returns: The export Resource class.
        """
        resource_index = self.get_resource_index(form)
        return self.get_export_resource_classes(request)[resource_index]

    def get_export_resource_kwargs(self, request, **kwargs):
        """
        Returns kwargs which will be passed to the Resource constructor.
        :param request: The request object.
        :param kwargs: Keyword arguments.
        :returns: The kwargs (dict)
        """
        return self.get_resource_kwargs(request, **kwargs)

    def get_export_resource_fields_from_form(self, form):
        if isinstance(form, SelectableFieldsExportForm):
            export_fields = form.get_selected_resource_export_fields()
            if export_fields:
                return export_fields

        return

    def get_data_for_export(self, request, queryset, **kwargs):
        export_form = kwargs.get("export_form")
        export_class = self.choose_export_resource_class(export_form, request)
        export_resource_kwargs = self.get_export_resource_kwargs(request, **kwargs)
        export_fields = self.get_export_resource_fields_from_form(export_form)
        cls = export_class(**export_resource_kwargs)
        export_data = cls.export(
            queryset=queryset, export_fields=export_fields, **kwargs
        )
        return export_data

    def get_export_filename(self, file_format):
        date_str = now().strftime("%Y-%m-%d")
        filename = "{}-{}.{}".format(
            self.model.__name__,
            date_str,
            file_format.get_extension(),
        )
        return filename

    def is_skip_export_form_enabled(self):
        return (
            getattr(settings, "IMPORT_EXPORT_SKIP_ADMIN_EXPORT_UI", False)
            or self.skip_export_form is True
        )

    def is_skip_export_form_from_action_enabled(self):
        return (
            getattr(settings, "IMPORT_EXPORT_SKIP_ADMIN_ACTION_EXPORT_UI", False)
            or self.skip_export_form_from_action is True
        )


class ExportViewMixin(BaseExportMixin):
    # Deprecated, and will be removed in a future release (see #1666)
    form_class = SelectableFieldsExportForm

    def get_export_data(self, file_format, queryset, **kwargs):
        """
        Returns file_format representation for given queryset.
        """
        data = self.get_data_for_export(self.request, queryset, **kwargs)
        export_data = file_format.export_data(data)
        return export_data

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs["formats"] = self.get_export_formats()
        kwargs["resources"] = self.get_export_resource_classes(self.request)
        return kwargs


class ExportViewFormMixin(ExportViewMixin, FormView):
    # Deprecated, and will be removed in a future release (see #1666)
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        warn(
            "ExportViewFormMixin is deprecated and will be removed "
            "in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )

    def form_valid(self, form):
        formats = self.get_export_formats()
        file_format = formats[int(form.cleaned_data["format"])]()
        if hasattr(self, "get_filterset"):
            queryset = self.get_filterset(self.get_filterset_class()).qs
        else:
            queryset = self.get_queryset()
        export_data = self.get_export_data(file_format, queryset)
        content_type = file_format.get_content_type()
        # Django 1.7 uses the content_type kwarg instead of mimetype
        try:
            response = HttpResponse(export_data, content_type=content_type)
        except TypeError:
            response = HttpResponse(export_data, mimetype=content_type)
        response["Content-Disposition"] = 'attachment; filename="{}"'.format(
            self.get_export_filename(file_format),
        )

        post_export.send(sender=None, model=self.model)
        return response
