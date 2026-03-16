import logging
import warnings
from urllib.parse import urlencode

import django
from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.models import ADDITION, CHANGE, DELETION, LogEntry
from django.contrib.auth import get_permission_codename
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import FieldError, PermissionDenied
from django.forms import MultipleChoiceField, MultipleHiddenInput
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils.decorators import method_decorator
from django.utils.module_loading import import_string
from django.utils.translation import gettext_lazy as _
from django.views.decorators.http import require_POST

from .formats.base_formats import BINARY_FORMATS
from .forms import ConfirmImportForm, ImportForm, SelectableFieldsExportForm
from .mixins import BaseExportMixin, BaseImportMixin
from .results import RowResult
from .signals import post_export, post_import
from .tmp_storages import TempFolderStorage

logger = logging.getLogger(__name__)


class ImportExportMixinBase:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_change_list_template()

    def init_change_list_template(self):
        # Store already set change_list_template to allow users to independently
        # customize the change list object tools. This treats the cases where
        # `self.change_list_template` is `None` (the default in `ModelAdmin`) or
        # where `self.import_export_change_list_template` is `None` as falling
        # back on the default templates.
        if getattr(self, "change_list_template", None):
            self.ie_base_change_list_template = self.change_list_template
        else:
            self.ie_base_change_list_template = "admin/change_list.html"

        try:
            self.change_list_template = getattr(
                self, "import_export_change_list_template", None
            )
        except AttributeError:
            logger.warning("failed to assign change_list_template attribute")

        if self.change_list_template is None:
            self.change_list_template = self.ie_base_change_list_template

    def get_model_info(self):
        app_label = self.model._meta.app_label
        return (app_label, self.model._meta.model_name)

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context["ie_base_change_list_template"] = (
            self.ie_base_change_list_template
        )
        return super().changelist_view(request, extra_context)


class ImportMixin(BaseImportMixin, ImportExportMixinBase):
    """
    Import mixin.

    This is intended to be mixed with django.contrib.admin.ModelAdmin
    https://docs.djangoproject.com/en/dev/ref/contrib/admin/
    """

    #: template for change_list view
    import_export_change_list_template = "admin/import_export/change_list_import.html"
    #: template for import view
    import_template_name = "admin/import_export/import.html"
    #: form class to use for the initial import step
    import_form_class = ImportForm
    #: form class to use for the confirm import step
    confirm_form_class = ConfirmImportForm
    #: import data encoding
    from_encoding = "utf-8-sig"
    #: control which UI elements appear when import errors are displayed.
    #: Available options: 'message', 'row', 'traceback'
    import_error_display = ("message",)

    skip_admin_log = None
    # storage class for saving temporary files
    tmp_storage_class = None

    def get_skip_admin_log(self):
        if self.skip_admin_log is None:
            return getattr(settings, "IMPORT_EXPORT_SKIP_ADMIN_LOG", False)
        else:
            return self.skip_admin_log

    def get_tmp_storage_class(self):
        if self.tmp_storage_class is None:
            tmp_storage_class = getattr(
                settings,
                "IMPORT_EXPORT_TMP_STORAGE_CLASS",
                TempFolderStorage,
            )
        else:
            tmp_storage_class = self.tmp_storage_class

        if isinstance(tmp_storage_class, str):
            tmp_storage_class = import_string(tmp_storage_class)
        return tmp_storage_class

    def get_tmp_storage_class_kwargs(self):
        """Override this method to provide additional kwargs to temp storage class."""
        return {}

    def has_import_permission(self, request):
        """
        Returns whether a request has import permission.
        """
        IMPORT_PERMISSION_CODE = getattr(
            settings, "IMPORT_EXPORT_IMPORT_PERMISSION_CODE", None
        )
        if IMPORT_PERMISSION_CODE is None:
            return True

        opts = self.opts
        codename = get_permission_codename(IMPORT_PERMISSION_CODE, opts)
        return request.user.has_perm(f"{opts.app_label}.{codename}")

    def get_urls(self):
        urls = super().get_urls()
        info = self.get_model_info()
        my_urls = [
            path(
                "process_import/",
                self.admin_site.admin_view(self.process_import),
                name="%s_%s_process_import" % info,
            ),
            path(
                "import/",
                self.admin_site.admin_view(self.import_action),
                name="%s_%s_import" % info,
            ),
        ]
        return my_urls + urls

    @method_decorator(require_POST)
    def process_import(self, request, **kwargs):
        """
        Perform the actual import action (after the user has confirmed the import)
        """
        if not self.has_import_permission(request):
            raise PermissionDenied

        confirm_form = self.create_confirm_form(request)
        if confirm_form.is_valid():
            import_formats = self.get_import_formats()
            input_format = import_formats[int(confirm_form.cleaned_data["format"])](
                encoding=self.from_encoding
            )
            encoding = None if input_format.is_binary() else self.from_encoding
            tmp_storage_cls = self.get_tmp_storage_class()
            tmp_storage = tmp_storage_cls(
                name=confirm_form.cleaned_data["import_file_name"],
                encoding=encoding,
                read_mode=input_format.get_read_mode(),
                **self.get_tmp_storage_class_kwargs(),
            )

            data = tmp_storage.read()
            dataset = input_format.create_dataset(data)
            result = self.process_dataset(dataset, confirm_form, request, **kwargs)

            tmp_storage.remove()

            return self.process_result(result, request)
        else:
            context = self.admin_site.each_context(request)
            context.update(
                {
                    "title": _("Import"),
                    "confirm_form": confirm_form,
                    "opts": self.model._meta,
                    "errors": confirm_form.errors,
                }
            )
            return TemplateResponse(request, [self.import_template_name], context)

    def process_dataset(
        self,
        dataset,
        form,
        request,
        **kwargs,
    ):
        # Get file_name from kwargs if provided, otherwise from form's cleaned_data
        # Must be extracted before passing kwargs to get_import_data_kwargs
        file_name = kwargs.pop("file_name", None)
        if file_name is None:
            file_name = form.cleaned_data.get("original_file_name")

        res_kwargs = self.get_import_resource_kwargs(request, form=form, **kwargs)
        resource = self.choose_import_resource_class(form, request)(**res_kwargs)
        imp_kwargs = self.get_import_data_kwargs(request=request, form=form, **kwargs)
        imp_kwargs["retain_instance_in_row_result"] = True

        return resource.import_data(
            dataset,
            dry_run=False,
            file_name=file_name,
            user=request.user,
            **imp_kwargs,
        )

    def process_result(self, result, request):
        self.generate_log_entries(result, request)
        self.add_success_message(result, request)
        post_import.send(sender=None, model=self.model)

        url = reverse(
            "admin:%s_%s_changelist" % self.get_model_info(),
            current_app=self.admin_site.name,
        )
        return HttpResponseRedirect(url)

    def generate_log_entries(self, result, request):
        if not self.get_skip_admin_log():
            # Add imported objects to LogEntry
            if django.VERSION >= (5, 1):
                self._log_actions(result, request)
            else:
                logentry_map = {
                    RowResult.IMPORT_TYPE_NEW: ADDITION,
                    RowResult.IMPORT_TYPE_UPDATE: CHANGE,
                    RowResult.IMPORT_TYPE_DELETE: DELETION,
                }
                content_type_id = ContentType.objects.get_for_model(self.model).pk
                for row in result:
                    if row.import_type in logentry_map:
                        with warnings.catch_warnings():
                            cat = DeprecationWarning
                            warnings.simplefilter("ignore", category=cat)
                            LogEntry.objects.log_action(
                                user_id=request.user.pk,
                                content_type_id=content_type_id,
                                object_id=row.object_id,
                                object_repr=row.object_repr,
                                action_flag=logentry_map[row.import_type],
                                change_message=_(
                                    "%s through import_export" % row.import_type
                                ),
                            )

    def add_success_message(self, result, request):
        opts = self.model._meta

        success_message = _(
            "Import finished: {} new, {} updated, {} deleted and {} skipped {}."
        ).format(
            result.totals[RowResult.IMPORT_TYPE_NEW],
            result.totals[RowResult.IMPORT_TYPE_UPDATE],
            result.totals[RowResult.IMPORT_TYPE_DELETE],
            result.totals[RowResult.IMPORT_TYPE_SKIP],
            opts.verbose_name_plural,
        )

        messages.success(request, success_message)

    def get_import_context_data(self, **kwargs):
        return self.get_context_data(**kwargs)

    def get_context_data(self, **kwargs):
        return {}

    def create_import_form(self, request):
        """
        .. versionadded:: 3.0

        Return a form instance to use for the 'initial' import step.
        This method can be extended to make dynamic form updates to the
        form after it has been instantiated. You might also look to
        override the following:

        * :meth:`~import_export.admin.ImportMixin.get_import_form_class`
        * :meth:`~import_export.admin.ImportMixin.get_import_form_kwargs`
        * :meth:`~import_export.admin.ImportMixin.get_import_form_initial`
        * :meth:`~import_export.mixins.BaseImportMixin.get_import_resource_classes`
        """
        formats = self.get_import_formats()
        form_class = self.get_import_form_class(request)
        kwargs = self.get_import_form_kwargs(request)

        return form_class(formats, self.get_import_resource_classes(request), **kwargs)

    def get_import_form_class(self, request):
        """
        .. versionadded:: 3.0

        Return the form class to use for the 'import' step. If you only have
        a single custom form class, you can set the ``import_form_class``
        attribute to change this for your subclass.
        """
        return self.import_form_class

    def get_import_form_kwargs(self, request):
        """
        .. versionadded:: 3.0

        Return a dictionary of values with which to initialize the 'import'
        form (including the initial values returned by
        :meth:`~import_export.admin.ImportMixin.get_import_form_initial`).
        """
        return {
            "data": request.POST or None,
            "files": request.FILES or None,
            "initial": self.get_import_form_initial(request),
        }

    def get_import_form_initial(self, request):
        """
        .. versionadded:: 3.0

        Return a dictionary of initial field values to be provided to the
        'import' form.
        """
        return {}

    def create_confirm_form(self, request, import_form=None):
        """
        .. versionadded:: 3.0

        Return a form instance to use for the 'confirm' import step.
        This method can be extended to make dynamic form updates to the
        form after it has been instantiated. You might also look to
        override the following:

        * :meth:`~import_export.admin.ImportMixin.get_confirm_form_class`
        * :meth:`~import_export.admin.ImportMixin.get_confirm_form_kwargs`
        * :meth:`~import_export.admin.ImportMixin.get_confirm_form_initial`
        """
        form_class = self.get_confirm_form_class(request)
        kwargs = self.get_confirm_form_kwargs(request, import_form)
        return form_class(**kwargs)

    def get_confirm_form_class(self, request):
        """
        .. versionadded:: 3.0

        Return the form class to use for the 'confirm' import step. If you only
        have a single custom form class, you can set the ``confirm_form_class``
        attribute to change this for your subclass.
        """
        return self.confirm_form_class

    def get_confirm_form_kwargs(self, request, import_form=None):
        """
        .. versionadded:: 3.0

        Return a dictionary of values with which to initialize the 'confirm'
        form (including the initial values returned by
        :meth:`~import_export.admin.ImportMixin.get_confirm_form_initial`).
        """
        if import_form:
            # When initiated from `import_action()`, the 'posted' data
            # is for the 'import' form, not this one.
            data = None
            files = None
        else:
            data = request.POST or None
            files = request.FILES or None

        return {
            "data": data,
            "files": files,
            "initial": self.get_confirm_form_initial(request, import_form),
        }

    def get_confirm_form_initial(self, request, import_form):
        """
        .. versionadded:: 3.0

        Return a dictionary of initial field values to be provided to the
        'confirm' form.
        """
        if import_form is None:
            return {}
        return {
            "import_file_name": import_form.cleaned_data[
                "import_file"
            ].tmp_storage_name,
            "original_file_name": import_form.cleaned_data["import_file"].name,
            "format": import_form.cleaned_data["format"],
            "resource": import_form.cleaned_data.get("resource", ""),
        }

    def get_import_data_kwargs(self, **kwargs):
        """
        Prepare kwargs for import_data.
        """
        form = kwargs.get("form")
        if form:
            kwargs.pop("form")
            return kwargs
        return kwargs

    def write_to_tmp_storage(self, import_file, input_format):
        encoding = None
        if not input_format.is_binary():
            encoding = self.from_encoding

        tmp_storage_cls = self.get_tmp_storage_class()
        tmp_storage = tmp_storage_cls(
            encoding=encoding,
            read_mode=input_format.get_read_mode(),
            **self.get_tmp_storage_class_kwargs(),
        )
        data = b""
        for chunk in import_file.chunks():
            data += chunk

        tmp_storage.save(data)
        return tmp_storage

    def add_data_read_fail_error_to_form(self, form, e):
        exc_name = repr(type(e).__name__)
        msg = _(
            "%(exc_name)s encountered while trying to read file. "
            "Ensure you have chosen the correct format for the file."
        ) % {"exc_name": exc_name}
        form.add_error("import_file", msg)

    def import_action(self, request, **kwargs):
        """
        Perform a dry_run of the import to make sure the import will not
        result in errors.  If there are no errors, save the user
        uploaded file to a local temp file that will be used by
        'process_import' for the actual import.
        """
        if not self.has_import_permission(request):
            raise PermissionDenied

        context = self.get_import_context_data()

        import_formats = self.get_import_formats()
        import_form = self.create_import_form(request)
        resources = []
        if request.POST and import_form.is_valid():
            input_format = import_formats[int(import_form.cleaned_data["format"])]()
            if not input_format.is_binary():
                input_format.encoding = self.from_encoding
            import_file = import_form.cleaned_data["import_file"]

            if self.is_skip_import_confirm_enabled():
                # This setting means we are going to skip the import confirmation step.
                # Go ahead and process the file for import in a transaction
                # If there are any errors, we roll back the transaction.
                # rollback_on_validation_errors is set to True so that we rollback on
                # validation errors. If this is not done validation errors would be
                # silently skipped.
                data = b""
                for chunk in import_file.chunks():
                    data += chunk
                try:
                    dataset = input_format.create_dataset(data)
                except Exception as e:
                    self.add_data_read_fail_error_to_form(import_form, e)
                if not import_form.errors:
                    result = self.process_dataset(
                        dataset,
                        import_form,
                        request,
                        raise_errors=False,
                        rollback_on_validation_errors=True,
                        file_name=import_file.name,
                        **kwargs,
                    )
                    if not result.has_errors() and not result.has_validation_errors():
                        return self.process_result(result, request)
                    else:
                        context["result"] = result
            else:
                # first always write the uploaded file to disk as it may be a
                # memory file or else based on settings upload handlers
                tmp_storage = self.write_to_tmp_storage(import_file, input_format)
                # allows get_confirm_form_initial() to include both the
                # original and saved file names from form.cleaned_data
                import_file.tmp_storage_name = tmp_storage.name

                try:
                    # then read the file, using the proper format-specific mode
                    # warning, big files may exceed memory
                    data = tmp_storage.read()
                    dataset = input_format.create_dataset(data)
                except Exception as e:
                    self.add_data_read_fail_error_to_form(import_form, e)
                else:
                    if not dataset:
                        import_form.add_error(
                            "import_file",
                            _(
                                "No valid data to import. Ensure your file "
                                "has the correct headers or data for import."
                            ),
                        )

                if not import_form.errors:
                    # prepare kwargs for import data, if needed
                    res_kwargs = self.get_import_resource_kwargs(
                        request, form=import_form, **kwargs
                    )
                    resource = self.choose_import_resource_class(import_form, request)(
                        **res_kwargs
                    )
                    resources = [resource]

                    # prepare additional kwargs for import_data, if needed
                    imp_kwargs = self.get_import_data_kwargs(
                        request=request, form=import_form, **kwargs
                    )
                    result = resource.import_data(
                        dataset,
                        dry_run=True,
                        raise_errors=False,
                        file_name=import_file.name,
                        user=request.user,
                        **imp_kwargs,
                    )
                    context["result"] = result

                    if not result.has_errors() and not result.has_validation_errors():
                        context["confirm_form"] = self.create_confirm_form(
                            request, import_form=import_form
                        )
        else:
            res_kwargs = self.get_import_resource_kwargs(
                request=request, form=import_form, **kwargs
            )
            resource_classes = self.get_import_resource_classes(request)
            resources = [
                resource_class(**res_kwargs) for resource_class in resource_classes
            ]

        context.update(self.admin_site.each_context(request))

        context["title"] = _("Import")
        context["form"] = import_form
        context["opts"] = self.model._meta
        context["media"] = self.media + import_form.media
        context["fields_list"] = [
            (
                resource.get_display_name(),
                [f.column_name for f in resource.get_user_visible_fields()],
            )
            for resource in resources
        ]
        context["import_error_display"] = self.import_error_display

        request.current_app = self.admin_site.name
        return TemplateResponse(request, [self.import_template_name], context)

    def changelist_view(self, request, extra_context=None):
        if extra_context is None:
            extra_context = {}
        extra_context["has_import_permission"] = self.has_import_permission(request)
        return super().changelist_view(request, extra_context)

    def _log_actions(self, result, request):
        """
        Create appropriate LogEntry instances for the result.
        """
        rows = {}
        for row in result:
            rows.setdefault(row.import_type, [])
            rows[row.import_type].append(row.instance)

        self._create_log_entries(request.user.pk, rows)

    def _create_log_entries(self, user_pk, rows):
        logentry_map = {
            RowResult.IMPORT_TYPE_NEW: ADDITION,
            RowResult.IMPORT_TYPE_UPDATE: CHANGE,
            RowResult.IMPORT_TYPE_DELETE: DELETION,
        }
        missing = object()
        for import_type, instances in rows.items():
            action_flag = logentry_map.get(import_type, missing)
            if action_flag is not missing:
                self._create_log_entry(
                    user_pk, rows[import_type], import_type, action_flag
                )

    def _create_log_entry(self, user_pk, rows, import_type, action_flag):
        if len(rows) > 0:
            LogEntry.objects.log_actions(
                user_pk,
                rows,
                action_flag,
                change_message=_("%s through import_export" % import_type),
                single_object=len(rows) == 1,
            )


class ExportMixin(BaseExportMixin, ImportExportMixinBase):
    """
    Export mixin.

    This is intended to be mixed with
    `ModelAdmin <https://docs.djangoproject.com/en/stable/ref/contrib/admin/>`_.
    """

    #: template for change_list view
    import_export_change_list_template = "admin/import_export/change_list_export.html"
    #: template for export view
    export_template_name = "admin/import_export/export.html"
    #: export data encoding
    to_encoding = None
    #: Form class to use for the initial export step.
    #: Assign to :class:`~import_export.forms.ExportForm` if you would
    #: like to disable selectable fields feature.
    export_form_class = SelectableFieldsExportForm

    def get_urls(self):
        urls = super().get_urls()
        my_urls = [
            path(
                "export/",
                self.admin_site.admin_view(self.export_action),
                name="%s_%s_export" % self.get_model_info(),
            ),
        ]
        return my_urls + urls

    def has_export_permission(self, request):
        """
        Returns whether a request has export permission.
        """
        EXPORT_PERMISSION_CODE = getattr(
            settings, "IMPORT_EXPORT_EXPORT_PERMISSION_CODE", None
        )
        if EXPORT_PERMISSION_CODE is None:
            return True

        opts = self.opts
        codename = get_permission_codename(EXPORT_PERMISSION_CODE, opts)
        return request.user.has_perm(f"{opts.app_label}.{codename}")

    def get_export_queryset(self, request):
        """
        Returns export queryset. The queryset is obtained by calling
        ModelAdmin
        `get_queryset()
        <https://docs.djangoproject.com/en/dev/ref/contrib/admin/#django.contrib.admin.ModelAdmin.get_queryset>`_.

        Default implementation respects applied search and filters.
        """
        list_display = self.get_list_display(request)
        list_display_links = self.get_list_display_links(request, list_display)
        list_select_related = self.get_list_select_related(request)
        list_filter = self.get_list_filter(request)
        search_fields = self.get_search_fields(request)
        if self.get_actions(request):
            list_display = ["action_checkbox"] + list(list_display)

        ChangeList = self.get_changelist(request)
        changelist_kwargs = {
            "request": request,
            "model": self.model,
            "list_display": list_display,
            "list_display_links": list_display_links,
            "list_filter": list_filter,
            "date_hierarchy": self.date_hierarchy,
            "search_fields": search_fields,
            "list_select_related": list_select_related,
            "list_per_page": self.list_per_page,
            "list_max_show_all": self.list_max_show_all,
            "list_editable": self.list_editable,
            "model_admin": self,
            "sortable_by": self.sortable_by,
        }
        changelist_kwargs["search_help_text"] = self.search_help_text

        class ExportChangeList(ChangeList):
            def get_results(self, request):
                """
                Overrides ChangeList.get_results() to bypass default operations like
                pagination and result counting, which are not needed for export. This
                prevents executing unnecessary COUNT queries during ChangeList
                initialization.
                """
                pass

        cl = ExportChangeList(**changelist_kwargs)

        # get_queryset() is already called during initialization,
        # it is enough to get its results
        if hasattr(cl, "queryset"):
            return cl.queryset

        # Fallback in case the ChangeList doesn't have queryset attribute set
        return cl.get_queryset(request)

    def get_export_data(self, file_format, request, queryset, **kwargs):
        """
        Returns file_format representation for given queryset.
        """
        if not self.has_export_permission(request):
            raise PermissionDenied

        force_native_type = type(file_format) in BINARY_FORMATS
        data = self.get_data_for_export(
            request,
            queryset,
            force_native_type=force_native_type,
            **kwargs,
        )
        export_data = file_format.export_data(data)
        encoding = kwargs.get("encoding")
        if not file_format.is_binary() and encoding:
            export_data = export_data.encode(encoding)
        return export_data

    def get_export_context_data(self, **kwargs):
        return self.get_context_data(**kwargs)

    def get_context_data(self, **kwargs):
        return {}

    def get_export_form_class(self):
        """
        Get the form class used to read the export format.
        """
        return self.export_form_class

    def export_action(self, request):
        """
        Handles the default workflow for both the export form and the
        export of data to file.
        """
        if not self.has_export_permission(request):
            raise PermissionDenied

        form_type = self.get_export_form_class()
        formats = self.get_export_formats()
        queryset = self.get_export_queryset(request)
        if self.is_skip_export_form_enabled():
            return self._do_file_export(formats[0](), request, queryset)

        form = form_type(
            formats,
            self.get_export_resource_classes(request),
            data=request.POST or None,
        )
        if request.POST and "export_items" in request.POST:
            # this field is instantiated if the export is POSTed from the
            # 'action' drop down
            form.fields["export_items"] = MultipleChoiceField(
                widget=MultipleHiddenInput,
                required=False,
                choices=[(pk, pk) for pk in self.get_valid_export_item_pks(request)],
            )
        if form.is_valid():
            file_format = formats[int(form.cleaned_data["format"])]()

            if "export_items" in form.changed_data:
                # this request has arisen from an Admin UI action
                # export item pks are stored in form data
                # so generate the queryset from the stored pks
                queryset = queryset.filter(pk__in=form.cleaned_data["export_items"])

            try:
                return self._do_file_export(
                    file_format, request, queryset, export_form=form
                )
            except (ValueError, FieldError) as e:
                messages.error(request, str(e))

        context = self.init_request_context_data(request, form)
        request.current_app = self.admin_site.name
        return TemplateResponse(request, [self.export_template_name], context=context)

    def get_valid_export_item_pks(self, request):
        """
        DEPRECATED: This method is deprecated and will be removed in the future.
        Overwrite get_queryset() or get_export_queryset() instead.

        Returns a list of valid pks for export.
        This is used to validate which objects can be exported when exports are
        triggered from the Admin UI 'action' dropdown.
        This can be overridden to filter returned pks for performance and/or security
        reasons.

        :param request: The request object.
        :returns: a list of valid pks (by default is all pks in table).
        """
        cls = self.__class__
        warnings.warn(
            "The 'get_valid_export_item_pks()' method in "
            f"{cls.__module__}.{cls.__qualname__} "
            "is deprecated and will "
            "be removed in a future release",
            DeprecationWarning,
        )
        return self.model.objects.all().values_list("pk", flat=True)

    def changelist_view(self, request, extra_context=None):
        if extra_context is None:
            extra_context = {}
        extra_context["has_export_permission"] = self.has_export_permission(request)
        return super().changelist_view(request, extra_context)

    def get_export_filename(self, request, queryset, file_format):
        return super().get_export_filename(file_format)

    def init_request_context_data(self, request, form):
        context = self.get_export_context_data()
        context.update(self.admin_site.each_context(request))
        context["title"] = _("Export")
        context["form"] = form
        context["opts"] = self.model._meta
        context["fields_list"] = [
            (
                res.get_display_name(),
                [
                    field.column_name
                    for field in res(
                        **self.get_export_resource_kwargs(request)
                    ).get_user_visible_fields()
                ],
            )
            for res in self.get_export_resource_classes(request)
        ]
        return context

    def _do_file_export(self, file_format, request, queryset, export_form=None):
        export_data = self.get_export_data(
            file_format,
            request,
            queryset,
            encoding=self.to_encoding,
            export_form=export_form,
        )
        content_type = file_format.get_content_type()
        response = HttpResponse(export_data, content_type=content_type)
        response["Content-Disposition"] = 'attachment; filename="{}"'.format(
            self.get_export_filename(request, queryset, file_format),
        )
        post_export.send(sender=None, model=self.model)
        return response


class ImportExportMixin(ImportMixin, ExportMixin):
    """
    Import and export mixin.
    """

    #: template for change_list view
    import_export_change_list_template = (
        "admin/import_export/change_list_import_export.html"
    )


class ImportExportModelAdmin(ImportExportMixin, admin.ModelAdmin):
    """
    Subclass of ModelAdmin with import/export functionality.
    """


class ExportActionMixin(ExportMixin):
    """
    Mixin with export functionality implemented as an admin action.
    """

    #: template for change form
    change_form_template = "admin/import_export/change_form.html"

    #: Flag to indicate whether to show 'export' button on change form
    show_change_form_export = True

    # This action will receive a selection of items as a queryset,
    # store them in the context, and then render the 'export' admin form page,
    # so that users can select file format and resource

    def change_view(self, request, object_id, form_url="", extra_context=None):
        extra_context = extra_context or {}
        extra_context["show_change_form_export"] = (
            self.show_change_form_export and self.has_export_permission(request)
        )
        return super().change_view(
            request,
            object_id,
            form_url,
            extra_context=extra_context,
        )

    def response_change(self, request, obj):
        # called if the export is triggered from the instance detail page.
        if "_export-item" in request.POST:
            return self.export_admin_action(
                request, self.model.objects.filter(pk=obj.pk)
            )
        return super().response_change(request, obj)

    def export_admin_action(self, request, queryset):
        """
        Action runs on POST from instance action menu (if enabled).
        """
        formats = self.get_export_formats()
        if self.is_skip_export_form_from_action_enabled():
            file_format = formats[0]()
            return self._do_file_export(file_format, request, queryset)

        form_type = self.get_export_form_class()
        formats = self.get_export_formats()
        export_items = list(queryset.values_list("pk", flat=True))
        form = form_type(
            formats=formats,
            resources=self.get_export_resource_classes(request),
            initial={"export_items": export_items},
        )
        # selected items are to be stored as a hidden input on the form
        form.fields["export_items"] = MultipleChoiceField(
            widget=MultipleHiddenInput, required=False, choices=export_items
        )
        context = self.init_request_context_data(request, form)

        # this is necessary to render the FORM action correctly
        # i.e. so the POST goes to the correct URL
        export_url = reverse(
            "%s:%s_%s_export"
            % (
                self.admin_site.name,
                self.model._meta.app_label,
                self.model._meta.model_name,
            )
        )

        # Preserve admin changelist filters by including request GET parameters
        # This fixes issue #2097 where applied filters are lost during export
        if request.GET:
            export_url += "?" + urlencode(request.GET)

        context["export_url"] = export_url

        return render(request, "admin/import_export/export.html", context=context)

    def get_actions(self, request):
        """
        Adds the export action to the list of available actions.
        """
        actions = super().get_actions(request)
        if self.has_export_permission(request):
            actions.update(
                export_admin_action=(
                    type(self).export_admin_action,
                    "export_admin_action",
                    _("Export selected %(verbose_name_plural)s"),
                )
            )
        return actions


class ExportActionModelAdmin(ExportActionMixin, admin.ModelAdmin):
    """
    Subclass of ModelAdmin with export functionality implemented as an
    admin action.
    """


class ImportExportActionModelAdmin(ImportMixin, ExportActionModelAdmin):
    """
    Subclass of ExportActionModelAdmin with import/export functionality.
    Export functionality is implemented as an admin action.
    """
