from django.apps import apps
from django.core.management.base import CommandError
from django.http.response import mimetypes
from django.utils.module_loading import import_string

from import_export.formats.base_formats import DEFAULT_FORMATS
from import_export.resources import modelresource_factory


def get_resource_class(model_or_resource_class):
    try:
        # First, try to load it as a resource class
        resource_class = import_string(model_or_resource_class)
        return resource_class
    except ImportError:
        pass

    try:
        if model_or_resource_class.count(".") == 1:
            app_label, model_name = model_or_resource_class.split(".")
            model = apps.get_model(app_label, model_name)
            if model:
                resource_class = modelresource_factory(model)
                return resource_class
    except LookupError:
        pass

    raise CommandError(
        f"Cannot import '{model_or_resource_class}' as a resource class or model."
    )


MIME_TYPE_FORMAT_MAPPING = {format.CONTENT_TYPE: format for format in DEFAULT_FORMATS}


def get_format_class(format_name, file_name, encoding=None):
    if format_name:
        try:
            # Direct import attempt
            format_class = import_string(format_name)
        except ImportError:
            # Fallback to base_formats
            fallback_format_name = f"import_export.formats.base_formats.{format_name}"
            try:
                format_class = import_string(fallback_format_name)
            except ImportError:
                # fallback to uppercase format name
                try:
                    format_class = import_string(
                        f"import_export.formats.base_formats.{format_name.upper()}"
                    )
                except ImportError:
                    raise CommandError(
                        f"Cannot import '{format_name}' or '{fallback_format_name}'"
                        " format class."
                    )
        return format_class(encoding=encoding)

    else:
        # Determine MIME type from file name
        mimetype, file_encoding = mimetypes.guess_type(file_name)

        if not mimetype:
            raise CommandError(
                f"Cannot determine MIME type for '{file_name}'. "
                " Please specify format with --format."
            )

        try:
            format_class = MIME_TYPE_FORMAT_MAPPING[mimetype]
            return format_class(encoding=encoding or file_encoding)
        except KeyError:
            raise CommandError(
                f"Cannot find format for MIME type '{mimetype}'."
                " Please specify format with --format."
            )


def get_default_format_names():
    return ", ".join([f.__name__ for f in DEFAULT_FORMATS])
