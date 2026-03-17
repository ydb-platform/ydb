from . import docx, conversion, options, images, transforms, underline
from .raw_text import extract_raw_text_from_element
from .docx.style_map import write_style_map, read_style_map

__all__ = ["convert_to_html", "extract_raw_text", "images", "transforms", "underline"]


_undefined = object()


def convert_to_html(*args, **kwargs):
    return convert(*args, output_format="html", **kwargs)


def convert_to_markdown(*args, **kwargs):
    return convert(*args, output_format="markdown", **kwargs)


def convert(
    fileobj,
    transform_document=None,
    id_prefix=None,
    include_embedded_style_map=_undefined,
    external_file_access=_undefined,
    **kwargs
):
    if include_embedded_style_map is _undefined:
        include_embedded_style_map = True

    if transform_document is None:
        transform_document = lambda x: x

    if include_embedded_style_map:
        kwargs["embedded_style_map"] = read_style_map(fileobj)

    if external_file_access is _undefined:
        external_file_access = False

    return options.read_options(kwargs).bind(lambda convert_options:
        docx.read(fileobj, external_file_access=external_file_access).map(transform_document).bind(lambda document:
            conversion.convert_document_element_to_html(
                document,
                id_prefix=id_prefix,
                **convert_options
            )
        )
    )


def extract_raw_text(fileobj):
    return docx.read(fileobj).map(extract_raw_text_from_element)


def embed_style_map(fileobj, style_map):
    write_style_map(fileobj, style_map)

def read_embedded_style_map(fileobj):
    return read_style_map(fileobj)
