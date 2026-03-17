import binascii
import uuid
from enum import Enum
from typing import Optional

from .generic import (
    DictionaryObject,
    NameObject,
    PdfObject,
    StreamObject,
    pdf_name,
)
from .layout import BoxConstraints
from .reader import PdfFileReader
from .writer import BasePdfFileWriter

__all__ = [
    'ResourceType',
    'ResourceManagementError',
    'PdfResources',
    'PdfContent',
    'RawContent',
    'ImportedPdfPage',
]

# TODO have the merge_resources helper in incremental_writer rely on some
#  of the idioms established here


class ResourceType(Enum):
    """
    Enum listing resources that can be used as keys in a resource dictionary.

    See ISO 32000-1, § 7.8.3 Table 34.
    """

    EXT_G_STATE = pdf_name('/ExtGState')
    """
    External graphics state specifications.
    See ISO 32000-1, § 8.4.5.
    """

    COLOR_SPACE = pdf_name('/ColorSpace')
    """
    Colour space definitions.
    See ISO 32000-1, § 8.6.
    """

    PATTERN = pdf_name('/Pattern')
    """
    Pattern definitions.
    See ISO 32000-1, § 8.7.
    """

    SHADING = pdf_name('/Shading')
    """
    Shading definitions.
    See ISO 32000-1, § 8.7.4.3.
    """

    XOBJECT = pdf_name('/XObject')
    """
    External object definitions (images and form XObjects).
    See ISO 32000-1, § 8.8.
    """

    FONT = pdf_name('/Font')
    """
    Font specifications.
    See ISO 32000-1, § 9.
    """

    PROPERTIES = pdf_name('/Properties')
    """
    Marked content properties.
    See ISO 32000-1, § 14.6.2.
    """


class ResourceManagementError(ValueError):
    """
    Used to signal problems with resource dictionaries.
    """

    pass


def _res_merge_helper(dict1, dict2):
    for k, v2 in dict2.items():
        if k in dict1:
            raise ResourceManagementError(
                f"Resource with name {k} occurs in both dictionaries."
            )
        dict1[k] = v2
    return dict1


class PdfResources:
    """
    Representation of a PDF resource dictionary.

    This class implements :meth:`__getitem__` with :class:`.ResourceType` keys
    for dynamic access to its attributes.
    To merge two instances of :class:`.PdfResources` into one another,
    the class overrides :meth:`__iadd__`, so you can write.

    .. code-block:: python

        res1 += res2

    *Note:* Merging two resource dictionaries with conflicting resource names
    will produce a :class:`.ResourceManagementError`.

    *Note:* This class is currently only used for new resource dictionaries.
    """

    def __init__(self):
        self.ext_g_state = DictionaryObject()
        self.color_space = DictionaryObject()
        self.pattern = DictionaryObject()
        self.shading = DictionaryObject()
        self.xobject = DictionaryObject()
        self.font = DictionaryObject()
        self.properties = DictionaryObject()

    def __getitem__(self, item: ResourceType):
        return getattr(self, item.name.lower())

    def as_pdf_object(self) -> DictionaryObject:
        """
        Render this instance of :class:`.PdfResources` to an actual resource
        dictionary.
        """

        def _gen():
            for k in ResourceType:
                val = self[k]
                if val:
                    yield k.value, val

        return DictionaryObject({k: v for k, v in _gen()})

    def __iadd__(self, other):
        """
        Merge another resource dictionary into this one.
        :param other:
            Another instance of :class:`.PdfResources`
        :return:
            Always returns ``self``
        :raises ResourceManagementError:
            Raised when there is a resource name conflict.
        """
        for k in ResourceType:
            _res_merge_helper(self[k], other[k])
        return self


class PdfContent:
    """
    Abstract representation of part of a PDF content stream.

    .. warning::

        Whether :class:`.PdfContent` instances can be reused or not
        is left up to the subclasses.
    """

    writer = None
    """
    The :meth:`__init__` method comes with an optional ``writer`` 
    parameter that can be used to let subclasses register external resources 
    with the writer by themselves.
    
    It can also be set after the fact by calling :meth:`set_writer`.
    """

    def __init__(
        self,
        resources: Optional[PdfResources] = None,
        box: Optional[BoxConstraints] = None,
        writer: Optional[BasePdfFileWriter] = None,
    ):
        self._resources: PdfResources = resources or PdfResources()
        self.box: BoxConstraints = box or BoxConstraints()
        self.writer = writer

    @property
    def _ensure_writer(self) -> BasePdfFileWriter:
        if self.writer is None:
            raise ValueError("PDF writer is not set")
        return self.writer

    # TODO support a set-if-not-taken mechanism, that suggests alternative names
    #  if necessary.
    def set_resource(
        self, category: ResourceType, name: NameObject, value: PdfObject
    ):
        """Set a value in the resource dictionary associated with this content
        fragment.

        :param category:
            The resource category to which the resource belongs.
        :param name:
            The resource's (internal) name.
        :param value:
            The resource's value.
        """
        self._resources[category][name] = value

    def import_resources(self, resources: PdfResources):
        """Import resources from another resource dictionary.

        :param resources:
            An instance of :class:`.PdfResources`.
        :raises ResourceManagementError:
            Raised when there is a resource name conflict.
        """
        self._resources += resources

    @property
    def resources(self) -> PdfResources:
        """
        :return:
            The :class:`.PdfResources` instance associated with this
            content fragment.
        """
        return self._resources

    def render(self) -> bytes:
        """
        Compile the content to graphics operators.
        """
        raise NotImplementedError

    # TODO allow the bounding box to be overridden/refitted
    #  (using matrix transforms)
    def as_form_xobject(self) -> StreamObject:
        """
        Render the object to a form XObject to be referenced by another
        content stream. See ISO 32000-1, § 8.8.

        *Note:* Even if :attr:`writer` is set, the resulting form XObject will
        not be registered. This is left up to the caller.

        :return:
            A :class:`~.generic.StreamObject` instance representing
            the resulting form XObject.
        """
        from pyhanko.pdf_utils.writer import init_xobject_dictionary

        command_stream = self.render()
        return init_xobject_dictionary(
            command_stream=command_stream,
            box_width=self.box.width,
            box_height=self.box.height,
            resources=self._resources.as_pdf_object(),
        )

    def set_writer(self, writer):
        """
        Override the currently registered writer object.

        :param writer:
            An instance of :class:`~.writer.BasePdfFileWriter`.
        """
        self.writer = writer

    def add_to_page(
        self, writer: BasePdfFileWriter, page_ix: int, prepend: bool = False
    ):
        """
        Convenience wrapper around :meth:`.BasePdfFileWriter.add_stream_to_page`
        to turn a :class:`.PdfContent` instance into a page
        content stream.

        :param writer:
            A PDF file writer.
        :param page_ix:
            Index of the page to modify.
            The first page has index `0`.
        :param prepend:
            Prepend the content stream to the list of content streams, as
            opposed to appending it to the end.
            This has the effect of causing the stream to be rendered
            underneath the already existing content on the page.
        :return:
            An :class:`~.generic.IndirectObject` reference to the page object
            that was modified.
        """
        as_stream = StreamObject({}, stream_data=self.render())
        return writer.add_stream_to_page(
            page_ix,
            writer.add_object(as_stream),
            resources=self.resources.as_pdf_object(),
            prepend=prepend,
        )


class RawContent(PdfContent):
    """Raw byte sequence to be used as PDF content."""

    def __init__(
        self,
        data: bytes,
        resources: Optional[PdfResources] = None,
        box: Optional[BoxConstraints] = None,
    ):
        super().__init__(resources, box)
        self.data = data

    def render(self) -> bytes:
        return self.data


class ImportedPdfPage(PdfContent):
    """Import a page from another PDF file (lazily)"""

    def __init__(self, file_name, page_ix=0):
        self.file_name = file_name
        self.page_ix = page_ix
        super().__init__()

    def render(self) -> bytes:
        from .writer import BasePdfFileWriter

        w: BasePdfFileWriter = self._ensure_writer
        with open(self.file_name, 'rb') as inf:
            r = PdfFileReader(inf)
            xobj = w.import_page_as_xobject(r, page_ix=self.page_ix)
        resource_name = b'/Import' + binascii.hexlify(uuid.uuid4().bytes)
        self.resources.xobject[resource_name.decode('ascii')] = xobj

        # make sure to take the bounding box (i.e. the page's MediaBox)
        # into account when doing layout computations
        x1, y1, x2, y2 = xobj.get_object()['/BBox']
        self.box = BoxConstraints(width=abs(x1 - x2), height=abs(y1 - y2))
        return resource_name + b' Do'
