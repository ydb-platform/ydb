import enum
from dataclasses import dataclass
from typing import Iterable, Optional

from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.generic import pdf_name

__all__ = ['DevExtensionMultivalued', 'DeveloperExtension']


class DevExtensionMultivalued(enum.Enum):
    """
    Setting indicating how an extension is expected to behave well w.r.t.
    the new mechanism for multivalued extensions in ISO 32000-2:2020.
    """

    ALWAYS = enum.auto()
    """
    Always serialise this extension as a multivalued extension.
    """

    NEVER = enum.auto()
    """
    Never serialise this extension as a multivalued extension.
    """

    MAYBE = enum.auto()
    """
    Make this extension single-valued whenever possible, but allow multiple
    values as well, e.g. when a different but non-comparable extension with
    the same prefix is already present in the file.
    """


@dataclass(frozen=True)
class DeveloperExtension:
    """
    PDF developer extension designation.
    """

    prefix_name: generic.NameObject
    """
    Registered developer prefix.
    """

    base_version: generic.NameObject
    """
    Base version on to which the extension applies.
    """

    extension_level: int
    """
    Extension level.
    """

    url: Optional[str] = None
    """
    Optional URL linking to the extension's documentation.
    """

    extension_revision: Optional[str] = None
    """
    Optional extra revision information. Not comparable.
    """

    compare_by_level: bool = False
    """
    Compare developer extensions by level number.
    If this value is ``True`` and a copy of this extension already exists in the
    target file with a higher level number, do not override it.
    If one exists with a lower level number, override it.

    If this value is ``False``, the decision is based on :attr:`subsumed_by`
    and :attr:`subsumes`.

    .. warning::
        It is generally not safe to assume that extension levels are used as a
        versioning system (i.e. that higher extension levels supersede lower
        ones), hence why the default is ``False``.
    """

    subsumed_by: Iterable[int] = ()
    """
    List of extension levels that would subsume this one. If one of these is
    present in the extensions dictionary, attempting to register this extension
    will not override it.

    Default value: empty.

    .. warning::
        This parameter is ignored if :attr:`compare_by_level` is ``True``.
    """

    subsumes: Iterable[int] = ()
    """
    List of extensions explicitly subsumed by this one. If one of these is
    present in the extensions dictionary, attempting to register this extension
    will override it.

    Default value: empty.

    .. warning::
        This parameter is ignored if :attr:`compare_by_level` is ``True``.
    """

    multivalued: DevExtensionMultivalued = DevExtensionMultivalued.MAYBE
    """
    Setting indicating whether this extension is expected to behave well w.r.t.
    the new mechanism for multivalued extensions in ISO 32000-2:2020.
    """

    def as_pdf_object(self) -> generic.DictionaryObject:
        """
        Format the data in this object into a PDF dictionary for registration
        into the `/Extensions` dictionary.

        :return:
            A :class:`.generic.DictionaryObject`.
        """

        result = generic.DictionaryObject(
            {
                pdf_name('/Type'): pdf_name('/DeveloperExtensions'),
                pdf_name('/BaseVersion'): self.base_version,
                pdf_name('/ExtensionLevel'): generic.NumberObject(
                    self.extension_level
                ),
            }
        )
        if self.url is not None:
            result['/URL'] = generic.TextStringObject(self.url)
        if self.extension_revision is not None:
            result['/ExtensionRevision'] = generic.TextStringObject(
                self.extension_revision
            )
        return result
