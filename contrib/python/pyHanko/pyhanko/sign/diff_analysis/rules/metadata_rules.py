from io import BytesIO
from typing import Iterable

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.reader import HistoricalResolver, RawPdfPath

from ..commons import safe_whitelist
from ..policy_api import SuspiciousModification
from ..rules_api import Context, ReferenceUpdate, WhitelistRule

__all__ = ['DocInfoRule', 'MetadataUpdateRule']


class DocInfoRule(WhitelistRule):
    """
    Rule that allows the ``/Info`` dictionary in the trailer to be updated.
    """

    def apply(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[ReferenceUpdate]:
        # updates to /Info are always OK (and must be through indirect objects)
        # Removing the /Info dictionary is no big deal, since most readers
        # will fall back to older revisions regardless
        new_info = new.trailer_view.get_value_as_reference(
            '/Info', optional=True
        )
        if new_info is None:
            return
        old_info = old.trailer_view.get_value_as_reference(
            '/Info', optional=True
        )
        yield from map(
            ReferenceUpdate.curry_ref(
                context_checked=Context.from_absolute(old, RawPdfPath('/Info'))
            ),
            safe_whitelist(old, old_info, new_info),
        )


class MetadataUpdateRule(WhitelistRule):
    """
    Rule to adjudicate updates to the XMP metadata stream.

    The content of the metadata isn't actually validated in any significant way;
    this class only checks whether the XML is well-formed.

    :param check_xml_syntax:
        Do a well-formedness check on the XML syntax. Default ``True``.
    :param always_refuse_stream_override:
        Always refuse to override the metadata stream if its object ID existed
        in a prior revision, including if the new stream overrides the old
        metadata stream and the syntax check passes. Default ``False``.

        .. note::
            In other situations, pyHanko will reject stream overrides on
            general principle, since combined with the fault-tolerance of some
            PDF readers, these can allow an attacker to manipulate parts of the
            signed content in subtle but significant ways.

            In case of the metadata stream, the risk is significantly mitigated
            thanks to the XML syntax check on both versions of the stream,
            but if you're feeling extra paranoid, you can turn the default
            behaviour back on by setting ``always_refuse_stream_override``
            to ``True``.
    """

    def __init__(
        self, check_xml_syntax=True, always_refuse_stream_override=False
    ):
        self.check_xml_syntax = check_xml_syntax
        self.always_refuse_stream_override = always_refuse_stream_override

    @staticmethod
    def is_well_formed_xml(metadata_ref: generic.Reference):
        """
        Checks whether the provided stream consists of well-formed XML data.
        Note that this does not perform any more advanced XML or XMP validation,
        the check is purely syntactic.

        :param metadata_ref:
            A reference to a (purported) metadata stream.
        :raises SuspiciousModification:
            if there are indications that the reference doesn't point to an XML
            stream.
        """
        metadata_stream = metadata_ref.get_object()

        if not isinstance(metadata_stream, generic.StreamObject):
            raise SuspiciousModification(
                "/Metadata should be a reference to a stream object"
            )

        from xml.sax import make_parser
        from xml.sax.handler import ContentHandler

        parser = make_parser()
        parser.setContentHandler(ContentHandler())
        try:
            parser.parse(BytesIO(metadata_stream.data))
        except Exception as e:
            raise SuspiciousModification(
                "/Metadata XML syntax could not be validated", e
            )

    def apply(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[ReferenceUpdate]:
        # /Metadata points to a stream, so we have to be careful allowing
        # object overrides!
        # we only approve the change if the metadata consists of well-formed xml
        # (note: this doesn't validate any XML schemata)

        def grab_metadata(root):
            try:
                return root.get_value_as_reference('/Metadata')
            except misc.IndirectObjectExpected:
                raise SuspiciousModification(
                    "/Metadata should be an indirect reference"
                )
            except KeyError:
                return

        new_metadata_ref = grab_metadata(new.root)
        if new_metadata_ref is None:
            return  # nothing to do

        if self.check_xml_syntax:
            MetadataUpdateRule.is_well_formed_xml(new_metadata_ref)

        old_metadata_ref = grab_metadata(old.root)

        if self.check_xml_syntax and old_metadata_ref is not None:
            MetadataUpdateRule.is_well_formed_xml(old_metadata_ref)

        same_ref_ok = (
            old_metadata_ref == new_metadata_ref
            and not self.always_refuse_stream_override
        )
        if same_ref_ok or old.is_ref_available(new_metadata_ref):
            yield ReferenceUpdate(
                new_metadata_ref,
                context_checked=Context.from_absolute(
                    old, RawPdfPath('/Root', '/Metadata')
                ),
            )
