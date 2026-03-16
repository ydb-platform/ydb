from typing import Iterable, Tuple

from pyhanko.pdf_utils.reader import HistoricalResolver, RawPdfPath

from ..commons import compare_dicts, compare_key_refs, qualify_transforming
from ..constants import ROOT_EXEMPT_STRICT_COMPARISON
from ..policy_api import ModificationLevel
from ..rules_api import (
    Context,
    QualifiedWhitelistRule,
    ReferenceUpdate,
    WhitelistRule,
)

__all__ = ['CatalogModificationRule', 'ObjectStreamRule', 'XrefStreamRule']


class CatalogModificationRule(QualifiedWhitelistRule):
    """
    Rule that adjudicates modifications to the document catalog.

    :param ignored_keys:
        Values in the document catalog that may change between revisions.
        The default ones are ``/AcroForm``, ``/DSS``, ``/Extensions``,
        ``/Metadata``, ``/MarkInfo`` and ``/Version``.

        Checking for ``/AcroForm``, ``/DSS`` and ``/Metadata`` is delegated to
        :class:`.FormUpdatingRule`, :class:`.DSSCompareRule` and
        :class:`.MetadataUpdateRule`, respectively.
    """

    def __init__(self, ignored_keys=None):
        self.ignored_keys = (
            ignored_keys
            if ignored_keys is not None
            else ROOT_EXEMPT_STRICT_COMPARISON
        )

    def apply_qualified(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[Tuple[ModificationLevel, ReferenceUpdate]]:
        old_root = old.root
        new_root = new.root
        # first, check if the keys in the document catalog are unchanged
        compare_dicts(old_root, new_root, self.ignored_keys)

        # As for the keys in the root dictionary that are allowed to change:
        #  - /Extensions requires no further processing since it must consist
        #    of direct objects anyway, BUT the /Extensions entry itself can
        #    be indirect
        #  - /MarkInfo: if it's an indirect reference (probably not) we can
        #    whitelist it (but do not process any further)
        #  - /DSS, /AcroForm and /Metadata are dealt with by other rules.
        for key in ('/Extensions', '/MarkInfo'):
            yield from qualify_transforming(
                ModificationLevel.LTA_UPDATES,
                compare_key_refs(key, old, old_root, new_root),
                transform=lambda ref: ReferenceUpdate(
                    ref,
                    context_checked=Context.from_absolute(
                        old, RawPdfPath('/Root', key)
                    ),
                ),
            )

        yield ModificationLevel.LTA_UPDATES, ReferenceUpdate(
            new.root_ref,
            # Things like /Data in a MDP policy can point to root
            # and since we checked with compare_dicts, doing a blanket
            # approval is much easier than figuring out all the ways
            # in which /Root can be cross-referenced.
            context_checked=None,
        )


class ObjectStreamRule(WhitelistRule):
    """
    Rule that allows object streams to be added.

    Note that this rule only whitelists the object streams themselves (provided
    they do not override any existing objects, obviously), not the objects
    in them.
    """

    def apply(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[ReferenceUpdate]:
        # object streams are OK, but overriding object streams is not.
        for objstream_ref in new.object_streams_used():
            if old.is_ref_available(objstream_ref):
                yield ReferenceUpdate(objstream_ref)


class XrefStreamRule(WhitelistRule):
    """
    Rule that allows new cross-reference streams to be defined.
    """

    def apply(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[ReferenceUpdate]:
        xrefs = new.reader.xrefs
        xref_meta = xrefs.get_xref_container_info(new.revision)
        xref_stm = xref_meta.stream_ref
        if xref_stm is not None and old.is_ref_available(xref_stm):
            yield ReferenceUpdate(xref_stm)

        # If this revision is followed by a hybrid one, then we must
        #  clear the ref to the hybrid stream as well. Let's take care of that.

        # Note: this check is only relevant in nonstrict mode because
        #  hybrid-reference docs are banned otherwise.
        if new.reader.strict:
            return

        try:
            next_rev_data = xrefs.get_xref_data(new.revision + 1)
        except IndexError:
            return

        if next_rev_data.hybrid is not None:
            hyb_xref_stm = next_rev_data.hybrid.meta_info.stream_ref
            if hyb_xref_stm and old.is_ref_available(hyb_xref_stm):
                yield ReferenceUpdate(hyb_xref_stm)
