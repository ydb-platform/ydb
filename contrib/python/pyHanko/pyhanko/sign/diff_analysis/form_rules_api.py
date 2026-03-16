"""
Module defining API types for use by form analysis rules.

In principle, these aren't relevant to the high-level validation API.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.reader import HistoricalResolver, RawPdfPath

from .commons import compare_dicts, compare_key_refs, qualify_transforming
from .constants import ACROFORM_EXEMPT_STRICT_COMPARISON
from .policy_api import ModificationLevel, SuspiciousModification
from .rules_api import Context, ReferenceUpdate

logger = logging.getLogger(__name__)


__all__ = [
    'FormUpdatingRule',
    'FormUpdate',
    'FieldMDPRule',
    'FieldComparisonSpec',
    'FieldComparisonContext',
]


@dataclass(frozen=True)
class FormUpdate(ReferenceUpdate):
    """
    Container for a reference together with (optional) metadata.

    Currently, this metadata consists of the relevant field's (fully qualified)
    name, and whether the update should be approved or not if said field
    is locked by the FieldMDP policy currently in force.
    """

    field_name: Optional[str] = None
    """
    The relevant field's fully qualified name, or ``None`` if there's either
    no obvious associated field, or if there are multiple reasonable candidates.
    """

    valid_when_locked: bool = False
    """
    Flag indicating whether the update is valid even when the field is locked.
    This is only relevant if :attr:`field_name` is not ``None``.
    """

    valid_when_certifying: bool = True
    """
    Flag indicating whether the update is valid when checking against an
    explicit DocMDP policy. Default is ``True``.
    If ``False``, the change will only be accepted if we are evaluating changes
    to a document after an approval signature.
    """


@dataclass(frozen=True)
class FieldComparisonSpec:
    """
    Helper object that specifies a form field name together with references
    to its old and new versions.
    """

    field_type: str
    """
    The (fully qualified) form field name.
    """

    old_field_ref: Optional[generic.Reference]
    """
    A reference to the field's dictionary in the old revision, if present.
    """

    new_field_ref: Optional[generic.Reference]
    """
    A reference to the field's dictionary in the new revision, if present.
    """

    old_canonical_path: Optional[RawPdfPath]
    """
    Path from the trailer through the AcroForm structure to this field (in the
    older revision). If the field is new, set to ``None``.
    """

    @property
    def old_field(self) -> Optional[generic.DictionaryObject]:
        """
        :return:
            The field's dictionary in the old revision, if present, otherwise
            ``None``.
        """
        ref = self.old_field_ref
        if ref is None:
            return None
        field = ref.get_object()
        assert isinstance(field, generic.DictionaryObject)
        return field

    @property
    def new_field(self) -> Optional[generic.DictionaryObject]:
        """
        :return:
            The field's dictionary in the new revision, if present, otherwise
            ``None``.
        """
        ref = self.new_field_ref
        if ref is None:
            return None
        field = ref.get_object()
        assert isinstance(field, generic.DictionaryObject)
        return field

    def expected_contexts(self) -> Set[Context]:
        old_field_ref = self.old_field_ref
        if old_field_ref is None:
            return set()
        # these are the paths where we expect the form field to be referred to
        paths = self._old_annotation_paths()
        contexts: Set[Context] = set(
            Context.from_absolute(old_field_ref.get_pdf_handler(), path)
            for path in paths
        )
        struct_context = self._find_in_structure_tree()
        if struct_context is not None:
            contexts.add(struct_context)
        if self.old_canonical_path:
            contexts.add(
                Context.from_absolute(
                    old_field_ref.get_pdf_handler(),
                    self.old_canonical_path,
                )
            )
        return contexts

    def _find_in_structure_tree(self) -> Optional[Context]:
        # collect paths (0 or 1) through which this field appears
        #  in the file's structure tree.

        # TODO check whether the structure element is a form control
        #  (or something role-mapped to it)
        # TODO if multiple paths exist, we should only whitelist the one
        #  that corresponds to the StructParent entry, not just the first one
        # Alternatively, we could also return a relative context and verify
        # that the reference count of the intermediates in the structure tree
        # is 1?

        # Note: the path simplifier suppresses the extra cross-references
        # from parent pointers in the tree and from the /ParentTree index.

        old_field_ref = self.old_field_ref
        assert old_field_ref is not None
        old = old_field_ref.get_pdf_handler()
        assert isinstance(old, HistoricalResolver)

        if '/StructTreeRoot' not in old.root:
            return None

        # check if the path ends in Form.K.Obj and
        # starts with Root.StructTreeRoot.K
        for pdf_path in old._get_usages_of_ref(old_field_ref):
            # Root.StructTreeRoot.K is three, and K.Obj at the end is
            # another 2
            if '/StructTreeRoot' in pdf_path and len(pdf_path) >= 5:
                root, struct_tree_root, k1 = pdf_path.path[:3]
                k2, obj = pdf_path.path[-2:]
                if (
                    k1 == k2 == '/K'
                    and obj == '/Obj'
                    and root == '/Root'
                    and struct_tree_root == '/StructTreeRoot'
                ):
                    return Context.from_absolute(old, pdf_path)
        return None

    # FIXME this is wrong now
    def _old_annotation_paths(self):
        # collect path(s) through which this field is used as an annotation
        # the clean way to accomplish this would be to follow /P
        # and go from there, but /P is optional, so we have to get a little
        # creative.
        old_field_ref = self.old_field_ref
        if old_field_ref is None:
            return set()  # pragma: nocover

        old = self.old_field_ref.get_pdf_handler()
        assert isinstance(old, HistoricalResolver)

        all_paths = old._get_usages_of_ref(old_field_ref)

        def _path_ok(pdf_path: RawPdfPath):
            # check if the path looks like a path to an annotation on a page

            # .Root.Pages.Kids[0].Annots[0] is the shortest you can get,
            # so 6 nodes is the minimum
            if len(pdf_path) < 6:
                return False
            fst, snd, *rest = pdf_path.path
            if fst != '/Root' or snd != '/Pages':
                return False

            # there should be one or more elements of the form /Kids[i] now
            descended = False
            nxt, nxt_ix, *rest = rest
            while nxt == '/Kids' and isinstance(nxt_ix, int):
                descended = True
                nxt, nxt_ix, *rest = rest

            # rest should be nothing and nxt should be /Annots
            return (
                descended
                and not rest
                and nxt == '/Annots'
                and isinstance(nxt_ix, int)
            )

        return {p for p in all_paths if _path_ok(p)}


@dataclass(frozen=True)
class FieldComparisonContext:
    """
    Context for a form diffing operation.
    """

    field_specs: Dict[str, FieldComparisonSpec]
    """
    Dictionary mapping field names to :class:`.FieldComparisonSpec` objects.
    """

    old: HistoricalResolver
    """
    The older, base revision.
    """

    new: HistoricalResolver
    """
    The newer revision.
    """


class FieldMDPRule:
    """
    Sub-rules attached to a :class:`.FormUpdatingRule`.
    """

    def apply(
        self, context: FieldComparisonContext
    ) -> Iterable[Tuple[ModificationLevel, FormUpdate]]:
        """
        Apply the rule to the given :class:`.FieldComparisonContext`.

        :param context:
            The context of this form revision evaluation, given as an instance
            of :class:`.FieldComparisonContext`.
        """
        raise NotImplementedError


def _list_fields(
    old_fields: Optional[generic.PdfObject],
    new_fields: Optional[generic.PdfObject],
    old_path: RawPdfPath,
    parent_name="",
    inherited_ft=None,
) -> Generator[Tuple[str, FieldComparisonSpec], None, None]:
    """
    Recursively construct a list of field names, together with their
    "incarnations" in either revision.
    """

    def _make_list(lst: Optional[generic.PdfObject], exc):
        if not isinstance(lst, generic.ArrayObject):
            raise exc("Field list is not an array.")
        names_seen = set()

        for ix, field_ref in enumerate(lst):
            if not isinstance(field_ref, generic.IndirectObject):
                raise exc("Fields must be indirect objects")

            field = field_ref.get_object()
            if not isinstance(field, generic.DictionaryObject):
                raise exc("Fields must be dictionary objects")

            try:
                name = field.raw_get('/T')
            except KeyError:
                continue
            if not isinstance(
                name, (generic.TextStringObject, generic.ByteStringObject)
            ):
                raise exc("Names must be strings")
            if name in names_seen:
                raise exc("Duplicate field name")
            elif '.' in name:
                raise exc("Partial names must not contain periods")
            names_seen.add(name)

            fq_name = parent_name + "." + name if parent_name else name
            try:
                field_type = field.raw_get('/FT')
            except KeyError:
                field_type = inherited_ft

            try:
                kids = field["/Kids"]
            except KeyError:
                kids = generic.ArrayObject()

            if not kids and field_type is None:
                raise exc(
                    f"Field type of terminal field {fq_name} could not be "
                    f"determined"
                )
            yield fq_name, (field_type, field_ref.reference, kids, ix)

    old_fields_by_name = dict(_make_list(old_fields, misc.PdfReadError))
    new_fields_by_name = dict(_make_list(new_fields, SuspiciousModification))

    names: Set[str] = set()
    names.update(old_fields_by_name.keys())
    names.update(new_fields_by_name.keys())

    for field_name in names:
        try:
            (
                old_field_type,
                old_field_ref,
                old_kids,
                field_index,
            ) = old_fields_by_name[field_name]
        except KeyError:
            old_field_type = old_field_ref = None
            old_kids = generic.ArrayObject()
            field_index = None

        try:
            new_field_type, new_field_ref, new_kids, _ = new_fields_by_name[
                field_name
            ]
        except KeyError:
            new_field_type = new_field_ref = None
            new_kids = generic.ArrayObject()

        if old_field_ref and new_field_ref:
            if new_field_type != old_field_type:
                raise SuspiciousModification(
                    f"Update changed field type of {field_name}"
                )
        common_ft = old_field_type or new_field_type
        if field_index is not None and old_path is not None:
            field_path = old_path + field_index
        else:
            field_path = None
        yield field_name, FieldComparisonSpec(
            field_type=common_ft,
            old_field_ref=old_field_ref,
            new_field_ref=new_field_ref,
            old_canonical_path=field_path,
        )

        # recursively descend into /Kids if necessary
        if old_kids or new_kids:
            yield from _list_fields(
                old_kids,
                new_kids,
                parent_name=field_name,
                old_path=(
                    field_path + '/Kids' if field_path is not None else None
                ),
                inherited_ft=common_ft,
            )


class FormUpdatingRule:
    """
    Special whitelisting rule that validates changes to the form attached to
    the input document.

    This rule is special in two ways:

    * it outputs :class:`.FormUpdate` objects instead of references;
    * it delegates most of the hard work to sub-rules (instances of
      :class:`.FieldMDPRule`).

    A :class:`.DiffPolicy` can have at most one :class:`.FormUpdatingRule`,
    but there is no limit on the number of :class:`.FieldMDPRule` objects
    attached to it.

    :class:`.FormUpdate` objects contain a reference plus metadata about
    the form field it belongs to.

    :param field_rules:
        A list of :class:`.FieldMDPRule` objects to validate the individual
        form fields.
    :param ignored_acroform_keys:
        Keys in the ``/AcroForm`` dictionary that may be changed.
        Changes are potentially subject to validation by other rules.
    """

    def __init__(
        self, field_rules: List[FieldMDPRule], ignored_acroform_keys=None
    ):
        self.field_rules = field_rules
        self.ignored_acroform_keys = (
            ignored_acroform_keys
            if ignored_acroform_keys is not None
            else ACROFORM_EXEMPT_STRICT_COMPARISON
        )

    def apply(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[Tuple[ModificationLevel, FormUpdate]]:
        """
        Evaluate changes in the document's form between two revisions.

        :param old:
            The older, base revision.
        :param new:
            The newer revision to be vetted.
        """

        acroform_context = Context.from_absolute(
            old, RawPdfPath('/Root', '/AcroForm')
        )
        old_acroform, new_acroform = yield from qualify_transforming(
            ModificationLevel.LTA_UPDATES,
            compare_key_refs('/AcroForm', old, old.root, new.root),
            transform=FormUpdate.curry_ref(
                field_name=None, context_checked=acroform_context
            ),
        )

        # first, compare the entries that aren't /Fields
        compare_dicts(old_acroform, new_acroform, self.ignored_acroform_keys)
        assert isinstance(old_acroform, generic.DictionaryObject)
        assert isinstance(new_acroform, generic.DictionaryObject)

        # mark /Fields ref as OK if it's an indirect reference
        # This is fine: the _list_fields logic checks that it really contains
        # stuff that looks like form fields, and other rules are responsible
        # for vetting the creation of other form fields anyway.
        fields_path = RawPdfPath('/Root', '/AcroForm', '/Fields')
        fields_context = Context.from_absolute(old, fields_path)
        old_fields, new_fields = yield from qualify_transforming(
            ModificationLevel.LTA_UPDATES,
            compare_key_refs('/Fields', old, old_acroform, new_acroform),
            transform=FormUpdate.curry_ref(
                field_name=None, context_checked=fields_context
            ),
        )

        # we also need to deal with the default resource dict, since
        # Acrobat / Adobe Reader sometimes mess with it
        dr_context = Context.from_absolute(
            old, RawPdfPath('/Root', '/AcroForm', '/DR')
        )
        old_dr, new_dr = yield from qualify_transforming(
            ModificationLevel.FORM_FILLING,
            compare_key_refs('/DR', old, old_acroform, new_acroform),
            transform=FormUpdate.curry_ref(
                field_name=None, context_checked=dr_context
            ),
        )
        if new_dr is not None:
            dr_deps = new.collect_dependencies(
                new_dr, since_revision=old.revision + 1
            )
            yield from qualify_transforming(
                ModificationLevel.FORM_FILLING,
                misc._as_gen(dr_deps),
                transform=FormUpdate.curry_ref(field_name=None),
            )

        context = FieldComparisonContext(
            field_specs=dict(
                _list_fields(old_fields, new_fields, old_path=fields_path)
            ),
            old=old,
            new=new,
        )

        for rule in self.field_rules:
            yield from rule.apply(context)
