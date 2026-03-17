from typing import Callable, Generator, Iterable, Tuple

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.generic import Reference
from pyhanko.pdf_utils.reader import HistoricalResolver, RawPdfPath

from ..commons import (
    assert_not_stream,
    compare_dicts,
    compare_key_refs,
    qualify,
    qualify_transforming,
    safe_whitelist,
)
from ..constants import (
    FORMFIELD_ALWAYS_MODIFIABLE,
    VALUE_UPDATE_KEYS,
    VRI_KEY_PATTERN,
)
from ..form_rules_api import (
    FieldComparisonContext,
    FieldComparisonSpec,
    FieldMDPRule,
    FormUpdate,
)
from ..policy_api import ModificationLevel, SuspiciousModification
from ..rules_api import Context, ReferenceUpdate, RelativeContext, WhitelistRule

__all__ = [
    'DSSCompareRule',
    'SigFieldCreationRule',
    'SigFieldModificationRule',
    'GenericFieldModificationRule',
    'BaseFieldModificationRule',
]


def _assert_stream_refs(der_obj_type, arr, err_cls, is_vri):
    arr = arr.get_object()
    all_stream_arr = isinstance(arr, generic.ArrayObject) and all(
        isinstance(obj, generic.IndirectObject)
        and isinstance(obj.get_object(), generic.StreamObject)
        for obj in arr
    )
    if not all_stream_arr:
        raise err_cls(
            f"Expected contents of '{der_obj_type}' in "
            f"{'VRI' if is_vri else 'DSS'} to be an array of stream references."
        )


def _validate_dss_substructure(
    old: HistoricalResolver,
    new: HistoricalResolver,
    old_dict,
    new_dict,
    der_stream_keys,
    is_vri,
    context: Context,
):
    for der_obj_type in der_stream_keys:
        as_update = ReferenceUpdate.curry_ref(
            context_checked=context.descend(der_obj_type)
        )
        try:
            value = new_dict.raw_get(der_obj_type)
        except KeyError:
            continue
        _assert_stream_refs(der_obj_type, value, SuspiciousModification, is_vri)
        if isinstance(value, generic.IndirectObject):
            new_ref = value.reference
            try:
                old_value = old_dict.raw_get(der_obj_type)
                if isinstance(old_value, generic.IndirectObject):
                    yield from map(
                        as_update,
                        safe_whitelist(old, old_value.reference, new_ref),
                    )
                _assert_stream_refs(
                    der_obj_type, old_value, misc.PdfReadError, is_vri
                )
                # We don't enforce the contents of the new array vs. the old one
                # deleting info is allowed by PAdES, and this check can get
                # pretty expensive.
            except KeyError:
                pass

        yield from map(
            as_update,
            new.collect_dependencies(value, since_revision=old.revision + 1),
        )


class DSSCompareRule(WhitelistRule):
    """
    Rule that allows changes to the document security store (DSS).

    This rule will validate the structure of the DSS quite rigidly, and
    will raise :class:`.SuspiciousModification` whenever it encounters
    structural problems with the DSS.
    Similarly, modifications that remove structural items from the DSS
    also count as suspicious. However, merely removing individual OCSP
    responses, CRLs or certificates when they become irrelevant is permitted.
    This is also allowed by PAdES.
    """

    def apply(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[ReferenceUpdate]:
        # TODO refactor these into less ad-hoc rules

        dss_context = Context.from_absolute(old, RawPdfPath('/Root', '/DSS'))
        old_dss, new_dss = yield from misc.map_with_return(
            compare_key_refs('/DSS', old, old.root, new.root),
            ReferenceUpdate.curry_ref(context_checked=dss_context),
        )
        if new_dss is None:
            return

        if old_dss is None:
            old_dss = generic.DictionaryObject()
        nodict_err = "/DSS is not a dictionary"
        if not isinstance(old_dss, generic.DictionaryObject):
            raise misc.PdfReadError(nodict_err)  # pragma: nocover
        if not isinstance(new_dss, generic.DictionaryObject):
            raise SuspiciousModification(nodict_err)

        dss_der_stream_keys = {'/Certs', '/CRLs', '/OCSPs'}
        dss_expected_keys = {'/Type', '/VRI'} | dss_der_stream_keys
        dss_keys = set(new_dss.keys())

        if not (dss_keys <= dss_expected_keys):
            raise SuspiciousModification(
                f"Unexpected keys in DSS: {dss_keys - dss_expected_keys}."
            )

        yield from _validate_dss_substructure(
            old,
            new,
            old_dss,
            new_dss,
            dss_der_stream_keys,
            is_vri=False,
            context=dss_context,
        )

        # check that the /VRI dictionary still contains all old keys, unchanged.
        old_vri, new_vri = yield from misc.map_with_return(
            compare_key_refs(
                '/VRI',
                old,
                old_dss,
                new_dss,
            ),
            ReferenceUpdate.curry_ref(
                context_checked=Context.from_absolute(
                    old, RawPdfPath('/Root', '/DSS', '/VRI')
                )
            ),
        )

        nodict_err = "/VRI is not a dictionary"
        if new_vri is not None:
            if not isinstance(new_vri, generic.DictionaryObject):
                raise SuspiciousModification(nodict_err)
            if old_vri is None:
                old_vri = generic.DictionaryObject()
            elif not isinstance(old_vri, generic.DictionaryObject):
                raise misc.PdfReadError(nodict_err)  # pragma: nocover
            yield from DSSCompareRule._check_vri(old, new, old_vri, new_vri)

        # The case where /VRI was deleted is checked by compare_key_refs

    @staticmethod
    def _check_vri(old, new, old_vri, new_vri):
        new_vri_hashes = set(new_vri.keys())
        for key, old_vri_value in old_vri.items():
            try:
                new_vri_dict = new_vri.raw_get(key)
            except KeyError:
                new_vri_dict = None

            if new_vri_dict != old_vri_value:
                # indirect or direct doesn't matter, they have to be the same
                raise SuspiciousModification(
                    f"VRI key {key} was modified or deleted."
                )

        # check the newly added entries
        vri_der_stream_keys = {'/Cert', '/CRL', '/OCSP'}
        vri_expected_keys = {'/Type', '/TU', '/TS'} | vri_der_stream_keys
        for key in new_vri_hashes - old_vri.keys():
            if not VRI_KEY_PATTERN.match(key):
                raise SuspiciousModification(
                    f"VRI key {key} is not formatted correctly."
                )

            new_vri_dict = new_vri.raw_get(key)
            if isinstance(
                new_vri_dict, generic.IndirectObject
            ) and old.is_ref_available(new_vri_dict.reference):
                yield ReferenceUpdate(new_vri_dict.reference)
                new_vri_dict = new_vri_dict.get_object()
            assert_not_stream(new_vri_dict)
            if not isinstance(new_vri_dict, generic.DictionaryObject):
                raise SuspiciousModification(
                    "VRI entries should be dictionaries"
                )

            new_vri_value_keys = new_vri_dict.keys()
            if not (new_vri_value_keys <= vri_expected_keys):
                raise SuspiciousModification(
                    "Unexpected keys in VRI dictionary: "
                    f"{new_vri_value_keys - vri_expected_keys}."
                )
            yield from _validate_dss_substructure(
                old,
                new,
                generic.DictionaryObject(),
                new_vri_dict,
                vri_der_stream_keys,
                is_vri=True,
                context=Context.from_absolute(
                    old, RawPdfPath('/Root', '/DSS', '/VRI', key)
                ),
            )

            # /TS is also a DER stream
            try:
                ts_ref = new_vri_dict.get_value_as_reference(
                    '/TS', optional=True
                )
                if ts_ref is not None and old.is_ref_available(ts_ref):
                    yield ReferenceUpdate(ts_ref)
            except misc.IndirectObjectExpected:
                pass


def is_annot_visible(annot_dict):
    try:
        x1, y1, x2, y2 = annot_dict['/Rect']
        area = abs(x1 - x2) * abs(y1 - y2)
    except (TypeError, ValueError, KeyError):
        area = 0

    return bool(area)


def is_field_visible(field_dict):
    if '/Kids' not in field_dict:
        return is_annot_visible(field_dict)
    else:
        return is_annot_visible(field_dict) or any(
            is_annot_visible(kid.get_object()) for kid in field_dict['/Kids']
        )


class SigFieldCreationRule(FieldMDPRule):
    """
    This rule allows signature fields to be created at the root of the form
    hierarchy, but disallows the creation of other types of fields.
    It also disallows field deletion.

    In addition, this rule will allow newly created signature fields to
    attach themselves as widget annotations to pages.

    The creation of invisible signature fields is considered a modification
    at level :attr:`.ModificationLevel.LTA_UPDATES`, but appearance-related
    changes will be qualified with :attr:`.ModificationLevel.FORM_FILLING`.

    :param allow_new_visible_after_certify:
        Creating new visible signature fields is disallowed after
        certification signatures by default; this is stricter than Acrobat.
        Set this parameter to ``True`` to disable this check.
    :param approve_widget_bindings:
        Set to ``False`` to reject new widget annotation registrations
        associated with approved new fields.
    """

    def __init__(
        self,
        approve_widget_bindings=True,
        allow_new_visible_after_certify=False,
    ):
        self.approve_widget_bindings = approve_widget_bindings
        self.allow_new_visible_after_certify = allow_new_visible_after_certify

    def apply(
        self, context: FieldComparisonContext
    ) -> Iterable[Tuple[ModificationLevel, FormUpdate]]:
        deleted = set(
            fq_name
            for fq_name, spec in context.field_specs.items()
            if spec.old_field_ref and not spec.new_field_ref
        )
        if deleted:
            raise SuspiciousModification(
                f"Fields {deleted} were deleted after signing."
            )

        def _collect():
            for fq_name, spec in context.field_specs.items():
                if spec.field_type != '/Sig' or spec.old_field_ref:
                    continue
                yield fq_name, spec.new_field_ref

        all_new_refs = dict(_collect())

        # The form MDP logic already vetted the /AcroForm dictionary itself
        # (including the /Fields ref), so our only responsibility is to match
        # up the names of new fields
        approved_new_fields = set(all_new_refs.keys())
        actual_new_fields = set(
            fq_name
            for fq_name, spec in context.field_specs.items()
            if spec.old_field_ref is None
        )

        if actual_new_fields != approved_new_fields:
            raise SuspiciousModification(
                "More form fields added than expected: expected "
                f"only {approved_new_fields}, but found new fields named "
                f"{actual_new_fields - approved_new_fields}."
            )

        # finally, deal with the signature fields themselves
        # The distinction between timestamps and signatures isn't relevant
        # yet, that's a problem for /V, which we don't bother with here.
        field_ref_reverse = {}

        for fq_name, sigfield_ref in all_new_refs.items():
            # New field, so all its dependencies are good to go
            # that said, only the field itself is cleared at LTA update level,
            # (and only if it is invisible)
            # the other deps bump the modification level up to FORM_FILL

            # Since LTA updates should arguably not trigger field locks either
            # (relevant for FieldMDP settings that use /All or /Exclude),
            # we pass valid_when_locked=True on these updates
            sigfield = sigfield_ref.get_object()
            visible = is_field_visible(sigfield)
            mod_level = (
                ModificationLevel.FORM_FILLING
                if visible
                else ModificationLevel.LTA_UPDATES
            )
            if context.old.is_ref_available(sigfield_ref):
                yield mod_level, FormUpdate(
                    updated_ref=sigfield_ref,
                    field_name=fq_name,
                    valid_when_locked=not visible,
                    valid_when_certifying=(
                        not visible or self.allow_new_visible_after_certify
                    ),
                )
            # checked by field listing routine already
            assert isinstance(sigfield, generic.DictionaryObject)

            def _handle_deps(pdf_dict, _key):
                try:
                    raw_value = pdf_dict.raw_get(_key)

                    deps = context.new.collect_dependencies(
                        raw_value, since_revision=context.old.revision + 1
                    )
                    yield from qualify_transforming(
                        ModificationLevel.FORM_FILLING,
                        misc._as_gen(deps),
                        transform=FormUpdate.curry_ref(field_name=fq_name),
                    )
                except KeyError:
                    pass

            for _key in ('/AP', '/Lock', '/SV'):
                yield from _handle_deps(sigfield, _key)

            # if the field has widget annotations in /Kids, add them to the
            #  field_ref_reverse dictionary for annotation processing later
            try:
                kids_arr_ref = sigfield.raw_get('/Kids')
                old = context.old
                if isinstance(
                    kids_arr_ref, generic.IndirectObject
                ) and old.is_ref_available(kids_arr_ref.reference):
                    yield mod_level, FormUpdate(
                        updated_ref=kids_arr_ref.reference,
                        field_name=fq_name,
                        valid_when_locked=not visible,
                    )
                kid_refs = _arr_to_refs(
                    kids_arr_ref.get_object(), SuspiciousModification
                )
                # process all widgets in /Kids
                # in principle there should be only one, but we don't enforce
                # that restriction here
                # TODO make that togglable?
                for kid in kid_refs:
                    if '/T' not in kid.get_object():
                        field_ref_reverse[kid] = fq_name
                        if old.is_ref_available(kid):
                            yield mod_level, FormUpdate(
                                updated_ref=kid,
                                field_name=fq_name,
                                valid_when_locked=not visible,
                            )
                        # pull in appearance dependencies
                        yield from _handle_deps(kid.get_object(), '/AP')
            except KeyError:
                # No /Kids => assume the field is its own annotation
                field_ref_reverse[sigfield_ref] = fq_name

        # Now we process (widget) annotations: newly added signature fields may
        #  be added to the /Annots entry of any page. These are processed as LTA
        #  updates, because even invisible signature fields / timestamps might
        #  be added to /Annots (this isn't strictly necessary, but more
        #  importantly it's not forbidden).
        # Note: we don't descend into the annotation dictionaries themselves.
        #  For modifications to form field values, this is the purview
        #  of the appearance checkers.
        # TODO allow other annotation modifications, but at level ANNOTATIONS
        # if no new sigfields were added, we skip this step.
        #  Any modifications to /Annots will be flagged by the xref
        #  crawler later.

        if not self.approve_widget_bindings or not all_new_refs:
            return

        # note: this is guaranteed to be equal to its signed counterpart,
        # since we already checked the document catalog for unauthorised
        # modifications
        old_page_root = context.old.root['/Pages']
        new_page_root = context.new.root['/Pages']

        yield from qualify(
            ModificationLevel.LTA_UPDATES,
            _walk_page_tree_annots(
                old_page_root,
                new_page_root,
                field_ref_reverse,
                context.old,
                valid_when_locked=True,
                refs_seen=set(),
            ),
        )


class BaseFieldModificationRule(FieldMDPRule):
    """
    Base class that implements some boilerplate to validate modifications
    to individual form fields.
    """

    def __init__(
        self,
        allow_in_place_appearance_stream_changes: bool = True,
        always_modifiable=None,
        value_update_keys=None,
    ):
        self.always_modifiable = (
            always_modifiable
            if always_modifiable is not None
            else FORMFIELD_ALWAYS_MODIFIABLE
        )
        self.value_update_keys = (
            value_update_keys
            if value_update_keys is not None
            else VALUE_UPDATE_KEYS
        )
        self.allow_in_place_appearance_stream_changes = (
            allow_in_place_appearance_stream_changes
        )

    def compare_fields(self, spec: FieldComparisonSpec) -> bool:
        """
        Helper method to compare field dictionaries.

        :param spec:
            The current :class:`.FieldComparisonSpec`.
        :return:
            ``True`` if the modifications are permissible even when the field is
            locked, ``False`` otherwise.
            If keys beyond those in :attr:`value_update_keys` are changed,
            a :class:`.SuspiciousModification` is raised.
        """

        # we compare twice: the first test ignores all value_update_keys,
        # and the second (stricter) test checks if the update would still
        # be OK on a locked field.
        old_field = spec.old_field
        new_field = spec.new_field
        compare_dicts(old_field, new_field, self.value_update_keys)
        assert old_field is not None and new_field is not None
        # Be strict about /Type since some processor's behaviour depends on it
        had_type = '/Type' in old_field
        has_type = '/Type' in new_field
        if has_type:
            type_val = new_field.raw_get('/Type')
            if not had_type and type_val != '/Annot':
                raise SuspiciousModification(
                    "/Type of form field set to something other than /Annot"
                )
            elif had_type and type_val != old_field.raw_get('/Type'):
                raise SuspiciousModification("/Type of form field altered")
        if had_type and not has_type:
            raise SuspiciousModification("/Type of form field deleted")
        return compare_dicts(
            old_field, new_field, self.always_modifiable, raise_exc=False
        )

    def apply(
        self, context: FieldComparisonContext
    ) -> Iterable[Tuple[ModificationLevel, FormUpdate]]:
        for fq_name, spec in context.field_specs.items():
            yield from self.check_form_field(fq_name, spec, context)

    def check_form_field(
        self,
        fq_name: str,
        spec: FieldComparisonSpec,
        context: FieldComparisonContext,
    ) -> Iterable[Tuple[ModificationLevel, FormUpdate]]:
        """
        Investigate updates to a particular form field.
        This function is called by :meth:`apply` for every form field in
        the new revision.

        :param fq_name:
            The fully qualified name of the form field.j
        :param spec:
            The :class:`.FieldComparisonSpec` object describing the old state
            of the field in relation to the new state.
        :param context:
            The full :class:`.FieldComparisonContext` that is currently
            being evaluated.
        :return:
            An iterable yielding :class:`.FormUpdate` objects qualified
            with an appropriate :class:`.ModificationLevel`.
        """
        raise NotImplementedError


class SigFieldModificationRule(BaseFieldModificationRule):
    """
    This rule allows signature fields to be filled in, and set an appearance
    if desired. Deleting values from signature fields is disallowed, as is
    modifying signature fields that already contain a signature.

    This rule will take field locks into account if the
    :class:`.FieldComparisonContext` includes a :class:`.FieldMDPSpec`.

    For (invisible) document timestamps, this is allowed at
    :class:`.ModificationLevel.LTA_UPDATES`, but in all other cases
    the modification level will be bumped to
    :class:`.ModificationLevel.FORM_FILLING`.
    """

    def check_form_field(
        self,
        fq_name: str,
        spec: FieldComparisonSpec,
        context: FieldComparisonContext,
    ) -> Iterable[Tuple[ModificationLevel, FormUpdate]]:
        # deal with "freshly signed" signature fields,
        # i.e. those that are filled now, but weren't previously
        #  + newly created ones
        if spec.field_type != '/Sig' or not spec.new_field_ref:
            return

        old_field = spec.old_field
        new_field = spec.new_field
        assert new_field

        previously_signed = old_field is not None and '/V' in old_field
        now_signed = '/V' in new_field

        if old_field:
            # operating on an existing field ---> check changes
            # (if the field we're dealing with is new, we don't need
            #  to bother, the sig field creation rule takes care of that)

            # here, we check that the form field didn't change
            # beyond the keys that we expect to change when updating,
            # and also register whether the changes made would be
            # permissible even when the field is locked.
            valid_when_locked = self.compare_fields(spec)

            field_ref_updates = (
                FormUpdate(
                    updated_ref=spec.new_field_ref,
                    field_name=fq_name,
                    valid_when_locked=valid_when_locked,
                    context_checked=expected,
                )
                for expected in spec.expected_contexts()
            )

            if not previously_signed and now_signed:
                yield from qualify(
                    ModificationLevel.LTA_UPDATES, field_ref_updates
                )

                # whitelist appearance updates at FORM_FILL level
                yield from qualify_transforming(
                    ModificationLevel.FORM_FILLING,
                    _allow_appearance_update(
                        old_field, new_field, context.old, context.new
                    ),
                    transform=FormUpdate.curry_ref(field_name=fq_name),
                )
                if self.allow_in_place_appearance_stream_changes:
                    yield from qualify(
                        ModificationLevel.FORM_FILLING,
                        _allow_in_place_appearance_update(
                            old_field,
                            new_field,
                            context.old,
                            context.new,
                            fq_name=fq_name,
                        ),
                    )
            else:
                # case where the field was already signed, or is still
                # not signed in the current revision.
                # in this case, the state of the field better didn't change
                # at all!
                # ... but Acrobat apparently sometimes sets /Ff rather
                #  liberally, so we have to make some allowances
                if valid_when_locked:
                    yield from qualify(
                        ModificationLevel.LTA_UPDATES, field_ref_updates
                    )
                # Skip the comparison logic on /V. In particular, if
                # the signature object in question was overridden,
                # it should trigger a suspicious modification later.
                return

        if not now_signed:
            return

        # We're now in the case where the form field did not exist or did
        # not have a value in the original revision, but does have one in
        # the revision we're auditing. If the signature is /DocTimeStamp,
        # this is a modification at level LTA_UPDATES. If it's a normal
        # signature, it requires FORM_FILLING.
        try:
            current_value_ref = new_field.get_value_as_reference('/V')
        except (misc.IndirectObjectExpected, KeyError):
            raise SuspiciousModification(
                f"Value of signature field {fq_name} should be an indirect "
                f"reference"
            )

        sig_obj = current_value_ref.get_object()
        if not isinstance(sig_obj, generic.DictionaryObject):
            raise SuspiciousModification(
                f"Value of signature field {fq_name} is not a dictionary"
            )

        visible = is_field_visible(new_field)

        # /DocTimeStamps added for LTA validation purposes shouldn't have
        # an appearance (as per the recommendation in ISO 32000-2, which we
        # enforce as a rigid rule here)
        if (
            '/Type' in sig_obj
            and sig_obj.raw_get('/Type') == '/DocTimeStamp'
            and not visible
        ):
            sig_whitelist = ModificationLevel.LTA_UPDATES
            valid_when_locked = True
        else:
            sig_whitelist = ModificationLevel.FORM_FILLING
            valid_when_locked = False

        # first, whitelist the actual signature object
        yield sig_whitelist, FormUpdate(
            updated_ref=current_value_ref,
            field_name=fq_name,
            valid_when_locked=valid_when_locked,
        )

        # since apparently Acrobat didn't get the memo about not having
        # indirect references in signature objects, we have to do some
        # tweaking to whitelist /TransformParams if necessary
        try:
            # the issue is with signature reference dictionaries
            for sigref_dict in sig_obj.raw_get('/Reference'):
                try:
                    tp = sigref_dict.raw_get('/TransformParams')
                    yield (
                        sig_whitelist,
                        FormUpdate(
                            updated_ref=tp.reference, field_name=fq_name
                        ),
                    )
                except (KeyError, AttributeError):
                    continue
        except KeyError:
            pass


class GenericFieldModificationRule(BaseFieldModificationRule):
    """
    This rule allows non-signature form fields to be modified at
    :class:`.ModificationLevel.FORM_FILLING`.

    This rule will take field locks into account if the
    :class:`.FieldComparisonContext` includes a :class:`.FieldMDPSpec`.
    """

    def check_form_field(
        self,
        fq_name: str,
        spec: FieldComparisonSpec,
        context: FieldComparisonContext,
    ) -> Iterable[Tuple[ModificationLevel, FormUpdate]]:
        if (
            spec.field_type == '/Sig'
            or not spec.new_field_ref
            or not spec.old_field_ref
        ):
            return

        valid_when_locked = self.compare_fields(spec)

        yield from qualify(
            ModificationLevel.FORM_FILLING,
            (
                FormUpdate(
                    updated_ref=spec.new_field_ref,
                    field_name=fq_name,
                    valid_when_locked=valid_when_locked,
                    context_checked=expected,
                )
                for expected in spec.expected_contexts()
            ),
        )
        old_field = spec.old_field
        new_field = spec.new_field
        # we already checked this further up
        assert old_field and new_field
        yield from qualify_transforming(
            ModificationLevel.FORM_FILLING,
            _allow_appearance_update(
                old_field, new_field, context.old, context.new
            ),
            transform=FormUpdate.curry_ref(field_name=fq_name),
        )
        if self.allow_in_place_appearance_stream_changes:
            yield from qualify(
                ModificationLevel.FORM_FILLING,
                _allow_in_place_appearance_update(
                    old_field,
                    new_field,
                    context.old,
                    context.new,
                    fq_name=fq_name,
                ),
            )
        try:
            new_value = new_field.raw_get('/V')
        except KeyError:
            # no current value => nothing else to check
            return
        try:
            old_value = old_field.raw_get('/V')
        except KeyError:
            old_value = None

        # if the value was changed, pull in newly defined objects.
        # TODO is this sufficient?
        if new_value != old_value:
            deps = context.new.collect_dependencies(
                new_value, since_revision=context.old.revision + 1
            )
            yield from qualify_transforming(
                ModificationLevel.FORM_FILLING,
                misc._as_gen(deps),
                transform=FormUpdate.curry_ref(field_name=fq_name),
            )


def _allow_appearance_update(
    old_field, new_field, old: HistoricalResolver, new: HistoricalResolver
) -> Generator[Reference, None, None]:
    old_ap_val, new_ap_val = yield from compare_key_refs(
        '/AP', old, old_field, new_field
    )

    if new_ap_val is None:
        return

    if not isinstance(new_ap_val, generic.DictionaryObject):
        raise SuspiciousModification('AP entry should point to a dictionary')

    # we generally *never* want to whitelist an update for an existing
    # stream object (too much potential for abuse), so we insist on
    # modifying the /N, /R, /D keys to point to new streams.
    # Some processors do perform such in-place upgrades, though, and we
    # optionally make allowances for that (outside the scope of this function)

    checked_ap_references = False
    for key in ('/N', '/R', '/D'):
        try:
            new_ap_stm_ref = new_ap_val.raw_get(key)
        except KeyError:
            continue

        yield from new.collect_dependencies(
            new_ap_stm_ref, since_revision=old.revision + 1
        )
        if isinstance(old_ap_val, generic.DictionaryObject):
            if not checked_ap_references:
                # We verify that the value of /AP, if indirect, is only
                # used once
                old_ap_raw = old_field.raw_get('/AP')
                if isinstance(old_ap_raw, generic.IndirectObject):
                    # reference count
                    contexts = {
                        Context.from_absolute(old, path).relative_view
                        for path in old._get_usages_of_ref(old_ap_raw.reference)
                    }
                    if len(contexts) > 1:
                        raise SuspiciousModification(
                            "Attempted to update an appearance "
                            "stream in an annotation appearance dictionary, "
                            "but that appearance dictionary is used in "
                            f"multiple contexts: {contexts}."
                        )
                checked_ap_references = True


def _allow_in_place_appearance_update(
    old_field,
    new_field,
    old: HistoricalResolver,
    new: HistoricalResolver,
    fq_name: str,
):
    # We can allow updating appearance streams in-place, but that requires
    # two conditions to be met
    #  - The stream is not referenced anywhere else (judging by relative
    #  contexts)
    #  - The /AP dictionary itself, if indirect, appears in exactly one
    #    relative context.
    #
    # The first check is implemented by yielding a relative context
    # as opposed to an absolute path.
    # The second case is checked in the other /AP update validation routine
    #
    # Overwriting appearance streams is inherently more risky, than other
    # operations, so this check can be disabled.

    try:
        old_ap_raw = old_field.raw_get('/AP')
        new_ap_raw = new_field.raw_get('/AP')
    except KeyError:
        return

    old_ap_val = old_ap_raw.get_object()

    # For new_ap_val, we checked this in the "regular" validation routine
    if not isinstance(old_ap_val, generic.DictionaryObject):
        return

    new_ap_val = new_ap_raw.get_object()
    for key in ('/N', '/R', '/D'):
        xrefs = old.reader.xrefs
        old_rev = old.revision
        if isinstance(old_ap_val, generic.DictionaryObject):
            old_ap_stm_ref = old_ap_val.get_value_as_reference(
                key, optional=True
            )
            new_ap_stm_ref = new_ap_val.get_value_as_reference(
                key, optional=True
            )
            # if the refs are distinct or the old ref hasn't changed
            # don't bother
            if (
                new_ap_stm_ref != old_ap_stm_ref
                or old_ap_stm_ref is None
                or xrefs.get_last_change(old_ap_stm_ref) == old_rev
            ):
                continue
            if isinstance(old_ap_raw, generic.IndirectObject):
                context = RelativeContext(
                    old_ap_raw, relative_path=RawPdfPath(key)
                )
            else:
                context = RelativeContext(
                    old_field.container_ref,
                    relative_path=RawPdfPath('/AP', key),
                )
            yield FormUpdate(
                new_ap_stm_ref, context_checked=context, field_name=fq_name
            )

            # pull in the newly added objects beyond the update boundary
            new_deps = new.collect_dependencies(
                new_ap_stm_ref.get_object(), since_revision=old.revision + 1
            )
            for ref in new_deps:
                yield FormUpdate(ref, field_name=fq_name)


def _arr_to_refs(arr_obj, exc, collector: Callable = list):
    arr_obj = arr_obj.get_object()
    if not isinstance(arr_obj, generic.ArrayObject):
        raise exc("Not an array object")

    def _convert():
        for indir in arr_obj:
            if not isinstance(indir, generic.IndirectObject):
                raise exc("Array contains direct objects")
            yield indir.reference

    return collector(_convert())


def _extract_annots_from_page(page, exc):
    if not isinstance(page, generic.DictionaryObject):
        raise exc("Page objects should be dictionaries")
    try:
        annots_value = page.raw_get('/Annots')
        annots_ref = (
            annots_value.reference
            if isinstance(annots_value, generic.IndirectObject)
            else None
        )
        annots = _arr_to_refs(
            annots_value, SuspiciousModification, collector=set
        )
        return annots, annots_ref
    except KeyError:
        raise


def _walk_page_tree_annots(
    old_page_root,
    new_page_root,
    field_name_dict,
    old: HistoricalResolver,
    valid_when_locked,
    refs_seen,
):
    def get_kids(page_root, exc):
        try:
            return _arr_to_refs(page_root['/Kids'], exc)
        except KeyError:
            raise exc("No /Kids in /Pages entry")

    old_kids = get_kids(old_page_root, misc.PdfReadError)
    new_kids = get_kids(new_page_root, SuspiciousModification)

    # /Kids should only contain indirect refs, so direct comparison is
    # appropriate (__eq__ ignores the attached PDF handler)
    if old_kids != new_kids:
        raise SuspiciousModification(
            "Unexpected change to page tree structure."
        )
    for new_kid_ref, old_kid_ref in zip(new_kids, old_kids):
        if old_kid_ref in refs_seen:
            raise misc.PdfReadError(
                "Circular reference in page tree during annotation analysis"
            )
        new_kid = new_kid_ref.get_object()
        old_kid = old_kid_ref.get_object()
        try:
            node_type = old_kid['/Type']
        except (KeyError, TypeError) as e:  # pragma: nocover
            raise misc.PdfReadError("Invalid page tree node") from e
        if node_type == '/Pages':
            yield from _walk_page_tree_annots(
                old_kid,
                new_kid,
                field_name_dict,
                old,
                valid_when_locked,
                refs_seen | {old_kid_ref},
            )
        elif node_type == '/Page':
            try:
                new_annots, new_annots_ref = _extract_annots_from_page(
                    new_kid, SuspiciousModification
                )
            except KeyError:
                # no annotations, continue
                continue
            try:
                old_annots, old_annots_ref = _extract_annots_from_page(
                    old_kid, misc.PdfReadError
                )
            except KeyError:
                old_annots_ref = None
                old_annots = set()

            # check if annotations were added
            if old_annots == new_annots:
                continue
            deleted_annots = old_annots - new_annots
            added_annots = new_annots - old_annots
            if deleted_annots:
                raise SuspiciousModification(
                    f"Annotations {deleted_annots} were deleted."
                )

            # look up the names of the associated form field(s)
            # if any of the refs are not in the list
            # -> unrelated annotation -> bail
            unknown_annots = added_annots - field_name_dict.keys()
            if unknown_annots:
                raise SuspiciousModification(
                    f"The newly added annotations {unknown_annots} were not "
                    "recognised."
                )

            # there are new annotations, and they're all changes we expect
            # => cleared to edit

            # if there's only one new annotation, we can set the field name
            # on the resulting FormUpdate object, but otherwise there's
            # not much we can do.
            field_name = None
            if len(added_annots) == 1:
                (uniq_annot_ref,) = added_annots
                field_name = field_name_dict[uniq_annot_ref]

            # Make sure the page dictionaries are the same, so that we
            #  can safely clear them for modification across ALL paths
            #  (not necessary if both /Annots entries are indirect references,
            #   but adding even more cases is pushing things)
            compare_dicts(old_kid, new_kid, frozenset(['/Annots']))
            # Page objects are often referenced from all sorts of places in the
            # file, and attempting to check all possible paths would probably
            # create more problems than it solves -> blanket approve
            yield FormUpdate(
                updated_ref=new_kid_ref,
                field_name=field_name,
                valid_when_locked=valid_when_locked and field_name is not None,
                context_checked=None,
            )
            if new_annots_ref:
                # current /Annots entry is an indirect reference

                # If the equality check fails,
                # either the /Annots array got reassigned to another
                # object ID, or it was moved from a direct object to an
                # indirect one, or the /Annots entry was newly created.
                # This is all fine, provided that the new  object
                # ID doesn't clobber an existing one.
                if old_annots_ref == new_annots_ref or old.is_ref_available(
                    new_annots_ref
                ):
                    yield FormUpdate(
                        updated_ref=new_annots_ref,
                        field_name=field_name,
                        valid_when_locked=(
                            valid_when_locked and field_name is not None
                        ),
                        context_checked=RelativeContext(
                            old_kid_ref, RawPdfPath('/Annots')
                        ),
                    )
