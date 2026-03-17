"""
This module describes and implements the low-level :class:`.PdfCMSEmbedder`
protocol for embedding CMS payloads into PDF signature objects.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import IO, Optional

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.generic import pdf_name
from pyhanko.pdf_utils.layout import BoxConstraints
from pyhanko.pdf_utils.writer import BasePdfFileWriter
from pyhanko.sign.fields import (
    FieldMDPSpec,
    MDPPerm,
    SigFieldSpec,
    annot_width_height,
    apply_sig_field_spec_properties,
    ensure_sig_flags,
    enumerate_sig_fields,
    get_sig_field_annot,
    prepare_sig_field,
)
from pyhanko.sign.general import SigningError
from pyhanko.stamp import BaseStampStyle, TextStampStyle

from .pdf_byterange import PdfSignedData

__all__ = [
    'PdfCMSEmbedder',
    'SigMDPSetup',
    'SigObjSetup',
    'SigAppearanceSetup',
    'SigIOSetup',
]


def docmdp_reference_dictionary(permission_level: MDPPerm):
    # this is part of the /Reference entry of the signature object.
    return generic.DictionaryObject(
        {
            pdf_name('/Type'): pdf_name('/SigRef'),
            pdf_name('/TransformMethod'): pdf_name('/DocMDP'),
            pdf_name('/TransformParams'): generic.DictionaryObject(
                {
                    pdf_name('/Type'): pdf_name('/TransformParams'),
                    pdf_name('/V'): pdf_name('/1.2'),
                    pdf_name('/P'): generic.NumberObject(
                        permission_level.value
                    ),
                }
            ),
        }
    )


def fieldmdp_reference_dictionary(
    field_mdp_spec: FieldMDPSpec, data_ref: generic.Reference
):
    data_ind_obj = generic.IndirectObject(
        data_ref.idnum, data_ref.generation, data_ref.pdf
    )
    # this is part of the /Reference entry of the signature object.
    return generic.DictionaryObject(
        {
            pdf_name('/Type'): pdf_name('/SigRef'),
            pdf_name('/TransformMethod'): pdf_name('/FieldMDP'),
            pdf_name('/Data'): data_ind_obj,
            pdf_name('/TransformParams'): field_mdp_spec.as_transform_params(),
        }
    )


# Wrapper around prepare_sig_field with some error reporting


def _get_or_create_sigfield(
    field_name,
    pdf_out: BasePdfFileWriter,
    existing_fields_only,
    new_field_spec: Optional[SigFieldSpec] = None,
):
    root = pdf_out.root
    if field_name is None:
        if not existing_fields_only:
            raise SigningError(
                'Not specifying a field name is only allowed '
                'when existing_fields_only=True'
            )

        # most of the logic in prepare_sig_field has to do with preparing
        # for the potential addition of a new field. That is completely
        # irrelevant in this special case, so we might as well short circuit
        # things.
        field_created = False
        empty_fields = enumerate_sig_fields(pdf_out, filled_status=False)
        try:
            found_field_name, _, sig_field_ref = next(empty_fields)
        except StopIteration:
            raise SigningError('There are no empty signature fields.')

        others = ', '.join(fn for fn, _, _ in empty_fields if fn is not None)
        if others:
            raise SigningError(
                'There are several empty signature fields. Please specify '
                'a field name. The options are %s, %s.'
                % (found_field_name, others)
            )
    else:
        # grab or create a sig field
        if new_field_spec is not None:
            sig_field_kwargs = {
                'box': new_field_spec.box,
                'include_on_page': pdf_out.find_page_for_modification(
                    new_field_spec.on_page
                )[0],
                'combine_annotation': new_field_spec.combine_annotation,
                'invis_settings': new_field_spec.invis_sig_settings,
                'visible_settings': new_field_spec.visible_sig_settings,
            }
        else:
            sig_field_kwargs = {}

        field_created, sig_field_ref = prepare_sig_field(
            field_name,
            root,
            update_writer=pdf_out,
            existing_fields_only=existing_fields_only,
            **sig_field_kwargs,
        )
        if field_created and new_field_spec is not None:
            apply_sig_field_spec_properties(
                pdf_out,
                sig_field=sig_field_ref.get_object(),
                sig_field_spec=new_field_spec,
            )

    ensure_sig_flags(writer=pdf_out, lock_sig_flags=True)

    return field_created, sig_field_ref


@dataclass(frozen=True)
class SigMDPSetup:
    md_algorithm: str
    """
    Message digest algorithm to write into the signature reference dictionary,
    if one is written at all.

    .. warning::
        It is the caller's responsibility to make sure that this value agrees
        with the value embedded into the CMS object, and with the algorithm
        used to hash the document.
        The low-level :class:`.PdfCMSEmbedder` API *will* simply take it at
        face value.
    """

    certify: bool = False
    """
    Sign with an author (certification) signature, as opposed to an approval
    signature. A document can contain at most one such signature, and it must
    be the first one.
    """

    field_lock: Optional[FieldMDPSpec] = None
    """
    Field lock information to write to the signature reference dictionary.
    """

    docmdp_perms: Optional[MDPPerm] = None
    """
    DocMDP permissions to write to the signature reference dictionary.
    """

    def apply(self, sig_obj_ref, writer):
        """
        Apply the settings to a signature object.

        .. danger::
            This method is internal API.
        """

        certify = self.certify
        docmdp_perms = self.docmdp_perms

        lock = self.field_lock

        reference_array = generic.ArrayObject()

        if certify:
            assert docmdp_perms is not None
            # To make a certification signature, we need to leave a record
            #  in the document catalog.
            root = writer.root
            try:
                perms = root['/Perms']
            except KeyError:
                root['/Perms'] = perms = generic.DictionaryObject()
            perms[pdf_name('/DocMDP')] = sig_obj_ref
            writer.update_container(perms)
            reference_array.append(docmdp_reference_dictionary(docmdp_perms))

        if lock is not None:
            fieldmdp_ref = fieldmdp_reference_dictionary(
                lock, data_ref=writer.root_ref
            )
            reference_array.append(fieldmdp_ref)

            if docmdp_perms is not None:
                # NOTE: this is NOT spec-compatible, but emulates Acrobat
                # behaviour
                fieldmdp_ref['/TransformParams']['/P'] = generic.NumberObject(
                    docmdp_perms.value
                )

        if reference_array:
            sig_obj_ref.get_object()['/Reference'] = reference_array


@dataclass(frozen=True)
class SigAppearanceSetup:
    """
    Signature appearance configuration.

    Part of the low-level :class:`.PdfCMSEmbedder` API, see
    :class:`SigObjSetup`.
    """

    style: BaseStampStyle
    """
    Stamp style to use to generate the appearance.
    """

    timestamp: datetime
    """
    Timestamp to show in the signature appearance.
    """

    name: Optional[str]
    """
    Signer name to show in the signature appearance.
    """

    text_params: Optional[dict] = None
    """
    Additional text interpolation parameters to pass to the underlying
    stamp style.
    """

    def apply(self, sig_annot, writer):
        """
        Apply the settings to an annotation.

        .. danger::
            This method is internal API.
        """

        w, h = annot_width_height(sig_annot)
        if w and h:
            # the field is probably a visible one, so we change its appearance
            # stream to show some data about the signature
            stamp = self._appearance_stamp(
                writer, BoxConstraints(width=w, height=h)
            )
            sig_annot['/AP'] = stamp.as_appearances().as_pdf_object()
            try:
                # if there was an entry like this, it's meaningless now
                del sig_annot[pdf_name('/AS')]
            except KeyError:
                pass

    def _appearance_stamp(self, writer, box):
        style = self.style

        name = self.name
        timestamp = self.timestamp
        text_params = {}
        if name is not None:
            text_params['signer'] = name
        text_params.update(self.text_params or {})

        if isinstance(style, TextStampStyle):
            text_params['ts'] = timestamp.strftime(style.timestamp_format)

        return style.create_stamp(writer, box, text_params)


@dataclass(frozen=True)
class SigObjSetup:
    """
    Describes the signature dictionary to be embedded as the form field's value.
    """

    sig_placeholder: PdfSignedData
    """
    Bare-bones placeholder object, usually of type :class:`.SignatureObject`
    or :class:`.DocumentTimestamp`.

    In particular, this determines the number of bytes to allocate for the
    CMS object.
    """

    mdp_setup: Optional[SigMDPSetup] = None
    """
    Optional DocMDP settings, see :class:`.SigMDPSetup`.
    """

    appearance_setup: Optional[SigAppearanceSetup] = None
    """
    Optional appearance settings, see :class:`.SigAppearanceSetup`.
    """


@dataclass(frozen=True)
class SigIOSetup:
    """
    I/O settings for writing signed PDF documents.

    Objects of this type are used in the penultimate phase of
    the :class:`.PdfCMSEmbedder` protocol.
    """

    md_algorithm: str
    """
    Message digest algorithm to use to compute the document hash.
    It should be supported by `pyca/cryptography`.

    .. warning::
        This is also the message digest algorithm that should appear in the
        corresponding ``signerInfo`` entry in the CMS object that ends up
        being embedded in the signature field.
    """

    in_place: bool = False
    """
    Sign the input in-place. If ``False``, write output to a :class:`.BytesIO`
    object, or :attr:`output` if the latter is not ``None``.
    """

    chunk_size: int = misc.DEFAULT_CHUNK_SIZE
    """
    Size of the internal buffer (in bytes) used to feed data to the message 
    digest function if the input stream does not support ``memoryview``.
    """

    output: Optional[IO] = None
    """
    Write the output to the specified output stream. If ``None``, write to a 
    new :class:`.BytesIO` object. Default is ``None``.
    """


class PdfCMSEmbedder:
    """
    Low-level class that handles embedding CMS objects into PDF signature
    fields.

    It also takes care of appearance generation and DocMDP configuration,
    but does not otherwise offer any of the conveniences of
    :class:`.PdfSigner`.

    :param new_field_spec:
        :class:`.SigFieldSpec` to use when creating new fields on-the-fly.
    """

    def __init__(self, new_field_spec: Optional[SigFieldSpec] = None):
        self.new_field_spec = new_field_spec

    def write_cms(
        self,
        field_name: Optional[str],
        writer: BasePdfFileWriter,
        existing_fields_only=False,
    ):
        """
        .. versionadded:: 0.3.0

        .. versionchanged:: 0.7.0
            Digest wrapped in
            :class:`~pyhanko.sign.signers.pdf_byterange.PreparedByteRangeDigest`
            in step 3; ``output`` returned in step 3 instead of step 4.

        This method returns a generator coroutine that controls the process
        of embedding CMS data into a PDF signature field.
        Can be used for both timestamps and regular signatures.

        .. danger::
            This is a very low-level interface that performs virtually no
            error checking, and is intended to be used in situations
            where the construction of the CMS object to be embedded
            is not under the caller's control (e.g. a remote signer
            that produces full-fledged CMS objects).

            In almost every other case, you're better of using
            :class:`.PdfSigner` instead, with a custom :class:`.Signer`
            implementation to handle the cryptographic operations if necessary.

        The coroutine follows the following specific protocol.

        1. First, it retrieves or creates the signature field to embed the
           CMS object in, and yields a reference to said field.
        2. The caller should then send in a :class:`.SigObjSetup` object, which
           is subsequently processed by the coroutine. For convenience, the
           coroutine will then yield a reference to the signature dictionary
           (as embedded in the PDF writer).
        3. Next, the caller should send a :class:`.SigIOSetup` object,
           describing how the resulting document should be hashed and written
           to the output. The coroutine will write the entire document with a
           placeholder region reserved for the signature and compute the
           document's hash and yield it to the caller.
           It will then yield a ``prepared_digest, output`` tuple, where
           ``prepared_digest`` is a :class:`.PreparedByteRangeDigest` object
           containing the document digest and the relevant offsets, and
           ``output`` is the output stream to which the document to be
           signed was written.

           From this point onwards, **no objects may be changed or added** to
           the :class:`.IncrementalPdfFileWriter` currently in use.
        4. Finally, the caller should pass in a CMS object to place inside
           the signature dictionary. The CMS object can be supplied as a raw
           :class:`bytes` object, or an :mod:`asn1crypto`-style object.
           The coroutine's final yield is the value of the signature
           dictionary's ``/Contents`` entry, given as a hexadecimal string.

        .. caution::
            It is the caller's own responsibility to ensure that enough room
            is available in the placeholder signature object to contain
            the final CMS object.

        :param field_name:
            The name of the field to fill in. This should be a field of type
            ``/Sig``.
        :param writer:
            An :class:`.IncrementalPdfFileWriter` containing the
            document to sign.
        :param existing_fields_only:
            If ``True``, never create a new empty signature field to contain
            the signature.
            If ``False``, a new field may be created if no field matching
            ``field_name`` exists.
        :return:
            A generator coroutine implementing the protocol described above.
        """

        new_field_spec = (
            self.new_field_spec if not existing_fields_only else None
        )
        # start by creating or fetching the appropriate signature field
        field_created, sig_field_ref = _get_or_create_sigfield(
            field_name,
            writer,
            existing_fields_only,
            new_field_spec=new_field_spec,
        )

        # yield control to caller to further process the field dictionary
        # if necessary, request setup specs for sig object
        sig_obj_setup = yield sig_field_ref
        assert isinstance(sig_obj_setup, SigObjSetup)

        sig_field = sig_field_ref.get_object()

        # take care of the field's visual appearance (if applicable)
        appearance_setup = sig_obj_setup.appearance_setup
        if appearance_setup is not None:
            sig_annot = get_sig_field_annot(sig_field)
            appearance_setup.apply(sig_annot, writer)

        sig_obj = sig_obj_setup.sig_placeholder
        sig_obj_ref = writer.add_object(sig_obj)

        # fill in a reference to the (empty) signature object
        sig_field[pdf_name('/V')] = sig_obj_ref

        if not field_created:
            # still need to mark it for updating
            writer.mark_update(sig_field_ref)

        mdp_setup = sig_obj_setup.mdp_setup
        if mdp_setup is not None:
            mdp_setup.apply(sig_obj_ref, writer)

        # again, pass control to the caller
        # and request I/O parameters for putting the cryptographic signature
        # into the output.
        # We pass a reference to the embedded signature object as a convenience.

        sig_io = yield sig_obj_ref
        assert isinstance(sig_io, SigIOSetup)

        # pass control to the sig object's write_signature coroutine
        yield from sig_obj.fill(
            writer,
            sig_io.md_algorithm,
            in_place=sig_io.in_place,
            output=sig_io.output,
            chunk_size=sig_io.chunk_size,
        )
