"""
This module provides PKCS#11 integration for pyHanko, by providing a wrapper
for `python-pkcs11 <https://github.com/danni/python-pkcs11>`_ that can be
seamlessly plugged into a :class:`~.signers.PdfSigner`.
"""

import asyncio
import binascii
import logging
import struct
import warnings
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import pkcs11
from asn1crypto import algos, core, x509
from asn1crypto.algos import RSASSAPSSParams
from cryptography.hazmat.primitives import hashes
from pyhanko_certvalidator.registry import CertificateStore

from pyhanko.config.pkcs11 import (
    PKCS11PinEntryMode,
    PKCS11SignatureConfig,
    TokenCriteria,
)
from pyhanko.pdf_utils.misc import coalesce
from pyhanko.sign.general import SigningError, get_pyca_cryptography_hash
from pyhanko.sign.signers import Signer

try:
    from pkcs11 import (
        MGF,
        PROTECTED_AUTH,
        Attribute,
        Mechanism,
        ObjectClass,
        PKCS11Error,
        Session,
    )
    from pkcs11 import lib as p11_lib
    from pkcs11 import types as p11_types
except ImportError as e:  # pragma: nocover
    raise ImportError(
        "pyhanko.sign.pkcs11 requires pyHanko to be installed with "
        "the [pkcs11] option. You can install missing "
        "dependencies by running \"pip install 'pyHanko[pkcs11]'\".",
        e,
    )

__all__ = [
    'PKCS11Signer',
    'open_pkcs11_session',
    'PKCS11SigningContext',
    'find_token',
    'select_pkcs11_signing_params',
]

logger = logging.getLogger(__name__)


def criteria_mismatches(
    criteria: Optional[TokenCriteria], token: p11_types.Token
) -> List[Tuple[str, str]]:
    if criteria is None:
        return []

    err_items = []

    if criteria.label is not None and token.label != criteria.label:
        err_items.append(('label', criteria.label))
    if criteria.serial is not None and token.serial != criteria.serial:
        err_items.append(('serial', criteria.serial.hex()))
    return err_items


def criteria_satisfied_by(
    criteria: Optional[TokenCriteria], token: p11_types.Token
) -> bool:
    return not criteria_mismatches(criteria, token)


def find_token(
    slots: List[p11_types.Slot],
    slot_no: Optional[int] = None,
    token_criteria: Optional[TokenCriteria] = None,
) -> Optional[p11_types.Token]:
    """
    Internal helper method to find a token.

    :param slots:
        The list of slots.
    :param slot_no:
        Slot number to use. If not specified, the first slot containing a token
        satisfying the criteria will be used
    :param token_criteria:
        Criteria the token must satisfy.
    :return:
        A PKCS#11 token object, or ``None`` if none was found.
    """

    if token_criteria is None and slot_no is None:
        if len(slots) == 1:
            return slots[0].get_token()
        else:
            raise PKCS11Error(
                "Module has more than 1 slot; slot index or token criteria "
                "must be provided"
            )

    if slot_no is None:
        for slot in slots:
            try:
                token = slot.get_token()
                if criteria_satisfied_by(token_criteria, token):
                    return token
            except PKCS11Error:
                continue
    else:
        if slot_no >= len(slots):
            raise PKCS11Error(
                f"Slot index {slot_no} too large; there are only {len(slots)}"
            )
        token = slots[slot_no].get_token()
        errors = criteria_mismatches(token_criteria, token)
        if errors:
            err_str = ", ".join(
                f"{field} is not {val!r}" for field, val in errors
            )
            raise PKCS11Error(
                f"Token in slot {slot_no} does not satisfy criteria; {err_str}."
            )
        return token
    return None


@dataclass(frozen=True)
class PKCS11SignatureOperationSpec:
    """
    Internal helper class to describe how to invoke a signature operation on
    a key in a PKCS #11 token.
    """

    sign_kwargs: Dict[str, Any]
    """
    Keyword arguments to the ``sign`` function on the key handle.
    """

    pre_sign_transform: Optional[Callable[[bytes], bytes]]
    """
    An optional transformation to apply to the data prior to signing.
    """

    post_sign_transform: Optional[Callable[[bytes], bytes]]
    """
    An optional transformation to apply to the data after signing.
    """


RSA_MECH_MAP = {
    'sha1': Mechanism.SHA1_RSA_PKCS,
    'sha224': Mechanism.SHA224_RSA_PKCS,
    'sha256': Mechanism.SHA256_RSA_PKCS,
    'sha384': Mechanism.SHA384_RSA_PKCS,
    'sha512': Mechanism.SHA512_RSA_PKCS,
}

RSASSA_PSS_MECH_MAP = {
    'sha1': Mechanism.SHA1_RSA_PKCS_PSS,
    'sha224': Mechanism.SHA224_RSA_PKCS_PSS,
    'sha256': Mechanism.SHA256_RSA_PKCS_PSS,
    'sha384': Mechanism.SHA384_RSA_PKCS_PSS,
    'sha512': Mechanism.SHA512_RSA_PKCS_PSS,
}

MGF_MECH_MAP = {
    'sha1': MGF.SHA1,
    'sha224': MGF.SHA224,
    'sha256': MGF.SHA256,
    'sha384': MGF.SHA384,
    'sha512': MGF.SHA512,
}

ECDSA_MECH_MAP = {
    'sha1': Mechanism.ECDSA_SHA1,
    'sha224': Mechanism.ECDSA_SHA224,
    'sha256': Mechanism.ECDSA_SHA256,
    'sha384': Mechanism.ECDSA_SHA384,
    'sha512': Mechanism.ECDSA_SHA512,
}

DSA_MECH_MAP = {
    'sha1': Mechanism.DSA_SHA1,
    'sha224': Mechanism.DSA_SHA224,
    'sha256': Mechanism.DSA_SHA256,
    # These can't be used in CMS IIRC (since the key sizes required
    # to meaningfully use them are ridiculous),
    # but they're in the PKCS#11 spec, so let's add them for
    # completeness
    'sha384': Mechanism.DSA_SHA384,
    'sha512': Mechanism.DSA_SHA512,
}

DIGEST_MECH_MAP = {
    'sha1': Mechanism.SHA_1,
    'sha224': Mechanism.SHA224,
    'sha256': Mechanism.SHA256,
    'sha384': Mechanism.SHA384,
    'sha512': Mechanism.SHA512,
}


def select_pkcs11_signing_params(
    signature_mechanism: algos.SignedDigestAlgorithm,
    digest_algorithm: str,
    use_raw_mechanism: bool,
) -> PKCS11SignatureOperationSpec:
    """
    Internal helper function to set up a PKCS #11 signing operation.

    :param signature_mechanism:
        The signature mechanism to use (as an ASN.1 value)
    :param digest_algorithm:
        The digest algorithm to use
    :param use_raw_mechanism:
        Whether to attempt to use the raw mechanism on pre-hashed data.
    :return:
    """
    from pkcs11.util.dsa import encode_dsa_signature
    from pkcs11.util.ec import encode_ecdsa_signature

    pre_sign_transform = None
    post_sign_transform = None
    kwargs: Dict[str, Any] = {}

    try:
        signature_algo = signature_mechanism.signature_algo
    except ValueError:
        signature_algo = signature_mechanism['algorithm'].native

    if signature_algo == 'rsassa_pkcs1v15':
        if use_raw_mechanism:
            kwargs['mechanism'] = Mechanism.RSA_PKCS
            pre_sign_transform = _hash_fully(
                digest_algorithm, wrap_digest_info=True
            )
        else:
            kwargs['mechanism'] = RSA_MECH_MAP[digest_algorithm]
    elif signature_algo == 'dsa':
        if use_raw_mechanism:
            kwargs['mechanism'] = Mechanism.DSA
            pre_sign_transform = _hash_fully(
                digest_algorithm, wrap_digest_info=False
            )
        else:
            kwargs['mechanism'] = DSA_MECH_MAP[digest_algorithm]
        post_sign_transform = encode_dsa_signature
    elif signature_algo == 'ecdsa':
        if use_raw_mechanism:
            kwargs['mechanism'] = Mechanism.ECDSA
            pre_sign_transform = _hash_fully(
                digest_algorithm, wrap_digest_info=False
            )
        else:
            # TODO test these (unsupported in SoftHSMv2 right now)
            kwargs['mechanism'] = ECDSA_MECH_MAP[digest_algorithm]
        post_sign_transform = encode_ecdsa_signature
    elif signature_algo == 'rsassa_pss':
        if use_raw_mechanism:
            raise NotImplementedError("RSASSA-PSS not available in raw mode")
        params: RSASSAPSSParams = signature_mechanism['parameters']
        assert digest_algorithm == params['hash_algorithm']['algorithm'].native

        # unpack PSS parameters into PKCS#11 language
        kwargs['mechanism'] = RSASSA_PSS_MECH_MAP[digest_algorithm]

        pss_digest_param = DIGEST_MECH_MAP[digest_algorithm]

        mgf_val = params['mask_gen_algorithm']['parameters']['algorithm'].native
        pss_mgf_param = MGF_MECH_MAP[mgf_val]
        pss_salt_len = params['salt_length'].native

        kwargs['mechanism_param'] = (
            pss_digest_param,
            pss_mgf_param,
            pss_salt_len,
        )
    elif signature_algo == 'ed25519':
        if use_raw_mechanism:
            # Note: Ed25519-ph isn't the same thing.
            raise NotImplementedError("Ed25519 not available in raw mode")
        kwargs['mechanism'] = Mechanism.EDDSA
    elif signature_algo == 'ed448':
        if use_raw_mechanism:
            # Note: Ed448-ph isn't the same thing.
            raise NotImplementedError("Ed448 not available in raw mode")
        kwargs['mechanism'] = Mechanism.EDDSA
        # Definition of the param type:
        # typedef struct CK_EDDSA_PARAMS {
        #    CK_BBOOL     phFlag;
        #    CK_ULONG     ulContextDataLen;
        #    CK_BYTE_PTR  pContextData;
        # }  CK_EDDSA_PARAMS;
        # We use native size and alignment here on purpose

        # NOTE: I _think_ this is correct, but it looks like SoftHSMv2
        # doesn't really care about the params, so maybe I'm wrong.
        kwargs['mechanism_param'] = struct.pack('@?LP', False, 0, 0)
    else:
        raise NotImplementedError(
            f"Signature algorithm '{signature_algo}' is not supported."
        )

    return PKCS11SignatureOperationSpec(
        sign_kwargs=kwargs,
        pre_sign_transform=pre_sign_transform,
        post_sign_transform=post_sign_transform,
    )


def open_pkcs11_session(
    lib_location: str,
    slot_no: Optional[int] = None,
    token_label: Optional[str] = None,
    token_criteria: Optional[TokenCriteria] = None,
    user_pin: Union[str, object, None] = None,
) -> Session:
    """
    Open a PKCS#11 session

    :param lib_location:
        Path to the PKCS#11 module.
    :param slot_no:
        Slot number to use. If not specified, the first slot containing a token
        labelled ``token_label`` will be used.
    :param token_label:
        .. deprecated:: 0.14.0
            Use ``token_criteria`` instead.

        Label of the token to use. If ``None``, there is no constraint.
    :param token_criteria:
        Criteria that the token should match.
    :param user_pin:
        User PIN to use, or :attr:`.PROTECTED_AUTH`. If ``None``, authentication
        is skipped.

        .. note::
            Some PKCS#11 implementations do not require PIN when the token
            is opened, but will prompt for it out-of-band when signing.
            Whether :attr:`.PROTECTED_AUTH` or ``None`` is used in this case
            depends on the implementation.
    :return:
        An open PKCS#11 session object.
    """
    lib = p11_lib(lib_location)

    if token_criteria is None and token_label is not None:
        warnings.warn(
            "'token_label' is deprecated, use 'token_criteria' instead",
            DeprecationWarning,
        )
        token_criteria = TokenCriteria(label=token_label)

    slots = lib.get_slots()
    token = find_token(slots, slot_no=slot_no, token_criteria=token_criteria)
    if token is None:
        raise PKCS11Error(
            f'No token matching criteria {token_criteria!r} found'
            if token_criteria is not None
            else 'No token found'
        )

    kwargs = {}
    if user_pin is not None:
        kwargs['user_pin'] = user_pin

    return token.open(**kwargs)


def _format_pull_err_msg(
    no_results: bool,
    label: Optional[str] = None,
    cert_id: Optional[bytes] = None,
):
    info_strs = []
    if label is not None:
        info_strs.append(f"label '{label}'")
    if cert_id is not None:
        info_strs.append(f"ID '{binascii.hexlify(cert_id).decode('ascii')}'")
    qualifier = f" with {', '.join(info_strs)}" if info_strs else ""
    if no_results:
        err = f"Could not find cert{qualifier}."
    else:
        err = f"Found more than one cert{qualifier}."
    return err


def _pull_cert(
    pkcs11_session: Session,
    label: Optional[str] = None,
    cert_id: Optional[bytes] = None,
):
    query_params = {Attribute.CLASS: ObjectClass.CERTIFICATE}
    if label is not None:
        query_params[Attribute.LABEL] = label
    if cert_id is not None:
        query_params[Attribute.ID] = cert_id
    q = pkcs11_session.get_objects(query_params)

    # need to run through the full iterator to make sure the operation
    # terminates
    results = list(q)
    if len(results) == 1:
        cert_obj = results[0]
        return x509.Certificate.load(cert_obj[Attribute.VALUE])
    else:
        err = _format_pull_err_msg(
            no_results=not results, label=label, cert_id=cert_id
        )
        raise PKCS11Error(err)


def _hash_fully(digest_algorithm: str, *, wrap_digest_info: bool):
    md_spec = get_pyca_cryptography_hash(digest_algorithm)

    def _h(data: bytes) -> bytes:
        h = hashes.Hash(md_spec)
        h.update(data)
        digest = h.finalize()
        if wrap_digest_info:
            return algos.DigestInfo(
                {
                    'digest_algorithm': {
                        'algorithm': digest_algorithm.lower(),
                        'parameters': core.Null(),
                    },
                    'digest': digest,
                }
            ).dump()
        else:
            return digest

    return _h


# TODO: perhaps attempt automatic key discovery if the labels aren't provided?


class PKCS11Signer(Signer):
    """
    Signer implementation for PKCS11 devices.

    :param pkcs11_session:
        The PKCS11 session object to use.
    :param cert_label:
        The label of the certificate that will be used for signing, to
        be pulled from the PKCS#11 token.
    :param cert_id:
        ID of the certificate object that will be used for signing, to
        be pulled from the PKCS#11 token.
    :param signing_cert:
        The signer's certificate. If the signer's certificate is provided via
        this parameter, the ``cert_label`` and ``cert_id`` parameters will not
        be used to retrieve the signer's certificate.
    :param ca_chain:
        Set of other relevant certificates
        (as :class:`.asn1crypto.x509.Certificate` objects).
    :param key_label:
        The label of the key that will be used for signing.
        Defaults to the value of ``cert_label`` if left unspecified and
        ``key_id`` is also unspecified.

        .. note::
            At least one of ``key_id``, ``key_label`` and ``cert_label`` must
            be supplied.
    :param key_id:
        ID of the private key object (optional).
    :param other_certs_to_pull:
        List labels of other certificates to pull from the PKCS#11 device.
        Defaults to the empty tuple. If ``None``, pull *all* certificates.
    :param bulk_fetch:
        Boolean indicating the fetching strategy.
        If ``True``, fetch all certs and filter the unneeded ones.
        If ``False``, fetch the requested certs one by one.
        Default value is ``True``, unless ``other_certs_to_pull`` has one or
        fewer elements, in which case it is always treated as ``False``.
    :param use_raw_mechanism:
        Use the 'raw' equivalent of the selected signature mechanism. This is
        useful when working with tokens that do not support a hash-then-sign
        mode of operation.

        .. note::
            This functionality is only available for ECDSA at this time.
            Support for other signature schemes will be added on an as-needed
            basis.
    """

    def __init__(
        self,
        pkcs11_session: Session,
        cert_label: Optional[str] = None,
        signing_cert: Optional[x509.Certificate] = None,
        ca_chain=None,
        key_label: Optional[str] = None,
        prefer_pss=False,
        embed_roots=True,
        other_certs_to_pull=(),
        bulk_fetch=True,
        key_id: Optional[bytes] = None,
        cert_id: Optional[bytes] = None,
        use_raw_mechanism=False,
    ):
        """
        Initialise a PKCS11 signer.
        """
        self.cert_label = coalesce(
            cert_label, key_label if not cert_id else None
        )
        self.key_id = coalesce(key_id, cert_id if not key_label else None)
        self.cert_id = coalesce(cert_id, key_id if not cert_label else None)
        self.key_label = coalesce(key_label, cert_label if not key_id else None)
        self.pkcs11_session = pkcs11_session
        self.other_certs = other_certs_to_pull
        self._other_certs_loaded = False
        if other_certs_to_pull is not None and len(other_certs_to_pull) <= 1:
            self.bulk_fetch = False
        else:
            self.bulk_fetch = bulk_fetch
        self.use_raw_mechanism = use_raw_mechanism
        self._key_handle = None
        self._loaded = False
        self.__loading_event = None
        super().__init__(
            prefer_pss=prefer_pss,
            embed_roots=embed_roots,
            signing_cert=signing_cert,
        )
        if ca_chain is not None:
            self._cert_registry.register_multiple(ca_chain)
        if signing_cert is not None:
            self._cert_registry.register(signing_cert)

    def _init_cert_registry(self):
        # it's conceivable that one might want to load this separately from
        # the key data, so we allow for that.
        if not self._other_certs_loaded:
            certs = self._load_other_certs()
            self._cert_registry.register_multiple(certs)
            self._other_certs_loaded = True
        return self._cert_registry

    @property
    def cert_registry(self) -> CertificateStore:
        # apparently mypy doesn't like it when I write
        # cert_registry = property(_init_cert_registry)
        return self._init_cert_registry()

    @property
    def signing_cert(self):
        self._load_objects()
        return self._signing_cert

    def _select_pkcs11_signing_params(
        self, digest_algorithm: str
    ) -> PKCS11SignatureOperationSpec:
        digest_algorithm = digest_algorithm.lower()
        return select_pkcs11_signing_params(
            self.get_signature_mechanism_for_digest(digest_algorithm),
            digest_algorithm,
            use_raw_mechanism=self.use_raw_mechanism,
        )

    async def async_sign_raw(
        self, data: bytes, digest_algorithm: str, dry_run=False
    ) -> bytes:
        if dry_run:
            # allocate 4096 bits for the fake signature
            return b'0' * 512

        await self.ensure_objects_loaded()
        from pkcs11 import SignMixin

        kh: SignMixin = self._key_handle
        spec = self._select_pkcs11_signing_params(digest_algorithm)

        if spec.pre_sign_transform is not None:
            data = spec.pre_sign_transform(data)

        def _perform_signature():
            signature = kh.sign(data, **spec.sign_kwargs)
            if spec.post_sign_transform is not None:
                signature = spec.post_sign_transform(signature)
            return signature

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _perform_signature)

    def _load_other_certs(self) -> Set[x509.Certificate]:
        return set(self.__pull())

    def __pull(self):
        other_cert_labels = self.other_certs
        if other_cert_labels is not None and len(other_cert_labels) == 0:
            # if there's nothing to fetch, bail.
            # Recall: None -> fetch everything, so we check the length
            # explicitly
            return
        if other_cert_labels is None or self.bulk_fetch:
            # first, query all certs
            q = self.pkcs11_session.get_objects(
                {Attribute.CLASS: ObjectClass.CERTIFICATE}
            )
            logger.debug("Pulling all certificates from PKCS#11 token...")
            for cert_obj in q:
                label = cert_obj[Attribute.LABEL]
                if other_cert_labels is None or label in other_cert_labels:
                    # LGTM believes that we're logging sensitive info here, but
                    # I politely disagree: this is just the PKCS#11 label of the
                    # certificate
                    msg = f"Found certificate with label '{label}' on token."
                    logger.debug(msg)  # lgtm
                    yield x509.Certificate.load(cert_obj[Attribute.VALUE])
        else:
            # fetch certs one by one
            for label in other_cert_labels:
                # LGTM believes that we're logging sensitive info here, but
                # I politely disagree: this is just the PKCS#11 label of the
                # certificate
                msg = (
                    f"Pulling certificate with label '{label}' from "
                    f"PKCS#11 token..."
                )
                logger.debug(msg)  # lgtm
                yield _pull_cert(self.pkcs11_session, label)

    async def ensure_objects_loaded(self):
        """
        Async method that, when awaited, ensures that objects
        (relevant certificates, key handles, ...) are loaded.

        This coroutine is guaranteed to be called & awaited in :meth:`sign_raw`,
        but some property implementations may cause object loading to be
        triggered synchronously (for backwards compatibility reasons).
        This blocks the event loop the first time it happens.

        To avoid this behaviour, asynchronous code should ideally perform
        `await signer.ensure_objects_loaded()` after instantiating the signer.

        .. note::
            The asynchronous context manager on :class:`PKCS11SigningContext`
            takes care of that automatically.
        """

        if self._loaded:
            return
        if self.__loading_event is None:
            self.__loading_event = event = asyncio.Event()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._load_objects)
            event.set()
        else:  # pragma: nocover
            # some other coroutine is dealing with fetching already,
            # just wait for that one to finish
            await self.__loading_event.wait()

    def _load_objects(self):
        if self._loaded:
            return

        self._init_cert_registry()
        if self._signing_cert is None:
            self._signing_cert = _pull_cert(
                self.pkcs11_session, label=self.cert_label, cert_id=self.cert_id
            )

        kh = self.pkcs11_session.get_key(
            ObjectClass.PRIVATE_KEY, label=self.key_label, id=self.key_id
        )
        self._key_handle = kh

        self._loaded = True


class PKCS11SigningContext:
    """Context manager for PKCS#11 configurations."""

    def __init__(
        self, config: PKCS11SignatureConfig, user_pin: Optional[str] = None
    ):
        self.config = config
        self._session = None
        self._user_pin = user_pin

    def _handle_pin(self):
        pin = self._user_pin or self.config.user_pin
        # mode 'PROMPT' is irrelevant for library usage.
        if pin is not None:
            pin = str(pin)
        elif self.config.prompt_pin == PKCS11PinEntryMode.DEFER:
            pin = PROTECTED_AUTH
        else:
            pin = None
        return pin

    def _instantiate(self) -> PKCS11Signer:
        config = self.config
        pin = self._handle_pin()

        try:
            self._session = session = open_pkcs11_session(
                config.module_path,
                slot_no=config.slot_no,
                token_criteria=config.token_criteria,
                user_pin=pin,
            )
        except pkcs11.PKCS11Error as ex:
            raise SigningError(
                f"PKCS#11 error while opening session to {config.module_path}: [{type(ex).__name__}] {ex}"
            ) from ex
        return PKCS11Signer(
            session,
            config.cert_label,
            ca_chain=config.other_certs,
            key_label=config.key_label,
            prefer_pss=config.prefer_pss,
            use_raw_mechanism=config.raw_mechanism,
            other_certs_to_pull=config.other_certs_to_pull,
            bulk_fetch=config.bulk_fetch,
            key_id=config.key_id,
            cert_id=config.cert_id,
            signing_cert=config.signing_certificate,
        )

    def __enter__(self):
        return self._instantiate()

    async def __aenter__(self):
        loop = asyncio.get_running_loop()
        signer = await loop.run_in_executor(None, self._instantiate)
        await signer.ensure_objects_loaded()
        return signer

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._session.close()
