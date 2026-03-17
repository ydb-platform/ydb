# coding: utf-8
import itertools
from dataclasses import dataclass
from typing import FrozenSet, Iterable, Iterator, Optional, Union

from asn1crypto import cms, x509

from .asn1_types import AAControls
from .authority import (
    Authority,
    AuthorityWithCert,
    CertTrustAnchor,
    TrustAnchor,
)
from .util import get_ac_extension_value, get_issuer_dn


@dataclass(frozen=True)
class QualifiedPolicy:
    issuer_domain_policy_id: str
    """
    Policy OID in the issuer domain (i.e. as listed on the certificate).
    """

    user_domain_policy_id: str
    """
    Policy OID of the equivalent policy in the user domain.
    """

    qualifiers: frozenset
    """
    Set of x509.PolicyQualifierInfo objects.
    """


Leaf = Union[x509.Certificate, cms.AttributeCertificateV2]


class ValidationPath:
    """
    Represents a path going towards an end-entity certificate or attribute
    certificate.
    """

    _qualified_policies: Optional[FrozenSet[QualifiedPolicy]] = None

    _path_aa_controls = None

    def __init__(
        self,
        trust_anchor: TrustAnchor,
        interm: Iterable[x509.Certificate],
        leaf: Optional[Leaf],
    ):
        if interm and not leaf:
            raise ValueError("Leafless paths cannot have intermediate certs")
        self._interm = list(interm)
        self._root = trust_anchor
        self._leaf = leaf

    @property
    def trust_anchor(self) -> TrustAnchor:
        return self._root

    @property
    def first(self):
        """
        Returns the current beginning of the path - for a path to be complete,
        this certificate should be a trust root

        .. warning::
            This is a compatibility property, and will return the first non-root
            certificate if the trust root is not provisioned as a certificate.
            If you want the trust root itself (even when it doesn't have a
            certificate), use :attr:`trust_anchor`.

        :return:
            The first asn1crypto.x509.Certificate object in the path
        """
        root = self._root.authority
        if isinstance(root, AuthorityWithCert):
            return root.certificate
        elif self._interm:
            return self._interm[0]
        elif isinstance(self._leaf, x509.Certificate):
            return self._leaf

    @property
    def leaf(self) -> Optional[Leaf]:
        """
        Returns the current leaf certificate (AC or public-key).
        The trust root's certificate will be returned if there is one and
        there are no other certificates in the path.

        If the trust root is certificate-less and there are no certificates,
        the result will be ``None``.
        """
        if self._leaf is not None:
            return self._leaf
        elif not self._interm and isinstance(self._root, CertTrustAnchor):
            return self._root.certificate
        # __init__ ensures that leaf None -> there are no intermediate certs
        return None

    def describe_leaf(self) -> Optional[str]:
        leaf = self.leaf
        if isinstance(leaf, x509.Certificate):
            return leaf.subject.human_friendly
        elif isinstance(leaf, cms.AttributeCertificateV2):
            return '<Attribute certificate>'
        else:
            return None

    def get_ee_cert_safe(self) -> Optional[x509.Certificate]:
        """
        Returns the current leaf certificate if it is an X.509 public-key
        certificate, and ``None`` otherwise.
        :return:
        """

        leaf = self.leaf
        if isinstance(leaf, x509.Certificate):
            return leaf
        else:
            return None

    @property
    def last(self) -> x509.Certificate:
        """
        Returns the last certificate in the path if it is an X.509 public-key
        certificate, and throws an error otherwise.

        :return:
            The last asn1crypto.x509.Certificate object in the path
        """
        cert = self.get_ee_cert_safe()
        if cert:
            return cert
        else:
            raise LookupError

    def iter_authorities(self) -> Iterable[Authority]:
        """
        Iterate over all authorities in the path, including the trust root.
        """
        yield self._root.authority
        for cert in self._interm:
            yield AuthorityWithCert(cert)

    def find_issuing_authority(self, cert: Leaf):
        """
        Return the issuer of the cert specified, as defined by this path

        :param cert:
            A certificate to get the issuer of

        :raises:
            LookupError - when the issuer of the certificate could not be found

        :return:
            An asn1crypto.x509.Certificate object of the issuer
        """

        issuer_name = get_issuer_dn(cert)
        if isinstance(cert, x509.Certificate):
            aki = cert.authority_key_identifier
        else:
            aki_ext = get_ac_extension_value(cert, 'authority_key_identifier')
            aki = aki_ext['key_identifier'].native if aki_ext else None

        for authority in self.iter_authorities():
            if authority.name == issuer_name:
                keyid = authority.key_id
                if keyid and aki and keyid != aki:
                    continue
                return authority

        raise LookupError(
            'Unable to find the issuer of the certificate specified'
        )

    def truncate_to_and_append(self, cert: x509.Certificate, new_leaf: Leaf):
        """
        Remove all certificates in the path after the cert specified and return
        them in a new path.

        Internal API.

        :param cert:
            An asn1crypto.x509.Certificate object to find

        :param new_leaf:
            A new leaf certificate to append.

        :raises:
            LookupError - when the certificate could not be found

        :return:
            The current ValidationPath object, for chaining
        """

        if isinstance(self._root, CertTrustAnchor):
            if self._root.certificate.issuer_serial == cert.issuer_serial:
                return ValidationPath(self._root, interm=[], leaf=new_leaf)

        certs = self._interm
        cert_index = None
        for index, entry in enumerate(certs):
            if entry.issuer_serial == cert.issuer_serial:
                cert_index = index
                break

        if cert_index is None:
            raise LookupError('Unable to find the certificate specified')
        return ValidationPath(
            self._root, interm=certs[: cert_index + 1], leaf=new_leaf
        )

    # TODO generalise this to ACs as well?
    def truncate_to_issuer_and_append(self, cert: x509.Certificate):
        """
        Remove all certificates in the path after the issuer of the cert
        specified, as defined by this path, and append a new one.

        Internal API.

        :param cert:
            A new leaf certificate to append.

        :raises:
            LookupError - when the issuer of the certificate could not be found

        :return:
            The current ValidationPath object, for chaining
        """

        issuer_index = None

        # check the trust root separately
        if self.trust_anchor.authority.is_potential_issuer_of(cert):
            # in case of a match, truncate everything
            if cert.self_signed == 'maybe':
                # if the candidate leaf is self-signed (according to metadata),
                # then it's actually the authority itself -> no need to append.
                return ValidationPath(self._root, interm=[], leaf=None)
            else:
                return ValidationPath(self._root, interm=[], leaf=cert)

        # now run through the rest of the path
        certs = self._interm
        for index, entry in enumerate(certs):
            if entry.subject == cert.issuer:
                if entry.key_identifier and cert.authority_key_identifier:
                    if entry.key_identifier == cert.authority_key_identifier:
                        issuer_index = index
                        break
                else:
                    issuer_index = index
                    break

        if issuer_index is None:
            raise LookupError(
                'Unable to find the issuer of the certificate specified'
            )

        return ValidationPath(self._root, certs[: issuer_index + 1], leaf=cert)

    def copy_and_append(self, cert: Leaf):
        new_certs = self._interm[:]
        if self._leaf:
            new_certs.append(self._leaf)
        return ValidationPath(
            trust_anchor=self._root, interm=new_certs, leaf=cert
        )

    def copy_and_drop_leaf(self) -> 'ValidationPath':
        """
        Drop the leaf cert from this path and return a new path with the
        last intermediate certificate set as the leaf.
        """

        if len(self._interm) == 0:
            raise IndexError
        new_interm, new_leaf = self._interm[:-1], self._interm[-1]
        return ValidationPath(
            trust_anchor=self._root, interm=new_interm, leaf=new_leaf
        )

    def _set_qualified_policies(self, policies):
        self._qualified_policies = policies

    def qualified_policies(self) -> Optional[FrozenSet[QualifiedPolicy]]:
        return self._qualified_policies

    def aa_attr_in_scope(self, attr_id: cms.AttCertAttributeType) -> bool:
        aa_controls_extensions = [
            AAControls.read_extension_value(cert) for cert in self
        ]
        aa_controls_used = any(x is not None for x in aa_controls_extensions)
        if not aa_controls_used:
            return True
        else:
            # the path validation code ensures that all non-anchor certs
            # have an AAControls extension, but we still enforce the root's
            # AAControls if there is one (since we might as well treat it
            # as a configuration setting/failsafe at that point)
            # This is appropriate in PKIX-land (see RFC 5280, § 6.2 as
            # updated in RFC 6818, § 4)
            return all(
                ctrl.accept(attr_id)
                for ctrl in aa_controls_extensions
                # None check for defensiveness (already enforced by validation
                # algorithm), and to (potentially) skip the root
                if ctrl is not None
            )

    @property
    def pkix_len(self):
        return len(self._interm) + (1 if self._leaf else 0)

    def __len__(self):
        # backwards compat
        return 1 + self.pkix_len

    def __getitem__(self, key):
        # convoluted because of compatibility issues...
        if key > 0:
            leaf_ix = len(self._interm) + 1
            if key == leaf_ix and self._leaf is not None:
                return self._leaf
            return self._interm[key - 1]
        elif isinstance(self._root, CertTrustAnchor):
            # backwards compat
            return self._root.certificate
        else:
            # Throw an error instead of returning None, because we want this
            # to fail loudly.
            raise LookupError("Root has no certificate")

    def iter_certs(self, include_root: bool) -> Iterator[x509.Certificate]:
        """
        Iterate over the certificates in the path.

        :param include_root:
            Include the root (if it is supplied as a certificate)
        :return:
            An iterator.
        """
        root = self._root.authority
        from_root = (
            (root.certificate,)
            if include_root and isinstance(root, AuthorityWithCert)
            else ()
        )
        leaf = self._leaf
        from_leaf = (leaf,) if isinstance(leaf, x509.Certificate) else ()
        return itertools.chain(from_root, self._interm, from_leaf)

    def __iter__(self):
        # backwards compat, we iterate over all certs _including_ the root
        # if it is supplied as a cert
        return self.iter_certs(include_root=True)

    def __eq__(self, other):
        if not isinstance(other, ValidationPath):
            return False
        return (
            self.trust_anchor == other.trust_anchor
            and self._interm == other._interm
            and self._leaf == other._leaf
        )
