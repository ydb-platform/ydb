# coding: utf-8

import abc
import asyncio
from collections import defaultdict
from typing import AsyncGenerator, Iterable, Iterator, List, Optional, Union

from asn1crypto import x509
from oscrypto import trust_list

from .authority import CertTrustAnchor, TrustAnchor
from .errors import PathBuildingError
from .fetchers import CertificateFetcher
from .path import ValidationPath
from .util import CancelableAsyncIterator, ConsList


class CertificateCollection(abc.ABC):
    """
    Abstract base class for read-only access to a collection of certificates.
    """

    def retrieve_by_key_identifier(self, key_identifier: bytes):
        """
        Retrieves a cert via its key identifier

        :param key_identifier:
            A byte string of the key identifier

        :return:
            None or an asn1crypto.x509.Certificate object
        """
        candidates = self.retrieve_many_by_key_identifier(key_identifier)
        if not candidates:
            return None
        else:
            return candidates[0]

    def retrieve_many_by_key_identifier(self, key_identifier: bytes):
        """
        Retrieves possibly multiple certs via the corresponding key identifiers

        :param key_identifier:
            A byte string of the key identifier

        :return:
            A list of asn1crypto.x509.Certificate objects
        """
        raise NotImplementedError

    def retrieve_by_name(self, name: x509.Name):
        """
        Retrieves a list certs via their subject name

        :param name:
            An asn1crypto.x509.Name object

        :return:
            A list of asn1crypto.x509.Certificate objects
        """
        raise NotImplementedError

    def retrieve_by_issuer_serial(self, issuer_serial):
        """
        Retrieve a certificate by its ``issuer_serial`` value.

        :param issuer_serial:
            The ``issuer_serial`` value of the certificate.
        :return:
            The certificate corresponding to the ``issuer_serial`` key
            passed in.
        :return:
            None or an asn1crypto.x509.Certificate object
        """
        raise NotImplementedError


class CertificateStore(CertificateCollection, abc.ABC):
    def register(self, cert: x509.Certificate) -> bool:
        """
        Register a single certificate.

        :param cert:
            Certificate to add.
        :return:
            ``True`` if the certificate was added, ``False`` if it already
            existed in this store.
        """
        raise NotImplementedError

    def register_multiple(self, certs: Iterable[x509.Certificate]):
        """
        Register multiple certificates.

        :param certs:
            Certificates to register.
        :return:
            ``True`` if at least one certificate was added, ``False``
            if all certificates already existed in this store.
        """

        added = False
        for cert in certs:
            added |= self.register(cert)
        return added

    def __iter__(self):
        raise NotImplementedError


class SimpleCertificateStore(CertificateStore):
    """
    Simple trustless certificate store.
    """

    @classmethod
    def from_certs(cls, certs):
        result = cls()
        for cert in certs:
            result.register(cert)
        return result

    def __init__(self):
        self.certs = {}
        self._subject_map = defaultdict(list)
        self._key_identifier_map = defaultdict(list)

    def register(self, cert: x509.Certificate) -> bool:
        """
        Register a single certificate.

        :param cert:
            Certificate to add.
        :return:
            ``True`` if the certificate was added, ``False`` if it already
            existed in this store.
        """
        if cert.issuer_serial in self.certs:
            return False
        self.certs[cert.issuer_serial] = cert
        self._subject_map[cert.subject.hashable].append(cert)
        if cert.key_identifier:
            self._key_identifier_map[cert.key_identifier].append(cert)
        else:
            self._key_identifier_map[cert.public_key.sha1].append(cert)
        return True

    def __getitem__(self, item):
        return self.certs[item]

    def __iter__(self):
        return iter(self.certs.values())

    def retrieve_many_by_key_identifier(self, key_identifier: bytes):
        return self._key_identifier_map[key_identifier]

    def retrieve_by_name(self, name: x509.Name):
        return self._subject_map[name.hashable]

    def retrieve_by_issuer_serial(self, issuer_serial):
        try:
            return self[issuer_serial]
        except KeyError:
            return None


TrustRootList = Iterable[Union[x509.Certificate, TrustAnchor]]


class TrustManager:
    """
    Abstract trust manager API.
    """

    def is_root(self, cert: x509.Certificate) -> bool:
        """
        Checks if a certificate is in the list of trust roots in this registry

        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            A boolean - if the certificate is in the CA list
        """
        raise NotImplementedError

    def find_potential_issuers(
        self, cert: x509.Certificate
    ) -> Iterator[TrustAnchor]:
        """
        Find potential issuers that might have (directly) issued
        a particular certificate.

        :param cert:
            Issued certificate.
        :return:
            An iterator with potentially relevant trust anchors.
        """
        raise NotImplementedError


class SimpleTrustManager(TrustManager):
    """
    Trust manager backed by a list of trust roots, possibly in addition to the
    system trust list.
    """

    def __init__(self):
        self._roots = set()
        self._root_subject_map = defaultdict(list)

    @classmethod
    def build(
        cls,
        trust_roots: Optional[TrustRootList] = None,
        extra_trust_roots: Optional[TrustRootList] = None,
    ) -> 'SimpleTrustManager':
        """
        :param trust_roots:
            If the operating system's trust list should not be used, instead
            pass a list of asn1crypto.x509.Certificate objects. These
            certificates will be used as the trust roots for the path being
            built.

        :param extra_trust_roots:
            If the operating system's trust list should be used, but augmented
            with one or more extra certificates. This should be a list of
            asn1crypto.x509.Certificate objects.
        :return:
        """
        if trust_roots is None:
            trust_roots = [e[0] for e in trust_list.get_list()]
        else:
            trust_roots = list(trust_roots)

        if extra_trust_roots is not None:
            trust_roots.extend(extra_trust_roots)

        manager = SimpleTrustManager()
        for trust_root in trust_roots:
            manager._register_root(trust_root)
        return manager

    def _register_root(self, trust_root: Union[TrustAnchor, x509.Certificate]):
        if isinstance(trust_root, TrustAnchor):
            anchor = trust_root
        else:
            anchor = CertTrustAnchor(trust_root)
        if anchor not in self._roots:
            authority = anchor.authority
            self._roots.add(anchor)
            self._root_subject_map[authority.name.hashable].append(anchor)

    def is_root(self, cert: x509.Certificate):
        """
        Checks if a certificate is in the list of trust roots in this registry

        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            A boolean - if the certificate is in the CA list
        """

        return CertTrustAnchor(cert) in self._roots

    def iter_certs(self) -> Iterator[x509.Certificate]:
        return (
            root.certificate
            for root in self._roots
            if isinstance(root, CertTrustAnchor)
        )

    def find_potential_issuers(
        self, cert: x509.Certificate
    ) -> Iterator[TrustAnchor]:
        issuer_hashable = cert.issuer.hashable
        root: TrustAnchor
        for root in self._root_subject_map[issuer_hashable]:
            if root.authority.is_potential_issuer_of(cert):
                yield root


class CertificateRegistry(SimpleCertificateStore):
    """
    Contains certificate lists used to build validation paths, and
    is also capable of fetching missing certificates if a certificate
    fetcher is supplied.
    """

    def __init__(self, *, cert_fetcher: Optional[CertificateFetcher] = None):
        super().__init__()
        self.fetcher = cert_fetcher

    @classmethod
    def build(
        cls,
        certs: Iterable[x509.Certificate] = (),
        *,
        cert_fetcher: Optional[CertificateFetcher] = None,
    ):
        """
        Convenience method to set up a certificate registry and import
        certs into it.

        :param certs:
            Initial list of certificates to import.
        :param cert_fetcher:
            Certificate fetcher to handle retrieval of missing certificates
            (in situations where that is possible).
        :return:
            A populated certificate registry.
        """

        result: CertificateRegistry = cls(cert_fetcher=cert_fetcher)
        for cert in certs:
            result.register(cert)

        result.fetcher = cert_fetcher
        return result

    def retrieve_by_name(
        self,
        name: x509.Name,
        first_certificate: Optional[x509.Certificate] = None,
    ):
        """
        Retrieves a list certs via their subject name

        :param name:
            An asn1crypto.x509.Name object

        :param first_certificate:
            An asn1crypto.x509.Certificate object that if found, should be
            placed first in the result list

        :return:
            A list of asn1crypto.x509.Certificate objects
        """

        output = []
        first = None
        for cert in super().retrieve_by_name(name):
            if first_certificate and first_certificate.sha256 == cert.sha256:
                first = cert
            else:
                output.append(cert)
        if first:
            output.insert(0, first)
        return output

    def find_potential_issuers(
        self, cert: x509.Certificate, trust_manager: TrustManager
    ) -> Iterator[Union[TrustAnchor, x509.Certificate]]:
        issuer_hashable = cert.issuer.hashable

        # Info from the authority key identifier extension can be used to
        # eliminate possible options when multiple keys with the same
        # subject exist, such as during a transition, or with cross-signing.

        # go through matching trust roots first
        yield from trust_manager.find_potential_issuers(cert)

        for issuer in self._subject_map[issuer_hashable]:
            if trust_manager.is_root(issuer):
                continue  # skip, we've had these in the previous step
            if cert.authority_key_identifier and issuer.key_identifier:
                if cert.authority_key_identifier != issuer.key_identifier:
                    continue
            elif cert.authority_issuer_serial:
                if cert.authority_issuer_serial != issuer.issuer_serial:
                    continue

            yield issuer

    async def fetch_missing_potential_issuers(self, cert: x509.Certificate):
        if self.fetcher is None:
            return

        issuers = [
            issuer async for issuer in self.fetcher.fetch_cert_issuers(cert)
        ]
        self.register_multiple(issuers)

        for issuer in issuers:
            yield issuer


class PathBuilder:
    """
    Class to handle path building.
    """

    def __init__(
        self, trust_manager: TrustManager, registry: CertificateRegistry
    ):
        self.trust_manager = trust_manager
        self.registry = registry

    def build_paths(self, end_entity_cert):
        """
        Builds a list of ValidationPath objects from a certificate in the
        operating system trust store to the end-entity certificate

        .. note::
            This is a synchronous equivalent of :meth:`async_build_paths`
            that calls the latter in a new event loop. As such, it can't be used
            from within asynchronous code.

        :param end_entity_cert:
            A byte string of a DER or PEM-encoded X.509 certificate, or an
            instance of asn1crypto.x509.Certificate

        :return:
            A list of pyhanko_certvalidator.path.ValidationPath objects that
            represent the possible paths from the end-entity certificate to one
            of the CA certs.
        """
        return asyncio.run(self.async_build_paths(end_entity_cert))

    async def async_build_paths(self, end_entity_cert: x509.Certificate):
        """
        Builds a list of ValidationPath objects from a certificate in the
        operating system trust store to the end-entity certificate, returning
        all paths in a single list.

        :param end_entity_cert:
            A byte string of a DER or PEM-encoded X.509 certificate, or an
            instance of asn1crypto.x509.Certificate

        :return:
            A list of pyhanko_certvalidator.path.ValidationPath objects that
            represent the possible paths from the end-entity certificate to one
            of the CA certs.
        """

        paths: List[ValidationPath] = []
        async for result in self.async_build_paths_lazy(end_entity_cert):
            paths.append(result)

        return paths

    def async_build_paths_lazy(
        self, end_entity_cert: x509.Certificate
    ) -> CancelableAsyncIterator[ValidationPath]:
        """
        Builds a list of ValidationPath objects from a certificate in the
        operating system trust store to the end-entity certificate, and emit
        them as an asynchronous generator.

        :param end_entity_cert:
            A byte string of a DER or PEM-encoded X.509 certificate, or an
            instance of asn1crypto.x509.Certificate

        :return:
            An asynchronous iterator that yields
            pyhanko_certvalidator.path.ValidationPath objects that
            represent the possible paths from the end-entity certificate to one
            of the CA certs, and raises PathBuildingError
            if no paths could be built
        """

        walker = _PathWalker(
            self,
            path=ConsList.sing(end_entity_cert),
            certs_seen=ConsList.sing(end_entity_cert.issuer_serial),
            failed_paths=[],
        )
        return LazyPathIterator(walker, end_entity_cert)


class _IssuerFetcher:
    def __init__(
        self,
        path_builder: 'PathBuilder',
        cert: x509.Certificate,
        certs_seen: ConsList[bytes],
    ):
        self.cert = cert
        self.path_builder = path_builder
        self.certs_seen = certs_seen
        local_issuers = self.path_builder.registry.find_potential_issuers(
            cert, self.path_builder.trust_manager
        )
        self.local_iss_iter = iter(local_issuers)
        self.local_issuers_found = 0
        self.fetched_issuers_found = 0
        self._fetched_cas: Optional[AsyncGenerator[x509.Certificate, None]] = (
            None
        )
        self._fetching_done = False

    @property
    def issuers_found(self):
        return self.local_issuers_found + self.fetched_issuers_found

    def __aiter__(self):
        return self

    def __iter__(self):
        return self

    def __next__(self) -> Union[TrustAnchor, x509.Certificate]:
        for issuer in self.local_iss_iter:
            if isinstance(issuer, x509.Certificate):
                cert_id = issuer.issuer_serial
                if cert_id in self.certs_seen:  # no duplicates
                    continue
            self.local_issuers_found += 1
            return issuer
        raise StopIteration

    async def __anext__(self) -> Union[TrustAnchor, x509.Certificate]:
        try:
            return next(self)
        except StopIteration:
            pass

        if (
            self._fetched_cas is None
            and not self.local_issuers_found
            and not self._fetching_done
        ):
            # attempt to download certs only if we didn't find anything locally
            self._fetched_cas = (
                self.path_builder.registry.fetch_missing_potential_issuers(
                    self.cert
                )
            )

        if self._fetched_cas is not None:
            async for issuer in self._fetched_cas:
                cert_id = issuer.issuer_serial
                if cert_id in self.certs_seen:
                    continue
                self.fetched_issuers_found += 1
                return issuer
            self._fetching_done = True
        raise StopAsyncIteration

    async def cancel(self):
        if self._fetched_cas is not None:
            await self._fetched_cas.aclose()
            self._fetched_cas = None
            self._fetching_done = True


class _PathWalker:
    def __init__(
        self,
        path_builder: 'PathBuilder',
        path: ConsList[x509.Certificate],
        certs_seen: ConsList[bytes],
        failed_paths: List[ConsList[x509.Certificate]],
    ):
        self.path = path
        self.path_builder = path_builder
        self.certs_seen = certs_seen
        cert = path.head
        assert isinstance(cert, x509.Certificate)
        self._issuer_fetcher = _IssuerFetcher(path_builder, cert, certs_seen)
        self.failed_paths = failed_paths
        self._next_level: Optional[_PathWalker] = None

    async def cancel(self):
        if self._issuer_fetcher is not None:
            await self._issuer_fetcher.cancel()
            self._issuer_fetcher = None
        if self._next_level is not None:
            await self._next_level.cancel()
            self._next_level = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._issuer_fetcher is None:
            raise StopAsyncIteration  # pragma: nocover
        next_path = None
        while next_path is None:
            if self._next_level is None:
                # Fetch the next candidate issuer in the list
                try:
                    next_issuer = await self._issuer_fetcher.__anext__()
                except StopAsyncIteration as e:
                    if not self._issuer_fetcher.issuers_found:
                        self.failed_paths.append(self.path)
                    self._issuer_fetcher = None
                    raise e
                if isinstance(next_issuer, TrustAnchor):
                    # We've reached a trust root -> emit path and stop
                    certs = list(self.path)
                    return ValidationPath(next_issuer, certs[:-1], certs[-1])
                else:
                    # if it's not a trust root, we need a new child _PathWalker
                    self._next_level = _PathWalker(
                        self.path_builder,
                        self.path.cons(next_issuer),
                        self.certs_seen.cons(next_issuer.issuer_serial),
                        self.failed_paths,
                    )
            # check if next_level has any paths left, if not we clear it
            # and loop around to look at the next issuer
            try:
                next_path = await self._next_level.__anext__()
            except StopAsyncIteration:
                self._next_level = None
        return next_path


class LazyPathIterator(CancelableAsyncIterator[ValidationPath]):
    _as_root: Optional[ValidationPath] = None

    def __init__(self, walker: _PathWalker, cert: x509.Certificate):
        # special case for root certs
        if walker.path_builder.trust_manager.is_root(cert):
            self._as_root = ValidationPath(CertTrustAnchor(cert), [], None)
        self._walker: Optional[_PathWalker] = walker
        self.emitted_count = 0
        self._name = cert.subject.human_friendly

    async def cancel(self):
        if self._walker is not None:
            await self._walker.cancel()

    def __aiter__(self):
        return self

    async def __anext__(self) -> ValidationPath:
        if self._walker is None:
            raise StopAsyncIteration
        elif self._as_root is not None:
            self.emitted_count += 1
            self._walker = None
            return self._as_root

        try:
            next_path = await self._walker.__anext__()
            self.emitted_count += 1
            return next_path
        except StopAsyncIteration:
            pass

        if self.emitted_count == 0:
            path_head = self._walker.failed_paths[0].head
            assert isinstance(path_head, x509.Certificate)
            missing_issuer_name = path_head.issuer.human_friendly
            self._walker = None
            raise PathBuildingError(
                f"Unable to build a validation path for the certificate "
                f"\"{self._name}\" - no issuer matching "
                f"\"{missing_issuer_name}\" was found"
            )
        raise StopAsyncIteration


class LayeredCertificateStore(CertificateCollection):
    """
    Trustless certificate store that looks up certificates in other stores
    in a specific order.
    """

    def __init__(self, stores: List[CertificateCollection]):
        self._stores = stores

    def retrieve_many_by_key_identifier(self, key_identifier: bytes):
        def _gen():
            for store in self._stores:
                yield from store.retrieve_many_by_key_identifier(key_identifier)

        return list(_gen())

    def retrieve_by_name(self, name: x509.Name):
        def _gen():
            for store in self._stores:
                yield from store.retrieve_by_name(name)

        return list(_gen())

    def retrieve_by_issuer_serial(self, issuer_serial):
        for store in self._stores:
            result = store.retrieve_by_issuer_serial(issuer_serial)
            if result is not None:
                return result
        return None
