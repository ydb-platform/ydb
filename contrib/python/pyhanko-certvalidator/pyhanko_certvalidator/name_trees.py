import enum
import logging
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import Callable, Dict, Iterable, List, Optional, Set, Union

from asn1crypto import x509
from uritools import urisplit

logger = logging.getLogger(__name__)


class NameConstraintError(ValueError):
    pass


def host_tree_contains(base_host: str, other_host: str) -> bool:
    # if the constraint starts with '.', it specifies a domain, and must be
    # expanded with one or more labels, otherwise it refers to a single host.
    if base_host[0] == '.':
        pre, _, post = other_host.rpartition(base_host)
        return bool(pre) and not bool(post)
    else:
        return other_host == base_host


def _host_regname(cand_uri):
    cand_host = urisplit(cand_uri).gethost()
    if not cand_host or isinstance(cand_host, (IPv4Address, IPv6Address)):
        host_err = (
            f'has host {cand_host}.'
            if cand_host is not None
            else ('is not a well-formed URI.')
        )
        msg = (
            "URI constraints require URIs with a host specified as a FQDN; "
            f"URI '{cand_uri}' {host_err}."
        )
        logger.warning(msg)
        raise NameConstraintError(msg)
    return cand_host


def uri_tree_contains(base: str, other: str) -> bool:
    # The constraint applies to the host part
    other_host: str = _host_regname(other)
    return host_tree_contains(base, other_host)


def dns_tree_contains(base: str, other: str):
    # check if 'other' consists of adding zero or more labels to 'base'
    #  (from the left)
    base_labels = base.split('.')
    other_labels = other.split('.')
    if len(other_labels) < len(base_labels):
        return False
    return len(other_labels) >= len(base_labels) and all(
        x == y for x, y in zip(reversed(other_labels), reversed(base_labels))
    )


def email_tree_contains(base: str, other: str):
    # use rpartition instead of rsplit to deal with the case where there's no @
    # uniformly
    base_mailbox, _, base_host_or_domain = base.rpartition('@')
    other_mailbox, _, other_host_or_domain = other.rpartition('@')

    if base_mailbox:
        # only exact match
        return base == other
    else:
        return host_tree_contains(base_host_or_domain, other_host_or_domain)


def dirname_tree_contains(base: x509.Name, other: x509.Name):
    base_rdn_sequence = base.chosen
    other_rdn_sequence = other.chosen

    return len(other_rdn_sequence) >= len(base_rdn_sequence) and all(
        x == y for x, y in zip(base_rdn_sequence, other_rdn_sequence)
    )


# TODO support IP address constraints as well


class GeneralNameType(enum.Enum):
    OTHER_NAME = enum.auto()
    RFC822_NAME = enum.auto()
    DNS_NAME = enum.auto()
    X400_ADDRESS = enum.auto()
    DIRECTORY_NAME = enum.auto()
    EDI_PARTY_NAME = enum.auto()
    UNIFORM_RESOURCE_IDENTIFIER = enum.auto()
    IP_ADDRESS = enum.auto()
    REGISTERED_ID = enum.auto()

    @property
    def check_membership(
        self,
    ) -> Optional[
        Callable[[Union[str, x509.Name], Union[str, x509.Name]], bool]
    ]:
        return _name_type_checkers.get(self, None)

    @classmethod
    def from_choice(cls, choice) -> 'GeneralNameType':
        return getattr(cls, choice.upper())


_name_type_checkers = {
    GeneralNameType.DIRECTORY_NAME: dirname_tree_contains,
    GeneralNameType.RFC822_NAME: email_tree_contains,
    GeneralNameType.DNS_NAME: dns_tree_contains,
    GeneralNameType.UNIFORM_RESOURCE_IDENTIFIER: uri_tree_contains,
}


class UnsupportedNameTypeError(NotImplementedError):
    def __init__(self, name_type: GeneralNameType):
        super().__init__(name_type.name.lower())


def _interpret_general_name(gname: x509.GeneralName):
    gname_type = GeneralNameType.from_choice(gname.name)
    value = gname.chosen
    # for directory names, we keep the Name object,but everything
    # else gets converted to a string representation
    if gname_type != GeneralNameType.DIRECTORY_NAME:
        value = value.native
    return gname_type, value


def _enumerate_names_in_cert(cert: x509.Certificate):
    # start with the subject's distinguished name, if it is non-empty
    if len(cert.subject.chosen):
        yield GeneralNameType.DIRECTORY_NAME, cert.subject

    subject_alt_names: x509.GeneralNames = cert.subject_alt_name_value
    if subject_alt_names is None:
        # if the subject has email address component(s) and no subjectAltName
        # name constraints for rfc822Name-type names should also apply to those
        # addresses
        name_pair: x509.NameTypeAndValue
        for rdn in cert.subject.chosen:
            for name_pair in rdn:
                if name_pair['type'].native == 'email_address':
                    yield GeneralNameType.RFC822_NAME, name_pair['value'].native
    else:
        for name in subject_alt_names:
            yield _interpret_general_name(name)


class _StringOrName:
    # Wrapper class for hashing purposes. Not for external use.

    def __init__(self, value: Union[str, x509.Name]):
        self.value = value

    @property
    def _code(self):
        val = self.value
        if isinstance(val, x509.Name):
            return 0, val.dump()
        else:
            return 1, val

    def __hash__(self):
        return hash(self._code)

    def __eq__(self, other):
        return isinstance(other, _StringOrName) and self._code == other._code


@dataclass(frozen=True)
class NameSubtree:
    name_type: GeneralNameType
    tree_base: Optional[_StringOrName]
    min: int = 0
    max: Optional[int] = None

    def __contains__(self, item: Union[str, x509.Name]) -> bool:
        if self.tree_base is None:  # special value: accept all certs
            return True

        # TODO processing min / max for DNs and DNS names would make sense
        if self.min != 0 or self.max is not None:
            raise NotImplementedError(
                "The minimum/maximum fields on a name constraint are not "
                "meaningful in the PKIX (RFC 5280) profile --- not processing."
            )
        checker = self.name_type.check_membership
        if checker is None:
            raise NotImplementedError(
                f"No containment checker available for {self.name_type}"
            )
        return checker(self.tree_base.value, item)

    @classmethod
    def from_name(cls, name_type: GeneralNameType, name: Union[str, x509.Name]):
        return NameSubtree(name_type=name_type, tree_base=_StringOrName(name))

    @classmethod
    def from_general_subtree(cls, subtree) -> 'NameSubtree':
        gname = subtree['base']
        name_type, name_obj = _interpret_general_name(gname)
        return NameSubtree(
            name_type,
            _StringOrName(name_obj),
            min=subtree['minimum'].native,
            max=subtree['maximum'].native,
        )

    @classmethod
    def universal_tree(cls, name_type: GeneralNameType) -> 'NameSubtree':
        """
        Tree that contains all names of a given type.

        :param name_type:
            The name type to use.
        :return:
        """
        return NameSubtree(name_type=name_type, tree_base=None)


# a subtree collection as used in the PKIX validation algorithm
PKIXSubtrees = Dict[GeneralNameType, Set[NameSubtree]]


def x509_names_to_subtrees(names: Iterable[x509.Name]) -> PKIXSubtrees:
    def _subtree(name: x509.Name):
        return NameSubtree.from_name(
            name_type=GeneralNameType.DIRECTORY_NAME, name=name
        )

    return {GeneralNameType.DIRECTORY_NAME: {_subtree(n) for n in names}}


def _group_subtrees(trees: Iterable[NameSubtree]) -> PKIXSubtrees:
    # This should NOT be a defaultdict, because the semantics of a tree
    # type not being present vs. the set being empty are very different!
    # If necessary, the caller can do a setdefault()
    result: PKIXSubtrees = {}
    for tree in trees:
        try:
            result[tree.name_type].add(tree)
        except KeyError:
            result[tree.name_type] = {tree}
    return result


def process_general_subtrees(subtrees: x509.GeneralSubtrees) -> PKIXSubtrees:
    return _group_subtrees(
        NameSubtree.from_general_subtree(subtree) for subtree in subtrees
    )


class NameConstraintValidationResult:
    def __init__(
        self,
        failing_name_type: Optional[GeneralNameType] = None,
        failing_name: Union[str, x509.Name, None] = None,
    ):
        self.failing_name_type: Optional[GeneralNameType] = failing_name_type
        self.failing_name: Union[str, x509.Name, None] = failing_name

    def __bool__(self):
        return self.failing_name_type is None

    @property
    def error_message(self):
        assert self.failing_name_type is not None
        name_str = self.failing_name
        if isinstance(name_str, x509.Name):
            name_str = name_str.human_friendly

        name_type = self.failing_name_type.name.lower()
        return f"The name '{name_str}' of type {name_type} is not allowed."


class PermittedSubtrees:
    def __init__(self, initial_permitted_subtrees: PKIXSubtrees):
        # The structure of self._trees is name_type -> list[tree set]
        # where each tree set in the list denotes a generation
        # For each "generation", there must be at least one tree that accepts
        # the name (i.e. later certificates can only restrict existing
        # constraints).
        # note: if the set of applicable trees is empty,
        # we reject the cert.
        # However, initial-permitted-subtrees (by default) includes a
        # universal acceptor for each name type in our implementation,
        # which seems to be what most implementations do.

        # We deep-copy the initial permitted subtrees
        trees: Dict[GeneralNameType, List[Set[NameSubtree]]] = {
            name_type: [set(initial_permitted_subtrees.get(name_type, ()))]
            for name_type in GeneralNameType
        }
        self._trees = trees

    def intersect_with(self, trees: PKIXSubtrees):
        # only change the values that appear in the new tree set!
        for name_type, new_permitted in trees.items():
            self._trees[name_type].append(new_permitted)

    def accept_name(self, name_type: GeneralNameType, name) -> bool:
        # make sure that name is contained in the intersection of all whitelist
        # filters we accumulated.
        # Run through the list in reverse order (newest first) to apply the
        #  (generally) strictest conditions first
        try:
            return all(
                any(name in tree for tree in trees_in_generation)
                for trees_in_generation in reversed(self._trees[name_type])
            )
        except NameConstraintError:
            return False

    def accept_cert(
        self, cert: x509.Certificate
    ) -> NameConstraintValidationResult:
        try:
            failing_name_type, failing_name = next(
                (name_type, name)
                for name_type, name in _enumerate_names_in_cert(cert)
                if not self.accept_name(name_type, name)
            )
            return NameConstraintValidationResult(
                failing_name_type=failing_name_type, failing_name=failing_name
            )
        except StopIteration:
            return NameConstraintValidationResult()


class ExcludedSubtrees:
    def __init__(self, initial_excluded_subtrees: PKIXSubtrees):
        # The situation is not fully symmetric with the whitelist case:
        # here, we don't need to remember individual generations of blacklists,
        # we can just take unions to strictify conditions as we move along the
        # path under scrutiny.
        self._trees: PKIXSubtrees = {
            name_type: set(tree_set)
            for name_type, tree_set in initial_excluded_subtrees.items()
        }

    def union_with(self, trees: PKIXSubtrees):
        # only change the values that appear in the new tree set!
        for name_type, new_excluded in trees.items():
            self._trees[name_type].update(new_excluded)

    def reject_name(self, name_type: GeneralNameType, name) -> bool:
        try:
            return any(name in tree for tree in self._trees[name_type])
        except NameConstraintError:
            return True

    def accept_cert(
        self, cert: x509.Certificate
    ) -> NameConstraintValidationResult:
        try:
            failing_name_type, failing_name = next(
                (name_type, name)
                for name_type, name in _enumerate_names_in_cert(cert)
                if self.reject_name(name_type, name)
            )
            return NameConstraintValidationResult(
                failing_name_type=failing_name_type, failing_name=failing_name
            )
        except StopIteration:
            return NameConstraintValidationResult()


def default_permitted_subtrees() -> PKIXSubtrees:
    return {
        name_type: {NameSubtree.universal_tree(name_type)}
        for name_type in GeneralNameType
    }


def default_excluded_subtrees() -> PKIXSubtrees:
    return {name_type: set() for name_type in GeneralNameType}
