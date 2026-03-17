from collections import defaultdict
from typing import Iterable, Optional, Set

from asn1crypto import x509

from ._state import ValProcState
from .errors import PathValidationError


def update_policy_tree(
    certificate_policies,
    valid_policy_tree: 'PolicyTreeRoot',
    depth: int,
    any_policy_uninhibited: bool,
) -> Optional['PolicyTreeRoot']:
    """
    Internal method to update the policy tree during RFC 5280 validation.
    """

    cert_any_policy = None
    cert_policy_identifiers = set()

    # Step 2 d 1
    for policy in certificate_policies:
        policy_identifier = policy['policy_identifier'].native

        if policy_identifier == 'any_policy':
            cert_any_policy = policy
            continue

        cert_policy_identifiers.add(policy_identifier)

        policy_qualifiers = policy['policy_qualifiers']

        policy_id_match = False
        parent_any_policy = None

        # Step 2 d 1 i
        for node in valid_policy_tree.at_depth(depth - 1):
            if node.valid_policy == 'any_policy':
                parent_any_policy = node
            if policy_identifier not in node.expected_policy_set:
                continue
            policy_id_match = True
            node.add_child(
                policy_identifier, policy_qualifiers, {policy_identifier}
            )

        # Step 2 d 1 ii
        if not policy_id_match and parent_any_policy:
            parent_any_policy.add_child(
                policy_identifier, policy_qualifiers, {policy_identifier}
            )

    # Step 2 d 2
    if cert_any_policy and any_policy_uninhibited:
        for node in valid_policy_tree.at_depth(depth - 1):
            for expected_policy_identifier in node.expected_policy_set:
                if expected_policy_identifier not in cert_policy_identifiers:
                    node.add_child(
                        expected_policy_identifier,
                        cert_any_policy['policy_qualifiers'],
                        {expected_policy_identifier},
                    )

    # Step 2 d 3
    valid_policy_tree = _prune_policy_tree(valid_policy_tree, depth - 1)
    return valid_policy_tree


def _prune_policy_tree(valid_policy_tree, depth):
    for node in valid_policy_tree.walk_up(depth):
        if not node.children:
            node.parent.remove_child(node)
    if not valid_policy_tree.children:
        valid_policy_tree = None
    return valid_policy_tree


def enumerate_policy_mappings(
    mappings: Iterable[x509.PolicyMapping], proc_state: ValProcState
):
    """
    Internal function to process policy mapping extension values into
    a Python dictionary mapping issuer domain policies to the corresponding
    policies in the subject policy domain.
    """
    policy_map = defaultdict(set)
    for mapping in mappings:
        issuer_domain_policy = mapping['issuer_domain_policy'].native
        subject_domain_policy = mapping['subject_domain_policy'].native

        policy_map[issuer_domain_policy].add(subject_domain_policy)

        # Step 3 a
        if (
            issuer_domain_policy == 'any_policy'
            or subject_domain_policy == 'any_policy'
        ):
            raise PathValidationError.from_state(
                f"The path could not be validated because "
                f"{proc_state.describe_cert()} contains "
                f"a policy mapping for the \"any policy\"",
                proc_state,
            )

    return policy_map


def apply_policy_mapping(
    policy_map, valid_policy_tree, depth: int, policy_mapping_uninhibited: bool
):
    """
    Internal function to apply the policy mapping to the current policy tree
    in accordance with the algorithm in RFC 5280.
    """

    for issuer_domain_policy, subject_domain_policies in policy_map.items():
        # Step 3 b 1
        if policy_mapping_uninhibited:
            issuer_domain_policy_match = False
            cert_any_policy = None

            for node in valid_policy_tree.at_depth(depth):
                if node.valid_policy == 'any_policy':
                    cert_any_policy = node
                if node.valid_policy == issuer_domain_policy:
                    issuer_domain_policy_match = True
                    node.expected_policy_set = subject_domain_policies

            if not issuer_domain_policy_match and cert_any_policy:
                cert_any_policy.parent.add_child(
                    issuer_domain_policy,
                    cert_any_policy.qualifier_set,
                    subject_domain_policies,
                )

        # Step 3 b 2
        else:
            for node in valid_policy_tree.at_depth(depth):
                if node.valid_policy == issuer_domain_policy:
                    node.parent.remove_child(node)
            valid_policy_tree = _prune_policy_tree(valid_policy_tree, depth - 1)
    return valid_policy_tree


def prune_unacceptable_policies(
    path_length, valid_policy_tree, acceptable_policies
) -> Optional['PolicyTreeRoot']:
    # Step 4 g iii 1: compute nodes that branch off any_policy
    #  In other words, find all policies that are valid and meaningful in
    #  the trust root(s) namespace. We don't care about what policy mapping
    #  transformed them into; that's taken care of by the validation
    #  algorithm.
    #  Note: set() consumes the iterator to avoid operating on the tree
    #  while iterating over it. Performance is probably not a concern
    #  anyhow.
    valid_policy_node_set = set(valid_policy_tree.nodes_in_current_domain())

    # Step 4 g iii 2: eliminate unacceptable policies
    def _filter_acceptable():
        for policy_node in valid_policy_node_set:
            policy_id = policy_node.valid_policy
            if policy_id == 'any_policy' or policy_id in acceptable_policies:
                yield policy_id
            else:
                policy_node.parent.remove_child(policy_node)

    # list of policies that were explicitly valid
    valid_and_acceptable = set(_filter_acceptable())

    # Step 4 g iii 3: if the final layer contains an anyPolicy node
    # (there can be at most one), expand it out into acceptable policies
    # that are not explicitly qualified already
    try:
        final_any_policy: PolicyTreeNode = next(
            policy_node
            for policy_node in valid_policy_tree.at_depth(path_length)
            if policy_node.valid_policy == 'any_policy'
        )
        wildcard_parent = final_any_policy.parent
        assert wildcard_parent is not None
        wildcard_quals = final_any_policy.qualifier_set
        for acceptable_policy in acceptable_policies - valid_and_acceptable:
            wildcard_parent.add_child(
                acceptable_policy, wildcard_quals, {acceptable_policy}
            )
        # prune the anyPolicy node
        wildcard_parent.remove_child(final_any_policy)
    except StopIteration:
        pass

    # Step 4 g iii 4: prune the policy tree
    return _prune_policy_tree(valid_policy_tree, path_length - 1)


class PolicyTreeRoot:
    """
    A generic policy tree node, used for the root node in the tree
    """

    @classmethod
    def init_policy_tree(cls, valid_policy, qualifier_set, expected_policy_set):
        """
        Accepts values for a PolicyTreeNode that will be created at depth 0

        :param valid_policy:
            A unicode string of a policy name or OID

        :param qualifier_set:
            An instance of asn1crypto.x509.PolicyQualifierInfos

        :param expected_policy_set:
            A set of unicode strings containing policy names or OIDs
        """
        root = PolicyTreeRoot()
        root.add_child(valid_policy, qualifier_set, expected_policy_set)
        return root

    def __init__(self):
        self.parent = None
        self.children = []

    def add_child(self, valid_policy, qualifier_set, expected_policy_set):
        """
        Creates a new PolicyTreeNode as a child of this node

        :param valid_policy:
            A unicode string of a policy name or OID

        :param qualifier_set:
            An instance of asn1crypto.x509.PolicyQualifierInfos

        :param expected_policy_set:
            A set of unicode strings containing policy names or OIDs
        """

        child = PolicyTreeNode(valid_policy, qualifier_set, expected_policy_set)
        child.parent = self
        self.children.append(child)

    def remove_child(self, child):
        """
        Removes a child from this node

        :param child:
            An instance of PolicyTreeNode
        """

        self.children.remove(child)

    def at_depth(self, depth) -> Iterable['PolicyTreeNode']:
        """
        Returns a generator yielding all nodes in the tree at a specific depth

        :param depth:
            An integer >= 0 of the depth of nodes to yield

        :return:
            A generator yielding PolicyTreeNode objects
        """

        for child in list(self.children):
            if depth == 0:
                yield child
            else:
                for grandchild in child.at_depth(depth - 1):
                    yield grandchild

    def walk_up(self, depth):
        """
        Returns a generator yielding all nodes in the tree at a specific depth,
        or above. Yields nodes starting with leaves and traversing up to the
        root.

        :param depth:
            An integer >= 0 of the depth of nodes to walk up from

        :return:
            A generator yielding PolicyTreeNode objects
        """

        for child in list(self.children):
            if depth != 0:
                for grandchild in child.walk_up(depth - 1):
                    yield grandchild
            yield child

    def nodes_in_current_domain(self) -> Iterable['PolicyTreeNode']:
        """
        Returns a generator yielding all nodes in the tree that are children
        of an ``any_policy`` node.
        """

        for child in self.children:
            yield child
            if child.valid_policy == 'any_policy':
                yield from child.nodes_in_current_domain()


class PolicyTreeNode(PolicyTreeRoot):
    """
    A policy tree node that is used for all nodes but the root
    """

    def __init__(
        self,
        valid_policy: str,
        qualifier_set: x509.PolicyQualifierInfos,
        expected_policy_set: Set[str],
    ):
        """
        :param valid_policy:
            A unicode string of a policy name or OID

        :param qualifier_set:
            An instance of asn1crypto.x509.PolicyQualifierInfos

        :param expected_policy_set:
            A set of unicode strings containing policy names or OIDs
        """
        super().__init__()

        self.valid_policy = valid_policy
        self.qualifier_set = qualifier_set
        self.expected_policy_set = expected_policy_set

    def path_to_root(self):
        node = self
        while node is not None:
            yield node
            node = node.parent
