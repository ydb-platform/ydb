import abc
import logging
from collections.abc import Iterable, Mapping

from aiokafka.cluster import ClusterMetadata
from aiokafka.coordinator.protocol import (
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
)

log = logging.getLogger(__name__)


class AbstractPartitionAssignor(abc.ABC):
    """Abstract assignor implementation which does some common grunt work (in particular
    collecting partition counts which are always needed in assignors).
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """.name should be a string identifying the assignor"""

    @classmethod
    @abc.abstractmethod
    def assign(
        cls,
        cluster: ClusterMetadata,
        members: Mapping[str, ConsumerProtocolMemberMetadata],
    ) -> dict[str, ConsumerProtocolMemberAssignment]:
        """Perform group assignment given cluster metadata and member subscriptions

        Arguments:
            cluster (ClusterMetadata): metadata for use in assignment
            members (dict of {member_id: MemberMetadata}): decoded metadata for
                each member in the group.

        Returns:
            dict: {member_id: MemberAssignment}
        """

    @classmethod
    @abc.abstractmethod
    def metadata(cls, topics: Iterable[str]) -> ConsumerProtocolMemberMetadata:
        """Generate ProtocolMetadata to be submitted via JoinGroupRequest.

        Arguments:
            topics (set): a member's subscribed topics

        Returns:
            MemberMetadata struct
        """

    @classmethod
    @abc.abstractmethod
    def on_assignment(cls, assignment: ConsumerProtocolMemberAssignment) -> None:
        """Callback that runs on each assignment.

        This method can be used to update internal state, if any, of the
        partition assignor.

        Arguments:
            assignment (MemberAssignment): the member's assignment
        """
