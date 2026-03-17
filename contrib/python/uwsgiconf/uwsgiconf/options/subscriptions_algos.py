from ..base import ParametrizedValue


class BalancingAlgorithm(ParametrizedValue):

    name_separator = ''


class BalancingAlgorithmWithBackup(BalancingAlgorithm):

    def __init__(self, backup_level=None):
        self.backup_level = backup_level
        super().__init__()


class WeightedRoundRobin(BalancingAlgorithmWithBackup):
    """Weighted round robin algorithm with backup support.
    The default algorithm.

    """
    name = 'wrr'


class LeastReferenceCount(BalancingAlgorithmWithBackup):
    """Least reference count algorithm with backup support."""

    name = 'lrc'


class WeightedLeastReferenceCount(BalancingAlgorithmWithBackup):
    """Weighted least reference count algorithm with backup support."""

    name = 'wlrc'


class IpHash(BalancingAlgorithmWithBackup):
    """IP hash algorithm with backup support."""

    name = 'iphash'
