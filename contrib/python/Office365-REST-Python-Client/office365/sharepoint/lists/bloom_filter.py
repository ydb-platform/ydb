from typing import Optional

from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.entity import Entity


class ListBloomFilter(Entity):
    """Specifies a Bloom filter (probabilistic structure for checking the existence of list items)."""

    @property
    def bloom_filter_size(self):
        # type: () -> Optional[int]
        """The length of the Bloom Filter"""
        return self.properties.get("BloomFilterSize", None)

    @property
    def index_map(self):
        """Specifies a list of bloom indexes for item."""
        return self.properties.get("IndexMap", ClientValueCollection(int))
