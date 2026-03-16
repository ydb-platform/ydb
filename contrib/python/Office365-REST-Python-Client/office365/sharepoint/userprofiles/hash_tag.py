from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.entity import Entity


class HashTag(ClientValue):
    def __init__(self, name=None, use_count=None):
        """
        The HashTag type specifies a string that is being used as a hash tag and a count of the tags use.

        :param str name: The Name property specifies the hash tag string.
        :param int use_count: The UseCount property specifies the number of times that the hash tag is used.
        """
        self.Name = name
        self.UseCount = use_count

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.HashTag"


class HashTagCollection(Entity):
    """The HashTagCollection class specifies a collection of HashTags. For information about the HashTag type,
    see section 3.1.5.55"""

    @property
    def items(self):
        return self.properties.get("Items", ClientValueCollection(HashTag))
