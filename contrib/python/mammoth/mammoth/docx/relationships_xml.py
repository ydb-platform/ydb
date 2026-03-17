import collections


class Relationships(object):
    def __init__(self, relationships):
        self._targets_by_id = dict(
            (relationship.relationship_id, relationship.target)
            for relationship in relationships
        )
        self._targets_by_type = collections.defaultdict(list)
        for relationship in relationships:
            self._targets_by_type[relationship.type].append(relationship.target)
    
    def find_target_by_relationship_id(self, key):
        return self._targets_by_id[key]
    
    def find_targets_by_type(self, relationship_type):
        return self._targets_by_type[relationship_type]


Relationships.EMPTY = Relationships([])


Relationship = collections.namedtuple("Relationship", ["relationship_id", "target", "type"])


def read_relationships_xml_element(element):
    children = element.find_children("relationships:Relationship")
    return Relationships(list(map(_read_relationship, children)))


def _read_relationship(element):
    relationship = Relationship(
        relationship_id=element.attributes["Id"],
        target=element.attributes["Target"],
        type=element.attributes["Type"],
    )
    return relationship
