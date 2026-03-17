from office365.directory.security.cases.ediscovery import EdiscoveryCase
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class CasesRoot(Entity):
    """"""

    @property
    def ediscovery_cases(self):
        return self.properties.get(
            "ediscoveryCases",
            EntityCollection(
                self.context,
                EdiscoveryCase,
                ResourcePath("ediscoveryCases", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"ediscoveryCases": self.ediscovery_cases}
            default_value = property_mapping.get(name, None)
        return super(CasesRoot, self).get_property(name, default_value)
