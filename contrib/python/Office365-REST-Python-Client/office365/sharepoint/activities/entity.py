from office365.sharepoint.activities.action_facet import ActionFacet
from office365.sharepoint.activities.facets.activity_time import ActivityTimeFacet
from office365.sharepoint.activities.facets.coalesced import CoalescedFacet
from office365.sharepoint.activities.facets.in_doc import InDocFacet
from office365.sharepoint.activities.facets.resource import ResourceFacet
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.principal import Principal


class SPActivityEntity(Entity):
    """"""

    @property
    def action(self):
        return self.properties.get("action", ActionFacet())

    @property
    def actor(self):
        return self.properties.get("actor", Principal())

    @property
    def is_coalesced(self):
        return self.properties.get("isCoalesced", CoalescedFacet())

    @property
    def doc_details(self):
        return self.properties.get("docDetails", InDocFacet())

    @property
    def resource(self):
        return self.properties.get("resource", ResourceFacet())

    @property
    def times(self):
        return self.properties.get("times", ActivityTimeFacet())

    @property
    def property_ref_name(self):
        return "id"

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.SPActivityEntity"

    def set_property(self, name, value, persist_changes=True):
        super(SPActivityEntity, self).set_property(name, value, persist_changes)
        if name == self.property_ref_name:
            self._resource_path.patch(value)
        return self
