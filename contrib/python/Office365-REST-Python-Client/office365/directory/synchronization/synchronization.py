from office365.directory.synchronization.job import SynchronizationJob
from office365.directory.synchronization.template import SynchronizationTemplate
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class Synchronization(Entity):
    """
    Represents the capability for Azure Active Directory (Azure AD) identity synchronization through
    the Microsoft Graph API. Identity synchronization (also called provisioning) allows you to automate
    the provisioning (creation, maintenance) and de-provisioning (removal) of user identities and roles from
    Azure AD to supported cloud applications. For more information, see How Application Provisioning works
    in Azure Active Directory
    """

    @property
    def jobs(self):
        # type: () -> EntityCollection[SynchronizationJob]
        """
        Performs synchronization by periodically running in the background, polling for changes in one directory,
        and pushing them to another directory.
        """
        return self.properties.get(
            "jobs",
            EntityCollection(
                self.context,
                SynchronizationJob,
                ResourcePath("jobs", self.resource_path),
            ),
        )

    @property
    def templates(self):
        # type: () -> EntityCollection[SynchronizationTemplate]
        """
        Performs synchronization by periodically running in the background, polling for changes in one directory,
        and pushing them to another directory.
        """
        return self.properties.get(
            "templates",
            EntityCollection(
                self.context,
                SynchronizationTemplate,
                ResourcePath("templates", self.resource_path),
            ),
        )
