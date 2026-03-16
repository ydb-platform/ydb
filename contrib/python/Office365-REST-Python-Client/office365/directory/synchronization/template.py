from office365.directory.synchronization.schema import SynchronizationSchema
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class SynchronizationTemplate(Entity):
    """
    Provides pre-configured synchronization settings for a particular application. These settings will be used by
    default for any synchronization job that is based on the template. The application developer specifies the template;
    anyone can retrieve the template to see the default settings, including the synchronization schema.

    You can provide multiple templates for an application, and designate a default template. If multiple templates
    are available for the application you're interested in, seek application-specific guidance to determine which one
    best meets your needs.
    """

    @property
    def schema(self):
        """Default synchronization schema for the jobs based on this template."""
        return self.properties.get(
            "schema",
            SynchronizationSchema(
                self.context, ResourcePath("schema", self.resource_path)
            ),
        )
