from office365.directory.insights.shared import SharedInsight
from office365.directory.insights.trending import Trending
from office365.directory.insights.used import UsedInsight
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class OfficeGraphInsights(Entity):
    """
    Insights are relationships calculated using advanced analytics and machine learning techniques.
    You can, for example, identify OneDrive for Business documents trending around users.
    """

    @property
    def shared(self):
        # type: () -> EntityCollection[SharedInsight]
        """
        Calculated relationship identifying documents shared with or by the user. This includes URLs, file attachments,
        and reference attachments to OneDrive for Business and SharePoint files found in Outlook messages and meetings.
        This also includes URLs and reference attachments to Teams conversations. Ordered by recency of share.
        """
        return self.properties.get(
            "shared",
            EntityCollection(
                self.context, SharedInsight, ResourcePath("shared", self.resource_path)
            ),
        )

    @property
    def trending(self):
        # type: () -> EntityCollection[Trending]
        """
        Calculated relationship identifying documents trending around a user.
        Trending documents are calculated based on activity of the user's closest network of people and include
        files stored in OneDrive for Business and SharePoint. Trending insights help the user to discover
        potentially useful content that the user has access to, but has never viewed before.
        """
        return self.properties.get(
            "trending",
            EntityCollection(
                self.context, Trending, ResourcePath("trending", self.resource_path)
            ),
        )

    @property
    def used(self):
        # type: () -> EntityCollection[UsedInsight]
        """
        Calculated relationship identifying the latest documents viewed or modified by a user,
        including OneDrive for Business and SharePoint documents, ranked by recency of use.
        """
        return self.properties.get(
            "used",
            EntityCollection(
                self.context, UsedInsight, ResourcePath("used", self.resource_path)
            ),
        )
