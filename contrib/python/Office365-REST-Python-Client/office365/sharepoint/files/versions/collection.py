from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.files.versions.version import FileVersion


class FileVersionCollection(EntityCollection[FileVersion]):
    """Represents a collection of FileVersion."""

    def __init__(self, context, resource_path=None):
        super(FileVersionCollection, self).__init__(context, FileVersion, resource_path)

    def get_by_id(self, version_id):
        """Gets the file version with the specified ID."""
        return FileVersion(
            self.context,
            ServiceOperationPath("getById", [version_id], self.resource_path),
        )

    def get_by_label(self, label):
        """Gets the file version with the specified Label."""
        return self.single("VersionLabel eq '{0}'".format(label))

    def delete_all(self):
        """Deletes all the file version objects in the collection."""
        qry = ServiceOperationQuery(self, "DeleteAll")
        self.context.add_query(qry)
        return self

    def delete_by_id(self, vid):
        """Removes the file version object with the specified integer ID from the collection.
        :param int vid: The file version to remove.
        """
        qry = ServiceOperationQuery(self, "DeleteByID", {"vid": vid})
        self.context.add_query(qry)
        return self

    def delete_by_label(self, label):
        """
        Deletes the file version object with the specified version label.
        :param str label: The file version to remove.
        """
        qry = ServiceOperationQuery(self, "DeleteByLabel", {"versionlabel": label})
        self.context.add_query(qry)
        return self

    def recycle_by_id(self, vid):
        """
        Recycles a file version objects in the collection by version identifier.

        :param int vid: The file version to remove.
        """
        qry = ServiceOperationQuery(self, "RecycleByID", {"vid": vid})
        self.context.add_query(qry)
        return self

    def recycle_by_label(self, label):
        """
        Recycles the file version object with the specified version label.

        :param str label: The file version to remove.
        """
        qry = ServiceOperationQuery(self, "RecycleByLabel", {"versionlabel": label})
        self.context.add_query(qry)
        return self

    def restore_by_label(self, label):
        """
        Restores the file version object that has the specified version label.

        :param str label: The file version to remove.
        """
        qry = ServiceOperationQuery(self, "RestoreByLabel", {"versionlabel": label})
        self.context.add_query(qry)
        return self
