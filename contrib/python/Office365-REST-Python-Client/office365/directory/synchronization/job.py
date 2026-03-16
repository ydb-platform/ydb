from office365.directory.synchronization.schema import SynchronizationSchema
from office365.directory.synchronization.status import SynchronizationStatus
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class SynchronizationJob(Entity):
    """
    Performs synchronization by periodically running in the background, polling for changes in one directory,
    and pushing them to another directory. The synchronization job is always specific to a particular instance
    of an application in your tenant. As part of the synchronization job setup, you need to give authorization
    to read and write objects in your target directory, and customize the job's synchronization schema.
    """

    def pause(self):
        """
        Temporarily stop a running synchronization job. All the progress, including job state, is persisted, and the
        job will continue from where it left off when a start call is made.
        """
        qry = ServiceOperationQuery(self, "pause")
        self.context.add_query(qry)
        return self

    def start(self):
        """
        Start an existing synchronization job. If the job is in a paused state, it will continue processing changes
        from the point where it was paused. If the job is in quarantine, the quarantine status will be cleared.
        Do not create scripts to call the start job continuously while it's running because that can cause the service
        to stop running. Use the start job only when the job is currently paused or in quarantine.
        """
        qry = ServiceOperationQuery(self, "start")
        self.context.add_query(qry)
        return self

    @property
    def status(self):
        """Status of the job, which includes when the job was last run, current job state, and errors."""
        return self.properties.get("status", SynchronizationStatus())

    @property
    def schema(self):
        """The synchronization schema configured for the job."""
        return self.properties.get(
            "schema",
            SynchronizationSchema(
                self.context, ResourcePath("schema", self.resource_path)
            ),
        )
