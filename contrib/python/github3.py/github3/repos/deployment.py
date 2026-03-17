"""The module containing deployment logic."""
from .. import users
from ..models import GitHubCore


class Deployment(GitHubCore):
    """Representation of a deployment of a repository at a point in time.

    See also: https://developer.github.com/v3/repos/deployments/

    This object has the following attributes:

    .. attribute:: created_at

        A :class:`~datetime.datetime` representing the date and time when this
        deployment was created.

    .. attribute:: creator

        A :class:`~github3.users.ShortUser` representing the user who created
        this deployment.

    .. attribute:: description

        The description of this deployment as provided by the :attr:`creator`.

    .. attribute:: environment

        The environment targeted for this deployment, e.g., ``'production'``,
        ``'staging'``.

    .. attribute:: id

        The unique identifier of this deployment.

    .. attribute:: payload

        The JSON payload string sent as part to trigger this deployment.

    .. attribute:: ref

        The reference used to create this deployment, e.g.,
        ``'deploy-20140526'``.

    .. attribute:: sha

        The SHA1 of the branch on GitHub when it was deployed.

    .. attribute:: statuses_url

        The URL to retrieve the statuses of this deployment from the API.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the date and time
        when this deployment was most recently updated.
    """

    def _update_attributes(self, deployment):
        self._api = deployment["url"]
        self.created_at = self._strptime(deployment["created_at"])
        self.creator = users.ShortUser(deployment["creator"], self)
        self.description = deployment["description"]
        self.environment = deployment["environment"]
        self.id = deployment["id"]
        self.payload = deployment["payload"]
        self.ref = deployment["ref"]
        self.sha = deployment["sha"]
        self.statuses_url = deployment["statuses_url"]
        self.updated_at = self._strptime(deployment["updated_at"])

    def _repr(self):
        return f"<Deployment [{self.id} @ {self.sha}]>"

    def create_status(self, state, target_url=None, description=None):
        """Create a new deployment status for this deployment.

        :param str state:
            (required), The state of the status. Can be one of
            ``pending``, ``success``, ``error``, ``inactive``,
            ``in_progress``, ``queued``, or ``failure``.
        :param str target_url:
            The target URL to associate with this status.
            This URL should contain output to keep the user updated while the
            task is running or serve as historical information for what
            happened in the deployment. Default: ''.
        :param str description:
            A short description of the status. Default: ''.
        :return:
            the incomplete deployment status
        :rtype:
            :class:`~github3.repos.deployment.DeploymentStatus`
        """
        json = None

        if state in (
            "pending",
            "success",
            "error",
            "inactive",
            "in_progress",
            "queued",
            "failure",
        ):
            data = {
                "state": state,
                "target_url": target_url,
                "description": description,
            }
            self._remove_none(data)
            response = self._post(self.statuses_url, data=data)
            json = self._json(response, 201)

        return self._instance_or_null(DeploymentStatus, json)

    def statuses(self, number=-1, etag=None):
        """Iterate over the deployment statuses for this deployment.

        :param int number:
            (optional), the number of statuses to return.
            Default: -1, returns all statuses.
        :param str etag:
            (optional), the ETag header value from the last time
            you iterated over the statuses.
        :returns:
            generator of the statuses of this deployment
        :rtype:
            :class:`~github3.repos.deployment.DeploymentStatus`
        """
        i = self._iter(
            int(number), self.statuses_url, DeploymentStatus, etag=etag
        )
        return i


class DeploymentStatus(GitHubCore):
    """Representation of the status of a deployment of a repository.

    See also
    https://developer.github.com/v3/repos/deployments/#get-a-single-deployment-status

    This object has the following attributes:

    .. attribute:: created_at

        A :class:`~datetime.datetime` representing the date and time when this
        deployment status was created.

    .. attribute:: creator

        A :class:`~github3.users.ShortUser` representing the user who created
        this deployment status.

    .. attribute:: deployment_url

        The URL to retrieve the information about the deployment from the API.

    .. attribute:: description

        The description of this status as provided by the :attr:`creator`.

    .. attribute:: id

        The unique identifier of this deployment.

    .. attribute:: state

        The state of the deployment, e.g., ``'success'``.

    .. attribute:: target_url

        The URL to associate with this status. This should link to the output
        of the deployment.
    """

    def _update_attributes(self, status):
        self._api = status["url"]
        self.created_at = self._strptime(status["created_at"])
        self.creator = users.ShortUser(status["creator"], self)
        self.deployment_url = status["deployment_url"]
        self.description = status["description"]
        self.id = status["id"]
        self.state = status["state"]
        self.target_url = status["target_url"]
        self.updated_at = self._strptime(status["updated_at"])

    def _repr(self):
        return f"<DeploymentStatus [{self.id}]>"
