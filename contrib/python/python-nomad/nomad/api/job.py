"""Nomad job: https://developer.hashicorp.com/nomad/api-docs/jobs"""

from typing import Union

from nomad.api.base import Requester
import nomad.api.exceptions


class Job(Requester):
    """
    The job endpoint is used for CRUD on a single job.
    By default, the agent's local region is used.

    https://www.nomadproject.io/docs/http/job.html
    """

    ENDPOINT = "job"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def __contains__(self, item):
        try:
            self.get_job(item)
            return True
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __getitem__(self, item):
        try:
            job = self.get_job(item)
            if job["ID"] == item:
                return job
            if job["Name"] == item:
                return job

            raise KeyError
        except nomad.api.exceptions.URLNotFoundNomadException as exc:
            raise KeyError from exc

    def get_job(self, id_, namespace=None):
        """Query a single job for its specification and status.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
          - namespace :(str) optional, specifies the target namespace. Specifying * would return all jobs.
                    This is specified as a querystring parameter.
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        params = {}

        if namespace:
            params["namespace"] = namespace

        return self.request(id_, method="get", params=params).json()

    def get_versions(self, id_):
        """This endpoint reads information about all versions of a job.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id
        returns: list of dicts
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, "versions", method="get").json()

    def get_allocations(
        self,
        id_: str,
        all_: Union[bool, None] = None,
        namespace: Union[str, None] = None,
    ):
        """Query the allocations belonging to a single job.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
            - id_
            - all (bool optional)
            - namespace (str) optional.
            Specifies the target namespace. If ACL is enabled, this value
            must match a namespace that the token is allowed to access.
            This is specified as a query string parameter.
        returns: list
        raises:
            - nomad.api.exceptions.BaseNomadException
            - nomad.api.exceptions.URLNotFoundNomadException
        """
        params = {
            "all": all_,
            "namespace": namespace,
        }
        return self.request(id_, "allocations", params=params, method="get").json()

    def get_evaluations(
        self,
        id_: str,
        namespace: Union[str, None] = None,
    ):
        """Query the evaluations belonging to a single job.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
            - id_
            - namespace (str) optional.
            Specifies the target namespace. If ACL is enabled, this value
            must match a namespace that the token is allowed to access.
            This is specified as a query string parameter.
        returns: dict
        raises:
            - nomad.api.exceptions.BaseNomadException
            - nomad.api.exceptions.URLNotFoundNomadException
        """
        params = {
            "namespace": namespace,
        }
        return self.request(id_, "evaluations", params=params, method="get").json()

    def get_deployments(self, id_):
        """This endpoint lists a single job's deployments

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, "deployments", method="get").json()

    def get_deployment(self, id_):
        """This endpoint returns a single job's most recent deployment.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
        returns: list of dicts
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, "deployment", method="get").json()

    def get_summary(self, id_):
        """Query the summary of a job.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, "summary", method="get").json()

    def register_job(self, id_, job):
        """Registers a new job or updates an existing job

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, json=job, method="post").json()

    def evaluate_job(self, id_):
        """Creates a new evaluation for the given job.
        This can be used to force run the scheduling logic if necessary.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, "evaluate", method="post").json()

    def plan_job(self, id_, job, diff=False, policy_override=False):
        """Invoke a dry-run of the scheduler for the job.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
          - job, dict
          - diff, boolean
          - policy_override, boolean
         returns: dict
         raises:
           - nomad.api.exceptions.BaseNomadException
           - nomad.api.exceptions.URLNotFoundNomadException
        """
        json_dict = {}
        json_dict.update(job)
        json_dict.setdefault("Diff", diff)
        json_dict.setdefault("PolicyOverride", policy_override)
        return self.request(id_, "plan", json=json_dict, method="post").json()

    def periodic_job(self, id_):
        """Forces a new instance of the periodic job. A new instance will be
         created even if it violates the job's prohibit_overlap settings.
         As such, this should be only used to immediately
         run a periodic job.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, "periodic", "force", method="post").json()

    def dispatch_job(
        self,
        id_,
        payload=None,
        meta=None,
        id_prefix_template=None,
        idempotency_token=None,
    ):  # pylint: disable=too-many-arguments
        """Dispatches a new instance of a parameterized job.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
          - payload
          - meta
          - id_prefix_template
          - idempotency_token
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        dispatch_json = {
            "Meta": meta,
            "Payload": payload,
            "idempotency_token": idempotency_token,
            "IdPrefixTemplate": id_prefix_template,
        }
        return self.request(id_, "dispatch", json=dispatch_json, method="post").json()

    def revert_job(self, id_, version, enforce_prior_version=None):
        """This endpoint reverts the job to an older version.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
           - id_
           - version, Specifies the job version to revert to.
        optional_arguments:
          - enforce_prior_version, Optional value specifying the current job's version.
                                   This is checked and acts as a check-and-set value before reverting to the
                                   specified job.
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        revert_json = {
            "JobID": id_,
            "JobVersion": version,
            "EnforcePriorVersion": enforce_prior_version,
        }
        return self.request(id_, "revert", json=revert_json, method="post").json()

    def stable_job(self, id_, version, stable):
        """This endpoint sets the job's stability.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
          - id_
          - version, Specifies the job version to revert to.
          - stable, Specifies whether the job should be marked as stable or not.
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        revert_json = {"JobID": id_, "JobVersion": version, "Stable": stable}
        return self.request(id_, "stable", json=revert_json, method="post").json()

    def deregister_job(
        self,
        id_: str,
        eval_priority: Union[int, None] = None,
        global_: Union[bool, None] = None,
        namespace: Union[str, None] = None,
        purge: Union[bool, None] = None,
    ):  # pylint: disable=too-many-arguments
        """Deregisters a job, and stops all allocations part of it.

        https://www.nomadproject.io/docs/http/job.html

        arguments:
            - id_
            - eval_priority (int) optional.
            Override the priority of the evaluations produced as a result
            of this job deregistration. By default, this is set to the
            priority of the job.
            - global_ (bool) optional.
            Stop a multi-region job in all its regions. By default, job
            stop will stop only a single region at a time. Ignored for
            single-region jobs.
            - purge (bool) optional.
            Specifies that the job should be stopped and purged immediately.
            This means the job will not be queryable after being stopped.
            If not set, the job will be purged by the garbage collector.
            - namespace (str) optional.
            Specifies the target namespace. If ACL is enabled, this value
            must match a namespace that the token is allowed to access.
            This is specified as a query string parameter.

        returns: dict
        raises:
            - nomad.api.exceptions.BaseNomadException
            - nomad.api.exceptions.URLNotFoundNomadException
            - nomad.api.exceptions.InvalidParameters
        """
        params = {
            "eval_priority": eval_priority,
            "global": global_,
            "namespace": namespace,
            "purge": purge,
        }
        return self.request(id_, params=params, method="delete").json()
