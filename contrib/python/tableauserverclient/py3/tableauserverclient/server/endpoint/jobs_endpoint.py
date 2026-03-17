import logging
from typing_extensions import Self, overload


from tableauserverclient.models import JobItem, BackgroundJobItem, PaginationItem
from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api
from tableauserverclient.server.endpoint.exceptions import JobCancelledException, JobFailedException
from tableauserverclient.server.query import QuerySet
from tableauserverclient.server.request_options import RequestOptionsBase
from tableauserverclient.exponential_backoff import ExponentialBackoffTimer

from tableauserverclient.helpers.logging import logger

from typing import Optional, Union


class Jobs(QuerysetEndpoint[BackgroundJobItem]):
    @property
    def baseurl(self):
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/jobs"

    @overload  # type: ignore[override]
    def get(self: Self, job_id: str, req_options: Optional[RequestOptionsBase] = None) -> JobItem:  # type: ignore[override]
        ...

    @overload  # type: ignore[override]
    def get(self: Self, job_id: RequestOptionsBase, req_options: None) -> tuple[list[BackgroundJobItem], PaginationItem]:  # type: ignore[override]
        ...

    @overload  # type: ignore[override]
    def get(self: Self, job_id: None, req_options: Optional[RequestOptionsBase]) -> tuple[list[BackgroundJobItem], PaginationItem]:  # type: ignore[override]
        ...

    @api(version="2.6")
    def get(self, job_id=None, req_options=None):
        """
        Retrieve jobs for the site. Endpoint is paginated and will return a
        list of jobs and pagination information. If a job_id is provided, the
        method will return information about that specific job. Specifying a
        job_id is deprecated and will be removed in a future version.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#query_jobs

        Parameters
        ----------
        job_id : str or RequestOptionsBase
            The ID of the job to retrieve. If None, the method will return all
            jobs for the site. If a RequestOptions object is provided, the
            method will use the options to filter the jobs.

        req_options : RequestOptionsBase
            The request options to filter the jobs. If None, the method will
            return all jobs for the site.

        Returns
        -------
        tuple[list[BackgroundJobItem], PaginationItem] or JobItem
            If a job_id is provided, the method will return a JobItem. If no
            job_id is provided, the method will return a tuple containing a
            list of BackgroundJobItems and a PaginationItem.
        """
        # Backwards Compatibility fix until we rev the major version
        if job_id is not None and isinstance(job_id, str):
            import warnings

            warnings.warn("Jobs.get(job_id) is deprecated, update code to use Jobs.get_by_id(job_id)")
            return self.get_by_id(job_id)
        if isinstance(job_id, RequestOptionsBase):
            req_options = job_id

        self.parent_srv.assert_at_least_version("3.1", "Jobs.get_by_id(job_id)")
        server_response = self.get_request(self.baseurl, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        jobs = BackgroundJobItem.from_response(server_response.content, self.parent_srv.namespace)
        return jobs, pagination_item

    @api(version="3.1")
    def cancel(self, job_id: Union[str, JobItem]):
        """
        Cancels a job specified by job ID. To get a list of job IDs for jobs that are currently queued or in-progress, use the Query Jobs method.

        The following jobs can be canceled using the Cancel Job method:

        Full extract refresh
        Incremental extract refresh
        Subscription
        Flow Run
        Data Acceleration (Data acceleration is not available in Tableau Server 2022.1 (API 3.16) and later. See View Acceleration(Link opens in a new window).)
        Bridge full extract refresh
        Bridge incremental extract refresh
        Queue upgrade Thumbnail (Job that puts the upgrade thumbnail job on the queue)
        Upgrade Thumbnail

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#cancel_job

        Parameters
        ----------
        job_id : str or JobItem
            The ID of the job to cancel. If a JobItem is provided, the method
            will use the ID from the JobItem.

        Returns
        -------
        None
        """
        if isinstance(job_id, JobItem):
            job_id = job_id.id
        assert isinstance(job_id, str)
        url = f"{self.baseurl}/{job_id}"
        return self.put_request(url)

    @api(version="2.6")
    def get_by_id(self, job_id: str) -> JobItem:
        """
        Returns status information about an asynchronous process that is tracked
        using a job. This method can be used to query jobs that are used to do
        the following:

        Import users from Active Directory (the result of a call to Create Group).
        Synchronize an existing Tableau Server group with Active Directory (the result of a call to Update Group).
        Run extract refresh tasks (the result of a call to Run Extract Refresh Task).
        Publish a workbook asynchronously (the result of a call to Publish Workbook).
        Run workbook or view subscriptions (the result of a call to Create Subscription or Update Subscription)
        Run a flow task (the result of a call to Run Flow Task)
        Status of Tableau Server site deletion (the result of a call to asynchronous Delete Site(Link opens in a new window) beginning API 3.18)
        Note: To query a site deletion job, the server administrator must be first signed into the default site (contentUrl=" ").

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#query_job

        Parameters
        ----------
        job_id : str
            The ID of the job to retrieve.

        Returns
        -------
        JobItem
            The JobItem object that contains information about the requested job.
        """
        logger.info("Query for information about job " + job_id)
        url = f"{self.baseurl}/{job_id}"
        server_response = self.get_request(url)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    def wait_for_job(self, job_id: Union[str, JobItem], *, timeout: Optional[float] = None) -> JobItem:
        """
        Waits for a job to complete. The method will poll the server for the job
        status until the job is completed. If the job is successful, the method
        will return the JobItem. If the job fails, the method will raise a
        JobFailedException. If the job is cancelled, the method will raise a
        JobCancelledException.

        Parameters
        ----------
        job_id : str or JobItem
            The ID of the job to wait for. If a JobItem is provided, the method
            will use the ID from the JobItem.

        timeout : float | None
            The maximum amount of time to wait for the job to complete. If None,
            the method will wait indefinitely.

        Returns
        -------
        JobItem
            The JobItem object that contains information about the completed job.

        Raises
        ------
        JobFailedException
            If the job failed to complete.

        JobCancelledException
            If the job was cancelled.
        """
        if isinstance(job_id, JobItem):
            job_id = job_id.id
        assert isinstance(job_id, str)
        logger.debug(f"Waiting for job {job_id}")

        backoffTimer = ExponentialBackoffTimer(timeout=timeout)
        job = self.get_by_id(job_id)
        while job.completed_at is None:
            backoffTimer.sleep()
            job = self.get_by_id(job_id)
            logger.debug(f"\tJob {job_id} progress={job.progress}")

        logger.info(f"Job {job_id} Completed: Finish Code: {job.finish_code} - Notes:{job.notes}")

        if job.finish_code in [JobItem.FinishCode.Success, JobItem.FinishCode.Completed]:
            return job
        elif job.finish_code == JobItem.FinishCode.Failed:
            raise JobFailedException(job)
        elif job.finish_code == JobItem.FinishCode.Cancelled:
            raise JobCancelledException(job)
        else:
            raise AssertionError("Unexpected finish_code in job", job)

    def filter(self, *invalid, page_size: Optional[int] = None, **kwargs) -> QuerySet[BackgroundJobItem]:
        """
        Queries the Tableau Server for items using the specified filters. Page
        size can be specified to limit the number of items returned in a single
        request. If not specified, the default page size is 100. Page size can
        be an integer between 1 and 1000.

        No positional arguments are allowed. All filters must be specified as
        keyword arguments. If you use the equality operator, you can specify it
        through <field_name>=<value>. If you want to use a different operator,
        you can specify it through <field_name>__<operator>=<value>. Field
        names can either be in snake_case or camelCase.

        This endpoint supports the following fields and operators:


        args__has=...
        completed_at=...
        completed_at__gt=...
        completed_at__gte=...
        completed_at__lt=...
        completed_at__lte=...
        created_at=...
        created_at__gt=...
        created_at__gte=...
        created_at__lt=...
        created_at__lte=...
        job_type=...
        job_type__in=...
        notes__has=...
        priority=...
        priority__gt=...
        priority__gte=...
        priority__lt=...
        priority__lte=...
        progress=...
        progress__gt=...
        progress__gte=...
        progress__lt=...
        progress__lte=...
        started_at=...
        started_at__gt=...
        started_at__gte=...
        started_at__lt=...
        started_at__lte=...
        status=...
        subtitle=...
        subtitle__has=...
        title=...
        title__has=...
        """

        return super().filter(*invalid, page_size=page_size, **kwargs)
