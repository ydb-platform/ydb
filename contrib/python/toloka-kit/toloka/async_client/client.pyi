__all__ = [
    'AsyncTolokaClient',
]
import datetime
import decimal
import pandas
import ssl
import toloka.client
import toloka.client.aggregation
import toloka.client.analytics_request
import toloka.client.app
import toloka.client.assignment
import toloka.client.attachment
import toloka.client.batch_create_results
import toloka.client.clone_results
import toloka.client.filter
import toloka.client.message_thread
import toloka.client.operation_log
import toloka.client.operations
import toloka.client.owner
import toloka.client.pool
import toloka.client.project
import toloka.client.requester
import toloka.client.search_requests
import toloka.client.search_results
import toloka.client.skill
import toloka.client.task
import toloka.client.task_suite
import toloka.client.training
import toloka.client.user
import toloka.client.user_bonus
import toloka.client.user_restriction
import toloka.client.user_skill
import toloka.client.webhook_subscription
import toloka.util.async_utils
import typing
import urllib3.util.retry
import uuid


class AsyncTolokaClient:
    """Class that implements interaction with [Toloka API](https://toloka.ai/docs/api/api-reference/), in an asynchronous way.

    All methods are wrapped as async. So all methods calls must be awaited.
    All arguments, same as in TolokaClient.
    """

    def __init__(
        self,
        token: str,
        environment: typing.Union[toloka.client.TolokaClient.Environment, str, None] = None,
        retries: typing.Union[int, urllib3.util.retry.Retry] = 3,
        timeout: typing.Union[float, typing.Tuple[float, float]] = 10.0,
        url: typing.Optional[str] = None,
        retry_quotas: typing.Union[typing.List[str], str, None] = 'MIN',
        retryer_factory: typing.Optional[typing.Callable[[], urllib3.util.retry.Retry]] = None,
        act_under_account_id: typing.Optional[str] = None,
        verify: typing.Union[str, bool, ssl.SSLContext] = True
    ): ...

    def __getattr__(self, name):
        """Access non function fields.

        All function fields should be already overridden with `generate_async_methods_from`.
        """
        ...

    def __getstate__(self): ...

    def __setstate__(self, state): ...

    @classmethod
    def from_sync_client(cls, client: toloka.client.TolokaClient) -> 'AsyncTolokaClient': ...

    async def wait_operation(
        self,
        op: toloka.client.operations.Operation,
        timeout: datetime.timedelta = ...,
        logger=...
    ) -> toloka.client.operations.Operation:
        """Asynchronous version of wait_operation
        """
        ...

    @typing.overload
    async def aggregate_solutions_by_pool(self, request: toloka.client.aggregation.PoolAggregatedSolutionRequest) -> toloka.client.operations.AggregatedSolutionOperation:
        """Starts aggregation of responses in all completed tasks in a pool.

        The method starts the aggregation process on the Toloka server. To wait for the completion of the operation use the [wait_operation](toloka.client.TolokaClient.wait_operation.md) method.

        {% note tip %}

        Try [crowd-kit library](https://toloka.ai/docs/crowd-kit). It has many aggregation methods and executes on your computer.

        {% endnote %}

        Args:
            request: Parameters describing in which pool to aggregate responses and by what rules.

        Returns:
            operations.AggregatedSolutionOperation: An object to track the progress of the operation.

        Example:
            The example shows how to aggregate responses in a pool.

            >>> aggregation_operation = toloka_client.aggregate_solutions_by_pool(
            >>>         type=toloka.client.aggregation.AggregatedSolutionType.WEIGHTED_DYNAMIC_OVERLAP,
            >>>         pool_id='36502086',
            >>>         answer_weight_skill_id='11294',
            >>>         fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
            >>>     )
            >>> aggregation_operation = toloka_client.wait_operation(aggregation_operation)
            >>> aggregation_results = list(toloka_client.get_aggregated_solutions(aggregation_operation.id))
            ...
        """
        ...

    @typing.overload
    async def aggregate_solutions_by_pool(
        self,
        *,
        type: typing.Union[toloka.client.aggregation.AggregatedSolutionType, str, None] = None,
        pool_id: typing.Optional[str] = None,
        answer_weight_skill_id: typing.Optional[str] = None,
        fields: typing.Optional[typing.List[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field]] = None
    ) -> toloka.client.operations.AggregatedSolutionOperation:
        """Starts aggregation of responses in all completed tasks in a pool.

        The method starts the aggregation process on the Toloka server. To wait for the completion of the operation use the [wait_operation](toloka.client.TolokaClient.wait_operation.md) method.

        {% note tip %}

        Try [crowd-kit library](https://toloka.ai/docs/crowd-kit). It has many aggregation methods and executes on your computer.

        {% endnote %}

        Args:
            request: Parameters describing in which pool to aggregate responses and by what rules.

        Returns:
            operations.AggregatedSolutionOperation: An object to track the progress of the operation.

        Example:
            The example shows how to aggregate responses in a pool.

            >>> aggregation_operation = toloka_client.aggregate_solutions_by_pool(
            >>>         type=toloka.client.aggregation.AggregatedSolutionType.WEIGHTED_DYNAMIC_OVERLAP,
            >>>         pool_id='36502086',
            >>>         answer_weight_skill_id='11294',
            >>>         fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
            >>>     )
            >>> aggregation_operation = toloka_client.wait_operation(aggregation_operation)
            >>> aggregation_results = list(toloka_client.get_aggregated_solutions(aggregation_operation.id))
            ...
        """
        ...

    @typing.overload
    async def aggregate_solutions_by_task(self, request: toloka.client.aggregation.WeightedDynamicOverlapTaskAggregatedSolutionRequest) -> toloka.client.aggregation.AggregatedSolution:
        """Aggregates responses to a single task on the Toloka server.

        {% note tip %}

        Try [crowd-kit library](https://toloka.ai/docs/crowd-kit). It has many aggregation methods and executes on your computer.

        {% endnote %}

        Args:
            request: Aggregation parameters.

        Returns:
            AggregatedSolution: Aggregated response.

        Example:
            The example shows how to aggregate responses to a single task.

            >>> aggregated_response = toloka_client.aggregate_solutions_by_task(
            >>>     pool_id='36502086',
            >>>     task_id='000012bb84--62d80429f20bf20e50f36a27',
            >>>     answer_weight_skill_id='11294',
            >>>     fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
            >>> )
            >>> print(aggregated_response.output_values['result'])
            ...
        """
        ...

    @typing.overload
    async def aggregate_solutions_by_task(
        self,
        *,
        task_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        answer_weight_skill_id: typing.Optional[str] = None,
        fields: typing.Optional[typing.List[toloka.client.aggregation.WeightedDynamicOverlapTaskAggregatedSolutionRequest.Field]] = None
    ) -> toloka.client.aggregation.AggregatedSolution:
        """Aggregates responses to a single task on the Toloka server.

        {% note tip %}

        Try [crowd-kit library](https://toloka.ai/docs/crowd-kit). It has many aggregation methods and executes on your computer.

        {% endnote %}

        Args:
            request: Aggregation parameters.

        Returns:
            AggregatedSolution: Aggregated response.

        Example:
            The example shows how to aggregate responses to a single task.

            >>> aggregated_response = toloka_client.aggregate_solutions_by_task(
            >>>     pool_id='36502086',
            >>>     task_id='000012bb84--62d80429f20bf20e50f36a27',
            >>>     answer_weight_skill_id='11294',
            >>>     fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
            >>> )
            >>> print(aggregated_response.output_values['result'])
            ...
        """
        ...

    @typing.overload
    async def find_aggregated_solutions(
        self,
        operation_id: str,
        request: toloka.client.search_requests.AggregatedSolutionSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AggregatedSolutionSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AggregatedSolutionSearchResult:
        """Finds aggregated responses that match certain criteria.

        Pass to the `find_aggregated_solutions` the ID of the operation started by the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method.

        The number of returned aggregated responses is limited. To find remaining responses call `find_aggregated_solutions` with updated search criteria.

        To iterate over all matching aggregated responses you may use the [get_aggregated_solutions](toloka.client.TolokaClient.get_aggregated_solutions.md) method.

        Args:
            operation_id: The ID of the aggregation operation.
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned aggregated responses limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            AggregatedSolutionSearchResult: Found responses and a flag showing whether there are more matching responses exceeding the limit.

        Example:
            The example shows how to get all aggregated responses using the `find_aggregated_solutions` method.
            First, run the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method and wait for the operation to complete.
            The ID of the operation is used to get aggregated results.

            >>> current_result = toloka_client.find_aggregated_solutions(aggregation_operation.id)
            >>> aggregation_results = current_result.items
            >>>
            >>> while current_result.has_more:
            >>>     current_result = toloka_client.find_aggregated_solutions(
            >>>         aggregation_operation.id,
            >>>         task_id_gt=current_result.items[-1].task_id,
            >>>     )
            >>>     aggregation_results = aggregation_results + current_result.items
            >>> print(len(aggregation_results))
            ...
        """
        ...

    @typing.overload
    async def find_aggregated_solutions(
        self,
        operation_id: str,
        task_id_lt: typing.Optional[str] = None,
        task_id_lte: typing.Optional[str] = None,
        task_id_gt: typing.Optional[str] = None,
        task_id_gte: typing.Optional[str] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AggregatedSolutionSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AggregatedSolutionSearchResult:
        """Finds aggregated responses that match certain criteria.

        Pass to the `find_aggregated_solutions` the ID of the operation started by the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method.

        The number of returned aggregated responses is limited. To find remaining responses call `find_aggregated_solutions` with updated search criteria.

        To iterate over all matching aggregated responses you may use the [get_aggregated_solutions](toloka.client.TolokaClient.get_aggregated_solutions.md) method.

        Args:
            operation_id: The ID of the aggregation operation.
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned aggregated responses limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            AggregatedSolutionSearchResult: Found responses and a flag showing whether there are more matching responses exceeding the limit.

        Example:
            The example shows how to get all aggregated responses using the `find_aggregated_solutions` method.
            First, run the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method and wait for the operation to complete.
            The ID of the operation is used to get aggregated results.

            >>> current_result = toloka_client.find_aggregated_solutions(aggregation_operation.id)
            >>> aggregation_results = current_result.items
            >>>
            >>> while current_result.has_more:
            >>>     current_result = toloka_client.find_aggregated_solutions(
            >>>         aggregation_operation.id,
            >>>         task_id_gt=current_result.items[-1].task_id,
            >>>     )
            >>>     aggregation_results = aggregation_results + current_result.items
            >>> print(len(aggregation_results))
            ...
        """
        ...

    @typing.overload
    def get_aggregated_solutions(
        self,
        operation_id: str,
        request: toloka.client.search_requests.AggregatedSolutionSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.aggregation.AggregatedSolution, None]:
        """Finds all aggregated responses that match certain criteria.

        Pass to the `get_aggregated_solutions` the ID of the operation started by the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method.

        `get_aggregated_solutions` returns a generator. You can iterate over all found aggregated responses using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort aggregated responses use the [find_aggregated_solutions](toloka.client.TolokaClient.find_aggregated_solutions.md) method.

        {% note tip %}

        Try [crowd-kit library](https://toloka.ai/docs/crowd-kit). It has many aggregation methods and executes on your computer.

        {% endnote %}

        Args:
            operation_id: The ID of the aggregation operation.
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            AggregatedSolution: The next matching aggregated response.

        Example:
            The example shows how to aggregate responses in a pool.

            >>> aggregation_operation = toloka_client.aggregate_solutions_by_pool(
            >>>     type=toloka.client.aggregation.AggregatedSolutionType.WEIGHTED_DYNAMIC_OVERLAP,
            >>>     pool_id=some_existing_pool_id,
            >>>     answer_weight_skill_id=some_skill_id,
            >>>     fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
            >>> )
            >>> aggregation_operation = toloka_client.wait_operation(aggregation_operation)
            >>> aggregation_results = list(toloka_client.get_aggregated_solutions(aggregation_operation.id))
            ...
        """
        ...

    @typing.overload
    def get_aggregated_solutions(
        self,
        operation_id: str,
        task_id_lt: typing.Optional[str] = None,
        task_id_lte: typing.Optional[str] = None,
        task_id_gt: typing.Optional[str] = None,
        task_id_gte: typing.Optional[str] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.aggregation.AggregatedSolution, None]:
        """Finds all aggregated responses that match certain criteria.

        Pass to the `get_aggregated_solutions` the ID of the operation started by the [aggregate_solutions_by_pool](toloka.client.TolokaClient.aggregate_solutions_by_pool.md) method.

        `get_aggregated_solutions` returns a generator. You can iterate over all found aggregated responses using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort aggregated responses use the [find_aggregated_solutions](toloka.client.TolokaClient.find_aggregated_solutions.md) method.

        {% note tip %}

        Try [crowd-kit library](https://toloka.ai/docs/crowd-kit). It has many aggregation methods and executes on your computer.

        {% endnote %}

        Args:
            operation_id: The ID of the aggregation operation.
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            AggregatedSolution: The next matching aggregated response.

        Example:
            The example shows how to aggregate responses in a pool.

            >>> aggregation_operation = toloka_client.aggregate_solutions_by_pool(
            >>>     type=toloka.client.aggregation.AggregatedSolutionType.WEIGHTED_DYNAMIC_OVERLAP,
            >>>     pool_id=some_existing_pool_id,
            >>>     answer_weight_skill_id=some_skill_id,
            >>>     fields=[toloka.client.aggregation.PoolAggregatedSolutionRequest.Field(name='result')]
            >>> )
            >>> aggregation_operation = toloka_client.wait_operation(aggregation_operation)
            >>> aggregation_results = list(toloka_client.get_aggregated_solutions(aggregation_operation.id))
            ...
        """
        ...

    async def accept_assignment(
        self,
        assignment_id: str,
        public_comment: str
    ) -> toloka.client.assignment.Assignment:
        """Accepts an assignment.

        Args:
            assignment_id: The ID of the assignment.
            public_comment: A comment visible to Tolokers.

        Returns:
            Assignment: The assignment object with the updated status field.

        Example:
            >>> toloka_client.accept_assignment(
            >>>     assignment_id='00001092da--61ef030400c684132d0da0de',
            >>>     public_comment='Well done!'
            >>> )
            ...
        """
        ...

    @typing.overload
    async def find_assignments(
        self,
        request: toloka.client.search_requests.AssignmentSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AssignmentSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AssignmentSearchResult:
        """Finds assignments that match certain criteria.

        The number of returned assignments is limited. To find remaining assignments call `find_assignments` with updated search criteria.

        To iterate over all matching assignments you may use the [get_assignments](toloka.client.TolokaClient.get_assignments.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned assignments limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            AssignmentSearchResult: Found assignments and a flag showing whether there are more matching assignments.

        Example:
            Search for `SKIPPED` or `EXPIRED` assignments in the specified pool.

            >>> find_result = toloka_client.find_assignments(
            >>>     pool_id='1080020',
            >>>     status=['SKIPPED', 'EXPIRED']
            >>> )
            >>> assignments = find_result.items
            ...
        """
        ...

    @typing.overload
    async def find_assignments(
        self,
        status: typing.Union[str, toloka.client.assignment.Assignment.Status, typing.List[typing.Union[str, toloka.client.assignment.Assignment.Status]]] = None,
        task_id: typing.Optional[str] = None,
        task_suite_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        user_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        submitted_lt: typing.Optional[datetime.datetime] = None,
        submitted_lte: typing.Optional[datetime.datetime] = None,
        submitted_gt: typing.Optional[datetime.datetime] = None,
        submitted_gte: typing.Optional[datetime.datetime] = None,
        accepted_lt: typing.Optional[datetime.datetime] = None,
        accepted_lte: typing.Optional[datetime.datetime] = None,
        accepted_gt: typing.Optional[datetime.datetime] = None,
        accepted_gte: typing.Optional[datetime.datetime] = None,
        rejected_lt: typing.Optional[datetime.datetime] = None,
        rejected_lte: typing.Optional[datetime.datetime] = None,
        rejected_gt: typing.Optional[datetime.datetime] = None,
        rejected_gte: typing.Optional[datetime.datetime] = None,
        skipped_lt: typing.Optional[datetime.datetime] = None,
        skipped_lte: typing.Optional[datetime.datetime] = None,
        skipped_gt: typing.Optional[datetime.datetime] = None,
        skipped_gte: typing.Optional[datetime.datetime] = None,
        expired_lt: typing.Optional[datetime.datetime] = None,
        expired_lte: typing.Optional[datetime.datetime] = None,
        expired_gt: typing.Optional[datetime.datetime] = None,
        expired_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AssignmentSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AssignmentSearchResult:
        """Finds assignments that match certain criteria.

        The number of returned assignments is limited. To find remaining assignments call `find_assignments` with updated search criteria.

        To iterate over all matching assignments you may use the [get_assignments](toloka.client.TolokaClient.get_assignments.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned assignments limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            AssignmentSearchResult: Found assignments and a flag showing whether there are more matching assignments.

        Example:
            Search for `SKIPPED` or `EXPIRED` assignments in the specified pool.

            >>> find_result = toloka_client.find_assignments(
            >>>     pool_id='1080020',
            >>>     status=['SKIPPED', 'EXPIRED']
            >>> )
            >>> assignments = find_result.items
            ...
        """
        ...

    async def get_assignment(self, assignment_id: str) -> toloka.client.assignment.Assignment:
        """Gets an assignment from Toloka.

        Args:
            assignment_id: The ID of the assignment.

        Returns:
            Assignment: The assignment.

        Example:
            >>> assignment = toloka_client.get_assignment(
            >>>     assignment_id='00001092da--61ef030400c684132d0da0de'
            >>> )
            ...
        """
        ...

    @typing.overload
    def get_assignments(
        self,
        request: toloka.client.search_requests.AssignmentSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.assignment.Assignment, None]:
        """Finds all assignments that match certain criteria.

        `get_assignments` returns a generator. You can iterate over all found assignments using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort assignments use the [find_assignments](toloka.client.TolokaClient.find_assignments.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            Assignment: The next matching assignment.

        Example:
            The following example creates a list with IDs of `SUBMITTED` assignments in the specified pool.

            >>> from toloka.client import Assignment
            >>> assignments = toloka_client.get_assignments(pool_id='1080020', status=Assignment.SUBMITTED)
            >>> assignment_ids = [assignment.id for assignment in assignments]
            ...
        """
        ...

    @typing.overload
    def get_assignments(
        self,
        status: typing.Union[str, toloka.client.assignment.Assignment.Status, typing.List[typing.Union[str, toloka.client.assignment.Assignment.Status]]] = None,
        task_id: typing.Optional[str] = None,
        task_suite_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        user_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        submitted_lt: typing.Optional[datetime.datetime] = None,
        submitted_lte: typing.Optional[datetime.datetime] = None,
        submitted_gt: typing.Optional[datetime.datetime] = None,
        submitted_gte: typing.Optional[datetime.datetime] = None,
        accepted_lt: typing.Optional[datetime.datetime] = None,
        accepted_lte: typing.Optional[datetime.datetime] = None,
        accepted_gt: typing.Optional[datetime.datetime] = None,
        accepted_gte: typing.Optional[datetime.datetime] = None,
        rejected_lt: typing.Optional[datetime.datetime] = None,
        rejected_lte: typing.Optional[datetime.datetime] = None,
        rejected_gt: typing.Optional[datetime.datetime] = None,
        rejected_gte: typing.Optional[datetime.datetime] = None,
        skipped_lt: typing.Optional[datetime.datetime] = None,
        skipped_lte: typing.Optional[datetime.datetime] = None,
        skipped_gt: typing.Optional[datetime.datetime] = None,
        skipped_gte: typing.Optional[datetime.datetime] = None,
        expired_lt: typing.Optional[datetime.datetime] = None,
        expired_lte: typing.Optional[datetime.datetime] = None,
        expired_gt: typing.Optional[datetime.datetime] = None,
        expired_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.assignment.Assignment, None]:
        """Finds all assignments that match certain criteria.

        `get_assignments` returns a generator. You can iterate over all found assignments using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort assignments use the [find_assignments](toloka.client.TolokaClient.find_assignments.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            Assignment: The next matching assignment.

        Example:
            The following example creates a list with IDs of `SUBMITTED` assignments in the specified pool.

            >>> from toloka.client import Assignment
            >>> assignments = toloka_client.get_assignments(pool_id='1080020', status=Assignment.SUBMITTED)
            >>> assignment_ids = [assignment.id for assignment in assignments]
            ...
        """
        ...

    @typing.overload
    async def patch_assignment(
        self,
        assignment_id: str,
        patch: toloka.client.assignment.AssignmentPatch
    ) -> toloka.client.assignment.Assignment:
        """Changes an assignment status and associated public comment.

        See also [reject_assignment](toloka.client.TolokaClient.reject_assignment.md) and [accept_assignment](toloka.client.TolokaClient.accept_assignment.md).

        Args:
            assignment_id: The ID of the assignment.
            patch: New status and comment.

        Returns:
            Assignment: Assignment object with updated fields.

        Example:
            >>> toloka_client.patch_assignment(assignment_id='00001092da--61ef030400c684132d0da0de',
            >>>     public_comment='Accepted. Good job.',
            >>>     status='ACCEPTED'
            >>> )
            ...
        """
        ...

    @typing.overload
    async def patch_assignment(
        self,
        assignment_id: str,
        *,
        public_comment: typing.Optional[str] = None,
        status: typing.Optional[toloka.client.assignment.Assignment.Status] = None
    ) -> toloka.client.assignment.Assignment:
        """Changes an assignment status and associated public comment.

        See also [reject_assignment](toloka.client.TolokaClient.reject_assignment.md) and [accept_assignment](toloka.client.TolokaClient.accept_assignment.md).

        Args:
            assignment_id: The ID of the assignment.
            patch: New status and comment.

        Returns:
            Assignment: Assignment object with updated fields.

        Example:
            >>> toloka_client.patch_assignment(assignment_id='00001092da--61ef030400c684132d0da0de',
            >>>     public_comment='Accepted. Good job.',
            >>>     status='ACCEPTED'
            >>> )
            ...
        """
        ...

    async def reject_assignment(
        self,
        assignment_id: str,
        public_comment: str
    ) -> toloka.client.assignment.Assignment:
        """Rejects an assignment.

        Args:
            assignment_id: The ID of the assignment.
            public_comment: A public comment visible to Tolokers.

        Returns:
            Assignment: Assignment object with updated fields.

        Example:
            >>> toloka_client.reject_assignment(
            >>>     assignment_id='00001092da--61ef030400c684132d0da0de',
            >>>     public_comment='Some questions skipped'
            >>> )
            ...
        """
        ...

    @typing.overload
    async def find_attachments(
        self,
        request: toloka.client.search_requests.AttachmentSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AttachmentSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AttachmentSearchResult:
        """Finds attachments that match certain criteria and returns their metadata.

        The number of returned attachments is limited. To find remaining attachments call `find_attachments` with updated search criteria.

        To iterate over all matching attachments you may use the [get_attachments](toloka.client.TolokaClient.get_attachments.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned attachments limit.
                The maximum allowed value: 100.

        Returns:
            AttachmentSearchResult: Found attachments and a flag showing whether there are more matching attachments exceeding the limit.

        Example:
            Let's find attachments in the pool and sort them by the ID and the date of creation in descending order.

            >>> attachments = toloka_client.find_attachments(pool_id='1080020', sort=['-created', '-id'], limit=10)
            ...

            If there are attachments exceeding the `limit`, then `attachments.has_more` is set to `True`.
        """
        ...

    @typing.overload
    async def find_attachments(
        self,
        name: typing.Optional[str] = None,
        type: typing.Optional[toloka.client.attachment.Attachment.Type] = None,
        user_id: typing.Optional[str] = None,
        assignment_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        owner_id: typing.Optional[str] = None,
        owner_company_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AttachmentSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AttachmentSearchResult:
        """Finds attachments that match certain criteria and returns their metadata.

        The number of returned attachments is limited. To find remaining attachments call `find_attachments` with updated search criteria.

        To iterate over all matching attachments you may use the [get_attachments](toloka.client.TolokaClient.get_attachments.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned attachments limit.
                The maximum allowed value: 100.

        Returns:
            AttachmentSearchResult: Found attachments and a flag showing whether there are more matching attachments exceeding the limit.

        Example:
            Let's find attachments in the pool and sort them by the ID and the date of creation in descending order.

            >>> attachments = toloka_client.find_attachments(pool_id='1080020', sort=['-created', '-id'], limit=10)
            ...

            If there are attachments exceeding the `limit`, then `attachments.has_more` is set to `True`.
        """
        ...

    async def get_attachment(self, attachment_id: str) -> toloka.client.attachment.Attachment:
        """Gets attachment metadata without downloading it.

        To download an attachment use the [download_attachment](toloka.client.TolokaClient.download_attachment.md) method.

        Args:
            attachment_id: The ID of the attachment.

        Returns:
            Attachment: The attachment metadata.

        Example:
            >>> attachment = toloka_client.get_attachment(attachment_id='0983459b-e26f-42f3-a5fd-6e3feee913e7')
            ...
        """
        ...

    @typing.overload
    def get_attachments(
        self,
        request: toloka.client.search_requests.AttachmentSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.attachment.Attachment, None]:
        """Finds all attachments that match certain criteria and returns their metadata.

        `get_attachments` returns a generator. You can iterate over all found attachments using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort attachments use the [find_attachments](toloka.client.TolokaClient.find_attachments.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100.

        Yields:
            Attachment: The next matching attachment.

        Example:
            Making a list of all received attachments in a pool.

            >>> attachments = list(toloka_client.get_attachments(pool_id='1080020'))
            ...
        """
        ...

    @typing.overload
    def get_attachments(
        self,
        name: typing.Optional[str] = None,
        type: typing.Optional[toloka.client.attachment.Attachment.Type] = None,
        user_id: typing.Optional[str] = None,
        assignment_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        owner_id: typing.Optional[str] = None,
        owner_company_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.attachment.Attachment, None]:
        """Finds all attachments that match certain criteria and returns their metadata.

        `get_attachments` returns a generator. You can iterate over all found attachments using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort attachments use the [find_attachments](toloka.client.TolokaClient.find_attachments.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100.

        Yields:
            Attachment: The next matching attachment.

        Example:
            Making a list of all received attachments in a pool.

            >>> attachments = list(toloka_client.get_attachments(pool_id='1080020'))
            ...
        """
        ...

    async def download_attachment(
        self,
        attachment_id: str,
        out: typing.BinaryIO
    ) -> None:
        """Downloads an attachment.

        Args:
            attachment_id: The ID of the attachment.
            out: A file object used to save the downloaded file.

        Example:
            How to download an attachment.

            >>> with open('my_new_file.txt', 'wb') as out_f:
            >>>     toloka_client.download_attachment(attachment_id='0983459b-e26f-42f3-a5fd-6e3feee913e7', out=out_f)
            ...
        """
        ...

    async def add_message_thread_to_folders(
        self,
        message_thread_id: str,
        folders: typing.Union[typing.List[typing.Union[toloka.client.message_thread.Folder, str]], toloka.client.message_thread.MessageThreadFolders]
    ) -> toloka.client.message_thread.MessageThread:
        """Adds a message thread to folders.

        Args:
            message_thread_id: The ID of the message thread.
            folders: A list of folders where to add the thread.

        Returns:
            MessageThread: The updated message thread.

        Example:
            >>> toloka_client.add_message_thread_to_folders(
            >>>     message_thread_id='62e2e2d68664736ed5df8488',
            >>>     folders=['IMPORTANT']
            >>> )
            ...
        """
        ...

    @typing.overload
    async def compose_message_thread(self, compose: toloka.client.message_thread.MessageThreadCompose) -> toloka.client.message_thread.MessageThread:
        """Creates a message thread and sends the first thread message to Tolokers.

        Args:
            compose: Parameters for creating the message thread.

        Returns:
            MessageThread: The created message thread.

        Example:
            A message is sent to all Tolokers who have tried to complete your tasks.
            The message is in English. Tolokers can't reply to your message.

            >>> message_text = "Amazing job! We've just trained our first model with the data you prepared for us. Thank you!"
            >>> toloka_client.compose_message_thread(
            >>>     recipients_select_type='ALL',
            >>>     topic={'EN': 'Thank you!'},
            >>>     text={'EN': message_text},
            >>>     answerable=False
            >>> )
            ...
        """
        ...

    @typing.overload
    async def compose_message_thread(
        self,
        *,
        recipients_select_type: typing.Union[toloka.client.message_thread.RecipientsSelectType, str, None] = None,
        topic: typing.Optional[typing.Dict[str, str]] = None,
        text: typing.Optional[typing.Dict[str, str]] = None,
        answerable: typing.Optional[bool] = None,
        recipients_ids: typing.Optional[typing.List[str]] = None,
        recipients_filter: typing.Optional[toloka.client.filter.FilterCondition] = None
    ) -> toloka.client.message_thread.MessageThread:
        """Creates a message thread and sends the first thread message to Tolokers.

        Args:
            compose: Parameters for creating the message thread.

        Returns:
            MessageThread: The created message thread.

        Example:
            A message is sent to all Tolokers who have tried to complete your tasks.
            The message is in English. Tolokers can't reply to your message.

            >>> message_text = "Amazing job! We've just trained our first model with the data you prepared for us. Thank you!"
            >>> toloka_client.compose_message_thread(
            >>>     recipients_select_type='ALL',
            >>>     topic={'EN': 'Thank you!'},
            >>>     text={'EN': message_text},
            >>>     answerable=False
            >>> )
            ...
        """
        ...

    @typing.overload
    async def find_message_threads(
        self,
        request: toloka.client.search_requests.MessageThreadSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.MessageThreadSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.MessageThreadSearchResult:
        """Finds message threads that match certain criteria.

        The number of returned message threads is limited. To find remaining threads call `find_message_threads` with updated search criteria.

        To iterate over all matching threads you may use the [get_message_threads](toloka.client.TolokaClient.get_message_threads.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned message threads limit.
                The maximum allowed value: 300. The default value: 50.

        Returns:
            MessageThreadSearchResult: Found message threads and a flag showing whether there are more matching threads.

        Example:
            Finding all message threads in the `INBOX` folder.

            >>> result = toloka_client.find_message_threads(folder='INBOX')
            >>> message_threads = result.items
            >>> print(len(message_threads), '+' if result.has_more else '')
            ...
        """
        ...

    @typing.overload
    async def find_message_threads(
        self,
        folder: typing.Union[str, toloka.client.message_thread.Folder, typing.List[typing.Union[str, toloka.client.message_thread.Folder]]] = None,
        folder_ne: typing.Union[str, toloka.client.message_thread.Folder, typing.List[typing.Union[str, toloka.client.message_thread.Folder]]] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.MessageThreadSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.MessageThreadSearchResult:
        """Finds message threads that match certain criteria.

        The number of returned message threads is limited. To find remaining threads call `find_message_threads` with updated search criteria.

        To iterate over all matching threads you may use the [get_message_threads](toloka.client.TolokaClient.get_message_threads.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned message threads limit.
                The maximum allowed value: 300. The default value: 50.

        Returns:
            MessageThreadSearchResult: Found message threads and a flag showing whether there are more matching threads.

        Example:
            Finding all message threads in the `INBOX` folder.

            >>> result = toloka_client.find_message_threads(folder='INBOX')
            >>> message_threads = result.items
            >>> print(len(message_threads), '+' if result.has_more else '')
            ...
        """
        ...

    async def reply_message_thread(
        self,
        message_thread_id: str,
        reply: toloka.client.message_thread.MessageThreadReply
    ) -> toloka.client.message_thread.MessageThread:
        """Sends a reply message in a thread.

        Args:
            message_thread_id: The ID of the thread.
            reply: The reply message.

        Returns:
            MessageThread: The updated message thread.

        Example:
            Sending a reply to all unread messages.

            >>> message_threads = toloka_client.get_message_threads(folder='UNREAD')
            >>> message_reply = {'EN': 'Thank you for your message! I will get back to you soon.'}
            >>> for thread in message_threads:
            >>>     toloka_client.reply_message_thread(
            >>>         message_thread_id=thread.id,
            >>>         reply=toloka.client.message_thread.MessageThreadReply(text=message_reply)
            >>>     )
            ...
        """
        ...

    @typing.overload
    def get_message_threads(
        self,
        request: toloka.client.search_requests.MessageThreadSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.message_thread.MessageThread, None]:
        """Finds all message threads that match certain criteria.

        `get_message_threads` returns a generator. You can iterate over all found message threads using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort message threads use the [find_message_threads](toloka.client.TolokaClient.find_message_threads.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300. The default value: 50.

        Yields:
            MessageThread: The next matching message thread.

        Example:
            How to get all unread incoming messages.

            >>> message_threads = toloka_client.get_message_threads(folder=['INBOX', 'UNREAD'])
            ...
        """
        ...

    @typing.overload
    def get_message_threads(
        self,
        folder: typing.Union[str, toloka.client.message_thread.Folder, typing.List[typing.Union[str, toloka.client.message_thread.Folder]]] = None,
        folder_ne: typing.Union[str, toloka.client.message_thread.Folder, typing.List[typing.Union[str, toloka.client.message_thread.Folder]]] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.message_thread.MessageThread, None]:
        """Finds all message threads that match certain criteria.

        `get_message_threads` returns a generator. You can iterate over all found message threads using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort message threads use the [find_message_threads](toloka.client.TolokaClient.find_message_threads.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300. The default value: 50.

        Yields:
            MessageThread: The next matching message thread.

        Example:
            How to get all unread incoming messages.

            >>> message_threads = toloka_client.get_message_threads(folder=['INBOX', 'UNREAD'])
            ...
        """
        ...

    async def remove_message_thread_from_folders(
        self,
        message_thread_id: str,
        folders: typing.Union[typing.List[typing.Union[toloka.client.message_thread.Folder, str]], toloka.client.message_thread.MessageThreadFolders]
    ) -> toloka.client.message_thread.MessageThread:
        """Removes a message thread from folders.

        Args:
            message_thread_id: The ID of the message thread.
            folders: A list of folders.

        Returns:
            MessageThread: The updated message thread.

        Example:
            >>> toloka_client.remove_message_thread_from_folders(
            >>>     message_thread_id='62e2e2d68664736ed5df8488',
            >>>     folders=['IMPORTANT']
            >>> )
            ...
        """
        ...

    async def archive_project(self, project_id: str) -> toloka.client.project.Project:
        """Archives a project.

        All pools in the project must be archived before archiving the project.

        The archived project is not deleted. You can access it if you need.

        Args:
            project_id: The ID of project to be archived.

        Returns:
            Project: The project with the updated status.

        Example:
            >>> archived_project = toloka_client.archive_project('117493')
            ...
        """
        ...

    async def archive_project_async(self, project_id: str) -> toloka.client.operations.ProjectArchiveOperation:
        """Archives a project. Sends an asynchronous request to Toloka.

        All pools in the project must be archived before archiving the project.

        The archived project is not deleted. You can access it if you need.

        Args:
            project_id: The ID of project to be archived.

        Returns:
            ProjectArchiveOperation: An object to track the progress of the operation.

        Example:
            >>> archive_op = toloka_client.archive_project_async(project_id='117493')
            >>> toloka_client.wait_operation(archive_op)
            ...
        """
        ...

    async def create_project(self, project: toloka.client.project.Project) -> toloka.client.project.Project:
        """Creates a new project in Toloka.

        You can send a maximum of 20 requests of this kind per minute and a maximum of 100 requests per day.

        Args:
            project: The project to be created.

        Returns:
            Project: The project with updated read-only fields.

        Example:
            Creating a new project.

            >>> new_project = toloka.client.project.Project(
            >>>     assignments_issuing_type=toloka.client.project.Project.AssignmentsIssuingType.AUTOMATED,
            >>>     public_name='Describe the image',
            >>>     public_description='Describe the image',
            >>>     public_instructions='Describe in a few words what is happening in the image.',
            >>>     task_spec=toloka.client.project.task_spec.TaskSpec(
            >>>         input_spec={'image': toloka.client.project.field_spec.UrlSpec()},
            >>>         output_spec={'result': toloka.client.project.field_spec.StringSpec()},
            >>>         view_spec=project_interface,
            >>>     ),
            >>> )
            >>> new_project = toloka_client.create_project(new_project)
            >>> print(new_project.id)
            ...
        """
        ...

    @typing.overload
    async def find_projects(
        self,
        request: toloka.client.search_requests.ProjectSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.ProjectSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.ProjectSearchResult:
        """Finds projects that match certain criteria.

        The number of returned projects is limited. To find remaining projects call `find_projects` with updated search criteria.

        To iterate over all matching projects you may use the [get_projects](toloka.client.TolokaClient.get_projects.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned projects limit.
                The maximum allowed value: 300. The default value: 20.

        Returns:
            ProjectSearchResult: Found projects and a flag showing whether there are more matching projects exceeding the limit.

        Example:
            The example shows how to find projects created before a specific date.

            >>> find_result = toloka_client.find_projects(created_lt='2021-06-01T00:00:00')
            >>> projects = find_result.items
            ...

            If there are projects exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    @typing.overload
    async def find_projects(
        self,
        status: typing.Optional[toloka.client.project.Project.ProjectStatus] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.ProjectSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.ProjectSearchResult:
        """Finds projects that match certain criteria.

        The number of returned projects is limited. To find remaining projects call `find_projects` with updated search criteria.

        To iterate over all matching projects you may use the [get_projects](toloka.client.TolokaClient.get_projects.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned projects limit.
                The maximum allowed value: 300. The default value: 20.

        Returns:
            ProjectSearchResult: Found projects and a flag showing whether there are more matching projects exceeding the limit.

        Example:
            The example shows how to find projects created before a specific date.

            >>> find_result = toloka_client.find_projects(created_lt='2021-06-01T00:00:00')
            >>> projects = find_result.items
            ...

            If there are projects exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    async def get_project(self, project_id: str) -> toloka.client.project.Project:
        """Gets project data from Toloka.

        Args:
            project_id: The ID of the project.

        Returns:
            Project: The project.

        Example:
            >>> project = toloka_client.get_project(project_id='92694')
            ...
        """
        ...

    @typing.overload
    def get_projects(
        self,
        request: toloka.client.search_requests.ProjectSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.project.Project, None]:
        """Finds all projects that match certain criteria.

        `get_projects` returns a generator. You can iterate over all found projects using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort projects use the [find_projects](toloka.client.TolokaClient.find_projects.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300. The default value: 20.

        Yields:
            Project: The next matching project.
        Example:
            Get all active projects.

            >>> active_projects = toloka_client.get_projects(status='ACTIVE')
            ...

            Get all your projects.

            >>> my_projects = toloka_client.get_projects()
            ...
        """
        ...

    @typing.overload
    def get_projects(
        self,
        status: typing.Optional[toloka.client.project.Project.ProjectStatus] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.project.Project, None]:
        """Finds all projects that match certain criteria.

        `get_projects` returns a generator. You can iterate over all found projects using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort projects use the [find_projects](toloka.client.TolokaClient.find_projects.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300. The default value: 20.

        Yields:
            Project: The next matching project.
        Example:
            Get all active projects.

            >>> active_projects = toloka_client.get_projects(status='ACTIVE')
            ...

            Get all your projects.

            >>> my_projects = toloka_client.get_projects()
            ...
        """
        ...

    async def update_project(
        self,
        project_id: str,
        project: toloka.client.project.Project
    ) -> toloka.client.project.Project:
        """Updates all project parameters in Toloka.

        Args:
            project_id: The ID of the project to be updated.
            project: The project with new parameters.

        Returns:
            Project: The project with updated parameters.

        Example:
            >>> updated_project = toloka_client.get_project(project_id='92694')
            >>> updated_project.private_comment = 'example project'
            >>> updated_project = toloka_client.update_project(project_id=updated_project.id, project=updated_project)
            ...
        """
        ...

    async def check_update_project_for_major_version_change(
        self,
        project_id: str,
        project: toloka.client.project.Project
    ) -> toloka.client.project.ProjectUpdateDifferenceLevel:
        """Checks if the project update is a breaking change or not.

        This method is similar to the `update_project` method, but instead of actually applying the changes it checks if
        the update is a breaking change or not. If the update is a breaking change, every pool in the project will not
        receive the current project update and any future project changes and will be tied to the last version of the
        project before the breaking change happened.

        Args:
            project_id: The ID of the project to be updated.
            project: The project with new parameters.

        Returns:
            ProjectUpdateDifferenceLevel: enum value that describes the level of difference between the current project
                and the project that would be created if the update is applied.
        """
        ...

    async def clone_project(
        self,
        project_id: str,
        reuse_controllers: bool = True
    ) -> toloka.client.clone_results.CloneResults:
        """Clones a project and all pools and trainings inside it.

        `clone_project` emulates cloning behavior via Toloka interface. Note that it calls several API methods. If some method fails then the project may be partially cloned.

        Important notes:
        * No tasks are cloned.
        * The expiration date is not changed in the new project.
        * The same skills are used.
        * If `reuse_controllers` is `True`, quality control collectors monitor both projects.
            For example, the `fast_submitted_count` rule counts fast responses in the cloned and new projects together.

        Args:
            project_id: The ID of the project to be cloned.
            reuse_controllers:
                * `True` — Use same quality controllers in cloned and created projects.
                * `False` — Use separate quality controllers.

                Default value: `True`.

        Returns:
            Tuple[Project, List[Pool], List[Training]]: Created project, pools and trainings.

        Example:

            >>> project, pools, trainings = toloka_client.clone_project(
            >>>     project_id='92694',
            >>>     reuse_controllers=False
            >>> )
            >>> # add tasks in pools and trainings
            ...
        """
        ...

    async def archive_pool(self, pool_id: str) -> toloka.client.pool.Pool:
        """Archives a pool.

        Only closed pools can be archived.

        You can't open archived pools, but you can [clone](toloka.client.TolokaClient.clone_pool.md) them if needed.

        Args:
            pool_id: The ID of the pool to be archived.

        Returns:
            Pool: The pool with updated status.

        Example:
            >>> closed_pool = next(toloka_client.get_pools(status='CLOSED'))
            >>> toloka_client.archive_pool(pool_id=closed_pool.id)
            ...
        """
        ...

    async def archive_pool_async(self, pool_id: str) -> typing.Optional[toloka.client.operations.PoolArchiveOperation]:
        """Archives a pool. Sends an asynchronous request to Toloka.

        Only closed pools can be archived.

        You can't open archived pools, but you can [clone](toloka.client.TolokaClient.clone_pool.md) them if needed.

        Args:
            pool_id: The ID of the pool to be archived.

        Returns:
            PoolArchiveOperation: An object to track the progress of the operation. If the pool is already archived then `None` is returned.

        Example:
            >>> closed_pool = next(toloka_client.get_pools(status='CLOSED'))
            >>> archive_op = toloka_client.archive_pool_async(pool_id=closed_pool.id)
            >>> toloka_client.wait_operation(archive_op)
            ...
        """
        ...

    async def close_pool(self, pool_id: str) -> toloka.client.pool.Pool:
        """Closes a pool.

        If all tasks in a pool are completed, then the pool is closed automatically.

        Args:
            pool_id: The ID of the pool to be closed.

        Returns:
            Pool: The pool with updated status.

        Example:
            >>> open_pool = next(toloka_client.get_pools(status='OPEN'))
            >>> toloka_client.close_pool(pool_id=open_pool.id)
            ...
        """
        ...

    async def close_pool_async(self, pool_id: str) -> typing.Optional[toloka.client.operations.PoolCloseOperation]:
        """Closes a pool. Sends an asynchronous request to Toloka.

        If all tasks in a pool are completed, then the pool is closed automatically.

        Args:
            pool_id: The ID of the pool to be closed.

        Returns:
            PoolCloseOperation: An object to track the progress of the operation. If the pool is already closed then `None` is returned.

        Example:
            >>> open_pool = next(toloka_client.get_pools(status='OPEN'))
            >>> close_op = toloka_client.close_pool_async(pool_id=open_pool.id)
            >>> toloka_client.wait_operation(close_op)
            ...
        """
        ...

    async def close_pool_for_update(self, pool_id: str) -> toloka.client.pool.Pool:
        """Closes a pool that is to be updated.

        To make changes to a pool, close it before updating parameters.
        If you don't open the pool after updating, it opens automatically in 15 minutes.

        Args:
            pool_id: The ID of the pool to be closed.

        Returns:
            Pool: The pool with updated status.

        Example:
            >>> toloka_client.close_pool_for_update(pool_id='1080020')
            ...
        """
        ...

    async def close_pool_for_update_async(self, pool_id: str) -> typing.Optional[toloka.client.operations.PoolCloseOperation]:
        """Closes a pool that is to be updated. Sends an asynchronous request to Toloka.

        To make changes to a pool, close it before updating parameters.
        If you don't open the pool after updating, it opens automatically in 15 minutes.

        Args:
            pool_id: The ID of the pool to be closed.

        Returns:
            PoolCloseOperation: An object to track the progress of the operation. If the pool is already closed then `None` is returned.

        Example:
            >>> close_op = toloka_client.close_pool_for_update_async(pool_id='1080020')
            >>> toloka_client.wait_operation(close_op)
            ...
        """
        ...

    async def clone_pool(self, pool_id: str) -> toloka.client.pool.Pool:
        """Clones an existing pool.

        An empty pool with the same parameters is created.
        The new pool is attached to the same project.

        Args:
            pool_id: The ID of the pool to be cloned.

        Returns:
            Pool: The new pool.

        Example:
            >>> new_pool = toloka_client.clone_pool(pool_id='1080020')
            ...
        """
        ...

    async def clone_pool_async(self, pool_id: str) -> toloka.client.operations.PoolCloneOperation:
        """Clones an existing pool. Sends an asynchronous request to Toloka.

        An empty pool with the same parameters is created.
        The new pool is attached to the same project.

        Args:
            pool_id: The ID of the pool to be cloned.

        Returns:
            PoolCloneOperation: An object to track the progress of the operation.

        Example:
            >>> clone_op = toloka_client.clone_pool_async(pool_id='1080020')
            >>> toloka_client.wait_operation(clone_op)
            ...
        """
        ...

    async def create_pool(
        self,
        pool: toloka.client.pool.Pool,
        tier: str = None
    ) -> toloka.client.pool.Pool:
        """Creates a new pool in Toloka.

        You can send a maximum of 20 requests of this kind per minute and 100 requests per day.

        Args:
            pool: The pool to be created.
            tier: Identificator of the pool data storage tier. By default, only 'default' tier is available. If no tier
                is specified, the pool is created in the 'default' tier.

        Returns:
            Pool: The pool with updated read-only fields.

        Example:
            Creating a new pool.

            >>> import datetime
            >>> new_pool = toloka.client.Pool(
            >>>     project_id='92694',
            >>>     private_name='Pool 1',
            >>>     may_contain_adult_content=False,
            >>>     will_expire=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365),
            >>>     reward_per_assignment=0.01,
            >>>     assignment_max_duration_seconds=60*20,
            >>>     defaults=toloka.client.Pool.Defaults(default_overlap_for_new_task_suites=3),
            >>>     filter=toloka.client.filter.Languages.in_('EN'),
            >>> )
            >>> new_pool.set_mixer_config(real_tasks_count=10, golden_tasks_count=0, training_tasks_count=0)
            >>> # To configure quality control rules call new_pool.quality_control.add_action()
            >>> new_pool = toloka_client.create_pool(new_pool)
            >>> print(new_pool.id)
            ...
        """
        ...

    @typing.overload
    async def find_pools(
        self,
        request: toloka.client.search_requests.PoolSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.PoolSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.PoolSearchResult:
        """Finds pools that match certain criteria.

        The number of returned pools is limited. To find remaining pools call `find_pools` with updated search criteria.

        To iterate over all matching pools you may use the [get_pools](toloka.client.TolokaClient.get_pools.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned pools limit.
                The maximum allowed value: 300. The default value: 20.

        Returns:
           PoolSearchResult: Found pools and a flag showing whether there are more matching pools exceeding the limit.

        Examples:
            Finding all pools in all projects.

            >>> find_result = toloka_client.find_pools()
            >>> pools = find_result.items
            ...

            Finding all open pools in all projects.

            >>> find_result = toloka_client.find_pools(status='OPEN')
            >>> pools = find_result.items
            ...

            Finding open pools in a specific project.

            >>> find_result = toloka_client.find_pools(status='OPEN', project_id='92694')
            >>> pools = find_result.items
            ...

            If there are pools exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    @typing.overload
    async def find_pools(
        self,
        status: typing.Optional[toloka.client.pool.Pool.Status] = None,
        project_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        last_started_lt: typing.Optional[datetime.datetime] = None,
        last_started_lte: typing.Optional[datetime.datetime] = None,
        last_started_gt: typing.Optional[datetime.datetime] = None,
        last_started_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.PoolSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.PoolSearchResult:
        """Finds pools that match certain criteria.

        The number of returned pools is limited. To find remaining pools call `find_pools` with updated search criteria.

        To iterate over all matching pools you may use the [get_pools](toloka.client.TolokaClient.get_pools.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned pools limit.
                The maximum allowed value: 300. The default value: 20.

        Returns:
           PoolSearchResult: Found pools and a flag showing whether there are more matching pools exceeding the limit.

        Examples:
            Finding all pools in all projects.

            >>> find_result = toloka_client.find_pools()
            >>> pools = find_result.items
            ...

            Finding all open pools in all projects.

            >>> find_result = toloka_client.find_pools(status='OPEN')
            >>> pools = find_result.items
            ...

            Finding open pools in a specific project.

            >>> find_result = toloka_client.find_pools(status='OPEN', project_id='92694')
            >>> pools = find_result.items
            ...

            If there are pools exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    async def get_pool(self, pool_id: str) -> toloka.client.pool.Pool:
        """Gets pool data from Toloka.

        Args:
            pool_id: The ID of the pool.

        Returns:
            Pool: The pool.

        Example:
            >>> pool = toloka_client.get_pool(pool_id='1080020')
            >>> print(pool.private_name, pool.status)
            ...
        """
        ...

    @typing.overload
    def get_pools(
        self,
        request: toloka.client.search_requests.PoolSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.pool.Pool, None]:
        """Finds all pools that match certain criteria.

        `get_pools` returns a generator. You can iterate over all found pools using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort pools use the [find_pools](toloka.client.TolokaClient.find_pools.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300. The default value: 20.

        Yields:
            Pool: The next matching pool.

        Example:
            Getting all open pools from a project.

            >>> open_pools = toloka_client.get_pools(project_id='92694', status='OPEN')
            ...
        """
        ...

    @typing.overload
    def get_pools(
        self,
        status: typing.Optional[toloka.client.pool.Pool.Status] = None,
        project_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        last_started_lt: typing.Optional[datetime.datetime] = None,
        last_started_lte: typing.Optional[datetime.datetime] = None,
        last_started_gt: typing.Optional[datetime.datetime] = None,
        last_started_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.pool.Pool, None]:
        """Finds all pools that match certain criteria.

        `get_pools` returns a generator. You can iterate over all found pools using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort pools use the [find_pools](toloka.client.TolokaClient.find_pools.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300. The default value: 20.

        Yields:
            Pool: The next matching pool.

        Example:
            Getting all open pools from a project.

            >>> open_pools = toloka_client.get_pools(project_id='92694', status='OPEN')
            ...
        """
        ...

    async def open_pool(self, pool_id: str) -> toloka.client.pool.Pool:
        """Opens a pool.

        After opening the pool, tasks can be assigned to Tolokers.

        Args:
            pool_id: The ID of the pool.

        Returns:
            Pool: The pool with updated status.

        Example:
            Opening a pool.

            >>> toloka_client.open_pool(pool_id='1080020')
            ...
        """
        ...

    async def open_pool_async(self, pool_id: str) -> typing.Optional[toloka.client.operations.PoolOpenOperation]:
        """Opens a pool. Sends an asynchronous request to Toloka.

        After opening the pool, tasks can be assigned to Tolokers.

        Args:
            pool_id: The ID of the pool.

        Returns:
            PoolOpenOperation: An object to track the progress of the operation. If the pool is already opened then `None` is returned.

        Example:
            Opening a pool.

            >>> open_op = toloka_client.open_pool(pool_id='1080020')
            >>> toloka_client.wait_operation(open_op)
            ...
        """
        ...

    @typing.overload
    async def patch_pool(
        self,
        pool_id: str,
        request: toloka.client.pool.PoolPatchRequest
    ) -> toloka.client.pool.Pool:
        """Changes pool parameters in Toloka.

        If a parameter is not specified in the `patch_pool` method, then it is left unchanged in Toloka.

        Args:
            pool_id: The ID of the pool to be changed.
            request: New pool parameters.

        Returns:
            Pool: The pool with updated parameters.

        Example:
            Changing priority of a pool.

            >>> pool = toloka_client.patch_pool(pool_id='1080020', priority=100)
            ...
        """
        ...

    @typing.overload
    async def patch_pool(
        self,
        pool_id: str,
        priority: typing.Optional[int] = None
    ) -> toloka.client.pool.Pool:
        """Changes pool parameters in Toloka.

        If a parameter is not specified in the `patch_pool` method, then it is left unchanged in Toloka.

        Args:
            pool_id: The ID of the pool to be changed.
            request: New pool parameters.

        Returns:
            Pool: The pool with updated parameters.

        Example:
            Changing priority of a pool.

            >>> pool = toloka_client.patch_pool(pool_id='1080020', priority=100)
            ...
        """
        ...

    async def update_pool(
        self,
        pool_id: str,
        pool: toloka.client.pool.Pool
    ) -> toloka.client.pool.Pool:
        """Updates all pool parameters in Toloka.

        Args:
            pool_id: The ID of the pool to be updated.
            pool: The pool with new parameters.

        Returns:
            Pool: The pool with updated parameters.

        Example:
            >>> updated_pool = toloka_client.get_pool(pool_id='1544394')
            >>> updated_pool.will_expire = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30)
            >>> toloka_client.update_pool(pool_id=updated_pool.id, pool=updated_pool)
            ...
        """
        ...

    async def archive_training(self, training_id: str) -> toloka.client.training.Training:
        """Archives a training.

        Only closed trainings can be archived.

        You can access archived trainings later.

        Args:
            training_id: The ID of the training to be archived.

        Returns:
            Training: The training with updated status.

        Example:
            >>> closed_training = next(toloka_client.get_trainings(status='CLOSED'))
            >>> toloka_client.archive_training(training_id=closed_training.id)
            ...
        """
        ...

    async def archive_training_async(self, training_id: str) -> typing.Optional[toloka.client.operations.TrainingArchiveOperation]:
        """Archives a training. Sends an asynchronous request to Toloka.

        Only closed trainings can be archived.

        You can access archived trainings later.

        Args:
            training_id: The ID of the training to be archived.

        Returns:
            TrainingArchiveOperation: An object to track the progress of the operation. If the training is already archived then `None` is returned.

        Example:
            >>> closed_training = next(toloka_client.find_trainings(status='CLOSED'))
            >>> archive_op = toloka_client.archive_training_async(training_id=closed_training.id)
            >>> toloka_client.wait_operation(archive_op)
            ...
        """
        ...

    async def close_training(self, training_id: str) -> toloka.client.training.Training:
        """Closes a training.

        Tasks from closed trainings are not assigned to Tolokers.

        Args:
            training_id: The ID of the training to be closed.

        Returns:
            Training: The training with updated status.

        Example:
            >>> opened_training = next(toloka_client.get_trainings(status='OPEN'))
            >>> toloka_client.close_training(training_id=opened_training.id)
            ...
        """
        ...

    async def close_training_async(self, training_id: str) -> typing.Optional[toloka.client.operations.TrainingCloseOperation]:
        """Closes a training. Sends an asynchronous request to Toloka.

        Tasks from closed trainings are not assigned to Tolokers.

        Args:
            training_id: The ID of the training to be closed.

        Returns:
            TrainingCloseOperation: An object to track the progress of the operation. If the training is already closed then `None` is returned.

        Example:
            >>> opened_training = next(toloka_client.get_trainings(status='OPEN'))
            >>> close_op = toloka_client.close_training_async(training_id=opened_training.id)
            >>> toloka_client.wait_operation(close_op)
            ...
        """
        ...

    async def clone_training(self, training_id: str) -> toloka.client.training.Training:
        """Clones an existing training.

        An empty training with the same parameters is created.
        The new training is attached to the same project.

        Args:
            training_id: The ID of the training to be cloned.

        Returns:
            Training: The new training.

        Example:
            >>> new_training = toloka_client.clone_training(training_id='1239110')
            ...
        """
        ...

    async def clone_training_async(self, training_id: str) -> toloka.client.operations.TrainingCloneOperation:
        """Clones an existing training. Sends an asynchronous request to Toloka.

        An empty training with the same parameters is created.
        The new training is attached to the same project.

        Args:
            training_id: The ID of the training to be cloned.

        Returns:
            TrainingCloneOperation: An object to track the progress of the operation.

        Example:
            >>> clone_op = toloka_client.clone_training_async(training_id='1239110')
            >>> toloka_client.wait_operation(clone_op)
            ...
        """
        ...

    async def create_training(self, training: toloka.client.training.Training) -> toloka.client.training.Training:
        """Creates a new training in Toloka.

        Args:
            training: A training to be created.

        Returns:
            Training: Created training with initialized read-only fields.

        Example:
            Creating a new training.

            >>> new_training = toloka.client.Training(
            >>>     project_id='118252',
            >>>     private_name='Some training in my project',
            >>>     may_contain_adult_content=True,
            >>>     assignment_max_duration_seconds=60*5,
            >>>     mix_tasks_in_creation_order=True,
            >>>     shuffle_tasks_in_task_suite=True,
            >>>     training_tasks_in_task_suite_count=3,
            >>>     task_suites_required_to_pass=1,
            >>>     retry_training_after_days=7,
            >>>     inherited_instructions=True,
            >>>     public_instructions='',
            >>> )
            >>> new_training = toloka_client.create_training(new_training)
            >>> print(new_training.id)
            ...
        """
        ...

    @typing.overload
    async def find_trainings(
        self,
        request: toloka.client.search_requests.TrainingSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.TrainingSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.TrainingSearchResult:
        """Finds trainings that match certain criteria.

        The number of returned trainings is limited. To find remaining trainings call `find_trainings` with updated search criteria.

        To iterate over all matching trainings you may use the [get_trainings](toloka.client.TolokaClient.get_trainings.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned trainings limit.
                The maximum allowed value: 300.

        Returns:
           TrainingSearchResult: Found trainings and a flag showing whether there are more matching trainings exceeding the limit.

        Examples:
            Finding all trainings in all projects.

            >>> find_result = toloka_client.find_trainings()
            >>> trainings = find_result.items
            ...

            Finding all opened trainings in all projects.

            >>> find_result = toloka_client.find_trainings(status='OPEN')
            >>> trainings = find_result.items
            ...

            Finding all opened trainings in a specific project.

            >>> find_result = toloka_client.find_trainings(status='OPEN', project_id='92694')
            >>> trainings = find_result.items
            ...

            If there are trainings exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    @typing.overload
    async def find_trainings(
        self,
        status: typing.Optional[toloka.client.training.Training.Status] = None,
        project_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        last_started_lt: typing.Optional[datetime.datetime] = None,
        last_started_lte: typing.Optional[datetime.datetime] = None,
        last_started_gt: typing.Optional[datetime.datetime] = None,
        last_started_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.TrainingSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.TrainingSearchResult:
        """Finds trainings that match certain criteria.

        The number of returned trainings is limited. To find remaining trainings call `find_trainings` with updated search criteria.

        To iterate over all matching trainings you may use the [get_trainings](toloka.client.TolokaClient.get_trainings.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned trainings limit.
                The maximum allowed value: 300.

        Returns:
           TrainingSearchResult: Found trainings and a flag showing whether there are more matching trainings exceeding the limit.

        Examples:
            Finding all trainings in all projects.

            >>> find_result = toloka_client.find_trainings()
            >>> trainings = find_result.items
            ...

            Finding all opened trainings in all projects.

            >>> find_result = toloka_client.find_trainings(status='OPEN')
            >>> trainings = find_result.items
            ...

            Finding all opened trainings in a specific project.

            >>> find_result = toloka_client.find_trainings(status='OPEN', project_id='92694')
            >>> trainings = find_result.items
            ...

            If there are trainings exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    async def get_training(self, training_id: str) -> toloka.client.training.Training:
        """Gets information about a training from Toloka.

        Args:
            training_id: The ID of the training.

        Returns:
            Training: The training.

        Example:
            >>> training = toloka_client.get_training(training_id='1239110')
            ...
        """
        ...

    @typing.overload
    def get_trainings(
        self,
        request: toloka.client.search_requests.TrainingSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.training.Training, None]:
        """Finds all trainings that match certain criteria.

        `get_trainings` returns a generator. You can iterate over all found trainings using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort trainings use the [find_trainings](toloka.client.TolokaClient.find_trainings.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300.

        Yields:
            Training: The next matching training.

        Example:
            Getting all trainings in a project.

            >>> trainings = toloka_client.get_trainings(project_id='92694')
            ...
        """
        ...

    @typing.overload
    def get_trainings(
        self,
        status: typing.Optional[toloka.client.training.Training.Status] = None,
        project_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        last_started_lt: typing.Optional[datetime.datetime] = None,
        last_started_lte: typing.Optional[datetime.datetime] = None,
        last_started_gt: typing.Optional[datetime.datetime] = None,
        last_started_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.training.Training, None]:
        """Finds all trainings that match certain criteria.

        `get_trainings` returns a generator. You can iterate over all found trainings using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort trainings use the [find_trainings](toloka.client.TolokaClient.find_trainings.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300.

        Yields:
            Training: The next matching training.

        Example:
            Getting all trainings in a project.

            >>> trainings = toloka_client.get_trainings(project_id='92694')
            ...
        """
        ...

    async def open_training(self, training_id: str) -> toloka.client.training.Training:
        """Opens a training.

        Tasks from opened trainings can be assigned to Tolokers.

        Args:
            training_id: The ID of the training.

        Returns:
            Training: The training with updated status.

        Example:
            Opening a training.

            >>> toloka_client.open_training(training_id='1239110')
            ...
        """
        ...

    async def open_training_async(self, training_id: str) -> typing.Optional[toloka.client.operations.TrainingOpenOperation]:
        """Opens a training. Sends an asynchronous request to Toloka.

        Tasks from opened trainings can be assigned to Tolokers.

        Args:
            training_id: The ID of the training.

        Returns:
            TrainingOpenOperation: An object to track the progress of the operation.
                If the training is already opened then `None` is returned.

        Example:
            Opening a training.

            >>> open_op = toloka_client.open_training_async(training_id='1239110')
            >>> toloka_client.wait_operation(open_op)
            ...
        """
        ...

    async def update_training(
        self,
        training_id: str,
        training: toloka.client.training.Training
    ) -> toloka.client.training.Training:
        """Updates parameters of a training in Toloka.

        Args:
            training_id: The ID of the training to be updated.
            training: A training object with new parameter values.

        Returns:
            Training: The updated training.

        Example:
            The example shows how to set new time limit in a training.

            >>> updated_training = toloka_client.get_training(training_id='1239110')
            >>> updated_training.assignment_max_duration_seconds = 600
            >>> toloka_client.update_training(training_id=updated_training.id, training=updated_training)
            ...
        """
        ...

    @typing.overload
    async def create_skill(self, skill: toloka.client.skill.Skill) -> toloka.client.skill.Skill:
        """Creates a new skill.

        You can send a maximum of 10 requests of this kind per minute and 100 requests per day.

        Args:
            skill: The skill to be created.

        Returns:
            Skill: The skill with updated read-only fields.

        Example:
            >>> new_skill = toloka_client.create_skill(
            >>>     name='Area selection of road signs',
            >>>     public_requester_description={
            >>>         'EN': 'Tolokers annotate road signs',
            >>>         'FR': "Les Tolokers annotent les signaux routier",
            >>>     },
            >>> )
            >>> print(new_skill.id)
            ...
        """
        ...

    @typing.overload
    async def create_skill(
        self,
        *,
        name: typing.Optional[str] = None,
        private_comment: typing.Optional[str] = None,
        hidden: typing.Optional[bool] = None,
        skill_ttl_hours: typing.Optional[int] = None,
        training: typing.Optional[bool] = None,
        public_name: typing.Optional[typing.Dict[str, str]] = None,
        public_requester_description: typing.Optional[typing.Dict[str, str]] = None,
        owner: typing.Optional[toloka.client.owner.Owner] = None
    ) -> toloka.client.skill.Skill:
        """Creates a new skill.

        You can send a maximum of 10 requests of this kind per minute and 100 requests per day.

        Args:
            skill: The skill to be created.

        Returns:
            Skill: The skill with updated read-only fields.

        Example:
            >>> new_skill = toloka_client.create_skill(
            >>>     name='Area selection of road signs',
            >>>     public_requester_description={
            >>>         'EN': 'Tolokers annotate road signs',
            >>>         'FR': "Les Tolokers annotent les signaux routier",
            >>>     },
            >>> )
            >>> print(new_skill.id)
            ...
        """
        ...

    @typing.overload
    async def find_skills(
        self,
        request: toloka.client.search_requests.SkillSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.SkillSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.SkillSearchResult:
        """Finds skills that match certain criteria.

        The number of returned skills is limited. To find remaining skills call `find_skills` with updated search criteria.

        To iterate over all matching skills you may use the [get_skills](toloka.client.TolokaClient.get_skills.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned skills limit.
                The maximum allowed value: 100.

        Returns:
           SkillSearchResult: Found skills and a flag showing whether there are more matching skills exceeding the limit.

        Example:
            The example shows how to find ten most recently created skills.

            >>> find_result = toloka_client.find_skills(sort=['-created', '-id'], limit=10)
            >>> skills = find_result.items
            ...
        """
        ...

    @typing.overload
    async def find_skills(
        self,
        name: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.SkillSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.SkillSearchResult:
        """Finds skills that match certain criteria.

        The number of returned skills is limited. To find remaining skills call `find_skills` with updated search criteria.

        To iterate over all matching skills you may use the [get_skills](toloka.client.TolokaClient.get_skills.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned skills limit.
                The maximum allowed value: 100.

        Returns:
           SkillSearchResult: Found skills and a flag showing whether there are more matching skills exceeding the limit.

        Example:
            The example shows how to find ten most recently created skills.

            >>> find_result = toloka_client.find_skills(sort=['-created', '-id'], limit=10)
            >>> skills = find_result.items
            ...
        """
        ...

    async def get_skill(self, skill_id: str) -> toloka.client.skill.Skill:
        """Gets skill information from Toloka.

        Args:
            skill_id: The ID of the skill.

        Returns:
            Skill: The skill.

        Example:
            >>> skill = toloka_client.get_skill(skill_id='14486')
            ...
        """
        ...

    @typing.overload
    def get_skills(
        self,
        request: toloka.client.search_requests.SkillSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.skill.Skill, None]:
        """Finds all skills that match certain criteria.

        `get_skills` returns a generator. You can iterate over all found skills using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort skills use the [find_skills](toloka.client.TolokaClient.find_skills.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100.

        Yields:
            Skill: The next matching skill.

        Example:
            How to check that a skill exists.

            >>> segmentation_skill = next(toloka_client.get_skills(name='Area selection of road signs'), None)
            >>> if segmentation_skill:
            >>>     print(f'Segmentation skill already exists, with id {segmentation_skill.id}')
            >>> else:
            >>>     print('Create new segmentation skill here')
            ...
        """
        ...

    @typing.overload
    def get_skills(
        self,
        name: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.skill.Skill, None]:
        """Finds all skills that match certain criteria.

        `get_skills` returns a generator. You can iterate over all found skills using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort skills use the [find_skills](toloka.client.TolokaClient.find_skills.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100.

        Yields:
            Skill: The next matching skill.

        Example:
            How to check that a skill exists.

            >>> segmentation_skill = next(toloka_client.get_skills(name='Area selection of road signs'), None)
            >>> if segmentation_skill:
            >>>     print(f'Segmentation skill already exists, with id {segmentation_skill.id}')
            >>> else:
            >>>     print('Create new segmentation skill here')
            ...
        """
        ...

    async def update_skill(
        self,
        skill_id: str,
        skill: toloka.client.skill.Skill
    ) -> toloka.client.skill.Skill:
        """Updates all skill parameters in Toloka.

        Args:
            skill_id: The ID of the skill to be updated.
            skill: The skill with new parameters.

        Returns:
            Skill: The skill with updated parameters.

        Example:
            >>> updated_skill = toloka_client.get_skill(skill_id='14486')
            >>> updated_skill.hidden = False
            >>> toloka_client.update_skill(skill_id=updated_skill.id, skill=updated_skill)
            ...
        """
        ...

    async def get_analytics(self, stats: typing.List[toloka.client.analytics_request.AnalyticsRequest]) -> toloka.client.operations.AnalyticsOperation:
        """Sends analytics requests to Toloka.

        You can request up to 10 metrics at a time.

        The values of different analytical metrics are returned in the `details` field of the operation when it is completed.

        Args:
            stats: A list of analytics requests.

        Returns:
            operations.AnalyticsOperation: An object to track the progress of the operation.

        Example:
            The example shows how get the percentage of completed tasks in the pool.

            >>> from toloka.client.analytics_request import CompletionPercentagePoolAnalytics
            >>>
            >>> operation = toloka_client.get_analytics([CompletionPercentagePoolAnalytics(subject_id='1080020')])
            >>> operation = toloka_client.wait_operation(operation)
            >>> print(operation.details['value'][0])
            >>> completed_task_percentage = operation.details['value'][0]['result']['value']
            ...

            The example monitors the percentage of completed tasks in the pool every minute until the pool is closed.

            >>> from toloka.client.analytics_request import CompletionPercentagePoolAnalytics
            >>>
            >>> pool = toloka_client.get_pool('1080020')
            >>> while not pool.is_closed():
            >>>     op = toloka_client.get_analytics( [CompletionPercentagePoolAnalytics(subject_id=pool.id)] )
            >>>     op = toloka_client.wait_operation(op)
            >>>     percentage = op.details['value'][0]['result']['value']
            >>>     print(f'{percentage}%')
            >>>     time.sleep(60)
            >>>     pool = toloka_client.get_pool(pool.id)
            >>> print('The pool is closed.')
            ...
        """
        ...

    @typing.overload
    async def create_task(
        self,
        task: toloka.client.task.Task,
        parameters: typing.Optional[toloka.client.task.CreateTaskParameters] = None
    ) -> toloka.client.task.Task:
        """Creates a new task in Toloka.

        If `async_mode` is `True` then an asynchronous operation is started internally and `create_task` waits for the completion of it.

        You can send a maximum of 100,000 requests of this kind per minute and a maximum of 2,000,000 requests per day.

        To create several tasks at once use the [create_tasks](toloka.client.TolokaClient.create_tasks.md) method.

        Args:
            task: The task to be created.
            parameters: Additional parameters of the request.
                Default: `None` — default overlap is used and the pool is started after task creation.

        Returns:
            Task: The created task.

        Example:
            >>> task = toloka.client.Task(
            >>>     input_values={'image': 'https://tlk.s3.yandex.net/dataset/cats_vs_dogs/dogs/048e5760fc5a46faa434922b2447a527.jpg'},
            >>>     pool_id='1080020'
            >>> )
            >>> toloka_client.create_task(task=task, allow_defaults=True)
            ...
        """
        ...

    @typing.overload
    async def create_task(
        self,
        task: toloka.client.task.Task,
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        allow_defaults: typing.Optional[bool] = None,
        open_pool: typing.Optional[bool] = None
    ) -> toloka.client.task.Task:
        """Creates a new task in Toloka.

        If `async_mode` is `True` then an asynchronous operation is started internally and `create_task` waits for the completion of it.

        You can send a maximum of 100,000 requests of this kind per minute and a maximum of 2,000,000 requests per day.

        To create several tasks at once use the [create_tasks](toloka.client.TolokaClient.create_tasks.md) method.

        Args:
            task: The task to be created.
            parameters: Additional parameters of the request.
                Default: `None` — default overlap is used and the pool is started after task creation.

        Returns:
            Task: The created task.

        Example:
            >>> task = toloka.client.Task(
            >>>     input_values={'image': 'https://tlk.s3.yandex.net/dataset/cats_vs_dogs/dogs/048e5760fc5a46faa434922b2447a527.jpg'},
            >>>     pool_id='1080020'
            >>> )
            >>> toloka_client.create_task(task=task, allow_defaults=True)
            ...
        """
        ...

    @typing.overload
    async def create_tasks(
        self,
        tasks: typing.List[toloka.client.task.Task],
        parameters: typing.Optional[toloka.client.task.CreateTasksParameters] = None
    ) -> toloka.client.batch_create_results.TaskBatchCreateResult:
        """Creates several tasks in Toloka.

        You can create general and control tasks together. Tasks can be added to different pools.
        Note that pools must be configured before accepting new tasks. For example, [mixer configuration](toloka.client.pool.mixer_config.MixerConfig.md) must be set.

        If `async_mode` is `True`, `create_tasks` starts asynchronous operation internally and waits for the completion of it.
        It is recommended to create no more than 10,000 tasks per request in this mode.

        If `async_mode` is `False`, no more than 5000 tasks can be created in a single request.
        Do not change `async_mode` to `False`, if you don't understand clearly why you need it.

        You can send no more than 100,000 requests per minute and no more than 2,000,000 requests per day.

        Args:
            tasks: A list of tasks to be created.
            parameters: Additional parameters of the request.

        Returns:
            batch_create_results.TaskBatchCreateResult: The result of the operation.

        Raises:
            ValidationApiError:
                * No tasks were created.
                * Validation errors found while the `skip_invalid_items` parameter was `False`.

        Example:
            The first example shows how to create tasks using a TSV file.

            >>> import pandas
            >>> dataset = pandas.read_csv('dataset.tsv', sep=';')
            >>> tasks = [
            >>>     toloka.client.Task(input_values={'image': url}, pool_id='1080020')
            >>>     for url in dataset['image'].values[:50]
            >>> ]
            >>> result = toloka_client.create_tasks(tasks, allow_defaults=True)
            >>> print(len(result.items))
            ...

            The second example shows how to add control tasks.

            >>> import pandas
            >>> dataset = pandas.read_csv('labeled_dataset.tsv', sep=';')
            >>> golden_tasks = []
            >>> for _, row in dataset.iterrows():
            >>>     golden_tasks.append(
            >>>         toloka.client.Task(
            >>>             input_values={'image': row['image']},
            >>>             known_solutions=[
            >>>                 toloka.client.BaseTask.KnownSolution(output_values={'animal': row['label']})
            >>>             ],
            >>>             pool_id='1080020',
            >>>         )
            >>>     )
            >>> result = toloka_client.create_tasks(golden_tasks, allow_defaults=True)
            >>> print(len(result.items))
            ...
        """
        ...

    @typing.overload
    async def create_tasks(
        self,
        tasks: typing.List[toloka.client.task.Task],
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        allow_defaults: typing.Optional[bool] = None,
        open_pool: typing.Optional[bool] = None,
        skip_invalid_items: typing.Optional[bool] = None
    ) -> toloka.client.batch_create_results.TaskBatchCreateResult:
        """Creates several tasks in Toloka.

        You can create general and control tasks together. Tasks can be added to different pools.
        Note that pools must be configured before accepting new tasks. For example, [mixer configuration](toloka.client.pool.mixer_config.MixerConfig.md) must be set.

        If `async_mode` is `True`, `create_tasks` starts asynchronous operation internally and waits for the completion of it.
        It is recommended to create no more than 10,000 tasks per request in this mode.

        If `async_mode` is `False`, no more than 5000 tasks can be created in a single request.
        Do not change `async_mode` to `False`, if you don't understand clearly why you need it.

        You can send no more than 100,000 requests per minute and no more than 2,000,000 requests per day.

        Args:
            tasks: A list of tasks to be created.
            parameters: Additional parameters of the request.

        Returns:
            batch_create_results.TaskBatchCreateResult: The result of the operation.

        Raises:
            ValidationApiError:
                * No tasks were created.
                * Validation errors found while the `skip_invalid_items` parameter was `False`.

        Example:
            The first example shows how to create tasks using a TSV file.

            >>> import pandas
            >>> dataset = pandas.read_csv('dataset.tsv', sep=';')
            >>> tasks = [
            >>>     toloka.client.Task(input_values={'image': url}, pool_id='1080020')
            >>>     for url in dataset['image'].values[:50]
            >>> ]
            >>> result = toloka_client.create_tasks(tasks, allow_defaults=True)
            >>> print(len(result.items))
            ...

            The second example shows how to add control tasks.

            >>> import pandas
            >>> dataset = pandas.read_csv('labeled_dataset.tsv', sep=';')
            >>> golden_tasks = []
            >>> for _, row in dataset.iterrows():
            >>>     golden_tasks.append(
            >>>         toloka.client.Task(
            >>>             input_values={'image': row['image']},
            >>>             known_solutions=[
            >>>                 toloka.client.BaseTask.KnownSolution(output_values={'animal': row['label']})
            >>>             ],
            >>>             pool_id='1080020',
            >>>         )
            >>>     )
            >>> result = toloka_client.create_tasks(golden_tasks, allow_defaults=True)
            >>> print(len(result.items))
            ...
        """
        ...

    @typing.overload
    async def create_tasks_async(
        self,
        tasks: typing.List[toloka.client.task.Task],
        parameters: typing.Optional[toloka.client.task.CreateTasksParameters] = None
    ) -> toloka.client.operations.TasksCreateOperation:
        """Creates tasks in Toloka asynchronously.

        You can send a maximum of 100,000 requests of this kind per minute and a maximum of 2,000,000 requests per day.

        See also the [create_tasks](toloka.client.TolokaClient.create_tasks.md) method.

        Args:
            tasks: A list of tasks to be created.
            parameters: Additional parameters of the request.

        Returns:
            TasksCreateOperation: An object to track the progress of the operation.

        Example:
            >>> tasks = [
            >>>     toloka.client.Task(input_values={'image': 'https://some.url/img0.png'}, pool_id='1080020'),
            >>>     toloka.client.Task(input_values={'image': 'https://some.url/img1.png'}, pool_id='1080020')
            >>> ]
            >>> tasks_op = toloka_client.create_tasks_async(tasks)
            >>> toloka_client.wait_operation(tasks_op)
            ...
        """
        ...

    @typing.overload
    async def create_tasks_async(
        self,
        tasks: typing.List[toloka.client.task.Task],
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        allow_defaults: typing.Optional[bool] = None,
        open_pool: typing.Optional[bool] = None,
        skip_invalid_items: typing.Optional[bool] = None
    ) -> toloka.client.operations.TasksCreateOperation:
        """Creates tasks in Toloka asynchronously.

        You can send a maximum of 100,000 requests of this kind per minute and a maximum of 2,000,000 requests per day.

        See also the [create_tasks](toloka.client.TolokaClient.create_tasks.md) method.

        Args:
            tasks: A list of tasks to be created.
            parameters: Additional parameters of the request.

        Returns:
            TasksCreateOperation: An object to track the progress of the operation.

        Example:
            >>> tasks = [
            >>>     toloka.client.Task(input_values={'image': 'https://some.url/img0.png'}, pool_id='1080020'),
            >>>     toloka.client.Task(input_values={'image': 'https://some.url/img1.png'}, pool_id='1080020')
            >>> ]
            >>> tasks_op = toloka_client.create_tasks_async(tasks)
            >>> toloka_client.wait_operation(tasks_op)
            ...
        """
        ...

    @typing.overload
    async def find_tasks(
        self,
        request: toloka.client.search_requests.TaskSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.TaskSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.TaskSearchResult:
        """Finds tasks that match certain criteria.

        The number of returned tasks is limited. To find remaining tasks call `find_tasks` with updated search criteria.

        To iterate over all matching tasks you may use the [get_tasks](toloka.client.TolokaClient.get_tasks.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned tasks limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            TaskSearchResult: Found tasks and a flag showing whether there are more matching tasks exceeding the limit.

        Example:
            To find three most recently created tasks in a pool, call the method with the following parameters:

            >>> find_result = toloka_client.find_tasks(pool_id='1086170', sort=['-created', '-id'], limit=3)
            >>> tasks = find_result.items
            ...
        """
        ...

    @typing.overload
    async def find_tasks(
        self,
        pool_id: typing.Optional[str] = None,
        overlap: typing.Optional[int] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        overlap_lt: typing.Optional[int] = None,
        overlap_lte: typing.Optional[int] = None,
        overlap_gt: typing.Optional[int] = None,
        overlap_gte: typing.Optional[int] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.TaskSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.TaskSearchResult:
        """Finds tasks that match certain criteria.

        The number of returned tasks is limited. To find remaining tasks call `find_tasks` with updated search criteria.

        To iterate over all matching tasks you may use the [get_tasks](toloka.client.TolokaClient.get_tasks.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned tasks limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            TaskSearchResult: Found tasks and a flag showing whether there are more matching tasks exceeding the limit.

        Example:
            To find three most recently created tasks in a pool, call the method with the following parameters:

            >>> find_result = toloka_client.find_tasks(pool_id='1086170', sort=['-created', '-id'], limit=3)
            >>> tasks = find_result.items
            ...
        """
        ...

    async def get_task(self, task_id: str) -> toloka.client.task.Task:
        """Gets a task with specified ID from Toloka.

        Args:
            task_id: The ID of the task.

        Returns:
            Task: The task with the ID specified in the request.

        Example:
            >>> task = toloka_client.get_task(task_id='00001092da--61ef01d5825234636bb088eb')
            >>> print(task.input_values)
            ...
        """
        ...

    @typing.overload
    def get_tasks(
        self,
        request: toloka.client.search_requests.TaskSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.task.Task, None]:
        """Finds all tasks that match certain criteria.

        `get_tasks` returns a generator. You can iterate over all found tasks using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort tasks use the [find_tasks](toloka.client.TolokaClient.find_tasks.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            Task: The next matching task.

        Example:
            Getting all tasks from a single pool.

            >>> tasks = list(toloka_client.get_tasks(pool_id='1086170'))
            ...
        """
        ...

    @typing.overload
    def get_tasks(
        self,
        pool_id: typing.Optional[str] = None,
        overlap: typing.Optional[int] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        overlap_lt: typing.Optional[int] = None,
        overlap_lte: typing.Optional[int] = None,
        overlap_gt: typing.Optional[int] = None,
        overlap_gte: typing.Optional[int] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.task.Task, None]:
        """Finds all tasks that match certain criteria.

        `get_tasks` returns a generator. You can iterate over all found tasks using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort tasks use the [find_tasks](toloka.client.TolokaClient.find_tasks.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            Task: The next matching task.

        Example:
            Getting all tasks from a single pool.

            >>> tasks = list(toloka_client.get_tasks(pool_id='1086170'))
            ...
        """
        ...

    @typing.overload
    async def patch_task(
        self,
        task_id: str,
        patch: toloka.client.task.TaskPatch
    ) -> toloka.client.task.Task:
        """Changes a task overlap value.

        Args:
            task_id: The ID of the task.
            patch: New task parameters.

        Returns:
            Task: The task with updated fields.

        Example:
            >>> toloka_client.patch_task(task_id='000012bb84--62d80429f20bf20e50f36a27', overlap=5)
            ...
        """
        ...

    @typing.overload
    async def patch_task(
        self,
        task_id: str,
        *,
        overlap: typing.Optional[int] = None,
        infinite_overlap: typing.Optional[bool] = None,
        baseline_solutions: typing.Optional[typing.List[toloka.client.task.Task.BaselineSolution]] = None,
        known_solutions: typing.Optional[typing.List[toloka.client.task.BaseTask.KnownSolution]] = None,
        message_on_unknown_solution: typing.Optional[str] = None
    ) -> toloka.client.task.Task:
        """Changes a task overlap value.

        Args:
            task_id: The ID of the task.
            patch: New task parameters.

        Returns:
            Task: The task with updated fields.

        Example:
            >>> toloka_client.patch_task(task_id='000012bb84--62d80429f20bf20e50f36a27', overlap=5)
            ...
        """
        ...

    @typing.overload
    async def patch_task_overlap_or_min(
        self,
        task_id: str,
        patch: toloka.client.task.TaskOverlapPatch
    ) -> toloka.client.task.Task:
        """Stops assigning a task to Tolokers.

        Args:
            task_id: The ID of the task.
            patch: New overlap value.

        Returns:
            Task: The task with updated fields.

        Example:
            Setting an infinite overlap for a training task.

            >>> toloka_client.patch_task_overlap_or_min(task_id='00001092da--61ef01d5825234636bb088eb',
            >>>     overlap=0, infinite_overlap=False
            >>> )
            ...
        """
        ...

    @typing.overload
    async def patch_task_overlap_or_min(
        self,
        task_id: str,
        *,
        overlap: typing.Optional[int] = None,
        infinite_overlap: typing.Optional[bool] = None
    ) -> toloka.client.task.Task:
        """Stops assigning a task to Tolokers.

        Args:
            task_id: The ID of the task.
            patch: New overlap value.

        Returns:
            Task: The task with updated fields.

        Example:
            Setting an infinite overlap for a training task.

            >>> toloka_client.patch_task_overlap_or_min(task_id='00001092da--61ef01d5825234636bb088eb',
            >>>     overlap=0, infinite_overlap=False
            >>> )
            ...
        """
        ...

    @typing.overload
    async def create_task_suite(
        self,
        task_suite: toloka.client.task_suite.TaskSuite,
        parameters: typing.Optional[toloka.client.task_suite.TaskSuiteCreateRequestParameters] = None
    ) -> toloka.client.task_suite.TaskSuite:
        """Creates a task suite in Toloka.

        Usually, you don't need to create a task suite manually, because Toloka can group tasks into suites automatically.

        Use this method if you need to group specific tasks together or to set different parameters in different task suites.

        You can send a maximum of 100,000 requests of this kind per minute and 2,000,000 requests per day.
        To create several task suites at once use the [create_task_suites](toloka.client.TolokaClient.create_task_suites.md) method.

        Args:
            task_suite: A task suite to be created.
            parameters: Additional parameters of the request. Default: `None`

        Returns:
            TaskSuite: Created task suite.

        Example:
            >>> new_task_suite = toloka.client.TaskSuite(
            >>>     pool_id='1086170',
            >>>     tasks=[toloka.client.Task(input_values={'label': 'Cats vs Dogs'})],
            >>>     overlap=2
            >>> )
            >>> toloka_client.create_task_suite(new_task_suite)
            ...
        """
        ...

    @typing.overload
    async def create_task_suite(
        self,
        task_suite: toloka.client.task_suite.TaskSuite,
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        allow_defaults: typing.Optional[bool] = None,
        open_pool: typing.Optional[bool] = None
    ) -> toloka.client.task_suite.TaskSuite:
        """Creates a task suite in Toloka.

        Usually, you don't need to create a task suite manually, because Toloka can group tasks into suites automatically.

        Use this method if you need to group specific tasks together or to set different parameters in different task suites.

        You can send a maximum of 100,000 requests of this kind per minute and 2,000,000 requests per day.
        To create several task suites at once use the [create_task_suites](toloka.client.TolokaClient.create_task_suites.md) method.

        Args:
            task_suite: A task suite to be created.
            parameters: Additional parameters of the request. Default: `None`

        Returns:
            TaskSuite: Created task suite.

        Example:
            >>> new_task_suite = toloka.client.TaskSuite(
            >>>     pool_id='1086170',
            >>>     tasks=[toloka.client.Task(input_values={'label': 'Cats vs Dogs'})],
            >>>     overlap=2
            >>> )
            >>> toloka_client.create_task_suite(new_task_suite)
            ...
        """
        ...

    @typing.overload
    async def create_task_suites(
        self,
        task_suites: typing.List[toloka.client.task_suite.TaskSuite],
        parameters: typing.Optional[toloka.client.task_suite.TaskSuitesCreateRequestParameters] = None
    ) -> toloka.client.batch_create_results.TaskSuiteBatchCreateResult:
        """Creates several task suites in Toloka.

        Usually, you don't need to create task suites manually, because Toloka can group tasks into suites automatically.

        Use this method if you need to group specific tasks together or to set different parameters in different task suites.
        You can create general and control tasks or task suites in different pools with a single method call.

        If `async_mode` is `True`, `create_task_suites` starts asynchronous operation internally and waits for the completion of it.

        If `async_mode` is `False`, no more than 5000 tasks can be created in a single request. Don't
        change `async_mode` to `False`, if you do not understand clearly why you need it.

        You can send a maximum of 100,000 requests of this kind per minute and 2,000,000 requests per day.
        It is recommended that you create no more than 10,000 task suites in a single request if the `async_mode` parameter is `True`.

        Args:
            task_suites: A list of task suites to be created.
            parameters: Additional parameters of the request. Default: `None`

        Returns:
            TaskSuiteBatchCreateResult: The result of the operation.

        Raises:
            ValidationApiError:
                * No tasks were created.
                * Validation errors found while the `skip_invalid_items` parameter was `False`.

        Example:
            >>> task_suites = [
            >>>     toloka.client.TaskSuite(
            >>>         pool_id='1086170',
            >>>         overlap=1,
            >>>         tasks=[
            >>>             toloka.client.Task(input_values={
            >>>                 'question': 'Choose a random number'
            >>>             })
            >>>         ]
            >>>     )
            >>> ]
            >>> task_suites = toloka_client.create_task_suites(task_suites)
            ...
        """
        ...

    @typing.overload
    async def create_task_suites(
        self,
        task_suites: typing.List[toloka.client.task_suite.TaskSuite],
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        allow_defaults: typing.Optional[bool] = None,
        open_pool: typing.Optional[bool] = None,
        skip_invalid_items: typing.Optional[bool] = None
    ) -> toloka.client.batch_create_results.TaskSuiteBatchCreateResult:
        """Creates several task suites in Toloka.

        Usually, you don't need to create task suites manually, because Toloka can group tasks into suites automatically.

        Use this method if you need to group specific tasks together or to set different parameters in different task suites.
        You can create general and control tasks or task suites in different pools with a single method call.

        If `async_mode` is `True`, `create_task_suites` starts asynchronous operation internally and waits for the completion of it.

        If `async_mode` is `False`, no more than 5000 tasks can be created in a single request. Don't
        change `async_mode` to `False`, if you do not understand clearly why you need it.

        You can send a maximum of 100,000 requests of this kind per minute and 2,000,000 requests per day.
        It is recommended that you create no more than 10,000 task suites in a single request if the `async_mode` parameter is `True`.

        Args:
            task_suites: A list of task suites to be created.
            parameters: Additional parameters of the request. Default: `None`

        Returns:
            TaskSuiteBatchCreateResult: The result of the operation.

        Raises:
            ValidationApiError:
                * No tasks were created.
                * Validation errors found while the `skip_invalid_items` parameter was `False`.

        Example:
            >>> task_suites = [
            >>>     toloka.client.TaskSuite(
            >>>         pool_id='1086170',
            >>>         overlap=1,
            >>>         tasks=[
            >>>             toloka.client.Task(input_values={
            >>>                 'question': 'Choose a random number'
            >>>             })
            >>>         ]
            >>>     )
            >>> ]
            >>> task_suites = toloka_client.create_task_suites(task_suites)
            ...
        """
        ...

    @typing.overload
    async def create_task_suites_async(
        self,
        task_suites: typing.List[toloka.client.task_suite.TaskSuite],
        parameters: typing.Optional[toloka.client.task_suite.TaskSuitesCreateRequestParameters] = None
    ) -> toloka.client.operations.TaskSuiteCreateBatchOperation:
        """Creates several task suites in Toloka asynchronously.

        You can send a maximum of 100,000 requests of this kind per minute and 2,000,000 requests per day.
        It is recommended that you create no more than 10,000 task suites in a single request.

        See also the [create_task_suites](toloka.client.TolokaClient.create_task_suites.md) method.

        Args:
            task_suites: A list of task suites to be created.
            parameters: Additional parameters of the request. Default: `None`

        Returns:
            TaskSuiteCreateBatchOperation: An object to track the progress of the operation.

        Example:
            >>> task_suites = [
            >>>     toloka.client.TaskSuite(
            >>>         pool_id='1086170',
            >>>         overlap=1,
            >>>         tasks=[
            >>>             toloka.client.Task(input_values={
            >>>                 'question': 'Choose a random country'
            >>>             })
            >>>         ]
            >>>     )
            >>> ]
            >>> task_suites_op = toloka_client.create_task_suites_async(task_suites)
            >>> toloka_client.wait_operation(task_suites_op)
            ...
        """
        ...

    @typing.overload
    async def create_task_suites_async(
        self,
        task_suites: typing.List[toloka.client.task_suite.TaskSuite],
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        allow_defaults: typing.Optional[bool] = None,
        open_pool: typing.Optional[bool] = None,
        skip_invalid_items: typing.Optional[bool] = None
    ) -> toloka.client.operations.TaskSuiteCreateBatchOperation:
        """Creates several task suites in Toloka asynchronously.

        You can send a maximum of 100,000 requests of this kind per minute and 2,000,000 requests per day.
        It is recommended that you create no more than 10,000 task suites in a single request.

        See also the [create_task_suites](toloka.client.TolokaClient.create_task_suites.md) method.

        Args:
            task_suites: A list of task suites to be created.
            parameters: Additional parameters of the request. Default: `None`

        Returns:
            TaskSuiteCreateBatchOperation: An object to track the progress of the operation.

        Example:
            >>> task_suites = [
            >>>     toloka.client.TaskSuite(
            >>>         pool_id='1086170',
            >>>         overlap=1,
            >>>         tasks=[
            >>>             toloka.client.Task(input_values={
            >>>                 'question': 'Choose a random country'
            >>>             })
            >>>         ]
            >>>     )
            >>> ]
            >>> task_suites_op = toloka_client.create_task_suites_async(task_suites)
            >>> toloka_client.wait_operation(task_suites_op)
            ...
        """
        ...

    @typing.overload
    async def find_task_suites(
        self,
        request: toloka.client.search_requests.TaskSuiteSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.TaskSuiteSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.TaskSuiteSearchResult:
        """Finds task suites that match certain criteria.

        The number of returned task suites is limited. To find remaining task suites call `find_task_suites` with updated search criteria.

        To iterate over all matching task suites you may use the [get_task_suites](toloka.client.TolokaClient.get_task_suites.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned task suites limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            TaskSuiteSearchResult: Found task suites and a flag showing whether there are more matching task suites exceeding the limit.

        Example:
            Find three most recently created task suites in a specified pool.

            >>> find_result = toloka_client.find_task_suites(pool_id='1086170', sort=['-created', '-id'], limit=3)
            >>> task_suites = find_result.items
            ...
        """
        ...

    @typing.overload
    async def find_task_suites(
        self,
        task_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        overlap: typing.Optional[int] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        overlap_lt: typing.Optional[int] = None,
        overlap_lte: typing.Optional[int] = None,
        overlap_gt: typing.Optional[int] = None,
        overlap_gte: typing.Optional[int] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.TaskSuiteSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.TaskSuiteSearchResult:
        """Finds task suites that match certain criteria.

        The number of returned task suites is limited. To find remaining task suites call `find_task_suites` with updated search criteria.

        To iterate over all matching task suites you may use the [get_task_suites](toloka.client.TolokaClient.get_task_suites.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned task suites limit.
                The maximum allowed value: 100,000. The default value: 50.

        Returns:
            TaskSuiteSearchResult: Found task suites and a flag showing whether there are more matching task suites exceeding the limit.

        Example:
            Find three most recently created task suites in a specified pool.

            >>> find_result = toloka_client.find_task_suites(pool_id='1086170', sort=['-created', '-id'], limit=3)
            >>> task_suites = find_result.items
            ...
        """
        ...

    async def get_task_suite(self, task_suite_id: str) -> toloka.client.task_suite.TaskSuite:
        """Gets task suite data from Toloka.

        Args:
            task_suite_id: The ID of the task suite.

        Returns:
            TaskSuite: The task suite.

        Example:
            >>> task_suite = toloka_client.get_task_suite(task_suite_id='00001092da--61ef030400c684132d0da0dc')
            >>> print(len(task_suite.tasks))
            ...
        """
        ...

    @typing.overload
    def get_task_suites(
        self,
        request: toloka.client.search_requests.TaskSuiteSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.task_suite.TaskSuite, None]:
        """Finds all task suites that match certain criteria.

        `get_task_suites` returns a generator. You can iterate over all found task suites using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort task suites use the [find_task_suites](toloka.client.TolokaClient.find_task_suites.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            TaskSuite: The next matching task suite.

        Example:
            >>> task_suites = list(toloka_client.get_task_suites(pool_id='1086170'))
            ...
        """
        ...

    @typing.overload
    def get_task_suites(
        self,
        task_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        overlap: typing.Optional[int] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        overlap_lt: typing.Optional[int] = None,
        overlap_lte: typing.Optional[int] = None,
        overlap_gt: typing.Optional[int] = None,
        overlap_gte: typing.Optional[int] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.task_suite.TaskSuite, None]:
        """Finds all task suites that match certain criteria.

        `get_task_suites` returns a generator. You can iterate over all found task suites using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort task suites use the [find_task_suites](toloka.client.TolokaClient.find_task_suites.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 100,000. The default value: 50.

        Yields:
            TaskSuite: The next matching task suite.

        Example:
            >>> task_suites = list(toloka_client.get_task_suites(pool_id='1086170'))
            ...
        """
        ...

    @typing.overload
    async def patch_task_suite(
        self,
        task_suite_id: str,
        patch: toloka.client.task_suite.TaskSuitePatch
    ) -> toloka.client.task_suite.TaskSuite:
        """Changes task suite parameter values in Toloka.

        Args:
            task_suite_id: The ID of the task suite.
            patch: New parameter values.

        Returns:
            TaskSuite: The task suite with updated fields.

        Example:
            Changing the task suite's priority.

            >>> toloka_client.patch_task_suite(
            >>>     task_suite_id='00001092da--61ef030400c684132d0da0dc',
            >>>     issuing_order_override=100
            >>> )
            ...
        """
        ...

    @typing.overload
    async def patch_task_suite(
        self,
        task_suite_id: str,
        *,
        infinite_overlap=None,
        overlap=None,
        issuing_order_override: typing.Optional[float] = None,
        open_pool: typing.Optional[bool] = None
    ) -> toloka.client.task_suite.TaskSuite:
        """Changes task suite parameter values in Toloka.

        Args:
            task_suite_id: The ID of the task suite.
            patch: New parameter values.

        Returns:
            TaskSuite: The task suite with updated fields.

        Example:
            Changing the task suite's priority.

            >>> toloka_client.patch_task_suite(
            >>>     task_suite_id='00001092da--61ef030400c684132d0da0dc',
            >>>     issuing_order_override=100
            >>> )
            ...
        """
        ...

    @typing.overload
    async def patch_task_suite_overlap_or_min(
        self,
        task_suite_id: str,
        patch: toloka.client.task_suite.TaskSuiteOverlapPatch
    ) -> toloka.client.task_suite.TaskSuite:
        """Stops assigning a task suite to Tolokers.

        Args:
            task_suite_id: The ID of the task suite.
            patch: New overlap value.

        Returns:
            TaskSuite: Task suite with updated fields.

        Example:
            >>> toloka_client.patch_task_suite_overlap_or_min(
            >>>     task_suite_id='00001092da--61ef030400c684132d0da0dc',
            >>>     overlap=0
            >>> )
            ...
        """
        ...

    @typing.overload
    async def patch_task_suite_overlap_or_min(
        self,
        task_suite_id: str,
        *,
        overlap: typing.Optional[int] = None
    ) -> toloka.client.task_suite.TaskSuite:
        """Stops assigning a task suite to Tolokers.

        Args:
            task_suite_id: The ID of the task suite.
            patch: New overlap value.

        Returns:
            TaskSuite: Task suite with updated fields.

        Example:
            >>> toloka_client.patch_task_suite_overlap_or_min(
            >>>     task_suite_id='00001092da--61ef030400c684132d0da0dc',
            >>>     overlap=0
            >>> )
            ...
        """
        ...

    async def get_operation(self, operation_id: str) -> toloka.client.operations.Operation:
        """Gets information about an operation from Toloka.

        Some API requests, for example uploading tasks or opening a pool, are processed as asynchronous operations that run in the background.
        You can track their progress or wait for them to complete by calling the [wait_operation](toloka.client.TolokaClient.wait_operation.md) method.

        Args:
            operation_id: The ID of the operation.

        Returns:
            Operation: The operation.

        Example:
            >>> operation = toloka_client.get_operation(operation_id='6d84114f-fcfc-473d-8249-1a4f3ea550eb')
            >>> print(operation.status, operation.finished)
            ...
        """
        ...

    @typing.overload
    async def find_operations(
        self,
        request: toloka.client.search_requests.OperationSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.OperationSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.OperationSearchResult:
        """Finds operations that match certain criteria.

        The number of returned operations is limited. To find remaining operations call `find_operations` with updated search criteria.

        To iterate over all matching operations you may use the [get_operations](toloka.client.TolokaClient.get_operations.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned operations limit.
                The maximum allowed value: 500. The default value: 50.

        Returns:
            OperationSearchResult: Found operations and a flag showing whether there are more matching operations exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_operations(
            >>>     type='POOL.OPEN', status='SUCCESS', sort=['-finished'], limit=3
            >>> )
            >>> operations = find_result.items
            ...
        """
        ...

    @typing.overload
    async def find_operations(
        self,
        type: typing.Optional[toloka.client.operations.OperationType] = None,
        status: typing.Optional[toloka.client.operations.Operation.Status] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        submitted_lt: typing.Optional[datetime.datetime] = None,
        submitted_lte: typing.Optional[datetime.datetime] = None,
        submitted_gt: typing.Optional[datetime.datetime] = None,
        submitted_gte: typing.Optional[datetime.datetime] = None,
        finished_lt: typing.Optional[datetime.datetime] = None,
        finished_lte: typing.Optional[datetime.datetime] = None,
        finished_gt: typing.Optional[datetime.datetime] = None,
        finished_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.OperationSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.OperationSearchResult:
        """Finds operations that match certain criteria.

        The number of returned operations is limited. To find remaining operations call `find_operations` with updated search criteria.

        To iterate over all matching operations you may use the [get_operations](toloka.client.TolokaClient.get_operations.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned operations limit.
                The maximum allowed value: 500. The default value: 50.

        Returns:
            OperationSearchResult: Found operations and a flag showing whether there are more matching operations exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_operations(
            >>>     type='POOL.OPEN', status='SUCCESS', sort=['-finished'], limit=3
            >>> )
            >>> operations = find_result.items
            ...
        """
        ...

    @typing.overload
    def get_operations(
        self,
        request: toloka.client.search_requests.OperationSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.operations.Operation, None]:
        """Finds all operations that match certain criteria.

        `get_operations` returns a generator. You can iterate over all found operations using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort operations use the [find_operations](toloka.client.TolokaClient.find_operations.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 500. The default value: 50.

        Yields:
            Operation: The next matching operation.

        Example:
            >>> some_operations = list(toloka_client.get_operations(submitted_lt='2023-06-01T00:00:00'))
            ...
        """
        ...

    @typing.overload
    def get_operations(
        self,
        type: typing.Optional[toloka.client.operations.OperationType] = None,
        status: typing.Optional[toloka.client.operations.Operation.Status] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        submitted_lt: typing.Optional[datetime.datetime] = None,
        submitted_lte: typing.Optional[datetime.datetime] = None,
        submitted_gt: typing.Optional[datetime.datetime] = None,
        submitted_gte: typing.Optional[datetime.datetime] = None,
        finished_lt: typing.Optional[datetime.datetime] = None,
        finished_lte: typing.Optional[datetime.datetime] = None,
        finished_gt: typing.Optional[datetime.datetime] = None,
        finished_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.operations.Operation, None]:
        """Finds all operations that match certain criteria.

        `get_operations` returns a generator. You can iterate over all found operations using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort operations use the [find_operations](toloka.client.TolokaClient.find_operations.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 500. The default value: 50.

        Yields:
            Operation: The next matching operation.

        Example:
            >>> some_operations = list(toloka_client.get_operations(submitted_lt='2023-06-01T00:00:00'))
            ...
        """
        ...

    async def get_operation_log(self, operation_id: str) -> typing.List[toloka.client.operation_log.OperationLogItem]:
        """Gets an operation log.

        You can get the log for operations: creating one or multiple tasks or task suites, or issuing bonuses to Tolokers.

        If the operation was successful, the log contains the IDs of the created objects, otherwise it contains the details of validation errors.

        Logs are available only for the last month.

        Args:
            operation_id: The ID of the operation.

        Returns:
            List[OperationLogItem]: A list with log items.

        Example:
            >>> operation_log = toloka_client.get_operation_log(operation_id='6d84114f-fcfc-473d-8249-1a4f3ea550eb')
            ...
        """
        ...

    @typing.overload
    async def create_user_bonus(
        self,
        user_bonus: toloka.client.user_bonus.UserBonus,
        parameters: typing.Optional[toloka.client.user_bonus.UserBonusCreateRequestParameters] = None
    ) -> toloka.client.user_bonus.UserBonus:
        """Issues a bonus payment to a Toloker.

        You can send a maximum of 10,000 requests of this kind per day.

        Args:
            user_bonus: The bonus.
            parameters: Parameters of the request.

        Returns:
            UserBonus: Created bonus.

        Example:
            Issuing a bonus to a Toloker with a message in 2 languages.

            >>> from decimal import Decimal
            >>> new_bonus = toloka_client.create_user_bonus(
            >>>     toloka.client.UserBonus(
            >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
            >>>         amount=Decimal('0.50'),
            >>>         public_title={
            >>>             'EN': 'Perfect job!',
            >>>             'RU': 'Прекрасная работа!',
            >>>         },
            >>>         public_message={
            >>>             'EN': 'You are the best!',
            >>>             'RU': 'Молодец!',
            >>>         },
            >>>         assignment_id='00001092da--61ef030400c684132d0da0de'
            >>>     )
            >>> )
            ...
        """
        ...

    @typing.overload
    async def create_user_bonus(
        self,
        user_bonus: toloka.client.user_bonus.UserBonus,
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True
    ) -> toloka.client.user_bonus.UserBonus:
        """Issues a bonus payment to a Toloker.

        You can send a maximum of 10,000 requests of this kind per day.

        Args:
            user_bonus: The bonus.
            parameters: Parameters of the request.

        Returns:
            UserBonus: Created bonus.

        Example:
            Issuing a bonus to a Toloker with a message in 2 languages.

            >>> from decimal import Decimal
            >>> new_bonus = toloka_client.create_user_bonus(
            >>>     toloka.client.UserBonus(
            >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
            >>>         amount=Decimal('0.50'),
            >>>         public_title={
            >>>             'EN': 'Perfect job!',
            >>>             'RU': 'Прекрасная работа!',
            >>>         },
            >>>         public_message={
            >>>             'EN': 'You are the best!',
            >>>             'RU': 'Молодец!',
            >>>         },
            >>>         assignment_id='00001092da--61ef030400c684132d0da0de'
            >>>     )
            >>> )
            ...
        """
        ...

    @typing.overload
    async def create_user_bonuses(
        self,
        user_bonuses: typing.List[toloka.client.user_bonus.UserBonus],
        parameters: typing.Optional[toloka.client.user_bonus.UserBonusesCreateRequestParameters] = None
    ) -> toloka.client.batch_create_results.UserBonusBatchCreateResult:
        """Issues several bonus payments to Tolokers.

        You can send a maximum of 10,000 requests of this kind per day.

        Args:
            user_bonuses: A list of bonuses.
            parameters: Parameters of the request.

        Returns:
            UserBonusBatchCreateResult: The result of the operation.

        Example:
            >>> from decimal import Decimal
            >>> new_bonuses=[
            >>>     toloka.client.UserBonus(
            >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
            >>>         amount=Decimal('1.00'),
            >>>         public_title={'EN': 'Perfect job!'},
            >>>         public_message={'EN': 'You are the best!'},
            >>>         assignment_id='00001092da--61ef030400c684132d0da0de'
            >>>     ),
            >>>     toloka.client.UserBonus(
            >>>         user_id='a1b0b42923c429daa2c764d7ccfc364d',
            >>>         amount=Decimal('0.80'),
            >>>         public_title={'EN': 'Excellent work!'},
            >>>         public_message={'EN': 'You have completed all tasks!'},
            >>>         assignment_id='000015fccc--63bfc4c358d7a46c32a7b233'
            >>>     )
            >>> ]
            >>> result = toloka_client.create_user_bonuses(new_bonuses)
            ...
        """
        ...

    @typing.overload
    async def create_user_bonuses(
        self,
        user_bonuses: typing.List[toloka.client.user_bonus.UserBonus],
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        skip_invalid_items: typing.Optional[bool] = None
    ) -> toloka.client.batch_create_results.UserBonusBatchCreateResult:
        """Issues several bonus payments to Tolokers.

        You can send a maximum of 10,000 requests of this kind per day.

        Args:
            user_bonuses: A list of bonuses.
            parameters: Parameters of the request.

        Returns:
            UserBonusBatchCreateResult: The result of the operation.

        Example:
            >>> from decimal import Decimal
            >>> new_bonuses=[
            >>>     toloka.client.UserBonus(
            >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
            >>>         amount=Decimal('1.00'),
            >>>         public_title={'EN': 'Perfect job!'},
            >>>         public_message={'EN': 'You are the best!'},
            >>>         assignment_id='00001092da--61ef030400c684132d0da0de'
            >>>     ),
            >>>     toloka.client.UserBonus(
            >>>         user_id='a1b0b42923c429daa2c764d7ccfc364d',
            >>>         amount=Decimal('0.80'),
            >>>         public_title={'EN': 'Excellent work!'},
            >>>         public_message={'EN': 'You have completed all tasks!'},
            >>>         assignment_id='000015fccc--63bfc4c358d7a46c32a7b233'
            >>>     )
            >>> ]
            >>> result = toloka_client.create_user_bonuses(new_bonuses)
            ...
        """
        ...

    @typing.overload
    async def create_user_bonuses_async(
        self,
        user_bonuses: typing.List[toloka.client.user_bonus.UserBonus],
        parameters: typing.Optional[toloka.client.user_bonus.UserBonusesCreateRequestParameters] = None
    ) -> toloka.client.operations.UserBonusCreateBatchOperation:
        """Issues bonus payments to Tolokers asynchronously.

        You can send a maximum of 10,000 requests of this kind per day.

        Args:
            user_bonuses: A list of bonuses.
            parameters: Parameters of the request.

        Returns:
            UserBonusCreateBatchOperation: An object to track the progress of the operation.

        Example:
            >>> from decimal import Decimal
            >>> new_bonuses=[
            >>>     toloka.client.UserBonus(
            >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
            >>>         amount=Decimal('1.00'),
            >>>         public_title={'EN': 'Perfect job!'},
            >>>         public_message={'EN': 'You are the best!'},
            >>>         assignment_id='00001092da--61ef030400c684132d0da0de'
            >>>     ),
            >>>     toloka.client.UserBonus(
            >>>         user_id='a1b0b42923c429daa2c764d7ccfc364d',
            >>>         amount=Decimal('0.80'),
            >>>         public_title={'EN': 'Excellent work!'},
            >>>         public_message={'EN': 'You have completed all tasks!'},
            >>>         assignment_id='000015fccc--63bfc4c358d7a46c32a7b233'
            >>>     )
            >>> ]
            >>> bonus_op = toloka_client.create_user_bonuses_async(new_bonuses)
            >>> toloka_client.wait_operation(bonus_op)
            ...
        """
        ...

    @typing.overload
    async def create_user_bonuses_async(
        self,
        user_bonuses: typing.List[toloka.client.user_bonus.UserBonus],
        *,
        operation_id: typing.Optional[uuid.UUID] = ...,
        async_mode: typing.Optional[bool] = True,
        skip_invalid_items: typing.Optional[bool] = None
    ) -> toloka.client.operations.UserBonusCreateBatchOperation:
        """Issues bonus payments to Tolokers asynchronously.

        You can send a maximum of 10,000 requests of this kind per day.

        Args:
            user_bonuses: A list of bonuses.
            parameters: Parameters of the request.

        Returns:
            UserBonusCreateBatchOperation: An object to track the progress of the operation.

        Example:
            >>> from decimal import Decimal
            >>> new_bonuses=[
            >>>     toloka.client.UserBonus(
            >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
            >>>         amount=Decimal('1.00'),
            >>>         public_title={'EN': 'Perfect job!'},
            >>>         public_message={'EN': 'You are the best!'},
            >>>         assignment_id='00001092da--61ef030400c684132d0da0de'
            >>>     ),
            >>>     toloka.client.UserBonus(
            >>>         user_id='a1b0b42923c429daa2c764d7ccfc364d',
            >>>         amount=Decimal('0.80'),
            >>>         public_title={'EN': 'Excellent work!'},
            >>>         public_message={'EN': 'You have completed all tasks!'},
            >>>         assignment_id='000015fccc--63bfc4c358d7a46c32a7b233'
            >>>     )
            >>> ]
            >>> bonus_op = toloka_client.create_user_bonuses_async(new_bonuses)
            >>> toloka_client.wait_operation(bonus_op)
            ...
        """
        ...

    @typing.overload
    async def find_user_bonuses(
        self,
        request: toloka.client.search_requests.UserBonusSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.UserBonusSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.UserBonusSearchResult:
        """Finds Tolokers' bonuses that match certain criteria.

        The number of returned bonuses is limited. To find remaining bonuses call `find_user_bonuses` with updated search criteria.

        To iterate over all matching Tolokers' bonuses you may use the [get_user_bonuses](toloka.client.TolokaClient.get_user_bonuses.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned Tolokers' bonuses limit.
                The maximum allowed value: 300.

        Returns:
            UserBonusSearchResult: Found Tolokers' bonuses and a flag showing whether there are more matching bonuses exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_user_bonuses(
            >>>     user_id='fac97860c7929add8048ed2ef63b66fd', sort=['-created', '-id'], limit=3
            >>> )
            >>> bonuses = find_result.items
            ...
        """
        ...

    @typing.overload
    async def find_user_bonuses(
        self,
        user_id: typing.Optional[str] = None,
        assignment_id: typing.Optional[str] = None,
        private_comment: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.UserBonusSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.UserBonusSearchResult:
        """Finds Tolokers' bonuses that match certain criteria.

        The number of returned bonuses is limited. To find remaining bonuses call `find_user_bonuses` with updated search criteria.

        To iterate over all matching Tolokers' bonuses you may use the [get_user_bonuses](toloka.client.TolokaClient.get_user_bonuses.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned Tolokers' bonuses limit.
                The maximum allowed value: 300.

        Returns:
            UserBonusSearchResult: Found Tolokers' bonuses and a flag showing whether there are more matching bonuses exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_user_bonuses(
            >>>     user_id='fac97860c7929add8048ed2ef63b66fd', sort=['-created', '-id'], limit=3
            >>> )
            >>> bonuses = find_result.items
            ...
        """
        ...

    async def get_user_bonus(self, user_bonus_id: str) -> toloka.client.user_bonus.UserBonus:
        """Gets information about a Toloker's bonus.

        Args:
            user_bonus_id: The ID of the bonus.

        Returns:
            UserBonus: The information about the bonus.

        Example:
            >>> bonus = toloka_client.get_user_bonus(user_bonus_id='3295')
            >>> print(bonus.amount)
            ...
        """
        ...

    @typing.overload
    def get_user_bonuses(
        self,
        request: toloka.client.search_requests.UserBonusSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.user_bonus.UserBonus, None]:
        """Finds all Tolokers' bonuses that match certain rules and returns them in an iterable object

        `get_user_bonuses` returns a generator. You can iterate over all found Tolokers' bonuses using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort bonuses use the [find_user_bonuses](toloka.client.TolokaClient.find_user_bonuses.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300.

        Yields:
            UserBonus: The next matching Toloker's bonus.

        Example:
            >>> bonuses = list(toloka_client.get_user_bonuses(created_lt='2023-06-01T00:00:00'))
            ...
        """
        ...

    @typing.overload
    def get_user_bonuses(
        self,
        user_id: typing.Optional[str] = None,
        assignment_id: typing.Optional[str] = None,
        private_comment: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.user_bonus.UserBonus, None]:
        """Finds all Tolokers' bonuses that match certain rules and returns them in an iterable object

        `get_user_bonuses` returns a generator. You can iterate over all found Tolokers' bonuses using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort bonuses use the [find_user_bonuses](toloka.client.TolokaClient.find_user_bonuses.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300.

        Yields:
            UserBonus: The next matching Toloker's bonus.

        Example:
            >>> bonuses = list(toloka_client.get_user_bonuses(created_lt='2023-06-01T00:00:00'))
            ...
        """
        ...

    @typing.overload
    async def find_user_restrictions(
        self,
        request: toloka.client.search_requests.UserRestrictionSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.UserRestrictionSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.UserRestrictionSearchResult:
        """Finds Toloker restrictions that match certain criteria.

        The number of returned restrictions is limited. To find remaining restrictions call `find_user_restrictions` with updated search criteria.

        To iterate over all matching Toloker restrictions you may use the [get_user_restrictions](toloka.client.TolokaClient.get_user_restrictions.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned Toloker restrictions limit.
                The maximum allowed value: 500.

        Returns:
            UserRestrictionSearchResult: Found Toloker restrictions and a flag showing whether there are more matching restrictions exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_user_restrictions(sort=['-created', '-id'], limit=10)
            >>> restrictions = find_result.items
            ...

            If there are restrictions exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    @typing.overload
    async def find_user_restrictions(
        self,
        scope: typing.Optional[toloka.client.user_restriction.UserRestriction.Scope] = None,
        user_id: typing.Optional[str] = None,
        project_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.UserRestrictionSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.UserRestrictionSearchResult:
        """Finds Toloker restrictions that match certain criteria.

        The number of returned restrictions is limited. To find remaining restrictions call `find_user_restrictions` with updated search criteria.

        To iterate over all matching Toloker restrictions you may use the [get_user_restrictions](toloka.client.TolokaClient.get_user_restrictions.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned Toloker restrictions limit.
                The maximum allowed value: 500.

        Returns:
            UserRestrictionSearchResult: Found Toloker restrictions and a flag showing whether there are more matching restrictions exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_user_restrictions(sort=['-created', '-id'], limit=10)
            >>> restrictions = find_result.items
            ...

            If there are restrictions exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    async def get_user_restriction(self, user_restriction_id: str) -> toloka.client.user_restriction.UserRestriction:
        """Gets information about a Toloker restriction.

        Args:
            user_restriction_id: The ID of the Toloker restriction.

        Returns:
            UserRestriction: The Toloker restriction.

        Example:
            >>> restriction = toloka_client.get_user_restriction(user_restriction_id='19124')
            >>> print(restriction.will_expire)
            ...
        """
        ...

    @typing.overload
    def get_user_restrictions(
        self,
        request: toloka.client.search_requests.UserRestrictionSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.user_restriction.UserRestriction, None]:
        """Finds all Toloker restrictions that match certain criteria.

        `get_user_restrictions` returns a generator. You can iterate over all found Toloker restrictions using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort Toloker restrictions use the [find_user_restrictions](toloka.client.TolokaClient.find_user_restrictions.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 500.

        Yields:
            UserRestriction: The next matching Toloker restriction.

        Example:
            >>> restrictions = list(toloka_client.get_user_restrictions(scope='ALL_PROJECTS'))
            ...
        """
        ...

    @typing.overload
    def get_user_restrictions(
        self,
        scope: typing.Optional[toloka.client.user_restriction.UserRestriction.Scope] = None,
        user_id: typing.Optional[str] = None,
        project_id: typing.Optional[str] = None,
        pool_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.user_restriction.UserRestriction, None]:
        """Finds all Toloker restrictions that match certain criteria.

        `get_user_restrictions` returns a generator. You can iterate over all found Toloker restrictions using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort Toloker restrictions use the [find_user_restrictions](toloka.client.TolokaClient.find_user_restrictions.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 500.

        Yields:
            UserRestriction: The next matching Toloker restriction.

        Example:
            >>> restrictions = list(toloka_client.get_user_restrictions(scope='ALL_PROJECTS'))
            ...
        """
        ...

    async def set_user_restriction(self, user_restriction: toloka.client.user_restriction.UserRestriction) -> toloka.client.user_restriction.UserRestriction:
        """Restricts access to projects or pools for a Toloker.

        Args:
            user_restriction: Restriction parameters.

        Returns:
            UserRestriction: Updated restriction object.

        Example:
            Restricting access to a project.

            >>> new_restriction = toloka_client.set_user_restriction(
            >>>     toloka.client.user_restriction.ProjectUserRestriction(
            >>>         user_id='fac97860c7929add8048ed2ef63b66fd',
            >>>         private_comment='The Toloker often makes mistakes',
            >>>         project_id='92694'
            >>>     )
            >>> )
            ...
        """
        ...

    async def delete_user_restriction(self, user_restriction_id: str) -> None:
        """Removes existing restriction.

        Args:
            user_restriction_id: The ID of the restriction you want to remove.

        Example:
            >>> toloka_client.delete_user_restriction(user_restriction_id='20974')
            ...
        """
        ...

    async def get_requester(self) -> toloka.client.requester.Requester:
        """Gets information about the requester and the account balance.

        Returns:
            Requester: Information about the requester's account.

        Examples:
            >>> requester = toloka_client.get_requester()
            >>> print(requester.public_name, requester.balance)
            ...
        """
        ...

    @typing.overload
    async def find_user_skills(
        self,
        request: toloka.client.search_requests.UserSkillSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.UserSkillSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.UserSkillSearchResult:
        """Finds Toloker's skills that match certain criteria.

        The number of returned Toloker's skills is limited. To find remaining skills call `find_user_skills` with updated search criteria.

        To iterate over all matching skills you may use the [get_user_skills](toloka.client.TolokaClient.get_user_skills.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned skills limit.
                The maximum allowed value: 1000.

        Returns:
            UserSkillSearchResult: Found Toloker's skills and a flag showing whether there are more matching skills exceeding the limit.

        Example:
            Getting a list of skills that a Toloker has.

            >>> find_result = toloka_client.find_user_skills(user_id='fac97860c7929add8048ed2ef63b66fd')
            >>> skills = find_result.items
            ...

            Getting a list of Tolokers who have a certain skill.

            >>> find_result = toloka_client.find_user_skills(skill_id='11294')
            >>> skills = find_result.items
            ...

            If there are skills exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    @typing.overload
    async def find_user_skills(
        self,
        user_id: typing.Optional[str] = None,
        skill_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        modified_lt: typing.Optional[datetime.datetime] = None,
        modified_lte: typing.Optional[datetime.datetime] = None,
        modified_gt: typing.Optional[datetime.datetime] = None,
        modified_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.UserSkillSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.UserSkillSearchResult:
        """Finds Toloker's skills that match certain criteria.

        The number of returned Toloker's skills is limited. To find remaining skills call `find_user_skills` with updated search criteria.

        To iterate over all matching skills you may use the [get_user_skills](toloka.client.TolokaClient.get_user_skills.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned skills limit.
                The maximum allowed value: 1000.

        Returns:
            UserSkillSearchResult: Found Toloker's skills and a flag showing whether there are more matching skills exceeding the limit.

        Example:
            Getting a list of skills that a Toloker has.

            >>> find_result = toloka_client.find_user_skills(user_id='fac97860c7929add8048ed2ef63b66fd')
            >>> skills = find_result.items
            ...

            Getting a list of Tolokers who have a certain skill.

            >>> find_result = toloka_client.find_user_skills(skill_id='11294')
            >>> skills = find_result.items
            ...

            If there are skills exceeding the `limit`, then `find_result.has_more` is set to `True`.
        """
        ...

    async def get_user_skill(self, user_skill_id: str) -> toloka.client.user_skill.UserSkill:
        """Gets the value of a Toloker's skill.

        `UserSkill` describes the skill value for a specific Toloker.

        Args:
            user_skill_id: The ID of the Toloker skill.

        Returns:
            UserSkill: The skill value.

        Example:
            >>> user_skill = toloka_client.get_user_skill(user_skill_id='54118009')
            >>> print(user_skill.value)
            ...
        """
        ...

    @typing.overload
    def get_user_skills(
        self,
        request: toloka.client.search_requests.UserSkillSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.user_skill.UserSkill, None]:
        """Finds all Toloker's skills that match certain criteria.

        `get_user_skills` returns a generator. You can iterate over all found Toloker's skills using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort Toloker's skills use the [find_user_skills](toloka.client.TolokaClient.find_user_skills.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            UserSkill: The next matching Toloker's skill.

        Example:
            >>> user_skills = list(toloka_client.get_user_skills(skill_id='11294'))
            ...
        """
        ...

    @typing.overload
    def get_user_skills(
        self,
        user_id: typing.Optional[str] = None,
        skill_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        modified_lt: typing.Optional[datetime.datetime] = None,
        modified_lte: typing.Optional[datetime.datetime] = None,
        modified_gt: typing.Optional[datetime.datetime] = None,
        modified_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.user_skill.UserSkill, None]:
        """Finds all Toloker's skills that match certain criteria.

        `get_user_skills` returns a generator. You can iterate over all found Toloker's skills using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort Toloker's skills use the [find_user_skills](toloka.client.TolokaClient.find_user_skills.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            UserSkill: The next matching Toloker's skill.

        Example:
            >>> user_skills = list(toloka_client.get_user_skills(skill_id='11294'))
            ...
        """
        ...

    async def get_user(self, user_id: str) -> toloka.client.user.User:
        """Gets information about a Toloker.

        Args:
            user_id: The Toloker ID.

        Returns:
            User: Information about the Toloker.

        Example:
            >>> toloker_info = toloka_client.get_user(user_id='fac97860c7929add8048ed2ef63b66fd')
            >>> print(toloker_info.country)
            ...
        """
        ...

    @typing.overload
    async def set_user_skill(self, request: toloka.client.user_skill.SetUserSkillRequest) -> toloka.client.user_skill.UserSkill:
        """Assigns a skill to a Toloker.

        Args:
            request: Skill parameters.

        Returns:
            UserSkill: Updated skill information.

        Example:
            >>> from decimal import Decimal
            >>> toloka_client.set_user_skill(
            >>>     skill_id='11294', user_id='fac97860c7929add8048ed2ef63b66fd', value=Decimal(100)
            >>> )
            ...
        """
        ...

    @typing.overload
    async def set_user_skill(
        self,
        *,
        skill_id: typing.Optional[str] = None,
        user_id: typing.Optional[str] = None,
        value: typing.Optional[decimal.Decimal] = None
    ) -> toloka.client.user_skill.UserSkill:
        """Assigns a skill to a Toloker.

        Args:
            request: Skill parameters.

        Returns:
            UserSkill: Updated skill information.

        Example:
            >>> from decimal import Decimal
            >>> toloka_client.set_user_skill(
            >>>     skill_id='11294', user_id='fac97860c7929add8048ed2ef63b66fd', value=Decimal(100)
            >>> )
            ...
        """
        ...

    async def delete_user_skill(self, user_skill_id: str) -> None:
        """Removes a skill from a Toloker.

        Tolokers' skill values are described by the [UserSkill](toloka.client.user_skill.UserSkill.md) class.

        Args:
            user_skill_id: The ID of the Toloker's skill value.

        Example:
            >>> toloka_client.delete_user_skill(user_skill_id='54118009')
            ...
        """
        ...

    async def upsert_webhook_subscriptions(self, subscriptions: typing.List[toloka.client.webhook_subscription.WebhookSubscription]) -> toloka.client.batch_create_results.WebhookSubscriptionBatchCreateResult:
        """Creates subscriptions.

        You can create a subscription and receive notifications about events.
        For example, when a pool is closed or a task's status changes, Toloka can send you a notification.

        Args:
            subscriptions: A list of subscriptions to be created.

        Returns:
            batch_create_results.WebhookSubscriptionBatchCreateResult: The result of the operation.

        Raises:
            ValidationApiError: No subscriptions were created.

        Example:

            >>> result = toloka_client.upsert_webhook_subscriptions([
            >>>     {
            >>>         'webhook_url': 'https://awesome-requester.com/toloka-webhook',
            >>>         'event_type': toloka.client.webhook_subscription.WebhookSubscription.EventType.ASSIGNMENT_CREATED,
            >>>         'pool_id': '121212'
            >>>     },
            >>>     {
            >>>         'webhook_url': 'https://awesome-requester.com/toloka-webhook',
            >>>         'event_type': toloka.client.webhook_subscription.WebhookSubscription.EventType.POOL_CLOSED,
            >>>         'pool_id': '121212'
            >>>     }
            >>> ])
            >>> print(len(result.items))
            ...
        """
        ...

    async def get_webhook_subscription(self, webhook_subscription_id: str) -> toloka.client.webhook_subscription.WebhookSubscription:
        """Gets the properties of a subscription from Toloka.

        Args:
            webhook_subscription_id: The ID of the subscription.

        Returns:
            WebhookSubscription: The subscription.

        Example:
            >>> subscription = toloka_client.get_webhook_subscription('62f29db0a451956b21e13ff2')
            >>> print(subscription.event_type, subscription.webhook_url)
            ...
        """
        ...

    @typing.overload
    async def find_webhook_subscriptions(
        self,
        request: toloka.client.search_requests.WebhookSubscriptionSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.WebhookSubscriptionSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.WebhookSubscriptionSearchResult:
        """Finds webhook subscriptions that match certain criteria.

        The number of returned webhook subscriptions is limited. To find remaining webhook subscriptions call `find_webhook_subscriptions` with updated search criteria.

        To iterate over all matching webhook subscriptions you may use the [get_webhook_subscriptions](toloka.client.TolokaClient.get_webhook_subscriptions.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned webhook subscriptions limit.
                The maximum allowed value: 300.

        Returns:
            WebhookSubscriptionSearchResult: Found webhook subscriptions and a flag showing whether there are more matching webhook subscriptions exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_webhook_subscriptions(pool_id='1080020')
            >>> subscriptions = find_result.items
            ...
        """
        ...

    @typing.overload
    async def find_webhook_subscriptions(
        self,
        event_type: typing.Optional[toloka.client.webhook_subscription.WebhookSubscription.EventType] = None,
        pool_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.WebhookSubscriptionSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.WebhookSubscriptionSearchResult:
        """Finds webhook subscriptions that match certain criteria.

        The number of returned webhook subscriptions is limited. To find remaining webhook subscriptions call `find_webhook_subscriptions` with updated search criteria.

        To iterate over all matching webhook subscriptions you may use the [get_webhook_subscriptions](toloka.client.TolokaClient.get_webhook_subscriptions.md) method.

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned webhook subscriptions limit.
                The maximum allowed value: 300.

        Returns:
            WebhookSubscriptionSearchResult: Found webhook subscriptions and a flag showing whether there are more matching webhook subscriptions exceeding the limit.

        Example:
            >>> find_result = toloka_client.find_webhook_subscriptions(pool_id='1080020')
            >>> subscriptions = find_result.items
            ...
        """
        ...

    @typing.overload
    def get_webhook_subscriptions(
        self,
        request: toloka.client.search_requests.WebhookSubscriptionSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.webhook_subscription.WebhookSubscription, None]:
        """Finds all webhook subscriptions that match certain criteria.

        `get_webhook_subscriptions` returns a generator. You can iterate over all found webhook subscriptions using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort webhook subscriptions use the [find_webhook_subscriptions](toloka.client.TolokaClient.find_webhook_subscriptions.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300.

        Yields:
            WebhookSubscription: The next matching webhook subscription.

        Example:
            >>> for subscription in toloka_client.get_webhook_subscriptions(pool_id='1080020'):
            >>>     print(subscription.id, subscription.event_type)
            ...
        """
        ...

    @typing.overload
    def get_webhook_subscriptions(
        self,
        event_type: typing.Optional[toloka.client.webhook_subscription.WebhookSubscription.EventType] = None,
        pool_id: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.webhook_subscription.WebhookSubscription, None]:
        """Finds all webhook subscriptions that match certain criteria.

        `get_webhook_subscriptions` returns a generator. You can iterate over all found webhook subscriptions using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort webhook subscriptions use the [find_webhook_subscriptions](toloka.client.TolokaClient.find_webhook_subscriptions.md) method.

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 300.

        Yields:
            WebhookSubscription: The next matching webhook subscription.

        Example:
            >>> for subscription in toloka_client.get_webhook_subscriptions(pool_id='1080020'):
            >>>     print(subscription.id, subscription.event_type)
            ...
        """
        ...

    async def delete_webhook_subscription(self, webhook_subscription_id: str) -> None:
        """Deletes a subscription.

        Args:
            webhook_subscription_id: The ID of the subscription.

        Example:
            >>> subscription = toloka_client.delete_webhook_subscription('62f29db0a451956b21e13ff2')
            ...
        """
        ...

    @typing.overload
    async def get_assignments_df(
        self,
        pool_id: str,
        parameters: toloka.client.assignment.GetAssignmentsTsvParameters
    ) -> pandas.DataFrame:
        """Downloads assignments as pandas.DataFrame.

        {% note warning %}

        Requires toloka-kit[pandas] extras. Install it with the following command:

        ```shell
        pip install toloka-kit[pandas]
        ```

        {% endnote %}

        Experimental method.
        Implements the same behavior as if you download results in web-interface and then read it by pandas.

        Args:
            pool_id: From which pool the results are loaded.
            parameters: Filters for the results and the set of fields that will be in the dataframe.

        Returns:
            pd.DataFrame: DataFrame with all results. Contains groups of fields with prefixes:
                * "INPUT" - Fields that were at the input in the task.
                * "OUTPUT" - Fields that were received as a result of execution.
                * "GOLDEN" - Fields with correct answers. Filled in only for golden tasks and training tasks.
                * "HINT" - Hints for completing tasks. Filled in for training tasks.
                * "ACCEPT" - Fields describing the deferred acceptance of tasks.
                * "ASSIGNMENT" - fields describing additional information about the Assignment.

        Example:
            Get all assignments from the specified pool by `pool_id` to [pandas.DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
            And apply the native pandas `rename` method to change columns' names.

            >>> answers_df = toloka_client.get_assignments_df(pool_id='1')
            >>> answers_df = answers_df.rename(columns={
            >>>     'INPUT:image': 'task',
            >>>     'OUTPUT:result': 'label',
            >>>     'ASSIGNMENT:worker_id': 'annotator'
            >>> })
            ...
        """
        ...

    @typing.overload
    async def get_assignments_df(
        self,
        pool_id: str,
        *,
        status: typing.Optional[typing.List[toloka.client.assignment.GetAssignmentsTsvParameters.Status]] = ...,
        start_time_from: typing.Optional[datetime.datetime] = None,
        start_time_to: typing.Optional[datetime.datetime] = None,
        exclude_banned: typing.Optional[bool] = None,
        field: typing.Optional[typing.List[toloka.client.assignment.GetAssignmentsTsvParameters.Field]] = ...
    ) -> pandas.DataFrame:
        """Downloads assignments as pandas.DataFrame.

        {% note warning %}

        Requires toloka-kit[pandas] extras. Install it with the following command:

        ```shell
        pip install toloka-kit[pandas]
        ```

        {% endnote %}

        Experimental method.
        Implements the same behavior as if you download results in web-interface and then read it by pandas.

        Args:
            pool_id: From which pool the results are loaded.
            parameters: Filters for the results and the set of fields that will be in the dataframe.

        Returns:
            pd.DataFrame: DataFrame with all results. Contains groups of fields with prefixes:
                * "INPUT" - Fields that were at the input in the task.
                * "OUTPUT" - Fields that were received as a result of execution.
                * "GOLDEN" - Fields with correct answers. Filled in only for golden tasks and training tasks.
                * "HINT" - Hints for completing tasks. Filled in for training tasks.
                * "ACCEPT" - Fields describing the deferred acceptance of tasks.
                * "ASSIGNMENT" - fields describing additional information about the Assignment.

        Example:
            Get all assignments from the specified pool by `pool_id` to [pandas.DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
            And apply the native pandas `rename` method to change columns' names.

            >>> answers_df = toloka_client.get_assignments_df(pool_id='1')
            >>> answers_df = answers_df.rename(columns={
            >>>     'INPUT:image': 'task',
            >>>     'OUTPUT:result': 'label',
            >>>     'ASSIGNMENT:worker_id': 'annotator'
            >>> })
            ...
        """
        ...

    @typing.overload
    async def find_app_projects(
        self,
        request: toloka.client.search_requests.AppProjectSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppProjectSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppProjectSearchResult:
        """Finds App projects that match certain criteria.

        The number of returned projects is limited. To find remaining projects call `find_app_projects` with updated search criteria.

        To iterate over all matching projects you may use the [get_app_projects](toloka.client.TolokaClient.get_app_projects.md) method.

        Example:
            Searching active projects based on the App solution with the specified ID.

            >>> search = toloka_client.find_app_projects(
            >>>     app_id='9lZaMl363jahzra1rrYq', status='READY')
            >>> for app_project in search.content:
            >>>     print(app_project.id, app_project.name)
            >>>
            >>> if search.has_more:
            >>>     print('There are more App projects...')
            ...

        Args:
            request: Search criteria.
            sort: The order and direction of sorting the results.
            limit: Returned projects limit.
                The maximum allowed value: 5000.

        Returns:
            AppProjectSearchResult: Found projects and a flag showing whether there are more matching projects exceeding the limit.
        """
        ...

    @typing.overload
    async def find_app_projects(
        self,
        app_id: typing.Optional[str] = None,
        parent_app_project_id: typing.Optional[str] = None,
        status: typing.Optional[toloka.client.app.AppProject.Status] = None,
        after_id: typing.Optional[str] = None,
        scope: typing.Optional[toloka.client.search_requests.AppProjectSearchRequest.Scope] = None,
        requester_ids: typing.Union[str, typing.List[str]] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        name_lt: typing.Optional[str] = None,
        name_lte: typing.Optional[str] = None,
        name_gt: typing.Optional[str] = None,
        name_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppProjectSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppProjectSearchResult:
        """Finds App projects that match certain criteria.

        The number of returned projects is limited. To find remaining projects call `find_app_projects` with updated search criteria.

        To iterate over all matching projects you may use the [get_app_projects](toloka.client.TolokaClient.get_app_projects.md) method.

        Example:
            Searching active projects based on the App solution with the specified ID.

            >>> search = toloka_client.find_app_projects(
            >>>     app_id='9lZaMl363jahzra1rrYq', status='READY')
            >>> for app_project in search.content:
            >>>     print(app_project.id, app_project.name)
            >>>
            >>> if search.has_more:
            >>>     print('There are more App projects...')
            ...

        Args:
            request: Search criteria.
            sort: The order and direction of sorting the results.
            limit: Returned projects limit.
                The maximum allowed value: 5000.

        Returns:
            AppProjectSearchResult: Found projects and a flag showing whether there are more matching projects exceeding the limit.
        """
        ...

    @typing.overload
    def get_app_projects(
        self,
        request: toloka.client.search_requests.AppProjectSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.AppProject, None]:
        """Finds all App projects that match certain criteria.

        `get_app_projects` returns a generator. You can iterate over all found projects using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort App projects use the [find_app_projects](toloka.client.TolokaClient.find_app_projects.md) method.

        Example:
            >>> app_projects = toloka_client.get_app_projects(scope='MY')
            >>> for app_project in app_projects:
            >>>     print(app_project.id, app_project.status, app_project.name)
            ...

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 5000.

        Yields:
            AppProject: The next matching App project.
        """
        ...

    @typing.overload
    def get_app_projects(
        self,
        app_id: typing.Optional[str] = None,
        parent_app_project_id: typing.Optional[str] = None,
        status: typing.Optional[toloka.client.app.AppProject.Status] = None,
        after_id: typing.Optional[str] = None,
        scope: typing.Optional[toloka.client.search_requests.AppProjectSearchRequest.Scope] = None,
        requester_ids: typing.Union[str, typing.List[str]] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        name_lt: typing.Optional[str] = None,
        name_lte: typing.Optional[str] = None,
        name_gt: typing.Optional[str] = None,
        name_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.AppProject, None]:
        """Finds all App projects that match certain criteria.

        `get_app_projects` returns a generator. You can iterate over all found projects using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort App projects use the [find_app_projects](toloka.client.TolokaClient.find_app_projects.md) method.

        Example:
            >>> app_projects = toloka_client.get_app_projects(scope='MY')
            >>> for app_project in app_projects:
            >>>     print(app_project.id, app_project.status, app_project.name)
            ...

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 5000.

        Yields:
            AppProject: The next matching App project.
        """
        ...

    async def create_app_project(self, app_project: toloka.client.app.AppProject) -> toloka.client.app.AppProject:
        """Creates an App project in Toloka.

        Example:
            >>> app_project = toloka.client.AppProject(
            >>>     app_id='9lZaMl363jahzra1rrYq',
            >>>     name='Example project (product relevance)',
            >>>     parameters={
            >>>         "default_language": "en",
            >>>         "name": "Product relevance project",
            >>>         "instruction_classes": [
            >>>             {
            >>>                 "description": "The product is relevant to the query.",
            >>>                 "label": "Relevant",
            >>>                 "value": "relevant"
            >>>             },
            >>>             {
            >>>                 "description": "The product is not completely relevant to the query.",
            >>>                 "label": "Irrelevant",
            >>>                 "value": "irrelevant"
            >>>             }
            >>>         ],
            >>>         "instruction_examples": [
            >>>             {
            >>>                 "description": "The product exactly matches the query.",
            >>>                 "label": "relevant",
            >>>                 "query": "some search query",
            >>>                 "screenshot_url": "https://example.com/1"
            >>>             },
            >>>             {
            >>>                 "description": "The product shape matches but the product color does not.",
            >>>                 "label": "irrelevant",
            >>>                 "query": "other search query",
            >>>                 "screenshot_url": "https://example.com/2"
            >>>             }
            >>>         ]
            >>>     }
            >>> )
            >>> app_project = toloka_client.create_app_project(app_project)
            >>> print(app_project.created, app_project.status)
            ...

        Args:
            app_project: The project with parameters.

        Returns:
            AppProject: Created App project with updated parameters.
        """
        ...

    async def get_app_project(self, app_project_id: str) -> toloka.client.app.AppProject:
        """Gets information from Toloka about an App project.

        Example:
            >>> app_project = toloka_client.get_app_project('Q2d15QBjpwWuDz8Z321g')
            >>> print(app_project.created, app_project.name)
            ...

        Args:
            app_project_id: The ID of the project.

        Returns:
            AppProject: The App project.
        """
        ...

    async def archive_app_project(self, app_project_id: str) -> toloka.client.app.AppProject:
        """Archives an App project.

        The project changes its status to `ARCHIVED`.

        Example:
            >>> toloka_client.archive_app_project('Q2d15QBjpwWuDz8Z321g')
            ...

        Args:
            app_project_id: The ID of the project.

        Returns:
            AppProject: The App project with updated status.
        """
        ...

    async def unarchive_app_project(self, app_project_id: str) -> toloka.client.app.AppProject:
        """Unarchives an App project.

        Previous project status, which was before archiving, is restored.

        Example:
            >>> toloka_client.unarchive_app_project('Q2d15QBjpwWuDz8Z321g')
            ...

        Args:
            app_project_id: The ID of the project.

        Returns:
            AppProject: The App project with updated status.
        """
        ...

    @typing.overload
    async def find_apps(
        self,
        request: toloka.client.search_requests.AppSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppSearchResult:
        """Finds App solutions that match certain criteria.

        The number of returned solutions is limited. To find remaining solutions call `find_apps` with updated search criteria.

        To iterate over all matching solutions you may use the [get_apps](toloka.client.TolokaClient.get_apps.md) method.

        Example:
            >>> search = toloka_client.find_apps()
            >>> for app in search.content:
            >>>     print(app.id, app.name)
            >>>
            >>> if search.has_more:
            >>>     print('There are more App solutions...')
            ...

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned solutions limit.
                The maximum allowed value: 1000.

        Returns:
            AppSearchResult: Found solutions and a flag showing whether there are more matching solutions exceeding the limit.
        """
        ...

    @typing.overload
    async def find_apps(
        self,
        after_id: typing.Optional[str] = None,
        lang: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppSearchResult:
        """Finds App solutions that match certain criteria.

        The number of returned solutions is limited. To find remaining solutions call `find_apps` with updated search criteria.

        To iterate over all matching solutions you may use the [get_apps](toloka.client.TolokaClient.get_apps.md) method.

        Example:
            >>> search = toloka_client.find_apps()
            >>> for app in search.content:
            >>>     print(app.id, app.name)
            >>>
            >>> if search.has_more:
            >>>     print('There are more App solutions...')
            ...

        Args:
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned solutions limit.
                The maximum allowed value: 1000.

        Returns:
            AppSearchResult: Found solutions and a flag showing whether there are more matching solutions exceeding the limit.
        """
        ...

    @typing.overload
    def get_apps(
        self,
        request: toloka.client.search_requests.AppSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.BaseApp, None]:
        """Finds all App solutions that match certain criteria.

        `get_apps` returns a generator. You can iterate over all found solutions using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort solutions use the [find_apps](toloka.client.TolokaClient.find_apps.md) method.

        Example:
            >>> apps = toloka_client.get_apps()
            >>> for app in apps:
            >>>     print(app.id, app.name)
            ...

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            App: The next matching solution.
        """
        ...

    @typing.overload
    def get_apps(
        self,
        after_id: typing.Optional[str] = None,
        lang: typing.Optional[str] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.BaseApp, None]:
        """Finds all App solutions that match certain criteria.

        `get_apps` returns a generator. You can iterate over all found solutions using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort solutions use the [find_apps](toloka.client.TolokaClient.find_apps.md) method.

        Example:
            >>> apps = toloka_client.get_apps()
            >>> for app in apps:
            >>>     print(app.id, app.name)
            ...

        Args:
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            App: The next matching solution.
        """
        ...

    async def get_app(
        self,
        app_id: str,
        lang: typing.Optional[str] = None
    ) -> toloka.client.app.App:
        """Gets information from Toloka about an App solution.

        Example:
            >>> app = toloka_client.get_app('2eN4l59qL2xHB5b8Jqp6')
            >>> print(app.id, app.name)
            >>> print(app.description)
            ...

        Args:
            app_id: The ID of the solution.
            lang: ISO 639 language code.

        Returns:
            App: The App solution.
        """
        ...

    @typing.overload
    async def find_app_items(
        self,
        app_project_id: str,
        request: toloka.client.search_requests.AppItemSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppItemSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppItemSearchResult:
        """Finds task items that match certain criteria in an App project.

        The number of returned items is limited. To find remaining items call `find_app_items` with updated search criteria.

        To iterate over all matching items you may use the [get_app_items](toloka.client.TolokaClient.get_app_items.md) method.

        Example:
            Finding items in an App project that were created starting some date.
            >>> search = toloka_client.find_app_items(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     created_gte='2022-06-16',
            >>>     sort='created')
            >>> for app_item in search.content:
            >>>     print(app_item.id, app_item.created_at)
            >>>
            >>> if search.has_more:
            >>>     print('...')
            ...

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned items limit.
                The maximum allowed value: 1000.

        Returns:
            AppItemSearchResult: Found task items and a flag showing whether there are more matching items exceeding the limit.
        """
        ...

    @typing.overload
    async def find_app_items(
        self,
        app_project_id: str,
        after_id: typing.Optional[str] = None,
        batch_id: typing.Optional[str] = None,
        status: typing.Optional[toloka.client.app.AppItem.Status] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        finished_lt: typing.Optional[datetime.datetime] = None,
        finished_lte: typing.Optional[datetime.datetime] = None,
        finished_gt: typing.Optional[datetime.datetime] = None,
        finished_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppItemSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppItemSearchResult:
        """Finds task items that match certain criteria in an App project.

        The number of returned items is limited. To find remaining items call `find_app_items` with updated search criteria.

        To iterate over all matching items you may use the [get_app_items](toloka.client.TolokaClient.get_app_items.md) method.

        Example:
            Finding items in an App project that were created starting some date.
            >>> search = toloka_client.find_app_items(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     created_gte='2022-06-16',
            >>>     sort='created')
            >>> for app_item in search.content:
            >>>     print(app_item.id, app_item.created_at)
            >>>
            >>> if search.has_more:
            >>>     print('...')
            ...

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned items limit.
                The maximum allowed value: 1000.

        Returns:
            AppItemSearchResult: Found task items and a flag showing whether there are more matching items exceeding the limit.
        """
        ...

    @typing.overload
    def get_app_items(
        self,
        app_project_id: str,
        request: toloka.client.search_requests.AppItemSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.AppItem, None]:
        """Finds all App task items that match certain criteria in an App project.

        `get_app_items` returns a generator. You can iterate over all found items using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort items use the [find_app_items](toloka.client.TolokaClient.find_app_items.md) method.

        Example:
            >>> items = toloka_client.get_app_items(app_project_id='Q2d15QBjpwWuDz8Z321g')
            >>> for item in items:
            >>>     print(item.id, item.status, item.finished_at)
            ...

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            AppItem: The next matching item.
        """
        ...

    @typing.overload
    def get_app_items(
        self,
        app_project_id: str,
        after_id: typing.Optional[str] = None,
        batch_id: typing.Optional[str] = None,
        status: typing.Optional[toloka.client.app.AppItem.Status] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        finished_lt: typing.Optional[datetime.datetime] = None,
        finished_lte: typing.Optional[datetime.datetime] = None,
        finished_gt: typing.Optional[datetime.datetime] = None,
        finished_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.AppItem, None]:
        """Finds all App task items that match certain criteria in an App project.

        `get_app_items` returns a generator. You can iterate over all found items using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort items use the [find_app_items](toloka.client.TolokaClient.find_app_items.md) method.

        Example:
            >>> items = toloka_client.get_app_items(app_project_id='Q2d15QBjpwWuDz8Z321g')
            >>> for item in items:
            >>>     print(item.id, item.status, item.finished_at)
            ...

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            AppItem: The next matching item.
        """
        ...

    @typing.overload
    async def create_app_item(
        self,
        app_project_id: str,
        app_item: toloka.client.app.AppItem,
        *,
        force_new_original: typing.Optional[bool] = None
    ) -> toloka.client.app.AppItem:
        """Creates an App task item in Toloka.

        Example:
            The following example is suitable for a project
            that requires `query` and `website_url` keys to be present in input data.

            >>> new_item = {
            >>>     'batch_id' : '4Va2BBWKL88S4QyAgVje',
            >>>     'input_data' : {
            >>>         'id':'40', 'query':'toloka kit', 'website_url':'https://toloka.ai/docs/toloka-kit'
            >>>     }
            >>> }
            >>> new_item = toloka_client.create_app_item(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', app_item=new_item
            >>> )
            >>> print(new_item.created_at)
            ...

        Args:
            app_project_id: The ID of the App project to create the item in.
            app_item: The task item with parameters.
            force_new_original: Whether to enable or disable the deduplication for the item in the request.
                When set to true, the item will be re-labeled regardless of whether pre-labeled duplicates exist. Default is `False`.

        Returns:
            AppItem: Created App task item with updated parameters.
        """
        ...

    @typing.overload
    async def create_app_item(
        self,
        app_project_id: str,
        *,
        batch_id: typing.Optional[str] = None,
        input_data: typing.Optional[typing.Dict[str, typing.Any]] = None,
        force_new_original: typing.Optional[bool] = None
    ) -> toloka.client.app.AppItem:
        """Creates an App task item in Toloka.

        Example:
            The following example is suitable for a project
            that requires `query` and `website_url` keys to be present in input data.

            >>> new_item = {
            >>>     'batch_id' : '4Va2BBWKL88S4QyAgVje',
            >>>     'input_data' : {
            >>>         'id':'40', 'query':'toloka kit', 'website_url':'https://toloka.ai/docs/toloka-kit'
            >>>     }
            >>> }
            >>> new_item = toloka_client.create_app_item(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', app_item=new_item
            >>> )
            >>> print(new_item.created_at)
            ...

        Args:
            app_project_id: The ID of the App project to create the item in.
            app_item: The task item with parameters.
            force_new_original: Whether to enable or disable the deduplication for the item in the request.
                When set to true, the item will be re-labeled regardless of whether pre-labeled duplicates exist. Default is `False`.

        Returns:
            AppItem: Created App task item with updated parameters.
        """
        ...

    @typing.overload
    async def create_app_items(
        self,
        app_project_id: str,
        request: toloka.client.app.AppItemsCreateRequest
    ) -> typing.List[str]:
        """Creates task items in an App project in Toloka and adds them to an existing batch.

        Example:
            The following example is suitable for a project
            that requires `query` and `website_url` keys to be present in input data.

            >>> new_items = [
            >>>     {'id':'20', 'query':'toloka kit', 'website_url':'https://toloka.ai/docs/toloka-kit'},
            >>>     {'id':'21', 'query':'crowd kit', 'website_url':'https://toloka.ai/docs/crowd-kit'}
            >>> ]
            >>> toloka_client.create_app_items(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', batch_id='4Va2BBWKL88S4QyAgVje',
            >>>     items=new_items
            >>> )
            ...

        Args:
            app_project_id: The ID of the App project.
            request: The request parameters.
        Returns:
            List[str]: The IDs of created app items.
        """
        ...

    @typing.overload
    async def create_app_items(
        self,
        app_project_id: str,
        *,
        batch_id: typing.Optional[str] = None,
        items: typing.List[typing.Dict[str, typing.Any]],
        force_new_original: typing.Optional[bool] = None,
        ignore_errors: typing.Optional[bool] = None
    ) -> typing.List[str]:
        """Creates task items in an App project in Toloka and adds them to an existing batch.

        Example:
            The following example is suitable for a project
            that requires `query` and `website_url` keys to be present in input data.

            >>> new_items = [
            >>>     {'id':'20', 'query':'toloka kit', 'website_url':'https://toloka.ai/docs/toloka-kit'},
            >>>     {'id':'21', 'query':'crowd kit', 'website_url':'https://toloka.ai/docs/crowd-kit'}
            >>> ]
            >>> toloka_client.create_app_items(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', batch_id='4Va2BBWKL88S4QyAgVje',
            >>>     items=new_items
            >>> )
            ...

        Args:
            app_project_id: The ID of the App project.
            request: The request parameters.
        Returns:
            List[str]: The IDs of created app items.
        """
        ...

    async def get_app_item(
        self,
        app_project_id: str,
        app_item_id: str
    ) -> toloka.client.app.AppItem:
        """Gets information from Toloka about an App task item.

        Example:
            >>> item = toloka_client.get_app_item(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     app_item_id='V40aPPA2j64TORQyY54Z'
            >>> )
            >>> print(item.input_data)
            >>> print(item.output_data)
            ...

        Args:
            app_project_id: The ID of the App project.
            app_item_id: The ID of the item.

        Returns:
            AppItem: The App task item.
        """
        ...

    @typing.overload
    async def find_app_batches(
        self,
        app_project_id: str,
        request: toloka.client.search_requests.AppBatchSearchRequest,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppBatchSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppBatchSearchResult:
        """Finds batches that match certain criteria in an App project.

        The number of returned batches is limited. To find remaining batches call `find_app_batches` with updated search criteria.

        To iterate over all matching batches you may use the [get_app_batches](toloka.client.TolokaClient.get_app_batches.md) method.

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned batches limit.
                The maximum allowed value: 1000.

        Returns:
            AppBatchSearchResult: Found batches and a flag showing whether there are more matching batches exceeding the limit.

        Example:
            >>> result = toloka_client.find_app_batches(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', status='NEW', sort='id'
            >>> )
            >>> batches = result.content
            >>> if result.has_more:
            >>>     print('There are more NEW batches...')
            ...
        """
        ...

    @typing.overload
    async def find_app_batches(
        self,
        app_project_id: str,
        after_id: typing.Optional[str] = None,
        status: typing.Optional[toloka.client.app.AppBatch.Status] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        name_lt: typing.Optional[str] = None,
        name_lte: typing.Optional[str] = None,
        name_gt: typing.Optional[str] = None,
        name_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        sort: typing.Union[typing.List[str], toloka.client.search_requests.AppBatchSortItems, None] = None,
        limit: typing.Optional[int] = None
    ) -> toloka.client.search_results.AppBatchSearchResult:
        """Finds batches that match certain criteria in an App project.

        The number of returned batches is limited. To find remaining batches call `find_app_batches` with updated search criteria.

        To iterate over all matching batches you may use the [get_app_batches](toloka.client.TolokaClient.get_app_batches.md) method.

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            sort: Sorting options. Default: `None`.
            limit: Returned batches limit.
                The maximum allowed value: 1000.

        Returns:
            AppBatchSearchResult: Found batches and a flag showing whether there are more matching batches exceeding the limit.

        Example:
            >>> result = toloka_client.find_app_batches(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', status='NEW', sort='id'
            >>> )
            >>> batches = result.content
            >>> if result.has_more:
            >>>     print('There are more NEW batches...')
            ...
        """
        ...

    @typing.overload
    def get_app_batches(
        self,
        app_project_id: str,
        request: toloka.client.search_requests.AppBatchSearchRequest,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.AppBatch, None]:
        """Finds all batches that match certain criteria in an App project.

        `get_app_batches` returns a generator. You can iterate over all found batches using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort batches use the [find_app_batches](toloka.client.TolokaClient.find_app_batches.md) method.

        Example:
            >>> batches = toloka_client.get_app_batches(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', status='NEW'
            >>> )
            >>> for batch in batches:
            >>>     print(batch.id, batch.status, batch.items_count)
            ...

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            AppBatch: The next matching batch.
        """
        ...

    @typing.overload
    def get_app_batches(
        self,
        app_project_id: str,
        after_id: typing.Optional[str] = None,
        status: typing.Optional[toloka.client.app.AppBatch.Status] = None,
        id_lt: typing.Optional[str] = None,
        id_lte: typing.Optional[str] = None,
        id_gt: typing.Optional[str] = None,
        id_gte: typing.Optional[str] = None,
        name_lt: typing.Optional[str] = None,
        name_lte: typing.Optional[str] = None,
        name_gt: typing.Optional[str] = None,
        name_gte: typing.Optional[str] = None,
        created_lt: typing.Optional[datetime.datetime] = None,
        created_lte: typing.Optional[datetime.datetime] = None,
        created_gt: typing.Optional[datetime.datetime] = None,
        created_gte: typing.Optional[datetime.datetime] = None,
        batch_size: typing.Optional[int] = None
    ) -> toloka.util.async_utils.AsyncGenAdapter[toloka.client.app.AppBatch, None]:
        """Finds all batches that match certain criteria in an App project.

        `get_app_batches` returns a generator. You can iterate over all found batches using the generator. Several requests to the Toloka server are possible while iterating.

        If you need to sort batches use the [find_app_batches](toloka.client.TolokaClient.find_app_batches.md) method.

        Example:
            >>> batches = toloka_client.get_app_batches(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g', status='NEW'
            >>> )
            >>> for batch in batches:
            >>>     print(batch.id, batch.status, batch.items_count)
            ...

        Args:
            app_project_id: The ID of the App project.
            request: Search criteria.
            batch_size: A limit of items returned by each request to Toloka.
                The maximum allowed value: 1000.

        Yields:
            AppBatch: The next matching batch.
        """
        ...

    @typing.overload
    async def create_app_batch(
        self,
        app_project_id: str,
        request: toloka.client.app.AppBatchCreateRequest
    ) -> toloka.client.app.AppBatch:
        """Creates a batch with task items in an App project in Toloka.

        Example:
            The following example is suitable for a project
            that requires `query` and `website_url` keys to be present in input data.

            >>> new_items = [
            >>>     {'id':'30', 'query':'toloka kit', 'website_url':'https://toloka.ai/docs/toloka-kit'},
            >>>     {'id':'31', 'query':'crowd kit', 'website_url':'https://toloka.ai/docs/crowd-kit'}
            >>> ]
            >>> toloka_client.create_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     items=new_items
            >>> )
            ...

        Args:
            app_project_id: The ID of the project.
            request: The request parameters.

        Returns:
            AppBatch: Created batch with updated parameters.
        """
        ...

    @typing.overload
    async def create_app_batch(
        self,
        app_project_id: str,
        *,
        name: typing.Optional[str] = None,
        items: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
        priority_order: typing.Optional[toloka.client.app.AppBatch.PriorityOrder] = None,
        force_new_original: typing.Optional[bool] = None,
        ignore_errors: typing.Optional[bool] = None
    ) -> toloka.client.app.AppBatch:
        """Creates a batch with task items in an App project in Toloka.

        Example:
            The following example is suitable for a project
            that requires `query` and `website_url` keys to be present in input data.

            >>> new_items = [
            >>>     {'id':'30', 'query':'toloka kit', 'website_url':'https://toloka.ai/docs/toloka-kit'},
            >>>     {'id':'31', 'query':'crowd kit', 'website_url':'https://toloka.ai/docs/crowd-kit'}
            >>> ]
            >>> toloka_client.create_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     items=new_items
            >>> )
            ...

        Args:
            app_project_id: The ID of the project.
            request: The request parameters.

        Returns:
            AppBatch: Created batch with updated parameters.
        """
        ...

    async def get_app_batch(
        self,
        app_project_id: str,
        batch_id: str
    ) -> toloka.client.app.AppBatch:
        """Gets information from Toloka about a batch in an App project.

        Example:
            >>> batch = toloka_client.get_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     app_batch_id='4Va2BBWKL88S4QyAgVje'
            >>> )
            >>> print(batch.status, batch.items_count, batch.cost)
            ...

        Args:
            app_project_id: The ID of the project.
            batch_id: The ID of the batch.

        Returns:
            AppBatch: The App batch.
        """
        ...

    @typing.overload
    async def patch_app_batch(
        self,
        app_project_id: str,
        batch_id: str,
        patch: toloka.client.app.AppBatchPatch
    ) -> toloka.client.app.AppBatch:
        """Updates an App batch.

        Args:
            app_project_id: The ID of the App project containing the batch.
            batch_id: The ID of the batch.
            patch: Parameters to update.

        Example:
            Changing the batch name.

            >>> batch = toloka_client.patch_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     batch_id='4Va2BBWKL88S4QyAgVje',
            >>>     name = 'Preliminary batch'
            >>> )
            ...

        Returns:
            AppBatch: The updated App batch.
        """
        ...

    @typing.overload
    async def patch_app_batch(
        self,
        app_project_id: str,
        batch_id: str,
        *,
        name: typing.Optional[str] = None,
        priority_order: typing.Optional[toloka.client.app.AppBatch.PriorityOrder] = None
    ) -> toloka.client.app.AppBatch:
        """Updates an App batch.

        Args:
            app_project_id: The ID of the App project containing the batch.
            batch_id: The ID of the batch.
            patch: Parameters to update.

        Example:
            Changing the batch name.

            >>> batch = toloka_client.patch_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     batch_id='4Va2BBWKL88S4QyAgVje',
            >>>     name = 'Preliminary batch'
            >>> )
            ...

        Returns:
            AppBatch: The updated App batch.
        """
        ...

    async def archive_app_batch(
        self,
        app_project_id: str,
        batch_id: str
    ) -> None:
        """Archives the batch with the ID specified in the request to hide its data.

        You can archive the batches which are not currently being processed (are not in the `PROCESSING` status). You
        must stop the batches before archiving them. The batch gets the `ARCHIVE` status.

        Example:
            >>> toloka_client.archive_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     batch_id='4Va2BBWKL88S4QyAgVje'
            >>> )
            ...

        Args:
            app_project_id: The ID of the project with which the batch is associated.
            batch_id: The ID of the batch you want to archive.
        """
        ...

    async def unarchive_app_batch(
        self,
        app_project_id: str,
        batch_id: str
    ) -> None:
        """Changes the batch status to the last one it had before archiving. After the operation, you can process the
        batch again.

        Example:
            >>> toloka_client.unarchive_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     batch_id='4Va2BBWKL88S4QyAgVje'
            >>> )
            ...

        Args:
            app_project_id: The ID of the project with which the batch is associated.
            batch_id: The ID of the batch you want to unarchive.
        """
        ...

    async def start_app_batch(
        self,
        app_project_id: str,
        batch_id: str
    ) -> None:
        """Launches annotation of a batch of task items in an App project.

        Example:
            >>> toloka_client.start_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     app_batch_id='4Va2BBWKL88S4QyAgVje'
            >>> )
            ...

        Args:
            app_project_id: The ID of the project.
            batch_id: The ID of the batch.
        """
        ...

    async def start_sync_batch_processing(
        self,
        app_project_id: str,
        request: toloka.client.app.SyncBatchCreateRequest
    ) -> toloka.client.app.AppBatch:
        """Starts processing the batch with the ID specified in the request.

        This batch processing is applicable for solutions which use synchronous protocols only.

        Args:
            app_project_id: The ID of the project with which the batch is associated.

        Returns:
            AppBatch: The response to the request to start sync batch processing.
        """
        ...

    async def stop_app_batch(
        self,
        app_project_id: str,
        batch_id: str
    ) -> None:
        """Stops annotation of a batch of task items in an App project.

        Processing can be stopped only for the batch with the `PROCESSING` status.

        Example:
            >>> toloka_client.stop_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     batch_id='4Va2BBWKL88S4QyAgVje'
            >>> )
            ...

        Args:
            app_project_id: The ID of the project.
            batch_id: The ID of the batch.
        """
        ...

    async def resume_app_batch(
        self,
        app_project_id: str,
        batch_id: str
    ) -> None:
        """Resumes annotation of a batch of task items in an App project.

        Processing can be resumed only for the batch with the `STOPPING` or `STOPPED` status.

        Example:
            >>> toloka_client.resume_app_batch(
            >>>     app_project_id='Q2d15QBjpwWuDz8Z321g',
            >>>     batch_id='4Va2BBWKL88S4QyAgVje'
            >>> )
            ...

        Args:
            app_project_id: The ID of the project.
            batch_id: The ID of the batch.
        """
        ...
