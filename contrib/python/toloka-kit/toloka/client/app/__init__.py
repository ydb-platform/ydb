__all__ = [
    'App',
    'AppBatch',
    'AppBatchCreateRequest',
    'AppBatchPatch',
    'AppItem',
    'AppItemCreateRequest',
    'AppItemsCreateRequest',
    'AppProject',
    'SyncBatchCreateRequest',
]
import datetime
import decimal
from enum import unique
from typing import Dict, Any, List, Optional

from ..primitives.base import BaseTolokaObject
from ..project.field_spec import FieldSpec
from ...util._codegen import attribute
from ...util._docstrings import inherit_docstrings
from ...util._extendable_enum import ExtendableStrEnum


class _AppError(BaseTolokaObject):
    """
    A structure for describing errors which may appear while working with App projects.

    Attributes:
        code: The short name of the error.
        message: The detailed description of the error.
        payload: Additional data provided with the error.
    """
    code: str
    message: str
    payload: Any


class AppLightestResult(BaseTolokaObject):
    """Brief information about the project template.

    Attributes:
        id: The ID of the App solution.
        name: The solution name.
    """
    id: str
    name: str


class AppProject(BaseTolokaObject):
    """An [App](https://toloka.ai/docs/api/apps-reference/#tag--app-project) project.

    An App project is based on one of App solutions. It is created with a template interface and preconfigured data specification and quality control rules.

    To get available App solutions use the [get_apps](toloka.client.TolokaClient.get_apps.md) method.

    Attributes:
        app_id: The ID of the App solution used to create the project.
        parent_app_project_id: The ID of the parent project. It is set if this project is a clone of other project. Otherwise it is empty.
        name: The project name.
        parameters: Parameters of the solution. The parameters should follow the schema described in the `param_spec` field of the [solution](toloka.client.app.App.md).
        id: The ID of the project.
        status: The project status:
            * `CREATING` — Toloka is checking the project.
            * `READY` — The project is active.
            * `ARCHIVED` — The project was archived.
            * `ERROR` — Project creation failed due to errors.
        created: The date and time when the project was created.
        item_price: The price you pay for a processed item.
        errors: Errors found during a project check.
        read_only:
            * `True` — The project is read-only.
            * `False` — The project can be modified.
        app: Brief information about the project template.

    Example:
        Creating an App project.

        >>> app_project = toloka.client.AppProject(
        >>>     app_id='9lZaMl363jahzra1rrYq',
        >>>     name='Example project (product relevance)',
        >>>     parameters={
        >>>         'default_language': 'en',
        >>>         'name': 'Product relevance project',
        >>>         'instruction_classes': [
        >>>             {
        >>>                 'description': 'The product is relevant to the query.',
        >>>                 'label': 'Relevant',
        >>>                 'value': 'relevant'
        >>>             },
        >>>             {
        >>>                 'description': 'The product is not completely relevant to the query.',
        >>>                 'label': 'Irrelevant',
        >>>                 'value': 'irrelevant'
        >>>             }
        >>>         ],
        >>>         'instruction_examples': [
        >>>             {
        >>>                 'description': 'The product exactly matches the query.',
        >>>                 'label': 'relevant',
        >>>                 'query': 'some search query',
        >>>                 'screenshot_url': 'https://example.com/1'
        >>>             },
        >>>             {
        >>>                 'description': 'The product shape matches but the product color does not.',
        >>>                 'label': 'irrelevant',
        >>>                 'query': 'other search query',
        >>>                 'screenshot_url': 'https://example.com/2'
        >>>             }
        >>>         ]
        >>>     }
        >>> )
        >>> app_project = toloka_client.create_app_project(app_project)
        >>> print(app_project.id, app_project.status)
        ...
    """

    @unique
    class Status(ExtendableStrEnum):
        CREATING = 'CREATING'
        READY = 'READY'
        ARCHIVED = 'ARCHIVED'
        ERROR = 'ERROR'

    app_id: str
    parent_app_project_id: str
    name: str
    parameters: Dict

    id: str = attribute(readonly=True)
    status: Status = attribute(readonly=True, autocast=True)
    created: datetime.datetime = attribute(readonly=True)
    item_price: decimal.Decimal = attribute(readonly=True)
    errors: List[_AppError] = attribute(readonly=True)
    read_only: bool = attribute(readonly=True)
    app: AppLightestResult = attribute(readonly=True)


class BaseApp(BaseTolokaObject):
    """A lightweight representation of an [App](https://toloka.ai/docs/api/apps-reference/#tag--app) solution.

    Attributes:
        id: The ID of the App solution.
        name: The solution name.
        image: A link to the solution interface preview image.
        description: The solution description.
        default_item_price: The default cost of one annotated item.
        examples: Example description of tasks which can be solved with this solution.
    """

    id: str
    name: str
    image: str
    description: str
    default_item_price: decimal.Decimal
    examples: Any


@inherit_docstrings
class App(BaseApp):
    """An [App](https://toloka.ai/docs/api/apps-reference/#tag--app) solution.

    Each App solution targets specific type of tasks which can be solved using Toloka.

    Attributes:
        constraints_description: The description of limitations.
        param_spec: The specification of parameters used to create a project.
        input_spec: The schema of solution input data.
        output_spec: The schema of solution output data.

        input_format_info: Information about the input data format.

    Example:
        Showing all available App solutions.

        >>> apps = toloka_client.get_apps()
        >>> for app in apps:
        >>>     print(app.id, app.name)
        ...
    """

    constraints_description: str
    param_spec: Dict
    input_spec: Dict[str, FieldSpec]
    output_spec: Dict[str, FieldSpec]
    input_format_info: Dict


class AppItem(BaseTolokaObject):
    """A task item.

    Items are uploaded to Toloka and are grouped in batches. After uploading the status of items is set to `NEW`.
    Items with that status can be edited. Then entire batches are sent for labeling.

    Attributes:
        id: The ID of the item.
        app_project_id: The ID of the project that contains the item.
        batch_id: The ID of the batch that contains the item.
        input_data: Input data. It must follow the solution schema described in `App.input_spec`.
        status: The item status:
            * `NEW` — The item is uploaded to Toloka and ready for processing.
            * `PROCESSING` — The item is being processed by Tolokers.
            * `COMPLETED` — Item annotation is completed.
            * `ERROR` — An error occurred during processing.
            * `CANCELLED` — Item processing cancelled.
            * `ARCHIVE` — The item is archived.
            * `NO_MONEY` — There are not enough money for processing.
        output_data: Annotated data.
        errors: Errors occurred during annotation.
        created_at: The date and time when the item was created.
        started_at: The date and time when the item processing started.
        finished_at: The date and time when the item processing was completed.

    Example:
        >>> item = toloka_client.get_app_item(app_project_id='Q2d15QBjpwWuDz8Z321g', app_item_id='V40aPPA2j64TORQyY54Z')
        >>> print(item.input_data)
        >>> print(item.output_data)
        ...
    """

    @unique
    class Status(ExtendableStrEnum):
        NEW = 'NEW'
        PROCESSING = 'PROCESSING'
        COMPLETED = 'COMPLETED'
        ERROR = 'ERROR'
        CANCELLED = 'CANCELLED'
        ARCHIVE = 'ARCHIVE'
        NO_MONEY = 'NO_MONEY'
        STOPPED = 'STOPPED'

    batch_id: str
    input_data: Dict[str, Any]

    id: str = attribute(readonly=True)
    app_project_id: str = attribute(readonly=True)
    status: Status = attribute(readonly=True, autocast=True)
    output_data: Dict[str, Any] = attribute(readonly=True)
    errors: List[_AppError] = attribute(readonly=True)
    created_at: datetime.datetime = attribute(readonly=True)
    started_at: datetime.datetime = attribute(readonly=True)
    finished_at: datetime.datetime = attribute(readonly=True)


class AppItemCreateRequest(BaseTolokaObject):
    """Parameters of a request for creating single item.

    Attributes:
        batch_id: The ID of the batch that contains the item.
        input_data: Input data. It must follow the solution schema described in `App.input_spec`.
        force_new_original: Whether to enable or disable the deduplication for the item in the request.
            When set to true, the item will be re-labeled regardless of whether pre-labeled duplicates exist. Default is `False`.
    """

    batch_id: str
    input_data: Dict[str, Any]
    force_new_original: Optional[bool] = None


class AppItemsCreateRequest(BaseTolokaObject):
    """Parameters of a request for creating multiple items.

    Attributes:
        batch_id: The ID of the batch to place items to.
        items: A list with items. The items must follow the solution schema described in `App.input_spec`.
        force_new_original: Whether to enable or disable the deduplication for all the items in the request.
            When set to true, all the items will be re-labeled regardless of whether pre-labeled duplicates exist. Default is `False`.
        ignore_errors: Whether the data with incorrect items can be uploaded. Default is `False`.
            * `True` — If incorrect task items are present, they will be skipped and the response will contain the information about errors.
            * `False` — If incorrect task items are present, the data will not be uploaded and the response will contain the information about the errors.
            You can only use this parameter if batch_id is specified in the request.
    """

    batch_id: str
    items: List[Dict[str, Any]] = attribute(required=True)
    force_new_original: bool
    ignore_errors: bool


class AppItemImport(BaseTolokaObject):
    """Information about an operation that adds items.

    Attributes:
        id: The ID.
        records_count: The total number of items sent by a client.
        records_processed: The number of items processed during the operation.
        records_skipped: The number of items which had incorrect parameters and were not uploaded during the operation. The detailed information about the error is returned in the `errors` field.
        errors: Information about items with incorrect parameters which were not added.
    """
    id: str
    records_count: int
    records_processed: int
    records_skipped: int
    errors: Dict


class AppBatch(BaseTolokaObject):
    """An App batch.

    A batch contains task items that are sent for labeling together.

    Attributes:
        id: The ID of the batch.
        app_project_id: The ID of the project containing the batch.
        name: The batch name.
        status: The batch [status](toloka.client.app.AppBatch.Status.md).
        items_count: The number of items in the batch.
        item_price: The cost of processing a single item in the batch.
        cost: The cost of processing the batch.
        cost_of_processed: Cost of already processed task items.
        created_at: The date and time when the batch was created.
        started_at: The date and time when batch processing started.
        finished_at: The date and time when batch processing was completed.
        read_only: Whether the batch can be updated or not.
        priority_order: The batch priority. See [PriorityOrder](toloka.client.app.AppBatch.PriorityOrder.md) for details. Default is `FIVE`.
        last_items_import: Information about the last operation that added items.
        confidence_avg: Average labeling quality.
        items_processed_count: The number of labeled items.
        eta: Expected date and time when batch processing will be completed.
        etd: The expected date and time when processing of the queued batch will be started. The parameter is present in the response when the batch status is equal to `QUEUED`, otherwise it's `None`.
        app_project_eta: The expected date and time when processing of all the batches associated with the project will be completed.
            The parameter is present in the response when the project contains batches in the status equal to `QUEUED` or `PROCESSING`, otherwise it's `None`.
        items_per_state: Statistics on the number of items in each state.
        current_time: The server-side date and time when the response was formed.

    Example:
        >>> batches = toloka_client.get_app_batches(app_project_id='Q2d15QBjpwWuDz8Z321g', status='NEW')
        >>> for batch in batches:
        >>>     print(batch.id, batch.status, batch.items_count)
        ...
    """

    @unique
    class Status(ExtendableStrEnum):
        """The status of an App batch.

        Attributes:
            NEW: The processing of the batch items is not started.
            QUEUED: The batch is ready for labeling but is currently queued because other batches with higher priority are being labeled.
                Labeling of these batches will automatically start once the labeling of the batches with higher priority finishes.
            PROCESSING: Batch items are being processed by Tolokers.
            COMPLETED: Annotation of all batch items is completed.
            ERROR: An error occurred during processing.
            CANCELLED: Batch processing cancelled.
            ARCHIVE: The batch is archived.
            NO_MONEY: There is not enough money for processing.
            LOADING: Tasks are loading to the batch.
            STOPPING: The batch is stopping.
            STOPPED: The batch has stopped.
        """

        NEW = 'NEW'
        QUEUED = 'QUEUED'
        PROCESSING = 'PROCESSING'
        COMPLETED = 'COMPLETED'
        ERROR = 'ERROR'
        CANCELLED = 'CANCELLED'
        NO_MONEY = 'NO_MONEY'
        ARCHIVE = 'ARCHIVE'
        LOADING = 'LOADING'
        STOPPING = 'STOPPING'
        STOPPED = 'STOPPED'

    @unique
    class PriorityOrder(ExtendableStrEnum):
        """The batch priority. ONE is the highest value. The batch items with this priority_order value will be sent
        for labeling first in the queue.
        """

        ONE = 'ONE'
        TWO = 'TWO'
        THREE = 'THREE'
        FOUR = 'FOUR'
        FIVE = 'FIVE'

    id: str
    app_project_id: str
    name: str
    status: Status = attribute(autocast=True)
    items_count: int
    item_price: decimal.Decimal
    cost: decimal.Decimal
    cost_of_processed: decimal.Decimal
    created_at: datetime.datetime
    started_at: datetime.datetime
    finished_at: datetime.datetime
    read_only: bool
    priority_order: PriorityOrder
    last_items_import: AppItemImport
    confidence_avg: float
    items_processed_count: int
    eta: datetime.datetime
    etd: datetime.datetime
    app_project_eta: datetime.datetime
    items_per_state: Dict
    current_time: datetime.datetime


class AppBatchCreateRequest(BaseTolokaObject):
    """Parameters of a request for creating multiple App task items in a batch.

    Attributes:
        name: The batch name.
        items: A list with task items. The items must follow the solution schema described in the `App.input_spec`.
        priority_order: The batch priority. See [PriorityOrder](toloka.client.app.AppBatch.PriorityOrder.md) for details. Default is `FIVE`.
        force_new_original: Whether to enable or disable the deduplication for all the items in the request.
            When set to true, all the items will be re-labeled regardless of whether pre-labeled duplicates exist. Default is `False`.
        ignore_errors: Whether the data with incorrect items can be uploaded. Default is `False`.
    """

    name: str
    items: List[Dict[str, Any]]
    priority_order: AppBatch.PriorityOrder
    force_new_original: bool
    ignore_errors: bool


class SyncBatchCreateRequest(BaseTolokaObject):
    """The batch to be created with the list of the task items.

    Attributes:
        name: The batch name.
        items: A list with task items. The items must follow the solution schema described in the `App.input_spec`.
    """

    name: str
    items: List[Dict[str, Any]]


class AppBatchPatch(BaseTolokaObject):
    """Parameters of a request for updating an App batch.

    Attributes:
        name: The new batch name.
        priority_order: The batch priority. See [PriorityOrder](toloka.client.app.AppBatch.PriorityOrder.md) for details.
    """
    name: str
    priority_order: AppBatch.PriorityOrder
