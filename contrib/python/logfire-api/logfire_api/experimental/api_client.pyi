from _typeshed import Incomplete
from collections.abc import Sequence
from httpx import AsyncClient, Client, Timeout
from httpx._client import BaseClient
from logfire._internal.config import get_base_url_from_token as get_base_url_from_token
from pydantic_evals import Case, Dataset
from pydantic_evals.evaluators import Evaluator
from types import TracebackType
from typing import Any, Generic, TypeVar, overload
from typing_extensions import Self

Case = Any
Dataset = Any
Evaluator = Any
DEFAULT_TIMEOUT: Incomplete
T = TypeVar('T', bound=BaseClient)
InputsT = TypeVar('InputsT')
OutputT = TypeVar('OutputT')
MetadataT = TypeVar('MetadataT')

class DatasetNotFoundError(Exception):
    """Raised when a dataset is not found."""
class CaseNotFoundError(Exception):
    """Raised when a case is not found."""

class DatasetApiError(Exception):
    """Raised when the API returns an error."""
    status_code: Incomplete
    detail: Incomplete
    def __init__(self, status_code: int, detail: Any) -> None: ...

class _BaseLogfireAPIClient(Generic[T]):
    """Base class for datasets clients."""
    client: T
    def __init__(self, client: T) -> None: ...

class LogfireAPIClient(_BaseLogfireAPIClient[Client]):
    '''Synchronous client for managing Logfire datasets.

    The client supports typed datasets that integrate with pydantic-evals.

    Example usage:
        ```python skip-run="true" skip-reason="external-connection"
        from dataclasses import dataclass
        from logfire.experimental.api_client import LogfireAPIClient
        from pydantic_evals import Case


        @dataclass
        class MyInput:
            question: str


        @dataclass
        class MyOutput:
            answer: str


        with LogfireAPIClient(api_key=\'your-api-key\') as client:
            # Create typed dataset
            dataset = client.create_dataset(
                name=\'qa-dataset\',
                input_type=MyInput,
                output_type=MyOutput,
            )

            # Add cases using pydantic-evals Case objects
            client.add_cases(
                dataset[\'id\'],
                cases=[
                    Case(name=\'q1\', inputs=MyInput(\'Hello?\'), expected_output=MyOutput(\'Hi!\')),
                ],
            )

            # Export as pydantic-evals Dataset
            dataset = client.export_dataset(\'qa-dataset\', MyInput, MyOutput)
        ```
    '''
    def __init__(self, api_key: str | None = None, base_url: str | None = None, timeout: Timeout = ..., *, client: Client | None = None) -> None:
        """Create a new datasets client.

        Args:
            api_key: A Logfire API key with datasets scopes (project:read_datasets, project:write_datasets).
            base_url: The base URL of the Logfire API. If not provided, inferred from API key.
            timeout: Request timeout configuration.
            client: A pre-configured httpx.Client. If provided, api_key/base_url/timeout are ignored.
        """
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: type[BaseException] | None = None, exc_value: BaseException | None = None, traceback: TracebackType | None = None) -> None: ...
    def list_datasets(self) -> list[dict[str, Any]]:
        """List all datasets in the project.

        Returns:
            List of dataset summaries with id, name, description, case_count, etc.
        """
    def get_dataset(self, id_or_name: str) -> dict[str, Any]:
        """Get a dataset by ID or name.

        Args:
            id_or_name: The dataset ID (UUID) or name.

        Returns:
            Dataset details including schemas and metadata.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
        """
    def create_dataset(self, name: str, *, input_type: type[Any] | None = None, output_type: type[Any] | None = None, metadata_type: type[Any] | None = None, description: str | None = None, guidance: str | None = None, ai_managed_guidance: bool = False) -> dict[str, Any]:
        '''Create a new dataset with optional type schemas.

        Args:
            name: The dataset name (must be unique within the project).
            input_type: Type for case inputs. JSON schema will be generated from this type.
            output_type: Type for expected outputs. JSON schema will be generated from this type.
            metadata_type: Type for case metadata. JSON schema will be generated from this type.
            description: Optional description of the dataset.
            guidance: Instructions for AI-assisted population.
            ai_managed_guidance: Whether guidance is managed by AI.

        Returns:
            The created dataset.

        Example:
            ```python skip-run="true" skip-reason="external-connection"
            from dataclasses import dataclass


            @dataclass
            class MyInput:
                question: str


            @dataclass
            class MyOutput:
                answer: str


            dataset = client.create_dataset(
                name=\'qa-dataset\',
                input_type=MyInput,
                output_type=MyOutput,
            )
            ```
        '''
    def update_dataset(self, id_or_name: str, *, name: str = ..., input_type: type[Any] | None = None, output_type: type[Any] | None = None, metadata_type: type[Any] | None = None, description: str | None = ..., guidance: str | None = ..., ai_managed_guidance: bool | None = None) -> dict[str, Any]:
        """Update an existing dataset.

        Args:
            id_or_name: The dataset ID (UUID) or name.
            name: New name for the dataset.
            input_type: New input type (generates schema).
            output_type: New output type (generates schema).
            metadata_type: New metadata type (generates schema).
            description: New description. Pass None to clear.
            guidance: New guidance instructions. Pass None to clear.
            ai_managed_guidance: Whether guidance is managed by AI.

        Returns:
            The updated dataset.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
        """
    def delete_dataset(self, id_or_name: str) -> None:
        """Delete a dataset and all its cases.

        Args:
            id_or_name: The dataset ID (UUID) or name.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
        """
    def list_cases(self, dataset_id_or_name: str, *, tags: list[str] | None = None) -> list[dict[str, Any]]:
        """List all cases in a dataset.

        Args:
            dataset_id_or_name: The dataset ID (UUID) or name.
            tags: Optional list of tags to filter cases by.

        Returns:
            List of cases with full details.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
        """
    def get_case(self, dataset_id_or_name: str, case_id: str) -> dict[str, Any]:
        """Get a specific case from a dataset.

        Args:
            dataset_id_or_name: The dataset ID (UUID) or name.
            case_id: The case ID.

        Returns:
            The case details.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            CaseNotFoundError: If the case does not exist.
        """
    def add_cases(self, dataset_id_or_name: str, cases: Sequence[Case[InputsT, OutputT, MetadataT]] | Sequence[dict[str, Any]], *, tags: list[str] | None = None, on_conflict: str = 'update') -> list[dict[str, Any]]:
        '''Add cases to a dataset.

        Accepts either pydantic-evals Case objects or plain dicts.

        By default, uses upsert behavior: cases with a name matching an existing
        case in the dataset are updated; cases without a name or with a new name
        are created. Set `on_conflict=\'error\'` to fail on name conflicts instead.

        Args:
            dataset_id_or_name: The dataset ID (UUID) or name.
            cases: A sequence of pydantic-evals Case objects or dicts.
            tags: Optional list of tags to associate with all cases.
            on_conflict: Conflict resolution strategy: `\'update\'` (default) to
                upsert cases with matching names, or `\'error\'` to fail on conflicts.

        Returns:
            The created/updated cases.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.

        Example:
            ```python skip-run="true" skip-reason="external-connection"
            from pydantic_evals import Case

            client.add_cases(
                \'my-dataset\',
                cases=[
                    Case(name=\'q1\', inputs=MyInput(\'What is 2+2?\'), expected_output=MyOutput(\'4\')),
                    Case(name=\'q2\', inputs=MyInput(\'What is 3+3?\'), expected_output=MyOutput(\'6\')),
                ],
            )
            ```
        '''
    def update_case(self, dataset_id_or_name: str, case_id: str, *, name: str | None = ..., inputs: Any | None = None, expected_output: Any | None = ..., metadata: Any | None = ..., evaluators: Sequence[Evaluator[Any, Any, Any]] | None = ..., tags: list[str] | None = ...) -> dict[str, Any]:
        """Update an existing case.

        Args:
            dataset_id_or_name: The dataset ID (UUID) or name.
            case_id: The case ID.
            name: New name for the case. Pass None to clear.
            inputs: New inputs (dict or typed object).
            expected_output: New expected output (dict or typed object). Pass None to clear.
            metadata: New metadata (dict or typed object). Pass None to clear.
            evaluators: New evaluators. Pass None to clear.
            tags: New tags for the case. Pass None to clear.

        Returns:
            The updated case.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            CaseNotFoundError: If the case does not exist.
        """
    def delete_case(self, dataset_id_or_name: str, case_id: str) -> None:
        """Delete a case from a dataset.

        Args:
            dataset_id_or_name: The dataset ID (UUID) or name.
            case_id: The case ID.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            CaseNotFoundError: If the case does not exist.
        """
    @overload
    def export_dataset(self, id_or_name: str) -> dict[str, Any]: ...
    @overload
    def export_dataset(self, id_or_name: str, input_type: type[InputsT], output_type: type[OutputT] | None = None, metadata_type: type[MetadataT] | None = None, *, custom_evaluator_types: Sequence[type[Evaluator[InputsT, OutputT, MetadataT]]] = ()) -> Dataset[InputsT, OutputT, MetadataT]: ...

class AsyncLogfireAPIClient(_BaseLogfireAPIClient[AsyncClient]):
    """Asynchronous client for managing Logfire datasets.

    See `LogfireAPIClient` for full documentation.
    """
    def __init__(self, api_key: str | None = None, base_url: str | None = None, timeout: Timeout = ..., *, client: AsyncClient | None = None) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type: type[BaseException] | None = None, exc_value: BaseException | None = None, traceback: TracebackType | None = None) -> None: ...
    async def list_datasets(self) -> list[dict[str, Any]]:
        """List all datasets."""
    async def get_dataset(self, id_or_name: str) -> dict[str, Any]:
        """Get a dataset by ID or name."""
    async def create_dataset(self, name: str, *, input_type: type[Any] | None = None, output_type: type[Any] | None = None, metadata_type: type[Any] | None = None, description: str | None = None, guidance: str | None = None, ai_managed_guidance: bool = False) -> dict[str, Any]:
        """Create a new dataset."""
    async def update_dataset(self, id_or_name: str, *, name: str = ..., input_type: type[Any] | None = None, output_type: type[Any] | None = None, metadata_type: type[Any] | None = None, description: str | None = ..., guidance: str | None = ..., ai_managed_guidance: bool | None = None) -> dict[str, Any]:
        """Update an existing dataset."""
    async def delete_dataset(self, id_or_name: str) -> None:
        """Delete a dataset."""
    async def list_cases(self, dataset_id_or_name: str, *, tags: list[str] | None = None) -> list[dict[str, Any]]:
        """List all cases in a dataset."""
    async def get_case(self, dataset_id_or_name: str, case_id: str) -> dict[str, Any]:
        """Get a specific case from a dataset."""
    async def add_cases(self, dataset_id_or_name: str, cases: Sequence[Case[InputsT, OutputT, MetadataT]] | Sequence[dict[str, Any]], *, tags: list[str] | None = None, on_conflict: str = 'update') -> list[dict[str, Any]]:
        """Add cases to a dataset.

        Accepts either pydantic-evals Case objects or plain dicts.

        By default, uses upsert behavior: cases with a name matching an existing
        case in the dataset are updated; cases without a name or with a new name
        are created. Set `on_conflict='error'` to fail on name conflicts instead.
        """
    async def update_case(self, dataset_id_or_name: str, case_id: str, *, name: str | None = ..., inputs: Any | None = None, expected_output: Any | None = ..., metadata: Any | None = ..., evaluators: Sequence[Evaluator[Any, Any, Any]] | None = ..., tags: list[str] | None = ...) -> dict[str, Any]:
        """Update an existing case."""
    async def delete_case(self, dataset_id_or_name: str, case_id: str) -> None:
        """Delete a case from a dataset."""
    @overload
    async def export_dataset(self, id_or_name: str) -> dict[str, Any]: ...
    @overload
    async def export_dataset(self, id_or_name: str, input_type: type[InputsT], output_type: type[OutputT] | None = None, metadata_type: type[MetadataT] | None = None, *, custom_evaluator_types: Sequence[type[Evaluator[InputsT, OutputT, MetadataT]]] = ()) -> Dataset[InputsT, OutputT, MetadataT]: ...
