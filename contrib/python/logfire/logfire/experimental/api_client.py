"""Logfire API client for managing datasets and cases.

This client provides typed, programmatic access to Logfire datasets that are
compatible with pydantic-evals for AI evaluation workflows.

Example usage:
    ```python skip-run="true" skip-reason="external-connection"
    from dataclasses import dataclass
    from logfire.experimental.api_client import LogfireAPIClient
    from pydantic_evals import Case, Dataset


    @dataclass
    class MyInput:
        question: str


    @dataclass
    class MyOutput:
        answer: str


    with LogfireAPIClient(api_key='your-api-key') as client:
        # Create a typed dataset
        dataset_info = client.create_dataset(
            name='my-dataset',
            input_type=MyInput,
            output_type=MyOutput,
        )

        # Add cases using pydantic-evals Case objects
        client.add_cases(
            dataset_info['id'],
            cases=[
                Case(name='q1', inputs=MyInput('What is 2+2?'), expected_output=MyOutput('4')),
                Case(name='q2', inputs=MyInput('What is 3+3?'), expected_output=MyOutput('6')),
            ],
        )

        # Export as pydantic-evals Dataset
        dataset: Dataset[MyInput, MyOutput, None] = client.export_dataset(
            'my-dataset',
            input_type=MyInput,
            output_type=MyOutput,
        )
    ```
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

from pydantic import TypeAdapter
from typing_extensions import Self

from logfire._internal.config import get_base_url_from_token

try:
    from httpx import AsyncClient, Client, Response, Timeout
    from httpx._client import BaseClient
except ImportError as e:  # pragma: no cover
    raise ImportError('httpx is required to use the Logfire datasets client') from e

if TYPE_CHECKING:
    from pydantic_evals import Case, Dataset
    from pydantic_evals.evaluators import Evaluator
else:
    Case = Dataset = Evaluator = Any  # type: ignore[assignment]

DEFAULT_TIMEOUT = Timeout(30.0)

T = TypeVar('T', bound=BaseClient)
InputsT = TypeVar('InputsT')
OutputT = TypeVar('OutputT')
MetadataT = TypeVar('MetadataT')

_UNSET: Any = object()
"""Sentinel to distinguish 'not provided' from explicit None."""


_DATASET_NAME_RE = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9._-]*$')


def _validate_dataset_name(name: str) -> None:
    """Validate that a dataset name is URL-safe."""
    if not _DATASET_NAME_RE.match(name):
        raise ValueError(
            f'Invalid dataset name {name!r}. '
            'Names must start with a letter or digit and contain only letters, digits, dots, hyphens, and underscores.'
        )


def _import_pydantic_evals() -> tuple[type, type]:
    """Import pydantic-evals types, raising ImportError if not available."""
    try:
        from pydantic_evals import Case, Dataset

        return Dataset, Case
    except ImportError:
        raise ImportError('pydantic-evals is required for this operation. Install with: pip install pydantic-evals')


class DatasetNotFoundError(Exception):
    """Raised when a dataset is not found."""

    pass


class CaseNotFoundError(Exception):
    """Raised when a case is not found."""

    pass


class DatasetApiError(Exception):
    """Raised when the API returns an error."""

    def __init__(self, status_code: int, detail: Any):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f'API error {status_code}: {detail}')


def _type_to_schema(type_: type[Any] | None) -> dict[str, Any] | None:
    """Convert a type to a JSON schema using Pydantic's TypeAdapter."""
    if type_ is None:
        return None
    adapter = TypeAdapter(type_)
    return adapter.json_schema()


def _serialize_value(value: Any) -> Any:
    """Serialize a value to JSON-compatible format."""
    if value is None:
        return None
    value_type: type[Any] = type(value)  # pyright: ignore[reportUnknownVariableType]
    adapter: TypeAdapter[Any] = TypeAdapter(value_type)
    return adapter.dump_python(value, mode='json')


def _serialize_evaluators(evaluators: Sequence[Any]) -> list[dict[str, Any]]:
    """Serialize evaluators to EvaluatorSpec format.

    Format: [{"name": "EvaluatorName", "arguments": {...}}]
    """
    result: list[dict[str, Any]] = []
    for evaluator in evaluators:
        # Get the serialization name from the evaluator class
        evaluator_cls: type[Any] = type(evaluator)  # pyright: ignore[reportUnknownVariableType]
        name = getattr(evaluator_cls, 'get_serialization_name', lambda: evaluator_cls.__name__)()

        # Get init arguments by serializing the evaluator
        # Most evaluators are dataclasses or Pydantic models
        if hasattr(evaluator, 'model_dump'):
            # Pydantic model
            arguments = evaluator.model_dump()
        elif hasattr(evaluator, '__dataclass_fields__'):
            # Dataclass - get field values
            from dataclasses import asdict

            arguments = asdict(evaluator)
        else:
            arguments = None

        # Use None if no arguments, otherwise use the dict
        if arguments is not None and len(arguments) == 0:
            arguments = None

        result.append({'name': name, 'arguments': arguments})

    return result


def _serialize_case(case: Case[Any, Any, Any]) -> dict[str, Any]:
    """Serialize a pydantic-evals Case to dict format for the API."""
    data: dict[str, Any] = {
        'inputs': _serialize_value(case.inputs) if not isinstance(case.inputs, dict) else case.inputs,  # pyright: ignore[reportUnknownMemberType]
    }

    if case.name is not None:
        data['name'] = case.name

    if case.expected_output is not None:
        data['expected_output'] = (
            _serialize_value(case.expected_output)
            if not isinstance(case.expected_output, dict)
            else case.expected_output  # pyright: ignore[reportUnknownMemberType]
        )

    if case.metadata is not None:
        data['metadata'] = _serialize_value(case.metadata) if not isinstance(case.metadata, dict) else case.metadata  # pyright: ignore[reportUnknownMemberType]

    if case.evaluators is not None:  # pyright: ignore[reportUnnecessaryComparison]
        data['evaluators'] = _serialize_evaluators(case.evaluators)

    return data


def _parse_response_detail(response: Response, *, default: Any) -> Any:
    """Parse the response body as JSON, falling back to a default on failure."""
    try:
        return response.json() if response.content else default
    except Exception:
        return response.text or default


def _is_case_error(detail: Any) -> bool:
    """Check if a parsed error detail indicates a case-not-found error."""
    if isinstance(detail, dict):
        message: Any = cast(dict[str, Any], detail).get('detail', '')
        return isinstance(message, str) and 'case' in message.lower()
    return False


class _BaseLogfireAPIClient(Generic[T]):
    """Base class for datasets clients."""

    def __init__(self, client: T):
        self.client: T = client

    def _handle_response(self, response: Response, *, is_case_endpoint: bool = False) -> Any:
        """Handle API response, raising appropriate errors."""
        if response.status_code == 404:
            detail = _parse_response_detail(response, default='Not found')
            if is_case_endpoint and _is_case_error(detail):
                raise CaseNotFoundError(detail)
            raise DatasetNotFoundError(detail)
        if response.status_code >= 400:
            detail = _parse_response_detail(response, default=response.text)
            raise DatasetApiError(response.status_code, detail)
        if response.status_code == 204:
            return None
        return response.json()


class LogfireAPIClient(_BaseLogfireAPIClient[Client]):
    """Synchronous client for managing Logfire datasets.

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


        with LogfireAPIClient(api_key='your-api-key') as client:
            # Create typed dataset
            dataset = client.create_dataset(
                name='qa-dataset',
                input_type=MyInput,
                output_type=MyOutput,
            )

            # Add cases using pydantic-evals Case objects
            client.add_cases(
                dataset['id'],
                cases=[
                    Case(name='q1', inputs=MyInput('Hello?'), expected_output=MyOutput('Hi!')),
                ],
            )

            # Export as pydantic-evals Dataset
            dataset = client.export_dataset('qa-dataset', MyInput, MyOutput)
        ```
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: Timeout = DEFAULT_TIMEOUT,
        *,
        client: Client | None = None,
    ):
        """Create a new datasets client.

        Args:
            api_key: A Logfire API key with datasets scopes (project:read_datasets, project:write_datasets).
            base_url: The base URL of the Logfire API. If not provided, inferred from API key.
            timeout: Request timeout configuration.
            client: A pre-configured httpx.Client. If provided, api_key/base_url/timeout are ignored.
        """
        if client is not None:
            super().__init__(client)
        elif api_key is not None:
            base_url = base_url or get_base_url_from_token(api_key)
            super().__init__(
                Client(
                    timeout=timeout,
                    base_url=base_url,
                    headers={'authorization': f'Bearer {api_key}'},
                )
            )
        else:
            raise ValueError('Either client or api_key must be provided')

    def __enter__(self) -> Self:
        self.client.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self.client.__exit__(exc_type, exc_value, traceback)

    # --- Dataset operations ---

    def list_datasets(self) -> list[dict[str, Any]]:
        """List all datasets in the project.

        Returns:
            List of dataset summaries with id, name, description, case_count, etc.
        """
        response = self.client.get('/v1/datasets/')
        return self._handle_response(response)

    def get_dataset(self, id_or_name: str) -> dict[str, Any]:
        """Get a dataset by ID or name.

        Args:
            id_or_name: The dataset ID (UUID) or name.

        Returns:
            Dataset details including schemas and metadata.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
        """
        response = self.client.get(f'/v1/datasets/{id_or_name}/')
        return self._handle_response(response)

    def create_dataset(
        self,
        name: str,
        *,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        metadata_type: type[Any] | None = None,
        description: str | None = None,
        guidance: str | None = None,
        ai_managed_guidance: bool = False,
    ) -> dict[str, Any]:
        """Create a new dataset with optional type schemas.

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
                name='qa-dataset',
                input_type=MyInput,
                output_type=MyOutput,
            )
            ```
        """
        _validate_dataset_name(name)
        data: dict[str, Any] = {'name': name}
        if description is not None:
            data['description'] = description

        # Generate schemas from types
        if input_type is not None:
            data['input_schema'] = _type_to_schema(input_type)
        if output_type is not None:
            data['output_schema'] = _type_to_schema(output_type)
        if metadata_type is not None:
            data['metadata_schema'] = _type_to_schema(metadata_type)

        if guidance is not None:
            data['guidance'] = guidance
        if ai_managed_guidance:
            data['ai_managed_guidance'] = ai_managed_guidance

        response = self.client.post('/v1/datasets/', json=data)
        return self._handle_response(response)

    def update_dataset(
        self,
        id_or_name: str,
        *,
        name: str = _UNSET,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        metadata_type: type[Any] | None = None,
        description: str | None = _UNSET,
        guidance: str | None = _UNSET,
        ai_managed_guidance: bool | None = None,
    ) -> dict[str, Any]:
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
        data: dict[str, Any] = {}
        if name is not _UNSET:
            _validate_dataset_name(name)
            data['name'] = name
        if description is not _UNSET:
            data['description'] = description
        if input_type is not None:
            data['input_schema'] = _type_to_schema(input_type)
        if output_type is not None:
            data['output_schema'] = _type_to_schema(output_type)
        if metadata_type is not None:
            data['metadata_schema'] = _type_to_schema(metadata_type)
        if guidance is not _UNSET:
            data['guidance'] = guidance
        if ai_managed_guidance is not None:
            data['ai_managed_guidance'] = ai_managed_guidance

        response = self.client.patch(f'/v1/datasets/{id_or_name}/', json=data)
        return self._handle_response(response)

    def delete_dataset(self, id_or_name: str) -> None:
        """Delete a dataset and all its cases.

        Args:
            id_or_name: The dataset ID (UUID) or name.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
        """
        response = self.client.delete(f'/v1/datasets/{id_or_name}/')
        self._handle_response(response)

    # --- Case operations ---

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
        params: dict[str, Any] = {}
        if tags is not None:
            params['tags'] = tags
        response = self.client.get(f'/v1/datasets/{dataset_id_or_name}/cases/', params=params)
        return self._handle_response(response)

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
        response = self.client.get(f'/v1/datasets/{dataset_id_or_name}/cases/{case_id}/')
        return self._handle_response(response, is_case_endpoint=True)

    def add_cases(
        self,
        dataset_id_or_name: str,
        cases: Sequence[Case[InputsT, OutputT, MetadataT]] | Sequence[dict[str, Any]],
        *,
        tags: list[str] | None = None,
        on_conflict: str = 'update',
    ) -> list[dict[str, Any]]:
        """Add cases to a dataset.

        Accepts either pydantic-evals Case objects or plain dicts.

        By default, uses upsert behavior: cases with a name matching an existing
        case in the dataset are updated; cases without a name or with a new name
        are created. Set `on_conflict='error'` to fail on name conflicts instead.

        Args:
            dataset_id_or_name: The dataset ID (UUID) or name.
            cases: A sequence of pydantic-evals Case objects or dicts.
            tags: Optional list of tags to associate with all cases.
            on_conflict: Conflict resolution strategy: `'update'` (default) to
                upsert cases with matching names, or `'error'` to fail on conflicts.

        Returns:
            The created/updated cases.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.

        Example:
            ```python skip-run="true" skip-reason="external-connection"
            from pydantic_evals import Case

            client.add_cases(
                'my-dataset',
                cases=[
                    Case(name='q1', inputs=MyInput('What is 2+2?'), expected_output=MyOutput('4')),
                    Case(name='q2', inputs=MyInput('What is 3+3?'), expected_output=MyOutput('6')),
                ],
            )
            ```
        """
        if cases and hasattr(cases[0], 'inputs') and not isinstance(cases[0], dict):
            serialized_cases = [_serialize_case(case) for case in cases]  # type: ignore[arg-type]
        else:
            # Already dicts — shallow copy to avoid mutating caller's data
            serialized_cases: list[dict[str, Any]] = [dict(c) for c in cases]  # type: ignore[arg-type]

        if tags is not None:
            for case_data in serialized_cases:
                case_data['tags'] = tags
        response = self.client.post(
            f'/v1/datasets/{dataset_id_or_name}/import/',
            json={'cases': serialized_cases},
            params={'on_conflict': on_conflict},
        )
        return self._handle_response(response)

    def update_case(
        self,
        dataset_id_or_name: str,
        case_id: str,
        *,
        name: str | None = _UNSET,
        inputs: Any | None = None,
        expected_output: Any | None = _UNSET,
        metadata: Any | None = _UNSET,
        evaluators: Sequence[Evaluator[Any, Any, Any]] | None = _UNSET,
        tags: list[str] | None = _UNSET,
    ) -> dict[str, Any]:
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
        data: dict[str, Any] = {}
        if name is not _UNSET:
            data['name'] = name
        if inputs is not None:
            data['inputs'] = _serialize_value(inputs) if not isinstance(inputs, dict) else inputs
        if expected_output is not _UNSET:
            data['expected_output'] = (
                (_serialize_value(expected_output) if not isinstance(expected_output, dict) else expected_output)
                if expected_output is not None
                else None
            )
        if metadata is not _UNSET:
            data['metadata'] = (
                (_serialize_value(metadata) if not isinstance(metadata, dict) else metadata)
                if metadata is not None
                else None
            )
        if evaluators is not _UNSET:
            data['evaluators'] = _serialize_evaluators(evaluators) if evaluators is not None else None
        if tags is not _UNSET:
            data['tags'] = tags

        response = self.client.patch(f'/v1/datasets/{dataset_id_or_name}/cases/{case_id}/', json=data)
        return self._handle_response(response, is_case_endpoint=True)

    def delete_case(self, dataset_id_or_name: str, case_id: str) -> None:
        """Delete a case from a dataset.

        Args:
            dataset_id_or_name: The dataset ID (UUID) or name.
            case_id: The case ID.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            CaseNotFoundError: If the case does not exist.
        """
        response = self.client.delete(f'/v1/datasets/{dataset_id_or_name}/cases/{case_id}/')
        self._handle_response(response, is_case_endpoint=True)

    # --- Export/Import operations ---

    @overload
    def export_dataset(self, id_or_name: str) -> dict[str, Any]: ...

    @overload
    def export_dataset(
        self,
        id_or_name: str,
        input_type: type[InputsT],
        output_type: type[OutputT] | None = None,
        metadata_type: type[MetadataT] | None = None,
        *,
        custom_evaluator_types: Sequence[type[Evaluator[InputsT, OutputT, MetadataT]]] = (),
    ) -> Dataset[InputsT, OutputT, MetadataT]: ...

    def export_dataset(
        self,
        id_or_name: str,
        input_type: type[InputsT] | None = None,
        output_type: type[OutputT] | None = None,
        metadata_type: type[MetadataT] | None = None,
        *,
        custom_evaluator_types: Sequence[type[Evaluator[Any, Any, Any]]] = (),
    ) -> Dataset[InputsT, OutputT, MetadataT] | dict[str, Any]:
        """Export a dataset, optionally as a typed pydantic-evals Dataset.

        When called with type arguments, returns a `pydantic_evals.Dataset` with
        properly typed cases. Without type arguments, returns raw dict data.

        Args:
            id_or_name: The dataset ID (UUID) or name.
            input_type: Type for case inputs.
            output_type: Type for expected outputs.
            metadata_type: Type for case metadata.
            custom_evaluator_types: Custom evaluator classes for deserializing case evaluators.

        Returns:
            If types provided: `pydantic_evals.Dataset[InputsT, OutputT, MetadataT]`
            Otherwise: Raw dict in pydantic-evals compatible format.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            ImportError: If pydantic-evals is not installed (when using types).

        Example:
            ```python skip-run="true" skip-reason="external-connection"
            from pydantic_evals import Dataset
            from pydantic_evals.evaluators import IsInstance

            # Export with types and custom evaluators
            dataset: Dataset[MyInput, MyOutput, None] = client.export_dataset(
                'my-dataset',
                input_type=MyInput,
                output_type=MyOutput,
                custom_evaluator_types=[MyCustomEvaluator],
            )


            # Use with evaluations (in an async context)
            # report = await dataset.evaluate(my_task)
            ```
        """
        response = self.client.get(f'/v1/datasets/{id_or_name}/export/')
        data = self._handle_response(response)

        # If no types provided, return raw dict
        if input_type is None:
            return data

        # Convert to typed Dataset using pydantic-evals
        Dataset, _ = _import_pydantic_evals()

        # Create a properly typed Dataset class
        typed_dataset_cls: type[Dataset[InputsT, OutputT, MetadataT]] = Dataset[input_type, output_type, metadata_type]  # type: ignore
        return typed_dataset_cls.from_dict(data, custom_evaluator_types=list(custom_evaluator_types))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]


class AsyncLogfireAPIClient(_BaseLogfireAPIClient[AsyncClient]):
    """Asynchronous client for managing Logfire datasets.

    See `LogfireAPIClient` for full documentation.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: Timeout = DEFAULT_TIMEOUT,
        *,
        client: AsyncClient | None = None,
    ):
        if client is not None:
            super().__init__(client)
        elif api_key is not None:
            base_url = base_url or get_base_url_from_token(api_key)
            super().__init__(
                AsyncClient(
                    timeout=timeout,
                    base_url=base_url,
                    headers={'authorization': f'Bearer {api_key}'},
                )
            )
        else:
            raise ValueError('Either client or api_key must be provided')

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.client.__aexit__(exc_type, exc_value, traceback)

    async def list_datasets(self) -> list[dict[str, Any]]:
        """List all datasets."""
        response = await self.client.get('/v1/datasets/')
        return self._handle_response(response)

    async def get_dataset(self, id_or_name: str) -> dict[str, Any]:
        """Get a dataset by ID or name."""
        response = await self.client.get(f'/v1/datasets/{id_or_name}/')
        return self._handle_response(response)

    async def create_dataset(
        self,
        name: str,
        *,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        metadata_type: type[Any] | None = None,
        description: str | None = None,
        guidance: str | None = None,
        ai_managed_guidance: bool = False,
    ) -> dict[str, Any]:
        """Create a new dataset."""
        _validate_dataset_name(name)
        data: dict[str, Any] = {'name': name}
        if description is not None:
            data['description'] = description
        if input_type is not None:
            data['input_schema'] = _type_to_schema(input_type)
        if output_type is not None:
            data['output_schema'] = _type_to_schema(output_type)
        if metadata_type is not None:
            data['metadata_schema'] = _type_to_schema(metadata_type)
        if guidance is not None:
            data['guidance'] = guidance
        if ai_managed_guidance:
            data['ai_managed_guidance'] = ai_managed_guidance

        response = await self.client.post('/v1/datasets/', json=data)
        return self._handle_response(response)

    async def update_dataset(
        self,
        id_or_name: str,
        *,
        name: str = _UNSET,
        input_type: type[Any] | None = None,
        output_type: type[Any] | None = None,
        metadata_type: type[Any] | None = None,
        description: str | None = _UNSET,
        guidance: str | None = _UNSET,
        ai_managed_guidance: bool | None = None,
    ) -> dict[str, Any]:
        """Update an existing dataset."""
        data: dict[str, Any] = {}
        if name is not _UNSET:
            _validate_dataset_name(name)
            data['name'] = name
        if description is not _UNSET:
            data['description'] = description
        if input_type is not None:
            data['input_schema'] = _type_to_schema(input_type)
        if output_type is not None:
            data['output_schema'] = _type_to_schema(output_type)
        if metadata_type is not None:
            data['metadata_schema'] = _type_to_schema(metadata_type)
        if guidance is not _UNSET:
            data['guidance'] = guidance
        if ai_managed_guidance is not None:
            data['ai_managed_guidance'] = ai_managed_guidance

        response = await self.client.patch(f'/v1/datasets/{id_or_name}/', json=data)
        return self._handle_response(response)

    async def delete_dataset(self, id_or_name: str) -> None:
        """Delete a dataset."""
        response = await self.client.delete(f'/v1/datasets/{id_or_name}/')
        self._handle_response(response)

    async def list_cases(self, dataset_id_or_name: str, *, tags: list[str] | None = None) -> list[dict[str, Any]]:
        """List all cases in a dataset."""
        params: dict[str, Any] = {}
        if tags is not None:
            params['tags'] = tags
        response = await self.client.get(f'/v1/datasets/{dataset_id_or_name}/cases/', params=params)
        return self._handle_response(response)

    async def get_case(self, dataset_id_or_name: str, case_id: str) -> dict[str, Any]:
        """Get a specific case from a dataset."""
        response = await self.client.get(f'/v1/datasets/{dataset_id_or_name}/cases/{case_id}/')
        return self._handle_response(response, is_case_endpoint=True)

    async def add_cases(
        self,
        dataset_id_or_name: str,
        cases: Sequence[Case[InputsT, OutputT, MetadataT]] | Sequence[dict[str, Any]],
        *,
        tags: list[str] | None = None,
        on_conflict: str = 'update',
    ) -> list[dict[str, Any]]:
        """Add cases to a dataset.

        Accepts either pydantic-evals Case objects or plain dicts.

        By default, uses upsert behavior: cases with a name matching an existing
        case in the dataset are updated; cases without a name or with a new name
        are created. Set `on_conflict='error'` to fail on name conflicts instead.
        """
        if cases and hasattr(cases[0], 'inputs') and not isinstance(cases[0], dict):
            serialized_cases = [_serialize_case(case) for case in cases]  # type: ignore[arg-type]
        else:
            # Already dicts — shallow copy to avoid mutating caller's data
            serialized_cases: list[dict[str, Any]] = [dict(c) for c in cases]  # type: ignore[arg-type]

        if tags is not None:
            for case_data in serialized_cases:
                case_data['tags'] = tags
        response = await self.client.post(
            f'/v1/datasets/{dataset_id_or_name}/import/',
            json={'cases': serialized_cases},
            params={'on_conflict': on_conflict},
        )
        return self._handle_response(response)

    async def update_case(
        self,
        dataset_id_or_name: str,
        case_id: str,
        *,
        name: str | None = _UNSET,
        inputs: Any | None = None,
        expected_output: Any | None = _UNSET,
        metadata: Any | None = _UNSET,
        evaluators: Sequence[Evaluator[Any, Any, Any]] | None = _UNSET,
        tags: list[str] | None = _UNSET,
    ) -> dict[str, Any]:
        """Update an existing case."""
        data: dict[str, Any] = {}
        if name is not _UNSET:
            data['name'] = name
        if inputs is not None:
            data['inputs'] = _serialize_value(inputs) if not isinstance(inputs, dict) else inputs
        if expected_output is not _UNSET:
            data['expected_output'] = (
                (_serialize_value(expected_output) if not isinstance(expected_output, dict) else expected_output)
                if expected_output is not None
                else None
            )
        if metadata is not _UNSET:
            data['metadata'] = (
                (_serialize_value(metadata) if not isinstance(metadata, dict) else metadata)
                if metadata is not None
                else None
            )
        if evaluators is not _UNSET:
            data['evaluators'] = _serialize_evaluators(evaluators) if evaluators is not None else None
        if tags is not _UNSET:
            data['tags'] = tags

        response = await self.client.patch(f'/v1/datasets/{dataset_id_or_name}/cases/{case_id}/', json=data)
        return self._handle_response(response, is_case_endpoint=True)

    async def delete_case(self, dataset_id_or_name: str, case_id: str) -> None:
        """Delete a case from a dataset."""
        response = await self.client.delete(f'/v1/datasets/{dataset_id_or_name}/cases/{case_id}/')
        self._handle_response(response, is_case_endpoint=True)

    @overload
    async def export_dataset(self, id_or_name: str) -> dict[str, Any]: ...

    @overload
    async def export_dataset(
        self,
        id_or_name: str,
        input_type: type[InputsT],
        output_type: type[OutputT] | None = None,
        metadata_type: type[MetadataT] | None = None,
        *,
        custom_evaluator_types: Sequence[type[Evaluator[InputsT, OutputT, MetadataT]]] = (),
    ) -> Dataset[InputsT, OutputT, MetadataT]: ...

    async def export_dataset(
        self,
        id_or_name: str,
        input_type: type[InputsT] | None = None,
        output_type: type[OutputT] | None = None,
        metadata_type: type[MetadataT] | None = None,
        *,
        custom_evaluator_types: Sequence[type[Evaluator[Any, Any, Any]]] = (),
    ) -> Dataset[InputsT, OutputT, MetadataT] | dict[str, Any]:
        """Export a dataset, optionally as a typed pydantic-evals Dataset."""
        response = await self.client.get(f'/v1/datasets/{id_or_name}/export/')
        data = self._handle_response(response)

        if input_type is None:
            return data

        Dataset, _ = _import_pydantic_evals()
        typed_dataset_cls: type[Dataset[InputsT, OutputT, MetadataT]] = Dataset[input_type, output_type, metadata_type]  # type: ignore
        return typed_dataset_cls.from_dict(data, custom_evaluator_types=list(custom_evaluator_types))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
