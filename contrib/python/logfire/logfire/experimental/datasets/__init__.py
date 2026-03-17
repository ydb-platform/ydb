"""Logfire Datasets SDK for managing typed datasets compatible with pydantic-evals.

This module provides sync and async clients for managing datasets that integrate
with pydantic-evals for AI evaluation workflows.

Example usage:
    ```python skip-run="true" skip-reason="external-connection"
    from dataclasses import dataclass
    from logfire.experimental.api_client import LogfireAPIClient


    @dataclass
    class MyInput:
        question: str
        context: str | None = None


    @dataclass
    class MyOutput:
        answer: str
        confidence: float


    with LogfireAPIClient(api_key='your-api-key') as client:
        # Create a typed dataset (generates JSON schemas from types)
        dataset_info = client.create_dataset(
            name='my-evaluation-dataset',
            input_type=MyInput,
            output_type=MyOutput,
            description='Test cases for my chatbot',
        )

        # Add typed cases
        from pydantic_evals import Case

        client.add_cases(
            dataset_info['id'],
            cases=[
                Case(
                    inputs=MyInput(question='What is 2+2?'),
                    expected_output=MyOutput(answer='4', confidence=1.0),
                ),
            ],
        )

        # Export as pydantic-evals Dataset for evaluation
        from pydantic_evals import Dataset

        dataset: Dataset[MyInput, MyOutput, None] = client.export_dataset(
            'my-evaluation-dataset',
            input_type=MyInput,
            output_type=MyOutput,
        )

        # Run evaluations (in an async context)
        # report = await dataset.evaluate(my_task)
    ```
"""

from logfire.experimental.api_client import (
    AsyncLogfireAPIClient,
    CaseNotFoundError,
    DatasetApiError,
    DatasetNotFoundError,
    LogfireAPIClient,
)

__all__ = [
    # Clients
    'LogfireAPIClient',
    'AsyncLogfireAPIClient',
    # Errors
    'DatasetNotFoundError',
    'CaseNotFoundError',
    'DatasetApiError',
]
