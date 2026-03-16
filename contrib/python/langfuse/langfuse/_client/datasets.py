import datetime as dt
import logging
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional

from opentelemetry.util._decorator import _agnosticcontextmanager

from langfuse.batch_evaluation import CompositeEvaluatorFunction
from langfuse.experiment import (
    EvaluatorFunction,
    ExperimentResult,
    RunEvaluatorFunction,
    TaskFunction,
)
from langfuse.model import (
    CreateDatasetRunItemRequest,
    Dataset,
    DatasetItem,
    DatasetStatus,
)

from .span import LangfuseSpan

if TYPE_CHECKING:
    from langfuse._client.client import Langfuse


class DatasetItemClient:
    """Class for managing dataset items in Langfuse.

    Args:
        id (str): Unique identifier of the dataset item.
        status (DatasetStatus): The status of the dataset item. Can be either 'ACTIVE' or 'ARCHIVED'.
        input (Any): Input data of the dataset item.
        expected_output (Optional[Any]): Expected output of the dataset item.
        metadata (Optional[Any]): Additional metadata of the dataset item.
        source_trace_id (Optional[str]): Identifier of the source trace.
        source_observation_id (Optional[str]): Identifier of the source observation.
        dataset_id (str): Identifier of the dataset to which this item belongs.
        dataset_name (str): Name of the dataset to which this item belongs.
        created_at (datetime): Timestamp of dataset item creation.
        updated_at (datetime): Timestamp of the last update to the dataset item.
        langfuse (Langfuse): Instance of Langfuse client for API interactions.

    Example:
        ```python
        from langfuse import Langfuse

        langfuse = Langfuse()

        dataset = langfuse.get_dataset("<dataset_name>")

        for item in dataset.items:
            # Generate a completion using the input of every item
            completion, generation = llm_app.run(item.input)

            # Evaluate the completion
            generation.score(
                name="example-score",
                value=1
            )
        ```
    """

    log = logging.getLogger("langfuse")

    id: str
    status: DatasetStatus
    input: Any
    expected_output: Optional[Any]
    metadata: Optional[Any]
    source_trace_id: Optional[str]
    source_observation_id: Optional[str]
    dataset_id: str
    dataset_name: str
    created_at: dt.datetime
    updated_at: dt.datetime

    langfuse: "Langfuse"

    def __init__(self, dataset_item: DatasetItem, langfuse: "Langfuse"):
        """Initialize the DatasetItemClient."""
        self.id = dataset_item.id
        self.status = dataset_item.status
        self.input = dataset_item.input
        self.expected_output = dataset_item.expected_output
        self.metadata = dataset_item.metadata
        self.source_trace_id = dataset_item.source_trace_id
        self.source_observation_id = dataset_item.source_observation_id
        self.dataset_id = dataset_item.dataset_id
        self.dataset_name = dataset_item.dataset_name
        self.created_at = dataset_item.created_at
        self.updated_at = dataset_item.updated_at

        self.langfuse = langfuse

    @_agnosticcontextmanager
    def run(
        self,
        *,
        run_name: str,
        run_metadata: Optional[Any] = None,
        run_description: Optional[str] = None,
    ) -> Generator[LangfuseSpan, None, None]:
        """Create a context manager for the dataset item run that links the execution to a Langfuse trace.

        This method is a context manager that creates a trace for the dataset run and yields a span
        that can be used to track the execution of the run.

        Args:
            run_name (str): The name of the dataset run.
            run_metadata (Optional[Any]): Additional metadata to include in dataset run.
            run_description (Optional[str]): Description of the dataset run.

        Yields:
            span: A LangfuseSpan that can be used to trace the execution of the run.
        """
        trace_name = f"Dataset run: {run_name}"

        with self.langfuse.start_as_current_span(name=trace_name) as span:
            span.update_trace(
                name=trace_name,
                metadata={
                    "dataset_item_id": self.id,
                    "run_name": run_name,
                    "dataset_id": self.dataset_id,
                },
            )

            self.log.debug(
                f"Creating dataset run item: run_name={run_name} id={self.id} trace_id={span.trace_id}"
            )

            self.langfuse.api.dataset_run_items.create(
                request=CreateDatasetRunItemRequest(
                    runName=run_name,
                    datasetItemId=self.id,
                    traceId=span.trace_id,
                    metadata=run_metadata,
                    runDescription=run_description,
                )
            )

            yield span


class DatasetClient:
    """Class for managing datasets in Langfuse.

    Attributes:
        id (str): Unique identifier of the dataset.
        name (str): Name of the dataset.
        description (Optional[str]): Description of the dataset.
        metadata (Optional[typing.Any]): Additional metadata of the dataset.
        project_id (str): Identifier of the project to which the dataset belongs.
        created_at (datetime): Timestamp of dataset creation.
        updated_at (datetime): Timestamp of the last update to the dataset.
        items (List[DatasetItemClient]): List of dataset items associated with the dataset.
        version (Optional[datetime]): Timestamp of the dataset version.
    Example:
        Print the input of each dataset item in a dataset.
        ```python
        from langfuse import Langfuse

        langfuse = Langfuse()

        dataset = langfuse.get_dataset("<dataset_name>")

        for item in dataset.items:
            print(item.input)
        ```
    """

    id: str
    name: str
    description: Optional[str]
    project_id: str
    metadata: Optional[Any]
    created_at: dt.datetime
    updated_at: dt.datetime
    items: List[DatasetItemClient]
    version: Optional[dt.datetime]

    def __init__(
        self,
        dataset: Dataset,
        items: List[DatasetItemClient],
        version: Optional[dt.datetime] = None,
    ):
        """Initialize the DatasetClient."""
        self.id = dataset.id
        self.name = dataset.name
        self.description = dataset.description
        self.project_id = dataset.project_id
        self.metadata = dataset.metadata
        self.created_at = dataset.created_at
        self.updated_at = dataset.updated_at
        self.items = items
        self.version = version
        self._langfuse: Optional["Langfuse"] = None

    def _get_langfuse_client(self) -> Optional["Langfuse"]:
        """Get the Langfuse client from the first item."""
        if self._langfuse is None and self.items:
            self._langfuse = self.items[0].langfuse
        return self._langfuse

    def run_experiment(
        self,
        *,
        name: str,
        run_name: Optional[str] = None,
        description: Optional[str] = None,
        task: TaskFunction,
        evaluators: List[EvaluatorFunction] = [],
        composite_evaluator: Optional[CompositeEvaluatorFunction] = None,
        run_evaluators: List[RunEvaluatorFunction] = [],
        max_concurrency: int = 50,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ExperimentResult:
        """Run an experiment on this Langfuse dataset with automatic tracking.

        This is a convenience method that runs an experiment using all items in this
        dataset. It automatically creates a dataset run in Langfuse for tracking and
        comparison purposes, linking all experiment results to the dataset.

        Key benefits of using dataset.run_experiment():
        - Automatic dataset run creation and linking in Langfuse UI
        - Built-in experiment tracking and versioning
        - Easy comparison between different experiment runs
        - Direct access to dataset items with their metadata and expected outputs
        - Automatic URL generation for viewing results in Langfuse dashboard

        Args:
            name: Human-readable name for the experiment run. This will be used as
                the dataset run name in Langfuse for tracking and identification.
            run_name: Optional exact name for the dataset run. If provided, this will be
                used as the exact dataset run name in Langfuse. If not provided, this will
                default to the experiment name appended with an ISO timestamp.
            description: Optional description of the experiment's purpose, methodology,
                or what you're testing. Appears in the Langfuse UI for context.
            task: Function that processes each dataset item and returns output.
                The function will receive DatasetItem objects with .input, .expected_output,
                .metadata attributes. Signature should be: task(*, item, **kwargs) -> Any
            evaluators: List of functions to evaluate each item's output individually.
                These will have access to the item's expected_output for comparison.
            composite_evaluator: Optional function that creates composite scores from item-level evaluations.
                Receives the same inputs as item-level evaluators (input, output, expected_output, metadata)
                plus the list of evaluations from item-level evaluators. Useful for weighted averages,
                pass/fail decisions based on multiple criteria, or custom scoring logic combining multiple metrics.
            run_evaluators: List of functions to evaluate the entire experiment run.
                Useful for computing aggregate statistics across all dataset items.
            max_concurrency: Maximum number of concurrent task executions (default: 50).
                Adjust based on API rate limits and system resources.
            metadata: Optional metadata to attach to the experiment run and all traces.
                Will be combined with individual item metadata.

        Returns:
            ExperimentResult object containing:
            - name: The experiment name.
            - run_name: The experiment run name (equivalent to the dataset run name).
            - description: Optional experiment description.
            - item_results: Results for each dataset item with outputs and evaluations.
            - run_evaluations: Aggregate evaluation results for the entire run.
            - dataset_run_id: ID of the created dataset run in Langfuse.
            - dataset_run_url: Direct URL to view the experiment results in Langfuse UI.

            The result object provides a format() method for human-readable output:
            ```python
            result = dataset.run_experiment(...)
            print(result.format())  # Summary view
            print(result.format(include_item_results=True))  # Detailed view
            ```

        Raises:
            ValueError: If the dataset has no items or no Langfuse client is available.

        Examples:
            Basic dataset experiment:
            ```python
            dataset = langfuse.get_dataset("qa-evaluation-set")

            def answer_questions(*, item, **kwargs):
                # item is a DatasetItem with .input, .expected_output, .metadata
                question = item.input
                return my_qa_system.answer(question)

            def accuracy_evaluator(*, input, output, expected_output=None, **kwargs):
                if not expected_output:
                    return {"name": "accuracy", "value": 0, "comment": "No expected output"}

                is_correct = output.strip().lower() == expected_output.strip().lower()
                return {
                    "name": "accuracy",
                    "value": 1.0 if is_correct else 0.0,
                    "comment": "Correct" if is_correct else "Incorrect"
                }

            result = dataset.run_experiment(
                name="QA System v2.0 Evaluation",
                description="Testing improved QA system on curated question set",
                task=answer_questions,
                evaluators=[accuracy_evaluator]
            )

            print(f"Evaluated {len(result['item_results'])} questions")
            print(f"View detailed results: {result['dataset_run_url']}")
            ```

            Advanced experiment with multiple evaluators and run-level analysis:
            ```python
            dataset = langfuse.get_dataset("content-generation-benchmark")

            async def generate_content(*, item, **kwargs):
                prompt = item.input
                response = await openai_client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7
                )
                return response.choices[0].message.content

            def quality_evaluator(*, input, output, expected_output=None, metadata=None, **kwargs):
                # Use metadata for context-aware evaluation
                content_type = metadata.get("type", "general") if metadata else "general"

                # Basic quality checks
                word_count = len(output.split())
                min_words = {"blog": 300, "tweet": 10, "summary": 100}.get(content_type, 50)

                return [
                    {
                        "name": "word_count",
                        "value": word_count,
                        "comment": f"Generated {word_count} words"
                    },
                    {
                        "name": "meets_length_requirement",
                        "value": word_count >= min_words,
                        "comment": f"{'Meets' if word_count >= min_words else 'Below'} minimum {min_words} words for {content_type}"
                    }
                ]

            def content_diversity(*, item_results, **kwargs):
                # Analyze diversity across all generated content
                all_outputs = [result["output"] for result in item_results]
                unique_words = set()
                total_words = 0

                for output in all_outputs:
                    words = output.lower().split()
                    unique_words.update(words)
                    total_words += len(words)

                diversity_ratio = len(unique_words) / total_words if total_words > 0 else 0

                return {
                    "name": "vocabulary_diversity",
                    "value": diversity_ratio,
                    "comment": f"Used {len(unique_words)} unique words out of {total_words} total ({diversity_ratio:.2%} diversity)"
                }

            result = dataset.run_experiment(
                name="Content Generation Diversity Test",
                description="Evaluating content quality and vocabulary diversity across different content types",
                task=generate_content,
                evaluators=[quality_evaluator],
                run_evaluators=[content_diversity],
                max_concurrency=3,  # Limit API calls
                metadata={"model": "gpt-4", "temperature": 0.7}
            )

            # Results are automatically linked to dataset in Langfuse
            print(f"Experiment completed! View in Langfuse: {result['dataset_run_url']}")

            # Access individual results
            for i, item_result in enumerate(result["item_results"]):
                print(f"Item {i+1}: {item_result['evaluations']}")
            ```

            Comparing different model versions:
            ```python
            # Run multiple experiments on the same dataset for comparison
            dataset = langfuse.get_dataset("model-benchmark")

            # Experiment 1: GPT-4
            result_gpt4 = dataset.run_experiment(
                name="GPT-4 Baseline",
                description="Baseline performance with GPT-4",
                task=lambda *, item, **kwargs: gpt4_model.generate(item.input),
                evaluators=[accuracy_evaluator, fluency_evaluator]
            )

            # Experiment 2: Custom model
            result_custom = dataset.run_experiment(
                name="Custom Model v1.2",
                description="Testing our fine-tuned model",
                task=lambda *, item, **kwargs: custom_model.generate(item.input),
                evaluators=[accuracy_evaluator, fluency_evaluator]
            )

            # Both experiments are now visible in Langfuse for easy comparison
            print("Compare results in Langfuse:")
            print(f"GPT-4: {result_gpt4.dataset_run_url}")
            print(f"Custom: {result_custom.dataset_run_url}")
            ```

        Note:
            - All experiment results are automatically tracked in Langfuse as dataset runs
            - Dataset items provide .input, .expected_output, and .metadata attributes
            - Results can be easily compared across different experiment runs in the UI
            - The dataset_run_url provides direct access to detailed results and analysis
            - Failed items are handled gracefully and logged without stopping the experiment
            - This method works in both sync and async contexts (Jupyter notebooks, web apps, etc.)
            - Async execution is handled automatically with smart event loop detection
        """
        langfuse_client = self._get_langfuse_client()
        if not langfuse_client:
            raise ValueError("No Langfuse client available. Dataset items are empty.")

        return langfuse_client.run_experiment(
            name=name,
            run_name=run_name,
            description=description,
            data=self.items,
            task=task,
            evaluators=evaluators,
            composite_evaluator=composite_evaluator,
            run_evaluators=run_evaluators,
            max_concurrency=max_concurrency,
            metadata=metadata,
            _dataset_version=self.version,
        )
