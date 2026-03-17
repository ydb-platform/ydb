"""Batch evaluation functionality for Langfuse.

This module provides comprehensive batch evaluation capabilities for running evaluations
on traces and observations fetched from Langfuse. It includes type definitions,
protocols, result classes, and the implementation for large-scale evaluation workflows
with error handling, retry logic, and resume capability.
"""

import asyncio
import json
import logging
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Dict,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
    cast,
)

from langfuse.api.resources.commons.types import (
    ObservationsView,
    TraceWithFullDetails,
)
from langfuse.experiment import Evaluation, EvaluatorFunction

if TYPE_CHECKING:
    from langfuse._client.client import Langfuse

logger = logging.getLogger("langfuse")


class EvaluatorInputs:
    """Input data structure for evaluators, returned by mapper functions.

    This class provides a strongly-typed container for transforming API response
    objects (traces, observations) into the standardized format expected
    by evaluator functions. It ensures consistent access to input, output, expected
    output, and metadata regardless of the source entity type.

    Attributes:
        input: The input data that was provided to generate the output being evaluated.
            For traces, this might be the initial prompt or request. For observations,
            this could be the span's input. The exact meaning depends on your use case.
        output: The actual output that was produced and needs to be evaluated.
            For traces, this is typically the final response. For observations,
            this might be the generation output or span result.
        expected_output: Optional ground truth or expected result for comparison.
            Used by evaluators to assess correctness. May be None if no ground truth
            is available for the entity being evaluated.
        metadata: Optional structured metadata providing additional context for evaluation.
            Can include information about the entity, execution context, user attributes,
            or any other relevant data that evaluators might use.

    Examples:
        Simple mapper for traces:
        ```python
        from langfuse import EvaluatorInputs

        def trace_mapper(trace):
            return EvaluatorInputs(
                input=trace.input,
                output=trace.output,
                expected_output=None,  # No ground truth available
                metadata={"user_id": trace.user_id, "tags": trace.tags}
            )
        ```

        Mapper for observations extracting specific fields:
        ```python
        def observation_mapper(observation):
            # Extract input/output from observation's data
            input_data = observation.input if hasattr(observation, 'input') else None
            output_data = observation.output if hasattr(observation, 'output') else None

            return EvaluatorInputs(
                input=input_data,
                output=output_data,
                expected_output=None,
                metadata={
                    "observation_type": observation.type,
                    "model": observation.model,
                    "latency_ms": observation.end_time - observation.start_time
                }
            )
        ```
        ```

    Note:
        All arguments must be passed as keywords when instantiating this class.
    """

    def __init__(
        self,
        *,
        input: Any,
        output: Any,
        expected_output: Any = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Initialize EvaluatorInputs with the provided data.

        Args:
            input: The input data for evaluation.
            output: The output data to be evaluated.
            expected_output: Optional ground truth for comparison.
            metadata: Optional additional context for evaluation.

        Note:
            All arguments must be provided as keywords.
        """
        self.input = input
        self.output = output
        self.expected_output = expected_output
        self.metadata = metadata


class MapperFunction(Protocol):
    """Protocol defining the interface for mapper functions in batch evaluation.

    Mapper functions transform API response objects (traces or observations)
    into the standardized EvaluatorInputs format that evaluators expect. This abstraction
    allows you to define how to extract and structure evaluation data from different
    entity types.

    Mapper functions must:
    - Accept a single item parameter (trace, observation)
    - Return an EvaluatorInputs instance with input, output, expected_output, metadata
    - Can be either synchronous or asynchronous
    - Should handle missing or malformed data gracefully
    """

    def __call__(
        self,
        *,
        item: Union["TraceWithFullDetails", "ObservationsView"],
        **kwargs: Dict[str, Any],
    ) -> Union[EvaluatorInputs, Awaitable[EvaluatorInputs]]:
        """Transform an API response object into evaluator inputs.

        This method defines how to extract evaluation-relevant data from the raw
        API response object. The implementation should map entity-specific fields
        to the standardized input/output/expected_output/metadata structure.

        Args:
            item: The API response object to transform. The type depends on the scope:
                - TraceWithFullDetails: When evaluating traces
                - ObservationsView: When evaluating observations

        Returns:
            EvaluatorInputs: A structured container with:
                - input: The input data that generated the output
                - output: The output to be evaluated
                - expected_output: Optional ground truth for comparison
                - metadata: Optional additional context

            Can return either a direct EvaluatorInputs instance or an awaitable
            (for async mappers that need to fetch additional data).

        Examples:
            Basic trace mapper:
            ```python
            def map_trace(trace):
                return EvaluatorInputs(
                    input=trace.input,
                    output=trace.output,
                    expected_output=None,
                    metadata={"trace_id": trace.id, "user": trace.user_id}
                )
            ```

            Observation mapper with conditional logic:
            ```python
            def map_observation(observation):
                # Extract fields based on observation type
                if observation.type == "GENERATION":
                    input_data = observation.input
                    output_data = observation.output
                else:
                    # For other types, use different fields
                    input_data = observation.metadata.get("input")
                    output_data = observation.metadata.get("output")

                return EvaluatorInputs(
                    input=input_data,
                    output=output_data,
                    expected_output=None,
                    metadata={"obs_id": observation.id, "type": observation.type}
                )
            ```

            Async mapper (if additional processing needed):
            ```python
            async def map_trace_async(trace):
                # Could do async processing here if needed
                processed_output = await some_async_transformation(trace.output)

                return EvaluatorInputs(
                    input=trace.input,
                    output=processed_output,
                    expected_output=None,
                    metadata={"trace_id": trace.id}
                )
            ```
        """
        ...


class CompositeEvaluatorFunction(Protocol):
    """Protocol defining the interface for composite evaluator functions.

    Composite evaluators create aggregate scores from multiple item-level evaluations.
    This is commonly used to compute weighted averages, combined metrics, or other
    composite assessments based on individual evaluation results.

    Composite evaluators:
    - Accept the same inputs as item-level evaluators (input, output, expected_output, metadata)
      plus the list of evaluations
    - Return either a single Evaluation, a list of Evaluations, or a dict
    - Can be either synchronous or asynchronous
    - Have access to both raw item data and evaluation results
    """

    def __call__(
        self,
        *,
        input: Optional[Any] = None,
        output: Optional[Any] = None,
        expected_output: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
        evaluations: List[Evaluation],
        **kwargs: Dict[str, Any],
    ) -> Union[
        Evaluation,
        List[Evaluation],
        Dict[str, Any],
        Awaitable[Evaluation],
        Awaitable[List[Evaluation]],
        Awaitable[Dict[str, Any]],
    ]:
        r"""Create a composite evaluation from item-level evaluation results.

        This method combines multiple evaluation scores into a single composite metric.
        Common use cases include weighted averages, pass/fail decisions based on multiple
        criteria, or custom scoring logic that considers multiple dimensions.

        Args:
            input: The input data that was provided to the system being evaluated.
            output: The output generated by the system being evaluated.
            expected_output: The expected/reference output for comparison (if available).
            metadata: Additional metadata about the evaluation context.
            evaluations: List of evaluation results from item-level evaluators.
                Each evaluation contains name, value, comment, and metadata.

        Returns:
            Can return any of:
            - Evaluation: A single composite evaluation result
            - List[Evaluation]: Multiple composite evaluations
            - Dict: A dict that will be converted to an Evaluation
                - name: Identifier for the composite metric (e.g., "composite_score")
                - value: The computed composite value
                - comment: Optional explanation of how the score was computed
                - metadata: Optional details about the composition logic

            Can return either a direct Evaluation instance or an awaitable
            (for async composite evaluators).

        Examples:
            Simple weighted average:
            ```python
            def weighted_composite(*, input, output, expected_output, metadata, evaluations):
                weights = {
                    "accuracy": 0.5,
                    "relevance": 0.3,
                    "safety": 0.2
                }

                total_score = 0.0
                total_weight = 0.0

                for eval in evaluations:
                    if eval.name in weights and isinstance(eval.value, (int, float)):
                        total_score += eval.value * weights[eval.name]
                        total_weight += weights[eval.name]

                final_score = total_score / total_weight if total_weight > 0 else 0.0

                return Evaluation(
                    name="composite_score",
                    value=final_score,
                    comment=f"Weighted average of {len(evaluations)} metrics"
                )
            ```

            Pass/fail composite based on thresholds:
            ```python
            def pass_fail_composite(*, input, output, expected_output, metadata, evaluations):
                # Must pass all criteria
                thresholds = {
                    "accuracy": 0.7,
                    "safety": 0.9,
                    "relevance": 0.6
                }

                passes = True
                failing_metrics = []

                for metric, threshold in thresholds.items():
                    eval_result = next((e for e in evaluations if e.name == metric), None)
                    if eval_result and isinstance(eval_result.value, (int, float)):
                        if eval_result.value < threshold:
                            passes = False
                            failing_metrics.append(metric)

                return Evaluation(
                    name="passes_all_checks",
                    value=passes,
                    comment=f"Failed: {', '.join(failing_metrics)}" if failing_metrics else "All checks passed",
                    data_type="BOOLEAN"
                )
            ```

            Async composite with external scoring:
            ```python
            async def llm_composite(*, input, output, expected_output, metadata, evaluations):
                # Use LLM to synthesize multiple evaluation results
                eval_summary = "\n".join(
                    f"- {e.name}: {e.value}" for e in evaluations
                )

                prompt = f"Given these evaluation scores:\n{eval_summary}\n"
                prompt += f"For the output: {output}\n"
                prompt += "Provide an overall quality score from 0-1."

                response = await openai.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}]
                )

                score = float(response.choices[0].message.content.strip())

                return Evaluation(
                    name="llm_composite_score",
                    value=score,
                    comment="LLM-synthesized composite score"
                )
            ```

            Context-aware composite:
            ```python
            def context_composite(*, input, output, expected_output, metadata, evaluations):
                # Adjust weighting based on metadata
                base_weights = {"accuracy": 0.5, "speed": 0.3, "cost": 0.2}

                # If metadata indicates high importance, prioritize accuracy
                if metadata and metadata.get('importance') == 'high':
                    weights = {"accuracy": 0.7, "speed": 0.2, "cost": 0.1}
                else:
                    weights = base_weights

                total = sum(
                    e.value * weights.get(e.name, 0)
                    for e in evaluations
                    if isinstance(e.value, (int, float))
                )

                return Evaluation(
                    name="weighted_composite",
                    value=total,
                    comment="Context-aware weighted composite"
                )
            ```
        """
        ...


class EvaluatorStats:
    """Statistics for a single evaluator's performance during batch evaluation.

    This class tracks detailed metrics about how a specific evaluator performed
    across all items in a batch evaluation run. It helps identify evaluator issues,
    understand reliability, and optimize evaluation pipelines.

    Attributes:
        name: The name of the evaluator function (extracted from __name__).
        total_runs: Total number of times the evaluator was invoked.
        successful_runs: Number of times the evaluator completed successfully.
        failed_runs: Number of times the evaluator raised an exception or failed.
        total_scores_created: Total number of evaluation scores created by this evaluator.
            Can be higher than successful_runs if the evaluator returns multiple scores.

    Examples:
        Accessing evaluator stats from batch evaluation result:
        ```python
        result = client.run_batched_evaluation(...)

        for stats in result.evaluator_stats:
            print(f"Evaluator: {stats.name}")
            print(f"  Success rate: {stats.successful_runs / stats.total_runs:.1%}")
            print(f"  Scores created: {stats.total_scores_created}")

            if stats.failed_runs > 0:
                print(f"  ⚠️  Failed {stats.failed_runs} times")
        ```

        Identifying problematic evaluators:
        ```python
        result = client.run_batched_evaluation(...)

        # Find evaluators with high failure rates
        for stats in result.evaluator_stats:
            failure_rate = stats.failed_runs / stats.total_runs
            if failure_rate > 0.1:  # More than 10% failures
                print(f"⚠️  {stats.name} has {failure_rate:.1%} failure rate")
                print(f"    Consider debugging or removing this evaluator")
        ```

    Note:
        All arguments must be passed as keywords when instantiating this class.
    """

    def __init__(
        self,
        *,
        name: str,
        total_runs: int = 0,
        successful_runs: int = 0,
        failed_runs: int = 0,
        total_scores_created: int = 0,
    ):
        """Initialize EvaluatorStats with the provided metrics.

        Args:
            name: The evaluator function name.
            total_runs: Total number of evaluator invocations.
            successful_runs: Number of successful completions.
            failed_runs: Number of failures.
            total_scores_created: Total scores created by this evaluator.

        Note:
            All arguments must be provided as keywords.
        """
        self.name = name
        self.total_runs = total_runs
        self.successful_runs = successful_runs
        self.failed_runs = failed_runs
        self.total_scores_created = total_scores_created


class BatchEvaluationResumeToken:
    """Token for resuming a failed batch evaluation run.

    This class encapsulates all the information needed to resume a batch evaluation
    that was interrupted or failed partway through. It uses timestamp-based filtering
    to avoid re-processing items that were already evaluated, even if the underlying
    dataset changed between runs.

    Attributes:
        scope: The type of items being evaluated ("traces", "observations").
        filter: The original JSON filter string used to query items.
        last_processed_timestamp: ISO 8601 timestamp of the last successfully processed item.
            Used to construct a filter that only fetches items after this timestamp.
        last_processed_id: The ID of the last successfully processed item, for reference.
        items_processed: Count of items successfully processed before interruption.

    Examples:
        Resuming a failed batch evaluation:
        ```python
        # Initial run that fails partway through
        try:
            result = client.run_batched_evaluation(
                scope="traces",
                mapper=my_mapper,
                evaluators=[evaluator1, evaluator2],
                filter='{"tags": ["production"]}',
                max_items=10000
            )
        except Exception as e:
            print(f"Evaluation failed: {e}")

            # Save the resume token
            if result.resume_token:
                # Store resume token for later (e.g., in a file or database)
                import json
                with open("resume_token.json", "w") as f:
                    json.dump({
                        "scope": result.resume_token.scope,
                        "filter": result.resume_token.filter,
                        "last_timestamp": result.resume_token.last_processed_timestamp,
                        "last_id": result.resume_token.last_processed_id,
                        "items_done": result.resume_token.items_processed
                    }, f)

        # Later, resume from where it left off
        with open("resume_token.json") as f:
            token_data = json.load(f)

        resume_token = BatchEvaluationResumeToken(
            scope=token_data["scope"],
            filter=token_data["filter"],
            last_processed_timestamp=token_data["last_timestamp"],
            last_processed_id=token_data["last_id"],
            items_processed=token_data["items_done"]
        )

        # Resume the evaluation
        result = client.run_batched_evaluation(
            scope="traces",
            mapper=my_mapper,
            evaluators=[evaluator1, evaluator2],
            resume_from=resume_token
        )

        print(f"Processed {result.total_items_processed} additional items")
        ```

        Handling partial completion:
        ```python
        result = client.run_batched_evaluation(...)

        if not result.completed:
            print(f"Evaluation incomplete. Processed {result.resume_token.items_processed} items")
            print(f"Last item: {result.resume_token.last_processed_id}")
            print(f"Resume from: {result.resume_token.last_processed_timestamp}")

            # Optionally retry automatically
            if result.resume_token:
                print("Retrying...")
                result = client.run_batched_evaluation(
                    scope=result.resume_token.scope,
                    mapper=my_mapper,
                    evaluators=my_evaluators,
                    resume_from=result.resume_token
                )
        ```

    Note:
        All arguments must be passed as keywords when instantiating this class.
        The timestamp-based approach means that items created after the initial run
        but before the timestamp will be skipped. This is intentional to avoid
        duplicates and ensure consistent evaluation.
    """

    def __init__(
        self,
        *,
        scope: str,
        filter: Optional[str],
        last_processed_timestamp: str,
        last_processed_id: str,
        items_processed: int,
    ):
        """Initialize BatchEvaluationResumeToken with the provided state.

        Args:
            scope: The scope type ("traces", "observations").
            filter: The original JSON filter string.
            last_processed_timestamp: ISO 8601 timestamp of last processed item.
            last_processed_id: ID of last processed item.
            items_processed: Count of items processed before interruption.

        Note:
            All arguments must be provided as keywords.
        """
        self.scope = scope
        self.filter = filter
        self.last_processed_timestamp = last_processed_timestamp
        self.last_processed_id = last_processed_id
        self.items_processed = items_processed


class BatchEvaluationResult:
    r"""Complete result structure for batch evaluation execution.

    This class encapsulates comprehensive statistics and metadata about a batch
    evaluation run, including counts, evaluator-specific metrics, timing information,
    error details, and resume capability.

    Attributes:
        total_items_fetched: Total number of items fetched from the API.
        total_items_processed: Number of items successfully evaluated.
        total_items_failed: Number of items that failed during evaluation.
        total_scores_created: Total scores created by all item-level evaluators.
        total_composite_scores_created: Scores created by the composite evaluator.
        total_evaluations_failed: Number of individual evaluator failures across all items.
        evaluator_stats: List of per-evaluator statistics (success/failure rates, scores created).
        resume_token: Token for resuming if evaluation was interrupted (None if completed).
        completed: True if all items were processed, False if stopped early or failed.
        duration_seconds: Total time taken to execute the batch evaluation.
        failed_item_ids: List of IDs for items that failed evaluation.
        error_summary: Dictionary mapping error types to occurrence counts.
        has_more_items: True if max_items limit was reached but more items exist.
        item_evaluations: Dictionary mapping item IDs to their evaluation results (both regular and composite).

    Examples:
        Basic result inspection:
        ```python
        result = client.run_batched_evaluation(...)

        print(f"Processed: {result.total_items_processed}/{result.total_items_fetched}")
        print(f"Scores created: {result.total_scores_created}")
        print(f"Duration: {result.duration_seconds:.2f}s")
        print(f"Success rate: {result.total_items_processed / result.total_items_fetched:.1%}")
        ```

        Detailed analysis with evaluator stats:
        ```python
        result = client.run_batched_evaluation(...)

        print(f"\n📊 Batch Evaluation Results")
        print(f"{'='*50}")
        print(f"Items processed: {result.total_items_processed}")
        print(f"Items failed: {result.total_items_failed}")
        print(f"Scores created: {result.total_scores_created}")

        if result.total_composite_scores_created > 0:
            print(f"Composite scores: {result.total_composite_scores_created}")

        print(f"\n📈 Evaluator Performance:")
        for stats in result.evaluator_stats:
            success_rate = stats.successful_runs / stats.total_runs if stats.total_runs > 0 else 0
            print(f"\n  {stats.name}:")
            print(f"    Success rate: {success_rate:.1%}")
            print(f"    Scores created: {stats.total_scores_created}")
            if stats.failed_runs > 0:
                print(f"    ⚠️  Failures: {stats.failed_runs}")

        if result.error_summary:
            print(f"\n⚠️  Errors encountered:")
            for error_type, count in result.error_summary.items():
                print(f"    {error_type}: {count}")
        ```

        Handling incomplete runs:
        ```python
        result = client.run_batched_evaluation(...)

        if not result.completed:
            print("⚠️  Evaluation incomplete!")

            if result.resume_token:
                print(f"Processed {result.resume_token.items_processed} items before failure")
                print(f"Use resume_from parameter to continue from:")
                print(f"  Timestamp: {result.resume_token.last_processed_timestamp}")
                print(f"  Last ID: {result.resume_token.last_processed_id}")

        if result.has_more_items:
            print(f"ℹ️  More items available beyond max_items limit")
        ```

        Performance monitoring:
        ```python
        result = client.run_batched_evaluation(...)

        items_per_second = result.total_items_processed / result.duration_seconds
        avg_scores_per_item = result.total_scores_created / result.total_items_processed

        print(f"Performance metrics:")
        print(f"  Throughput: {items_per_second:.2f} items/second")
        print(f"  Avg scores/item: {avg_scores_per_item:.2f}")
        print(f"  Total duration: {result.duration_seconds:.2f}s")

        if result.total_evaluations_failed > 0:
            failure_rate = result.total_evaluations_failed / (
                result.total_items_processed * len(result.evaluator_stats)
            )
            print(f"  Evaluation failure rate: {failure_rate:.1%}")
        ```

    Note:
        All arguments must be passed as keywords when instantiating this class.
    """

    def __init__(
        self,
        *,
        total_items_fetched: int,
        total_items_processed: int,
        total_items_failed: int,
        total_scores_created: int,
        total_composite_scores_created: int,
        total_evaluations_failed: int,
        evaluator_stats: List[EvaluatorStats],
        resume_token: Optional[BatchEvaluationResumeToken],
        completed: bool,
        duration_seconds: float,
        failed_item_ids: List[str],
        error_summary: Dict[str, int],
        has_more_items: bool,
        item_evaluations: Dict[str, List["Evaluation"]],
    ):
        """Initialize BatchEvaluationResult with comprehensive statistics.

        Args:
            total_items_fetched: Total items fetched from API.
            total_items_processed: Items successfully evaluated.
            total_items_failed: Items that failed evaluation.
            total_scores_created: Scores from item-level evaluators.
            total_composite_scores_created: Scores from composite evaluator.
            total_evaluations_failed: Individual evaluator failures.
            evaluator_stats: Per-evaluator statistics.
            resume_token: Token for resuming (None if completed).
            completed: Whether all items were processed.
            duration_seconds: Total execution time.
            failed_item_ids: IDs of failed items.
            error_summary: Error types and counts.
            has_more_items: Whether more items exist beyond max_items.
            item_evaluations: Dictionary mapping item IDs to their evaluation results.

        Note:
            All arguments must be provided as keywords.
        """
        self.total_items_fetched = total_items_fetched
        self.total_items_processed = total_items_processed
        self.total_items_failed = total_items_failed
        self.total_scores_created = total_scores_created
        self.total_composite_scores_created = total_composite_scores_created
        self.total_evaluations_failed = total_evaluations_failed
        self.evaluator_stats = evaluator_stats
        self.resume_token = resume_token
        self.completed = completed
        self.duration_seconds = duration_seconds
        self.failed_item_ids = failed_item_ids
        self.error_summary = error_summary
        self.has_more_items = has_more_items
        self.item_evaluations = item_evaluations

    def __str__(self) -> str:
        """Return a formatted string representation of the batch evaluation results.

        Returns:
            A multi-line string with a summary of the evaluation results.
        """
        lines = []
        lines.append("=" * 60)
        lines.append("Batch Evaluation Results")
        lines.append("=" * 60)

        # Summary statistics
        lines.append(f"\nStatus: {'Completed' if self.completed else 'Incomplete'}")
        lines.append(f"Duration: {self.duration_seconds:.2f}s")
        lines.append(f"\nItems fetched: {self.total_items_fetched}")
        lines.append(f"Items processed: {self.total_items_processed}")

        if self.total_items_failed > 0:
            lines.append(f"Items failed: {self.total_items_failed}")

        # Success rate
        if self.total_items_fetched > 0:
            success_rate = self.total_items_processed / self.total_items_fetched * 100
            lines.append(f"Success rate: {success_rate:.1f}%")

        # Scores created
        lines.append(f"\nScores created: {self.total_scores_created}")
        if self.total_composite_scores_created > 0:
            lines.append(f"Composite scores: {self.total_composite_scores_created}")

        total_scores = self.total_scores_created + self.total_composite_scores_created
        lines.append(f"Total scores: {total_scores}")

        # Evaluator statistics
        if self.evaluator_stats:
            lines.append("\nEvaluator Performance:")
            for stats in self.evaluator_stats:
                lines.append(f"  {stats.name}:")
                if stats.total_runs > 0:
                    success_rate = (
                        stats.successful_runs / stats.total_runs * 100
                        if stats.total_runs > 0
                        else 0
                    )
                    lines.append(
                        f"    Runs: {stats.successful_runs}/{stats.total_runs} "
                        f"({success_rate:.1f}% success)"
                    )
                    lines.append(f"    Scores created: {stats.total_scores_created}")
                    if stats.failed_runs > 0:
                        lines.append(f"    Failed runs: {stats.failed_runs}")

        # Performance metrics
        if self.total_items_processed > 0 and self.duration_seconds > 0:
            items_per_sec = self.total_items_processed / self.duration_seconds
            lines.append("\nPerformance:")
            lines.append(f"  Throughput: {items_per_sec:.2f} items/second")
            if self.total_scores_created > 0:
                avg_scores = self.total_scores_created / self.total_items_processed
                lines.append(f"  Avg scores per item: {avg_scores:.2f}")

        # Errors and warnings
        if self.error_summary:
            lines.append("\nErrors encountered:")
            for error_type, count in self.error_summary.items():
                lines.append(f"  {error_type}: {count}")

        # Incomplete run information
        if not self.completed:
            lines.append("\nWarning: Evaluation incomplete")
            if self.resume_token:
                lines.append(
                    f"  Last processed: {self.resume_token.last_processed_timestamp}"
                )
                lines.append(f"  Items processed: {self.resume_token.items_processed}")
                lines.append("  Use resume_from parameter to continue")

        if self.has_more_items:
            lines.append("\nNote: More items available beyond max_items limit")

        lines.append("=" * 60)
        return "\n".join(lines)


class BatchEvaluationRunner:
    """Handles batch evaluation execution for a Langfuse client.

    This class encapsulates all the logic for fetching items, running evaluators,
    creating scores, and managing the evaluation lifecycle. It provides a clean
    separation of concerns from the main Langfuse client class.

    The runner uses a streaming/pipeline approach to process items in batches,
    avoiding loading the entire dataset into memory. This makes it suitable for
    evaluating large numbers of items.

    Attributes:
        client: The Langfuse client instance used for API calls and score creation.
        _log: Logger instance for this runner.
    """

    def __init__(self, client: "Langfuse"):
        """Initialize the batch evaluation runner.

        Args:
            client: The Langfuse client instance.
        """
        self.client = client
        self._log = logger

    async def run_async(
        self,
        *,
        scope: str,
        mapper: MapperFunction,
        evaluators: List[EvaluatorFunction],
        filter: Optional[str] = None,
        fetch_batch_size: int = 50,
        fetch_trace_fields: Optional[str] = None,
        max_items: Optional[int] = None,
        max_concurrency: int = 5,
        composite_evaluator: Optional[CompositeEvaluatorFunction] = None,
        metadata: Optional[Dict[str, Any]] = None,
        _add_observation_scores_to_trace: bool = False,
        _additional_trace_tags: Optional[List[str]] = None,
        max_retries: int = 3,
        verbose: bool = False,
        resume_from: Optional[BatchEvaluationResumeToken] = None,
    ) -> BatchEvaluationResult:
        """Run batch evaluation asynchronously.

        This is the main implementation method that orchestrates the entire batch
        evaluation process: fetching items, mapping, evaluating, creating scores,
        and tracking statistics.

        Args:
            scope: The type of items to evaluate ("traces", "observations").
            mapper: Function to transform API response items to evaluator inputs.
            evaluators: List of evaluation functions to run on each item.
            filter: JSON filter string for querying items.
            fetch_batch_size: Number of items to fetch per API call.
            fetch_trace_fields: Comma-separated list of fields to include when fetching traces. Available field groups: 'core' (always included), 'io' (input, output, metadata), 'scores', 'observations', 'metrics'. If not specified, all fields are returned. Example: 'core,scores,metrics'. Note: Excluded 'observations' or 'scores' fields return empty arrays; excluded 'metrics' returns -1 for 'totalCost' and 'latency'. Only relevant if scope is 'traces'.
            max_items: Maximum number of items to process (None = all).
            max_concurrency: Maximum number of concurrent evaluations.
            composite_evaluator: Optional function to create composite scores.
            metadata: Metadata to add to all created scores.
            _add_observation_scores_to_trace: Private option to duplicate
                observation-level scores onto the parent trace.
            _additional_trace_tags: Private option to add tags on traces via
                ingestion trace-create events.
            max_retries: Maximum retries for failed batch fetches.
            verbose: If True, log progress to console.
            resume_from: Resume token from a previous failed run.

        Returns:
            BatchEvaluationResult with comprehensive statistics.
        """
        start_time = time.time()

        # Initialize tracking variables
        total_items_fetched = 0
        total_items_processed = 0
        total_items_failed = 0
        total_scores_created = 0
        total_composite_scores_created = 0
        total_evaluations_failed = 0
        failed_item_ids: List[str] = []
        error_summary: Dict[str, int] = {}
        item_evaluations: Dict[str, List[Evaluation]] = {}

        # Initialize evaluator stats
        evaluator_stats_dict = {
            getattr(evaluator, "__name__", "unknown_evaluator"): EvaluatorStats(
                name=getattr(evaluator, "__name__", "unknown_evaluator")
            )
            for evaluator in evaluators
        }

        # Handle resume token by modifying filter
        effective_filter = self._build_timestamp_filter(filter, resume_from)
        normalized_additional_trace_tags = (
            self._dedupe_tags(_additional_trace_tags)
            if _additional_trace_tags is not None
            else []
        )
        updated_trace_ids: Set[str] = set()

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_concurrency)

        # Pagination state
        page = 1
        has_more = True
        last_item_timestamp: Optional[str] = None
        last_item_id: Optional[str] = None

        if verbose:
            self._log.info(f"Starting batch evaluation on {scope}")
            if scope == "traces" and fetch_trace_fields:
                self._log.info(f"Fetching trace fields: {fetch_trace_fields}")
            if resume_from:
                self._log.info(
                    f"Resuming from {resume_from.last_processed_timestamp} "
                    f"({resume_from.items_processed} items already processed)"
                )

        # Main pagination loop
        while has_more:
            # Check if we've reached max_items
            if max_items is not None and total_items_fetched >= max_items:
                if verbose:
                    self._log.info(f"Reached max_items limit ({max_items})")
                has_more = True  # More items may exist
                break

            # Fetch next batch with retry logic
            try:
                items = await self._fetch_batch_with_retry(
                    scope=scope,
                    filter=effective_filter,
                    page=page,
                    limit=fetch_batch_size,
                    max_retries=max_retries,
                    fields=fetch_trace_fields,
                )
            except Exception as e:
                # Failed after max_retries - create resume token and return
                error_msg = f"Failed to fetch batch after {max_retries} retries"
                self._log.error(f"{error_msg}: {e}")

                resume_token = BatchEvaluationResumeToken(
                    scope=scope,
                    filter=filter,  # Original filter, not modified
                    last_processed_timestamp=last_item_timestamp or "",
                    last_processed_id=last_item_id or "",
                    items_processed=total_items_processed,
                )

                return self._build_result(
                    total_items_fetched=total_items_fetched,
                    total_items_processed=total_items_processed,
                    total_items_failed=total_items_failed,
                    total_scores_created=total_scores_created,
                    total_composite_scores_created=total_composite_scores_created,
                    total_evaluations_failed=total_evaluations_failed,
                    evaluator_stats_dict=evaluator_stats_dict,
                    resume_token=resume_token,
                    completed=False,
                    start_time=start_time,
                    failed_item_ids=failed_item_ids,
                    error_summary=error_summary,
                    has_more_items=has_more,
                    item_evaluations=item_evaluations,
                )

            # Check if we got any items
            if not items:
                has_more = False
                if verbose:
                    self._log.info("No more items to fetch")
                break

            total_items_fetched += len(items)

            if verbose:
                self._log.info(f"Fetched batch {page} ({len(items)} items)")

            # Limit items if max_items would be exceeded
            items_to_process = items
            if max_items is not None:
                remaining_capacity = max_items - total_items_processed
                if len(items) > remaining_capacity:
                    items_to_process = items[:remaining_capacity]
                    if verbose:
                        self._log.info(
                            f"Limiting batch to {len(items_to_process)} items "
                            f"to respect max_items={max_items}"
                        )

            # Process items concurrently
            async def process_item(
                item: Union[TraceWithFullDetails, ObservationsView],
            ) -> Tuple[str, Union[Tuple[int, int, int, List[Evaluation]], Exception]]:
                """Process a single item and return (item_id, result)."""
                async with semaphore:
                    item_id = self._get_item_id(item, scope)
                    try:
                        result = await self._process_batch_evaluation_item(
                            item=item,
                            scope=scope,
                            mapper=mapper,
                            evaluators=evaluators,
                            composite_evaluator=composite_evaluator,
                            metadata=metadata,
                            _add_observation_scores_to_trace=_add_observation_scores_to_trace,
                            evaluator_stats_dict=evaluator_stats_dict,
                        )
                        return (item_id, result)
                    except Exception as e:
                        return (item_id, e)

            # Run all items in batch concurrently
            tasks = [process_item(item) for item in items_to_process]
            results = await asyncio.gather(*tasks)

            # Process results and update statistics
            for item, (item_id, result) in zip(items_to_process, results):
                if isinstance(result, Exception):
                    # Item processing failed
                    total_items_failed += 1
                    failed_item_ids.append(item_id)
                    error_type = type(result).__name__
                    error_summary[error_type] = error_summary.get(error_type, 0) + 1
                    self._log.warning(f"Item {item_id} failed: {result}")
                else:
                    # Item processed successfully
                    total_items_processed += 1
                    scores_created, composite_created, evals_failed, evaluations = (
                        result
                    )
                    total_scores_created += scores_created
                    total_composite_scores_created += composite_created
                    total_evaluations_failed += evals_failed

                    # Store evaluations for this item
                    item_evaluations[item_id] = evaluations

                    if normalized_additional_trace_tags:
                        trace_id = (
                            item_id
                            if scope == "traces"
                            else cast(ObservationsView, item).trace_id
                        )

                        if trace_id and trace_id not in updated_trace_ids:
                            self.client._create_trace_tags_via_ingestion(
                                trace_id=trace_id,
                                tags=normalized_additional_trace_tags,
                            )
                            updated_trace_ids.add(trace_id)

                    # Update last processed tracking
                    last_item_timestamp = self._get_item_timestamp(item, scope)
                    last_item_id = item_id

            if verbose:
                if max_items is not None and max_items > 0:
                    progress_pct = total_items_processed / max_items * 100
                    self._log.info(
                        f"Progress: {total_items_processed}/{max_items} items "
                        f"({progress_pct:.1f}%), {total_scores_created} scores created"
                    )
                else:
                    self._log.info(
                        f"Progress: {total_items_processed} items processed, "
                        f"{total_scores_created} scores created"
                    )

            # Check if we should continue to next page
            if len(items) < fetch_batch_size:
                # Last page - no more items available
                has_more = False
            else:
                page += 1

                # Check max_items again before next fetch
                if max_items is not None and total_items_fetched >= max_items:
                    has_more = True  # More items exist but we're stopping
                    break

        # Flush all scores to Langfuse
        if verbose:
            self._log.info("Flushing scores to Langfuse...")
        self.client.flush()

        # Build final result
        duration = time.time() - start_time

        if verbose:
            self._log.info(
                f"Batch evaluation complete: {total_items_processed} items processed "
                f"in {duration:.2f}s"
            )

        # Completed successfully if we either:
        # 1. Ran out of items (has_more is False), OR
        # 2. Hit max_items limit (intentionally stopped)
        completed_successfully = not has_more or (
            max_items is not None and total_items_fetched >= max_items
        )

        return self._build_result(
            total_items_fetched=total_items_fetched,
            total_items_processed=total_items_processed,
            total_items_failed=total_items_failed,
            total_scores_created=total_scores_created,
            total_composite_scores_created=total_composite_scores_created,
            total_evaluations_failed=total_evaluations_failed,
            evaluator_stats_dict=evaluator_stats_dict,
            resume_token=None,  # No resume needed on successful completion
            completed=completed_successfully,
            start_time=start_time,
            failed_item_ids=failed_item_ids,
            error_summary=error_summary,
            has_more_items=(
                has_more and max_items is not None and total_items_fetched >= max_items
            ),
            item_evaluations=item_evaluations,
        )

    async def _fetch_batch_with_retry(
        self,
        *,
        scope: str,
        filter: Optional[str],
        page: int,
        limit: int,
        max_retries: int,
        fields: Optional[str],
    ) -> List[Union[TraceWithFullDetails, ObservationsView]]:
        """Fetch a batch of items with retry logic.

        Args:
            scope: The type of items ("traces", "observations").
            filter: JSON filter string for querying.
            page: Page number (1-indexed).
            limit: Number of items per page.
            max_retries: Maximum number of retry attempts.
            verbose: Whether to log retry attempts.
            fields: Trace fields to fetch

        Returns:
            List of items from the API.

        Raises:
            Exception: If all retry attempts fail.
        """
        if scope == "traces":
            response = self.client.api.trace.list(
                page=page,
                limit=limit,
                filter=filter,
                request_options={"max_retries": max_retries},
                fields=fields,
            )  # type: ignore
            return list(response.data)  # type: ignore
        elif scope == "observations":
            response = self.client.api.observations.get_many(
                page=page,
                limit=limit,
                filter=filter,
                request_options={"max_retries": max_retries},
            )  # type: ignore
            return list(response.data)  # type: ignore
        else:
            error_message = f"Invalid scope: {scope}"
            raise ValueError(error_message)

    async def _process_batch_evaluation_item(
        self,
        item: Union[TraceWithFullDetails, ObservationsView],
        scope: str,
        mapper: MapperFunction,
        evaluators: List[EvaluatorFunction],
        composite_evaluator: Optional[CompositeEvaluatorFunction],
        metadata: Optional[Dict[str, Any]],
        _add_observation_scores_to_trace: bool,
        evaluator_stats_dict: Dict[str, EvaluatorStats],
    ) -> Tuple[int, int, int, List[Evaluation]]:
        """Process a single item: map, evaluate, create scores.

        Args:
            item: The API response object to evaluate.
            scope: The type of item ("traces", "observations").
            mapper: Function to transform item to evaluator inputs.
            evaluators: List of evaluator functions.
            composite_evaluator: Optional composite evaluator function.
            metadata: Additional metadata to add to scores.
            _add_observation_scores_to_trace: Whether to duplicate
                observation-level scores at trace level.
            evaluator_stats_dict: Dictionary tracking evaluator statistics.

        Returns:
            Tuple of (scores_created, composite_scores_created, evaluations_failed, all_evaluations).

        Raises:
            Exception: If mapping fails or item processing encounters fatal error.
        """
        scores_created = 0
        composite_scores_created = 0
        evaluations_failed = 0

        # Run mapper to transform item
        evaluator_inputs = await self._run_mapper(mapper, item)

        # Run all evaluators
        evaluations: List[Evaluation] = []
        for evaluator in evaluators:
            evaluator_name = getattr(evaluator, "__name__", "unknown_evaluator")
            stats = evaluator_stats_dict[evaluator_name]
            stats.total_runs += 1

            try:
                eval_results = await self._run_evaluator_internal(
                    evaluator,
                    input=evaluator_inputs.input,
                    output=evaluator_inputs.output,
                    expected_output=evaluator_inputs.expected_output,
                    metadata=evaluator_inputs.metadata,
                )

                stats.successful_runs += 1
                stats.total_scores_created += len(eval_results)
                evaluations.extend(eval_results)

            except Exception as e:
                # Evaluator failed - log warning and continue with other evaluators
                stats.failed_runs += 1
                evaluations_failed += 1
                self._log.warning(
                    f"Evaluator {evaluator_name} failed on item "
                    f"{self._get_item_id(item, scope)}: {e}"
                )

        # Create scores for item-level evaluations
        item_id = self._get_item_id(item, scope)
        for evaluation in evaluations:
            scores_created += self._create_score_for_scope(
                scope=scope,
                item_id=item_id,
                trace_id=cast(ObservationsView, item).trace_id
                if scope == "observations"
                else None,
                evaluation=evaluation,
                additional_metadata=metadata,
                add_observation_score_to_trace=_add_observation_scores_to_trace,
            )

        # Run composite evaluator if provided and we have evaluations
        if composite_evaluator and evaluations:
            try:
                composite_evals = await self._run_composite_evaluator(
                    composite_evaluator,
                    input=evaluator_inputs.input,
                    output=evaluator_inputs.output,
                    expected_output=evaluator_inputs.expected_output,
                    metadata=evaluator_inputs.metadata,
                    evaluations=evaluations,
                )

                # Create scores for all composite evaluations
                for composite_eval in composite_evals:
                    composite_scores_created += self._create_score_for_scope(
                        scope=scope,
                        item_id=item_id,
                        trace_id=cast(ObservationsView, item).trace_id
                        if scope == "observations"
                        else None,
                        evaluation=composite_eval,
                        additional_metadata=metadata,
                        add_observation_score_to_trace=_add_observation_scores_to_trace,
                    )

                # Add composite evaluations to the list
                evaluations.extend(composite_evals)

            except Exception as e:
                self._log.warning(f"Composite evaluator failed on item {item_id}: {e}")

        return (
            scores_created,
            composite_scores_created,
            evaluations_failed,
            evaluations,
        )

    async def _run_evaluator_internal(
        self,
        evaluator: EvaluatorFunction,
        **kwargs: Any,
    ) -> List[Evaluation]:
        """Run an evaluator function and normalize the result.

        Unlike experiment._run_evaluator, this version raises exceptions
        so we can track failures in our statistics.

        Args:
            evaluator: The evaluator function to run.
            **kwargs: Arguments to pass to the evaluator.

        Returns:
            List of Evaluation objects.

        Raises:
            Exception: If evaluator raises an exception (not caught).
        """
        result = evaluator(**kwargs)

        # Handle async evaluators
        if asyncio.iscoroutine(result):
            result = await result

        # Normalize to list
        if isinstance(result, (dict, Evaluation)):
            return [result]  # type: ignore
        elif isinstance(result, list):
            return result
        else:
            return []

    async def _run_mapper(
        self,
        mapper: MapperFunction,
        item: Union[TraceWithFullDetails, ObservationsView],
    ) -> EvaluatorInputs:
        """Run mapper function (handles both sync and async mappers).

        Args:
            mapper: The mapper function to run.
            item: The API response object to map.

        Returns:
            EvaluatorInputs instance.

        Raises:
            Exception: If mapper raises an exception.
        """
        result = mapper(item=item)
        if asyncio.iscoroutine(result):
            return await result  # type: ignore
        return result  # type: ignore

    async def _run_composite_evaluator(
        self,
        composite_evaluator: CompositeEvaluatorFunction,
        input: Optional[Any],
        output: Optional[Any],
        expected_output: Optional[Any],
        metadata: Optional[Dict[str, Any]],
        evaluations: List[Evaluation],
    ) -> List[Evaluation]:
        """Run composite evaluator function (handles both sync and async).

        Args:
            composite_evaluator: The composite evaluator function.
            input: The input data provided to the system.
            output: The output generated by the system.
            expected_output: The expected/reference output.
            metadata: Additional metadata about the evaluation context.
            evaluations: List of item-level evaluations.

        Returns:
            List of Evaluation objects (normalized from single or list return).

        Raises:
            Exception: If composite evaluator raises an exception.
        """
        result = composite_evaluator(
            input=input,
            output=output,
            expected_output=expected_output,
            metadata=metadata,
            evaluations=evaluations,
        )
        if asyncio.iscoroutine(result):
            result = await result

        # Normalize to list (same as regular evaluator)
        if isinstance(result, (dict, Evaluation)):
            return [result]  # type: ignore
        elif isinstance(result, list):
            return result
        else:
            return []

    def _create_score_for_scope(
        self,
        *,
        scope: str,
        item_id: str,
        trace_id: Optional[str] = None,
        evaluation: Evaluation,
        additional_metadata: Optional[Dict[str, Any]],
        add_observation_score_to_trace: bool = False,
    ) -> int:
        """Create a score linked to the appropriate entity based on scope.

        Args:
            scope: The type of entity ("traces", "observations").
            item_id: The ID of the entity.
            trace_id: The trace ID of the entity; required if scope=observations
            evaluation: The evaluation result to create a score from.
            additional_metadata: Additional metadata to merge with evaluation metadata.
            add_observation_score_to_trace: Whether to duplicate observation
                score on parent trace as well.

        Returns:
            Number of score events created.
        """
        # Merge metadata
        score_metadata = {
            **(evaluation.metadata or {}),
            **(additional_metadata or {}),
        }

        if scope == "traces":
            self.client.create_score(
                trace_id=item_id,
                name=evaluation.name,
                value=evaluation.value,  # type: ignore
                comment=evaluation.comment,
                metadata=score_metadata,
                data_type=evaluation.data_type,  # type: ignore[arg-type]
                config_id=evaluation.config_id,
            )
            return 1
        elif scope == "observations":
            self.client.create_score(
                observation_id=item_id,
                trace_id=trace_id,
                name=evaluation.name,
                value=evaluation.value,  # type: ignore
                comment=evaluation.comment,
                metadata=score_metadata,
                data_type=evaluation.data_type,  # type: ignore[arg-type]
                config_id=evaluation.config_id,
            )
            score_count = 1

            if add_observation_score_to_trace and trace_id:
                self.client.create_score(
                    trace_id=trace_id,
                    name=evaluation.name,
                    value=evaluation.value,  # type: ignore
                    comment=evaluation.comment,
                    metadata=score_metadata,
                    data_type=evaluation.data_type,  # type: ignore[arg-type]
                    config_id=evaluation.config_id,
                )
                score_count += 1

            return score_count

        return 0

    def _build_timestamp_filter(
        self,
        original_filter: Optional[str],
        resume_from: Optional[BatchEvaluationResumeToken],
    ) -> Optional[str]:
        """Build filter with timestamp constraint for resume capability.

        Args:
            original_filter: The original JSON filter string.
            resume_from: Optional resume token with timestamp information.

        Returns:
            Modified filter string with timestamp constraint, or original filter.
        """
        if not resume_from:
            return original_filter

        # Parse original filter (should be array) or create empty array
        try:
            filter_list = json.loads(original_filter) if original_filter else []
            if not isinstance(filter_list, list):
                self._log.warning(
                    f"Filter should be a JSON array, got: {type(filter_list).__name__}"
                )
                filter_list = []
        except json.JSONDecodeError:
            self._log.warning(
                f"Invalid JSON in original filter, ignoring: {original_filter}"
            )
            filter_list = []

        # Add timestamp constraint to filter array
        timestamp_field = self._get_timestamp_field_for_scope(resume_from.scope)
        timestamp_filter = {
            "type": "datetime",
            "column": timestamp_field,
            "operator": ">",
            "value": resume_from.last_processed_timestamp,
        }
        filter_list.append(timestamp_filter)

        return json.dumps(filter_list)

    @staticmethod
    def _get_item_id(
        item: Union[TraceWithFullDetails, ObservationsView],
        scope: str,
    ) -> str:
        """Extract ID from item based on scope.

        Args:
            item: The API response object.
            scope: The type of item.

        Returns:
            The item's ID.
        """
        return item.id

    @staticmethod
    def _get_item_timestamp(
        item: Union[TraceWithFullDetails, ObservationsView],
        scope: str,
    ) -> str:
        """Extract timestamp from item based on scope.

        Args:
            item: The API response object.
            scope: The type of item.

        Returns:
            ISO 8601 timestamp string.
        """
        if scope == "traces":
            # Type narrowing for traces
            if hasattr(item, "timestamp"):
                return item.timestamp.isoformat()  # type: ignore[attr-defined]
        elif scope == "observations":
            # Type narrowing for observations
            if hasattr(item, "start_time"):
                return item.start_time.isoformat()  # type: ignore[attr-defined]
        return ""

    @staticmethod
    def _get_timestamp_field_for_scope(scope: str) -> str:
        """Get the timestamp field name for filtering based on scope.

        Args:
            scope: The type of items.

        Returns:
            The field name to use in filters.
        """
        if scope == "traces":
            return "timestamp"
        elif scope == "observations":
            return "start_time"
        return "timestamp"  # Default

    @staticmethod
    def _dedupe_tags(tags: Optional[List[str]]) -> List[str]:
        """Deduplicate tags while preserving order."""
        if tags is None:
            return []

        deduped: List[str] = []
        seen = set()
        for tag in tags:
            if tag not in seen:
                deduped.append(tag)
                seen.add(tag)

        return deduped

    def _build_result(
        self,
        total_items_fetched: int,
        total_items_processed: int,
        total_items_failed: int,
        total_scores_created: int,
        total_composite_scores_created: int,
        total_evaluations_failed: int,
        evaluator_stats_dict: Dict[str, EvaluatorStats],
        resume_token: Optional[BatchEvaluationResumeToken],
        completed: bool,
        start_time: float,
        failed_item_ids: List[str],
        error_summary: Dict[str, int],
        has_more_items: bool,
        item_evaluations: Dict[str, List[Evaluation]],
    ) -> BatchEvaluationResult:
        """Build the final BatchEvaluationResult.

        Args:
            total_items_fetched: Total items fetched.
            total_items_processed: Items successfully processed.
            total_items_failed: Items that failed.
            total_scores_created: Scores from item evaluators.
            total_composite_scores_created: Scores from composite evaluator.
            total_evaluations_failed: Individual evaluator failures.
            evaluator_stats_dict: Per-evaluator statistics.
            resume_token: Resume token if incomplete.
            completed: Whether evaluation completed fully.
            start_time: Start time (unix timestamp).
            failed_item_ids: IDs of failed items.
            error_summary: Error type counts.
            has_more_items: Whether more items exist.
            item_evaluations: Dictionary mapping item IDs to their evaluation results.

        Returns:
            BatchEvaluationResult instance.
        """
        duration = time.time() - start_time

        return BatchEvaluationResult(
            total_items_fetched=total_items_fetched,
            total_items_processed=total_items_processed,
            total_items_failed=total_items_failed,
            total_scores_created=total_scores_created,
            total_composite_scores_created=total_composite_scores_created,
            total_evaluations_failed=total_evaluations_failed,
            evaluator_stats=list(evaluator_stats_dict.values()),
            resume_token=resume_token,
            completed=completed,
            duration_seconds=duration,
            failed_item_ids=failed_item_ids,
            error_summary=error_summary,
            has_more_items=has_more_items,
            item_evaluations=item_evaluations,
        )
