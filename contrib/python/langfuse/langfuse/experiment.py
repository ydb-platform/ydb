"""Langfuse experiment functionality for running and evaluating tasks on datasets.

This module provides the core experiment functionality for the Langfuse Python SDK,
allowing users to run experiments on datasets with automatic tracing, evaluation,
and result formatting.
"""

import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Dict,
    List,
    Optional,
    Protocol,
    TypedDict,
    Union,
)

from langfuse.api import ScoreDataType

if TYPE_CHECKING:
    from langfuse._client.datasets import DatasetItemClient


class LocalExperimentItem(TypedDict, total=False):
    """Structure for local experiment data items (not from Langfuse datasets).

    This TypedDict defines the structure for experiment items when using local data
    rather than Langfuse-hosted datasets. All fields are optional to provide
    flexibility in data structure.

    Attributes:
        input: The input data to pass to the task function. Can be any type that
            your task function can process (string, dict, list, etc.). This is
            typically the prompt, question, or data that your task will operate on.
        expected_output: Optional expected/ground truth output for evaluation purposes.
            Used by evaluators to assess correctness or quality. Can be None if
            no ground truth is available.
        metadata: Optional metadata dictionary containing additional context about
            this specific item. Can include information like difficulty level,
            category, source, or any other relevant attributes that evaluators
            might use for context-aware evaluation.

    Examples:
        Simple text processing item:
        ```python
        item: LocalExperimentItem = {
            "input": "Summarize this article: ...",
            "expected_output": "Expected summary...",
            "metadata": {"difficulty": "medium", "category": "news"}
        }
        ```

        Classification item:
        ```python
        item: LocalExperimentItem = {
            "input": {"text": "This movie is great!", "context": "movie review"},
            "expected_output": "positive",
            "metadata": {"dataset_source": "imdb", "confidence": 0.95}
        }
        ```

        Minimal item with only input:
        ```python
        item: LocalExperimentItem = {
            "input": "What is the capital of France?"
        }
        ```
    """

    input: Any
    expected_output: Any
    metadata: Optional[Dict[str, Any]]


ExperimentItem = Union[LocalExperimentItem, "DatasetItemClient"]
"""Type alias for items that can be processed in experiments.

Can be either:
- LocalExperimentItem: Dict-like items with 'input', 'expected_output', 'metadata' keys
- DatasetItemClient: Items from Langfuse datasets with .input, .expected_output, .metadata attributes
"""

ExperimentData = Union[List[LocalExperimentItem], List["DatasetItemClient"]]
"""Type alias for experiment datasets.

Represents the collection of items to process in an experiment. Can be either:
- List[LocalExperimentItem]: Local data items as dictionaries
- List[DatasetItemClient]: Items from a Langfuse dataset (typically from dataset.items)
"""


class Evaluation:
    """Represents an evaluation result for an experiment item or an entire experiment run.

    This class provides a strongly-typed way to create evaluation results in evaluator functions.
    Users must use keyword arguments when instantiating this class.

    Attributes:
        name: Unique identifier for the evaluation metric. Should be descriptive
            and consistent across runs (e.g., "accuracy", "bleu_score", "toxicity").
            Used for aggregation and comparison across experiment runs.
        value: The evaluation score or result. Can be:
            - Numeric (int/float): For quantitative metrics like accuracy (0.85), BLEU (0.42)
            - String: For categorical results like "positive", "negative", "neutral"
            - Boolean: For binary assessments like "passes_safety_check"
        comment: Optional human-readable explanation of the evaluation result.
            Useful for providing context, explaining scoring rationale, or noting
            special conditions. Displayed in Langfuse UI for interpretability.
        metadata: Optional structured metadata about the evaluation process.
            Can include confidence scores, intermediate calculations, model versions,
            or any other relevant technical details.
        data_type: Optional score data type. Required if value is not NUMERIC.
            One of NUMERIC, CATEGORICAL, or BOOLEAN. Defaults to NUMERIC.
        config_id: Optional Langfuse score config ID.

    Examples:
        Basic accuracy evaluation:
        ```python
        from langfuse import Evaluation

        def accuracy_evaluator(*, input, output, expected_output=None, **kwargs):
            if not expected_output:
                return Evaluation(name="accuracy", value=0, comment="No expected output")

            is_correct = output.strip().lower() == expected_output.strip().lower()
            return Evaluation(
                name="accuracy",
                value=1.0 if is_correct else 0.0,
                comment="Correct answer" if is_correct else "Incorrect answer"
            )
        ```

        Multi-metric evaluator:
        ```python
        def comprehensive_evaluator(*, input, output, expected_output=None, **kwargs):
            return [
                Evaluation(name="length", value=len(output), comment=f"Output length: {len(output)} chars"),
                Evaluation(name="has_greeting", value="hello" in output.lower(), comment="Contains greeting"),
                Evaluation(
                    name="quality",
                    value=0.85,
                    comment="High quality response",
                    metadata={"confidence": 0.92, "model": "gpt-4"}
                )
            ]
        ```

        Categorical evaluation:
        ```python
        def sentiment_evaluator(*, input, output, **kwargs):
            sentiment = analyze_sentiment(output)  # Returns "positive", "negative", or "neutral"
            return Evaluation(
                name="sentiment",
                value=sentiment,
                comment=f"Response expresses {sentiment} sentiment",
                data_type="CATEGORICAL"
            )
        ```

        Failed evaluation with error handling:
        ```python
        def external_api_evaluator(*, input, output, **kwargs):
            try:
                score = external_api.evaluate(output)
                return Evaluation(name="external_score", value=score)
            except Exception as e:
                return Evaluation(
                    name="external_score",
                    value=0,
                    comment=f"API unavailable: {e}",
                    metadata={"error": str(e), "retry_count": 3}
                )
        ```

    Note:
        All arguments must be passed as keywords. Positional arguments are not allowed
        to ensure code clarity and prevent errors from argument reordering.
    """

    def __init__(
        self,
        *,
        name: str,
        value: Union[int, float, str, bool],
        comment: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        data_type: Optional[ScoreDataType] = None,
        config_id: Optional[str] = None,
    ):
        """Initialize an Evaluation with the provided data.

        Args:
            name: Unique identifier for the evaluation metric.
            value: The evaluation score or result.
            comment: Optional human-readable explanation of the result.
            metadata: Optional structured metadata about the evaluation process.
            data_type: Optional score data type (NUMERIC, CATEGORICAL, or BOOLEAN).
            config_id: Optional Langfuse score config ID.

        Note:
            All arguments must be provided as keywords. Positional arguments will raise a TypeError.
        """
        self.name = name
        self.value = value
        self.comment = comment
        self.metadata = metadata
        self.data_type = data_type
        self.config_id = config_id


class ExperimentItemResult:
    """Result structure for individual experiment items.

    This class represents the complete result of processing a single item
    during an experiment run, including the original input, task output,
    evaluations, and tracing information. Users must use keyword arguments when instantiating this class.

    Attributes:
        item: The original experiment item that was processed. Can be either
            a dictionary with 'input', 'expected_output', and 'metadata' keys,
            or a DatasetItemClient from Langfuse datasets.
        output: The actual output produced by the task function for this item.
            Can be any type depending on what your task function returns.
        evaluations: List of evaluation results for this item. Each evaluation
            contains a name, value, optional comment, and optional metadata.
        trace_id: Optional Langfuse trace ID for this item's execution. Used
            to link the experiment result with the detailed trace in Langfuse UI.
        dataset_run_id: Optional dataset run ID if this item was part of a
            Langfuse dataset. None for local experiments.

    Examples:
        Accessing item result data:
        ```python
        result = langfuse.run_experiment(...)
        for item_result in result.item_results:
            print(f"Input: {item_result.item}")
            print(f"Output: {item_result.output}")
            print(f"Trace: {item_result.trace_id}")

            # Access evaluations
            for evaluation in item_result.evaluations:
                print(f"{evaluation.name}: {evaluation.value}")
        ```

        Working with different item types:
        ```python
        # Local experiment item (dict)
        if isinstance(item_result.item, dict):
            input_data = item_result.item["input"]
            expected = item_result.item.get("expected_output")

        # Langfuse dataset item (object with attributes)
        else:
            input_data = item_result.item.input
            expected = item_result.item.expected_output
        ```

    Note:
        All arguments must be passed as keywords. Positional arguments are not allowed
        to ensure code clarity and prevent errors from argument reordering.
    """

    def __init__(
        self,
        *,
        item: ExperimentItem,
        output: Any,
        evaluations: List[Evaluation],
        trace_id: Optional[str],
        dataset_run_id: Optional[str],
    ):
        """Initialize an ExperimentItemResult with the provided data.

        Args:
            item: The original experiment item that was processed.
            output: The actual output produced by the task function for this item.
            evaluations: List of evaluation results for this item.
            trace_id: Optional Langfuse trace ID for this item's execution.
            dataset_run_id: Optional dataset run ID if this item was part of a Langfuse dataset.

        Note:
            All arguments must be provided as keywords. Positional arguments will raise a TypeError.
        """
        self.item = item
        self.output = output
        self.evaluations = evaluations
        self.trace_id = trace_id
        self.dataset_run_id = dataset_run_id


class ExperimentResult:
    """Complete result structure for experiment execution.

    This class encapsulates the complete results of running an experiment on a dataset,
    including individual item results, aggregate run-level evaluations, and metadata
    about the experiment execution.

    Attributes:
        name: The name of the experiment as specified during execution.
        run_name: The name of the current experiment run.
        description: Optional description of the experiment's purpose or methodology.
        item_results: List of results from processing each individual dataset item,
            containing the original item, task output, evaluations, and trace information.
        run_evaluations: List of aggregate evaluation results computed across all items,
            such as average scores, statistical summaries, or cross-item analyses.
        dataset_run_id: Optional ID of the dataset run in Langfuse (when using Langfuse datasets).
        dataset_run_url: Optional direct URL to view the experiment results in Langfuse UI.

    Examples:
        Basic usage with local dataset:
        ```python
        result = langfuse.run_experiment(
            name="Capital Cities Test",
            data=local_data,
            task=generate_capital,
            evaluators=[accuracy_check]
        )

        print(f"Processed {len(result.item_results)} items")
        print(result.format())  # Human-readable summary

        # Access individual results
        for item_result in result.item_results:
            print(f"Input: {item_result.item}")
            print(f"Output: {item_result.output}")
            print(f"Scores: {item_result.evaluations}")
        ```

        Usage with Langfuse datasets:
        ```python
        dataset = langfuse.get_dataset("qa-eval-set")
        result = dataset.run_experiment(
            name="GPT-4 QA Evaluation",
            task=answer_question,
            evaluators=[relevance_check, accuracy_check]
        )

        # View in Langfuse UI
        if result.dataset_run_url:
            print(f"View detailed results: {result.dataset_run_url}")
        ```

        Formatted output:
        ```python
        # Get summary view
        summary = result.format()
        print(summary)

        # Get detailed view with individual items
        detailed = result.format(include_item_results=True)
        with open("experiment_report.txt", "w") as f:
            f.write(detailed)
        ```
    """

    def __init__(
        self,
        *,
        name: str,
        run_name: str,
        description: Optional[str],
        item_results: List[ExperimentItemResult],
        run_evaluations: List[Evaluation],
        dataset_run_id: Optional[str] = None,
        dataset_run_url: Optional[str] = None,
    ):
        """Initialize an ExperimentResult with the provided data.

        Args:
            name: The name of the experiment.
            run_name: The current experiment run name.
            description: Optional description of the experiment.
            item_results: List of results from processing individual dataset items.
            run_evaluations: List of aggregate evaluation results for the entire run.
            dataset_run_id: Optional ID of the dataset run (for Langfuse datasets).
            dataset_run_url: Optional URL to view results in Langfuse UI.
        """
        self.name = name
        self.run_name = run_name
        self.description = description
        self.item_results = item_results
        self.run_evaluations = run_evaluations
        self.dataset_run_id = dataset_run_id
        self.dataset_run_url = dataset_run_url

    def format(self, *, include_item_results: bool = False) -> str:
        r"""Format the experiment result for human-readable display.

        Converts the experiment result into a nicely formatted string suitable for
        console output, logging, or reporting. The output includes experiment overview,
        aggregate statistics, and optionally individual item details.

        This method provides a comprehensive view of experiment performance including:
        - Experiment metadata (name, description, item count)
        - List of evaluation metrics used across items
        - Average scores computed across all processed items
        - Run-level evaluation results (aggregate metrics)
        - Links to view detailed results in Langfuse UI (when available)
        - Individual item details (when requested)

        Args:
            include_item_results: Whether to include detailed results for each individual
                item in the formatted output. When False (default), only shows aggregate
                statistics and summary information. When True, includes input/output/scores
                for every processed item, making the output significantly longer but more
                detailed for debugging and analysis purposes.

        Returns:
            A formatted multi-line string containing:
            - Experiment name and description (if provided)
            - Total number of items successfully processed
            - List of all evaluation metrics that were applied
            - Average scores across all items for each numeric metric
            - Run-level evaluation results with comments
            - Dataset run URL for viewing in Langfuse UI (if applicable)
            - Individual item details including inputs, outputs, and scores (if requested)

        Examples:
            Basic usage showing aggregate results only:
            ```python
            result = langfuse.run_experiment(
                name="Capital Cities",
                data=dataset,
                task=generate_capital,
                evaluators=[accuracy_evaluator]
            )

            print(result.format())
            # Output:
            # ──────────────────────────────────────────────────
            # 📊 Capital Cities
            # 100 items
            # Evaluations:
            #   • accuracy
            # Average Scores:
            #   • accuracy: 0.850
            ```

            Detailed output including all individual item results:
            ```python
            detailed_report = result.format(include_item_results=True)
            print(detailed_report)
            # Output includes each item:
            # 1. Item 1:
            #    Input:    What is the capital of France?
            #    Expected: Paris
            #    Actual:   The capital of France is Paris.
            #    Scores:
            #      • accuracy: 1.000
            #        💭 Correct answer found
            # [... continues for all items ...]
            ```

            Saving formatted results to file for reporting:
            ```python
            with open("experiment_report.txt", "w") as f:
                f.write(result.format(include_item_results=True))

            # Or create summary report
            summary = result.format()  # Aggregate view only
            print(f"Experiment Summary:\\n{summary}")
            ```

            Integration with logging systems:
            ```python
            import logging
            logger = logging.getLogger("experiments")

            # Log summary after experiment
            logger.info(f"Experiment completed:\\n{result.format()}")

            # Log detailed results for failed experiments
            if any(eval['value'] < threshold for eval in result.run_evaluations):
                logger.warning(f"Poor performance detected:\\n{result.format(include_item_results=True)}")
            ```
        """
        if not self.item_results:
            return "No experiment results to display."

        output = ""

        # Individual results section
        if include_item_results:
            for i, result in enumerate(self.item_results):
                output += f"\\n{i + 1}. Item {i + 1}:\\n"

                # Extract and display input
                item_input = None
                if isinstance(result.item, dict):
                    item_input = result.item.get("input")
                elif hasattr(result.item, "input"):
                    item_input = result.item.input

                if item_input is not None:
                    output += f"   Input:    {_format_value(item_input)}\\n"

                # Extract and display expected output
                expected_output = None
                if isinstance(result.item, dict):
                    expected_output = result.item.get("expected_output")
                elif hasattr(result.item, "expected_output"):
                    expected_output = result.item.expected_output

                if expected_output is not None:
                    output += f"   Expected: {_format_value(expected_output)}\\n"
                output += f"   Actual:   {_format_value(result.output)}\\n"

                # Display evaluation scores
                if result.evaluations:
                    output += "   Scores:\\n"
                    for evaluation in result.evaluations:
                        score = evaluation.value
                        if isinstance(score, (int, float)):
                            score = f"{score:.3f}"
                        output += f"     • {evaluation.name}: {score}"
                        if evaluation.comment:
                            output += f"\\n       💭 {evaluation.comment}"
                        output += "\\n"

                # Display trace link if available
                if result.trace_id:
                    output += f"\\n   Trace ID: {result.trace_id}\\n"
        else:
            output += f"Individual Results: Hidden ({len(self.item_results)} items)\\n"
            output += "💡 Set include_item_results=True to view them\\n"

        # Experiment overview section
        output += f"\\n{'─' * 50}\\n"
        output += f"🧪 Experiment: {self.name}"
        output += f"\n📋 Run name: {self.run_name}"
        if self.description:
            output += f" - {self.description}"

        output += f"\\n{len(self.item_results)} items"

        # Collect unique evaluation names across all items
        evaluation_names = set()
        for result in self.item_results:
            for evaluation in result.evaluations:
                evaluation_names.add(evaluation.name)

        if evaluation_names:
            output += "\\nEvaluations:"
            for eval_name in evaluation_names:
                output += f"\\n  • {eval_name}"
            output += "\\n"

        # Calculate and display average scores
        if evaluation_names:
            output += "\\nAverage Scores:"
            for eval_name in evaluation_names:
                scores = []
                for result in self.item_results:
                    for evaluation in result.evaluations:
                        if evaluation.name == eval_name and isinstance(
                            evaluation.value, (int, float)
                        ):
                            scores.append(evaluation.value)

                if scores:
                    avg = sum(scores) / len(scores)
                    output += f"\\n  • {eval_name}: {avg:.3f}"
            output += "\\n"

        # Display run-level evaluations
        if self.run_evaluations:
            output += "\\nRun Evaluations:"
            for run_eval in self.run_evaluations:
                score = run_eval.value
                if isinstance(score, (int, float)):
                    score = f"{score:.3f}"
                output += f"\\n  • {run_eval.name}: {score}"
                if run_eval.comment:
                    output += f"\\n    💭 {run_eval.comment}"
            output += "\\n"

        # Add dataset run URL if available
        if self.dataset_run_url:
            output += f"\\n🔗 Dataset Run:\\n   {self.dataset_run_url}"

        return output


class TaskFunction(Protocol):
    """Protocol defining the interface for experiment task functions.

    Task functions are the core processing functions that operate on each item
    in an experiment dataset. They receive an experiment item as input and
    produce some output that will be evaluated.

    Task functions must:
    - Accept 'item' as a keyword argument
    - Return any type of output (will be passed to evaluators)
    - Can be either synchronous or asynchronous
    - Should handle their own errors gracefully (exceptions will be logged)
    """

    def __call__(
        self,
        *,
        item: ExperimentItem,
        **kwargs: Dict[str, Any],
    ) -> Union[Any, Awaitable[Any]]:
        """Execute the task on an experiment item.

        This method defines the core processing logic for each item in your experiment.
        The implementation should focus on the specific task you want to evaluate,
        such as text generation, classification, summarization, etc.

        Args:
            item: The experiment item to process. Can be either:
                - Dict with keys like 'input', 'expected_output', 'metadata'
                - Langfuse DatasetItem object with .input, .expected_output attributes
            **kwargs: Additional keyword arguments that may be passed by the framework

        Returns:
            Any: The output of processing the item. This output will be:
            - Stored in the experiment results
            - Passed to all item-level evaluators for assessment
            - Traced automatically in Langfuse for observability

            Can return either a direct value or an awaitable (async) result.

        Examples:
            Simple synchronous task:
            ```python
            def my_task(*, item, **kwargs):
                prompt = f"Summarize: {item['input']}"
                return my_llm_client.generate(prompt)
            ```

            Async task with error handling:
            ```python
            async def my_async_task(*, item, **kwargs):
                try:
                    response = await openai_client.chat.completions.create(
                        model="gpt-4",
                        messages=[{"role": "user", "content": item["input"]}]
                    )
                    return response.choices[0].message.content
                except Exception as e:
                    # Log error and return fallback
                    print(f"Task failed for item {item}: {e}")
                    return "Error: Could not process item"
            ```

            Task using dataset item attributes:
            ```python
            def classification_task(*, item, **kwargs):
                # Works with both dict items and DatasetItem objects
                text = item["input"] if isinstance(item, dict) else item.input
                return classify_text(text)
            ```
        """
        ...


class EvaluatorFunction(Protocol):
    """Protocol defining the interface for item-level evaluator functions.

    Item-level evaluators assess the quality, correctness, or other properties
    of individual task outputs. They receive the input, output, expected output,
    and metadata for each item and return evaluation metrics.

    Evaluators should:
    - Accept input, output, expected_output, and metadata as keyword arguments
    - Return Evaluation dict(s) with 'name', 'value', 'comment', 'metadata' fields
    - Be deterministic when possible for reproducible results
    - Handle edge cases gracefully (missing expected output, malformed data, etc.)
    - Can be either synchronous or asynchronous
    """

    def __call__(
        self,
        *,
        input: Any,
        output: Any,
        expected_output: Any,
        metadata: Optional[Dict[str, Any]],
        **kwargs: Dict[str, Any],
    ) -> Union[
        Evaluation, List[Evaluation], Awaitable[Union[Evaluation, List[Evaluation]]]
    ]:
        r"""Evaluate a task output for quality, correctness, or other metrics.

        This method should implement specific evaluation logic such as accuracy checking,
        similarity measurement, toxicity detection, fluency assessment, etc.

        Args:
            input: The original input that was passed to the task function.
                This is typically the item['input'] or item.input value.
            output: The output produced by the task function for this input.
                This is the direct return value from your task function.
            expected_output: The expected/ground truth output for comparison.
                May be None if not available in the dataset. Evaluators should
                handle this case appropriately.
            metadata: Optional metadata from the experiment item that might
                contain additional context for evaluation (categories, difficulty, etc.)
            **kwargs: Additional keyword arguments that may be passed by the framework

        Returns:
            Evaluation results in one of these formats:
            - Single Evaluation dict: {"name": "accuracy", "value": 0.85, "comment": "..."}
            - List of Evaluation dicts: [{"name": "precision", ...}, {"name": "recall", ...}]
            - Awaitable returning either of the above (for async evaluators)

            Each Evaluation dict should contain:
            - name (str): Unique identifier for this evaluation metric
            - value (int|float|str|bool): The evaluation score or result
            - comment (str, optional): Human-readable explanation of the result
            - metadata (dict, optional): Additional structured data about the evaluation

        Examples:
            Simple accuracy evaluator:
            ```python
            def accuracy_evaluator(*, input, output, expected_output=None, **kwargs):
                if expected_output is None:
                    return {"name": "accuracy", "value": 0, "comment": "No expected output"}

                is_correct = output.strip().lower() == expected_output.strip().lower()
                return {
                    "name": "accuracy",
                    "value": 1.0 if is_correct else 0.0,
                    "comment": "Exact match" if is_correct else "No match"
                }
            ```

            Multi-metric evaluator:
            ```python
            def comprehensive_evaluator(*, input, output, expected_output=None, **kwargs):
                results = []

                # Length check
                results.append({
                    "name": "output_length",
                    "value": len(output),
                    "comment": f"Output contains {len(output)} characters"
                })

                # Sentiment analysis
                sentiment_score = analyze_sentiment(output)
                results.append({
                    "name": "sentiment",
                    "value": sentiment_score,
                    "comment": f"Sentiment score: {sentiment_score:.2f}"
                })

                return results
            ```

            Async evaluator using external API:
            ```python
            async def llm_judge_evaluator(*, input, output, expected_output=None, **kwargs):
                prompt = f"Rate the quality of this response on a scale of 1-10:\n"
                prompt += f"Question: {input}\nResponse: {output}"

                response = await openai_client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}]
                )

                try:
                    score = float(response.choices[0].message.content.strip())
                    return {
                        "name": "llm_judge_quality",
                        "value": score,
                        "comment": f"LLM judge rated this {score}/10"
                    }
                except ValueError:
                    return {
                        "name": "llm_judge_quality",
                        "value": 0,
                        "comment": "Could not parse LLM judge score"
                    }
            ```

            Context-aware evaluator:
            ```python
            def context_evaluator(*, input, output, metadata=None, **kwargs):
                # Use metadata for context-specific evaluation
                difficulty = metadata.get("difficulty", "medium") if metadata else "medium"

                # Adjust expectations based on difficulty
                min_length = {"easy": 50, "medium": 100, "hard": 150}[difficulty]

                meets_requirement = len(output) >= min_length
                return {
                    "name": f"meets_{difficulty}_requirement",
                    "value": meets_requirement,
                    "comment": f"Output {'meets' if meets_requirement else 'fails'} {difficulty} length requirement"
                }
            ```
        """
        ...


class RunEvaluatorFunction(Protocol):
    """Protocol defining the interface for run-level evaluator functions.

    Run-level evaluators assess aggregate properties of the entire experiment run,
    computing metrics that span across all items rather than individual outputs.
    They receive the complete results from all processed items and can compute
    statistics like averages, distributions, correlations, or other aggregate metrics.

    Run evaluators should:
    - Accept item_results as a keyword argument containing all item results
    - Return Evaluation dict(s) with aggregate metrics
    - Handle cases where some items may have failed processing
    - Compute meaningful statistics across the dataset
    - Can be either synchronous or asynchronous
    """

    def __call__(
        self,
        *,
        item_results: List[ExperimentItemResult],
        **kwargs: Dict[str, Any],
    ) -> Union[
        Evaluation, List[Evaluation], Awaitable[Union[Evaluation, List[Evaluation]]]
    ]:
        r"""Evaluate the entire experiment run with aggregate metrics.

        This method should implement aggregate evaluation logic such as computing
        averages, calculating distributions, finding correlations, detecting patterns
        across items, or performing statistical analysis on the experiment results.

        Args:
            item_results: List of results from all successfully processed experiment items.
                Each item result contains:
                - item: The original experiment item
                - output: The task function's output for this item
                - evaluations: List of item-level evaluation results
                - trace_id: Langfuse trace ID for this execution
                - dataset_run_id: Dataset run ID (if using Langfuse datasets)

                Note: This list only includes items that were successfully processed.
                Failed items are excluded but logged separately.
            **kwargs: Additional keyword arguments that may be passed by the framework

        Returns:
            Evaluation results in one of these formats:
            - Single Evaluation dict: {"name": "avg_accuracy", "value": 0.78, "comment": "..."}
            - List of Evaluation dicts: [{"name": "mean", ...}, {"name": "std_dev", ...}]
            - Awaitable returning either of the above (for async evaluators)

            Each Evaluation dict should contain:
            - name (str): Unique identifier for this run-level metric
            - value (int|float|str|bool): The aggregate evaluation result
            - comment (str, optional): Human-readable explanation of the metric
            - metadata (dict, optional): Additional structured data about the evaluation

        Examples:
            Average accuracy calculator:
            ```python
            def average_accuracy(*, item_results, **kwargs):
                if not item_results:
                    return {"name": "avg_accuracy", "value": 0.0, "comment": "No results"}

                accuracy_values = []
                for result in item_results:
                    for evaluation in result.evaluations:
                        if evaluation.name == "accuracy":
                            accuracy_values.append(evaluation.value)

                if not accuracy_values:
                    return {"name": "avg_accuracy", "value": 0, "comment": "No accuracy evaluations found"}

                avg = sum(accuracy_values) / len(accuracy_values)
                return {
                    "name": "avg_accuracy",
                    "value": avg,
                    "comment": f"Average accuracy across {len(accuracy_values)} items: {avg:.2%}"
                }
            ```

            Multiple aggregate metrics:
            ```python
            def statistical_summary(*, item_results, **kwargs):
                if not item_results:
                    return []

                results = []

                # Calculate output length statistics
                lengths = [len(str(result.output)) for result in item_results]
                results.extend([
                    {"name": "avg_output_length", "value": sum(lengths) / len(lengths)},
                    {"name": "min_output_length", "value": min(lengths)},
                    {"name": "max_output_length", "value": max(lengths)}
                ])

                # Success rate
                total_items = len(item_results)  # Only successful items are included
                results.append({
                    "name": "processing_success_rate",
                    "value": 1.0,  # All items in item_results succeeded
                    "comment": f"Successfully processed {total_items} items"
                })

                return results
            ```

            Async run evaluator with external analysis:
            ```python
            async def llm_batch_analysis(*, item_results, **kwargs):
                # Prepare batch analysis prompt
                outputs = [result.output for result in item_results]
                prompt = f"Analyze these {len(outputs)} outputs for common themes:\n"
                prompt += "\n".join(f"{i+1}. {output}" for i, output in enumerate(outputs))

                response = await openai_client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}]
                )

                return {
                    "name": "thematic_analysis",
                    "value": response.choices[0].message.content,
                    "comment": f"LLM analysis of {len(outputs)} outputs"
                }
            ```

            Performance distribution analysis:
            ```python
            def performance_distribution(*, item_results, **kwargs):
                # Extract all evaluation scores
                all_scores = []
                score_by_metric = {}

                for result in item_results:
                    for evaluation in result.evaluations:
                        metric_name = evaluation.name
                        value = evaluation.value

                        if isinstance(value, (int, float)):
                            all_scores.append(value)
                            if metric_name not in score_by_metric:
                                score_by_metric[metric_name] = []
                            score_by_metric[metric_name].append(value)

                results = []

                # Overall score distribution
                if all_scores:
                    import statistics
                    results.append({
                        "name": "score_std_dev",
                        "value": statistics.stdev(all_scores) if len(all_scores) > 1 else 0,
                        "comment": f"Standard deviation across all numeric scores"
                    })

                # Per-metric statistics
                for metric, scores in score_by_metric.items():
                    if len(scores) > 1:
                        results.append({
                            "name": f"{metric}_variance",
                            "value": statistics.variance(scores),
                            "comment": f"Variance in {metric} across {len(scores)} items"
                        })

                return results
            ```
        """
        ...


def _format_value(value: Any) -> str:
    """Format a value for display."""
    if isinstance(value, str):
        return value[:50] + "..." if len(value) > 50 else value
    return str(value)


async def _run_evaluator(
    evaluator: Union[EvaluatorFunction, RunEvaluatorFunction], **kwargs: Any
) -> List[Evaluation]:
    """Run an evaluator function and normalize the result."""
    try:
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

    except Exception as e:
        evaluator_name = getattr(evaluator, "__name__", "unknown_evaluator")
        logging.getLogger("langfuse").error(f"Evaluator {evaluator_name} failed: {e}")
        return []


async def _run_task(task: TaskFunction, item: ExperimentItem) -> Any:
    """Run a task function and handle sync/async."""
    result = task(item=item)

    # Handle async tasks
    if asyncio.iscoroutine(result):
        result = await result

    return result


def create_evaluator_from_autoevals(
    autoevals_evaluator: Any, **kwargs: Optional[Dict[str, Any]]
) -> EvaluatorFunction:
    """Create a Langfuse evaluator from an autoevals evaluator.

    Args:
        autoevals_evaluator: An autoevals evaluator instance
        **kwargs: Additional arguments passed to the evaluator

    Returns:
        A Langfuse-compatible evaluator function
    """

    def langfuse_evaluator(
        *,
        input: Any,
        output: Any,
        expected_output: Any,
        metadata: Optional[Dict[str, Any]],
        **langfuse_kwargs: Dict[str, Any],
    ) -> Evaluation:
        evaluation = autoevals_evaluator(
            input=input, output=output, expected=expected_output, **kwargs
        )

        return Evaluation(
            name=evaluation.name,
            value=evaluation.score,
            comment=(evaluation.metadata or {}).get("comment"),
            metadata=evaluation.metadata,
        )

    return langfuse_evaluator
