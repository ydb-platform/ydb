"""
A logger that logs training activity to MLflow.
"""

from typing import Dict, Any, Tuple, Callable, List, Optional, IO
from types import ModuleType
import os
import sys

from spacy import Language, load
from spacy.util import SimpleFrozenList
from .util import dict_to_dot, dot_to_dict, matcher_for_regex_patterns
from .util import setup_default_console_logger, LoggerT


# entry point: spacy.MLflowLogger.v2
def mlflow_logger_v2(
    run_id: Optional[str] = None,
    experiment_id: Optional[str] = None,
    run_name: Optional[str] = None,
    nested: bool = False,
    tags: Optional[Dict[str, Any]] = None,
    remove_config_values: List[str] = SimpleFrozenList(),
    log_custom_stats: Optional[List[str]] = None,
) -> LoggerT:
    """Creates a logger that interoperates with the MLflow framework.

    Args:
        run_id (Optional[str]):
            Unique ID of an existing MLflow run to which parameters and metrics are logged. Can be omitted if `experiment_id` and `run_id` are provided. Defaults to None.
        experiment_id (Optional[str]):
            ID of an existing experiment under which to create the current run. Only applicable when `run_id` is `None`. Defaults to None.
        run_name (Optional[str]):
            Name of new run. Only applicable when `run_id` is `None`. Defaults to None.
        nested (bool):
            Controls whether run is nested in parent run. `True` creates a nested run. Defaults to False.
        tags (Optional[Dict[str, Any]]):
            A dictionary of string keys and values to set as tags on the run. If a run is being resumed, these tags are set on the resumed run. If a new run is being created, these tags are set on the new run. Defaults to None.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to MLflow. Defaults to an empty list.
        log_custom_stats (Optional[List[str]]):
            A list of regular expressions that will be applied to the info dictionary passed to the logger. Statistics and metrics that match these regexps will be automatically logged. Defaults to None.

    Returns:
        LoggerT: Logger instance.
    """
    mlflow = _import_mlflow()

    def setup_logger(
        nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        match_stat = matcher_for_regex_patterns(log_custom_stats)
        _setup_mlflow(
            mlflow,
            nlp,
            run_id,
            experiment_id,
            run_name,
            nested,
            tags,
            remove_config_values,
        )

        def log_step(info: Optional[Dict[str, Any]]):
            _log_step_mlflow(mlflow, info)
            _log_custom_stats(mlflow, info, match_stat)

        def finalize() -> None:
            _finalize_mlflow(mlflow)

        return log_step, finalize

    return setup_logger


# entry point: spacy.MLflowLogger.v1
def mlflow_logger_v1(
    run_id: Optional[str] = None,
    experiment_id: Optional[str] = None,
    run_name: Optional[str] = None,
    nested: bool = False,
    tags: Optional[Dict[str, Any]] = None,
    remove_config_values: List[str] = SimpleFrozenList(),
) -> LoggerT:
    """Creates a logger that interoperates with the MLflow framework.

    Args:
        run_id (Optional[str]):
            Unique ID of an existing MLflow run to which parameters and metrics are logged. Can be omitted if `experiment_id` and `run_id` are provided. Defaults to None.
        experiment_id (Optional[str]):
            ID of an existing experiment under which to create the current run. Only applicable when `run_id` is `None`. Defaults to None.
        run_name (Optional[str]):
            Name of new run. Only applicable when `run_id` is `None`. Defaults to None.
        nested (bool):
            Controls whether run is nested in parent run. `True` creates a nested run. Defaults to False.
        tags (Optional[Dict[str, Any]]):
            A dictionary of string keys and values to set as tags on the run. If a run is being resumed, these tags are set on the resumed run. If a new run is being created, these tags are set on the new run. Defaults to None.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to MLflow. Defaults to an empty list.

    Returns:
        LoggerT: Logger instance.
    """
    mlflow = _import_mlflow()

    def setup_logger(
        nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        console_log_step, console_finalize = setup_default_console_logger(
            nlp, stdout, stderr
        )
        _setup_mlflow(
            mlflow,
            nlp,
            run_id,
            experiment_id,
            run_name,
            nested,
            tags,
            remove_config_values,
        )

        def log_step(info: Optional[Dict[str, Any]]):
            console_log_step(info)
            _log_step_mlflow(mlflow, info)

        def finalize() -> None:
            console_finalize()
            _finalize_mlflow(mlflow)

        return log_step, finalize

    return setup_logger


def _import_mlflow() -> ModuleType:
    try:
        import mlflow

        # test that these are available
        from mlflow import (
            start_run,
            log_metric,
            log_metrics,
            log_artifact,
            end_run,
        )  # noqa: F401
    except ImportError:
        raise ImportError(
            "The 'mlflow' library could not be found - did you install it? "
            "Alternatively, specify the 'ConsoleLogger' in the "
            "'training.logger' config section, instead of the 'MLflowLogger'."
        )
    return mlflow


def _setup_mlflow(
    mlflow: ModuleType,
    nlp: Language,
    run_id: Optional[str] = None,
    experiment_id: Optional[str] = None,
    run_name: Optional[str] = None,
    nested: bool = False,
    tags: Optional[Dict[str, Any]] = None,
    remove_config_values: List[str] = SimpleFrozenList(),
):
    config = nlp.config.interpolate()
    config_dot = dict_to_dot(config)
    for field in remove_config_values:
        del config_dot[field]
    config = dot_to_dict(config_dot)

    mlflow.start_run(
        run_id=run_id,
        experiment_id=experiment_id,
        run_name=run_name,
        nested=nested,
        tags=tags,
    )

    config_dot_items = list(config_dot.items())
    config_dot_batches = [
        config_dot_items[i : i + 100] for i in range(0, len(config_dot_items), 100)
    ]
    for batch in config_dot_batches:
        mlflow.log_params({k.replace("@", ""): v for k, v in batch})


def _log_step_mlflow(
    mlflow: ModuleType,
    info: Optional[Dict[str, Any]],
):
    if info is None:
        return

    score = info["score"]
    other_scores = info["other_scores"]
    losses = info["losses"]
    output_path = info.get("output_path", None)
    if score is not None:
        mlflow.log_metric("score", score)
    if losses:
        mlflow.log_metrics({f"loss_{k}": v for k, v in losses.items()})
    if isinstance(other_scores, dict):
        mlflow.log_metrics(
            {
                k: v
                for k, v in dict_to_dot(other_scores).items()
                if isinstance(v, float) or isinstance(v, int)
            }
        )
    if output_path and score == max(info["checkpoints"])[0]:
        nlp = load(output_path)
        mlflow.spacy.log_model(nlp, "best")


def _finalize_mlflow(
    mlflow: ModuleType,
):
    mlflow.end_run()


def _log_custom_stats(
    mlflow: ModuleType, info: Optional[Dict[str, Any]], matcher: Callable[[str], bool]
):
    if info is not None:
        matched = {k: v for k, v in info.items() if matcher(k)}
        if len(matched):
            mlflow.log_metrics(matched)
