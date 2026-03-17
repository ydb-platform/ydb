from typing import Dict, Any, Tuple, Callable, List, Optional, IO
from types import ModuleType
import os
import sys

from spacy import Language
from spacy.util import SimpleFrozenList
from .util import dict_to_dot, dot_to_dict, matcher_for_regex_patterns
from .util import setup_default_console_logger, LoggerT


# entry point: spacy.ClearMLLogger.v2
def clearml_logger_v2(
    project_name: str,
    task_name: str,
    remove_config_values: List[str] = SimpleFrozenList(),
    model_log_interval: Optional[int] = None,
    log_dataset_dir: Optional[str] = None,
    log_best_dir: Optional[str] = None,
    log_latest_dir: Optional[str] = None,
    log_custom_stats: Optional[List[str]] = None,
) -> LoggerT:
    """Creates a logger that interoperates with the ClearML framework.

    Args:
        project_name (str):
            The name of the project in the ClearML interface. The project will be created automatically if it doesn't exist yet.
        task_name (str):
            The name of the ClearML task. A task is an experiment that lives inside a project. Can be non-unique.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to ClearML. Defaults to [].
        model_log_interval (Optional[int]):
            Steps to wait between logging model checkpoints to the ClearML dasboard (default: `None`). Will have no effect without also setting `log_best_dir` or `log_latest_dir`. Defaults to None.
        log_dataset_dir (Optional[str]):
            Directory containing the dataset to be logged and versioned as a ClearML Dataset. Defaults to None.
        log_best_dir (Optional[str]):
            Directory containing the best trained model as saved by spaCy, to be logged and versioned as a ClearML artifact. Defaults to None.
        log_latest_dir (Optional[str]):
            Directory containing the latest trained model as saved by spaCy, to be logged and versioned as a ClearML artifact. Defaults to None.
        log_custom_stats (Optional[List[str]]):
            A list of regular expressions that will be applied to the info dictionary passed to the logger. Statistics and metrics that match these regexps will be automatically logged. Defaults to None.

    Returns:
        LoggerT: Logger instance.
    """
    clearml = _import_clearml()

    def setup_logger(
        nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        match_stat = matcher_for_regex_patterns(log_custom_stats)
        task, best_model, last_model = _setup_clearml(
            clearml,
            nlp,
            project_name,
            task_name,
            log_dataset_dir,
            log_best_dir,
            log_latest_dir,
            remove_config_values,
        )

        def log_step(info: Optional[Dict[str, Any]]):
            _log_step_clearml(
                info,
                task,
                best_model,
                last_model,
                model_log_interval,
                log_best_dir,
                log_latest_dir,
            )
            _log_custom_stats(clearml, info, match_stat)

        def finalize():
            _finalize_clearml(task)

        return log_step, finalize

    return setup_logger


# entry point: spacy.ClearMLLogger.v1
def clearml_logger_v1(
    project_name: str,
    task_name: str,
    remove_config_values: List[str] = SimpleFrozenList(),
    model_log_interval: Optional[int] = None,
    log_dataset_dir: Optional[str] = None,
    log_best_dir: Optional[str] = None,
    log_latest_dir: Optional[str] = None,
) -> LoggerT:
    """Creates a logger that interoperates with the ClearML framework.

    Args:
        project_name (str):
            The name of the project in the ClearML interface. The project will be created automatically if it doesn't exist yet.
        task_name (str):
            The name of the ClearML task. A task is an experiment that lives inside a project. Can be non-unique.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to ClearML. Defaults to [].
        model_log_interval (Optional[int]):
            Steps to wait between logging model checkpoints to the ClearML dasboard (default: `None`). Will have no effect without also setting `log_best_dir` or `log_latest_dir`. Defaults to None.
        log_dataset_dir (Optional[str]):
            Directory containing the dataset to be logged and versioned as a ClearML Dataset. Defaults to None.
        log_best_dir (Optional[str]):
            Directory containing the best trained model as saved by spaCy, to be logged and versioned as a ClearML artifact. Defaults to None.
        log_latest_dir (Optional[str]):
            Directory containing the latest trained model as saved by spaCy, to be logged and versioned as a ClearML artifact. Defaults to None.

    Returns:
        LoggerT: Logger instance.
    """
    clearml = _import_clearml()

    def setup_logger(
        nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        console_log_step, console_finalize = setup_default_console_logger(
            nlp, stdout, stderr
        )
        task, best_model, last_model = _setup_clearml(
            clearml,
            nlp,
            project_name,
            task_name,
            log_dataset_dir,
            log_best_dir,
            log_latest_dir,
            remove_config_values,
        )

        def log_step(info: Optional[Dict[str, Any]]):
            console_log_step(info)
            _log_step_clearml(
                info,
                task,
                best_model,
                last_model,
                model_log_interval,
                log_best_dir,
                log_latest_dir,
            )

        def finalize():
            console_finalize()
            _finalize_clearml(task)

        return log_step, finalize

    return setup_logger


def _import_clearml() -> ModuleType:
    try:
        import clearml

        # test that these are available
        from clearml import Task, Dataset, OutputModel  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "The 'clearml' library could not be found - did you install it? "
            "Alternatively, specify the 'ConsoleLogger' in the "
            "'training.logger' config section, instead of the 'ClearMLLogger'."
        ) from exc
    return clearml


def _setup_clearml(
    clearml: ModuleType,
    nlp: "Language",
    project_name: str,
    task_name: str,
    log_dataset_dir: Optional[str] = None,
    log_best_dir: Optional[str] = None,
    log_latest_dir: Optional[str] = None,
    remove_config_values: List[str] = SimpleFrozenList(),
) -> Tuple[Any, Any, Any]:
    config = nlp.config.interpolate()
    config_dot = dict_to_dot(config)
    for field in remove_config_values:
        del config_dot[field]
    config = dot_to_dict(config_dot)
    task = clearml.Task.init(
        project_name=project_name,
        task_name=task_name,
        output_uri=True,
    )
    for config_section, subconfig_or_value in config.items():
        task.connect(subconfig_or_value, name=config_section)

    if log_dataset_dir:
        dataset = clearml.Dataset.create(
            dataset_project=project_name,
            dataset_name=os.path.basename(log_dataset_dir),
        )
        dataset.add_files(log_dataset_dir)
        dataset.finalize(auto_upload=True)
        task.set_user_properties(
            {
                "name": "Created Dataset ID",
                "value": dataset.id,
            }
        )

    # Connect 2 models to the task, we will periodically update their weights later on
    if log_best_dir:
        best_model = clearml.OutputModel(
            task=task, framework="spaCy", name="Best Model"
        )
    else:
        best_model = None
    if log_latest_dir:
        last_model = clearml.OutputModel(
            task=task, framework="spaCy", name="Last Model"
        )
    else:
        last_model = None

    return task, best_model, last_model


def _log_step_clearml(
    info: Optional[Dict[str, Any]],
    task: Any,
    best_model: Optional[Any] = None,
    last_model: Optional[Any] = None,
    model_log_interval: Optional[int] = None,
    log_best_dir: Optional[str] = None,
    log_latest_dir: Optional[str] = None,
):
    if info is None:
        return

    score = info.get("score")
    other_scores = info.get("other_scores")
    losses = info.get("losses")
    if score:
        task.get_logger().report_scalar(
            "Score", "Score", iteration=info["step"], value=score
        )
    if losses:
        for metric, metric_value in losses.items():
            task.get_logger().report_scalar(
                title=f"loss_{metric}",
                series=f"loss_{metric}",
                iteration=info["step"],
                value=metric_value,
            )
    if isinstance(other_scores, dict):
        # other_scores is usually a nested dict, so group they by the first key and flatten the rest
        # combine flattened submetrics on the same ClearML graph when they have the same first key
        for metric, metric_value in other_scores.items():
            if isinstance(metric_value, dict):
                sub_metrics_dict = dict_to_dot(metric_value)
                for (
                    sub_metric,
                    sub_metric_value,
                ) in sub_metrics_dict.items():
                    # Scalars with the same title get plotted on the same graph as multiple traces
                    # This saves a lot of space in the UI
                    task.get_logger().report_scalar(
                        title=metric,
                        series=sub_metric,
                        iteration=info["step"],
                        value=sub_metric_value,
                    )
            elif isinstance(metric_value, (float, int)):
                task.get_logger().report_scalar(
                    metric,
                    metric,
                    iteration=info["step"],
                    value=metric_value,
                )

    if model_log_interval and info.get("output_path"):
        if info["step"] % model_log_interval == 0 and info["step"] != 0:
            if log_latest_dir:
                assert last_model is not None
                last_model.update_weights_package(
                    weights_path=log_latest_dir,
                    auto_delete_file=False,
                    target_filename="last_model",
                )
            if log_best_dir and info["score"] == max(info["checkpoints"])[0]:
                assert best_model is not None
                best_model.update_weights_package(
                    weights_path=log_best_dir,
                    auto_delete_file=False,
                    target_filename="best_model",
                )


def _finalize_clearml(task: Any):
    task.flush(wait_for_uploads=True)
    task.close()


def _log_custom_stats(
    task: Any, info: Optional[Dict[str, Any]], matcher: Callable[[str], bool]
):
    if info is not None:
        for k, v in info.items():
            if matcher(k):
                task.get_logger().report_scalar(
                    title=f"loss_{k}",
                    series=f"loss_{k}",
                    iteration=info["step"],
                    value=v,
                )
