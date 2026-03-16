"""
A logger that logs training activity to Weights and Biases.
"""

from typing import Dict, Any, Tuple, Callable, List, IO, Optional
from types import ModuleType
import sys

from spacy import Language
from spacy.util import SimpleFrozenList

from .util import dict_to_dot, dot_to_dict, matcher_for_regex_patterns
from .util import setup_default_console_logger, LoggerT


# entry point: spacy.WandbLogger.v5
def wandb_logger_v5(
    project_name: str,
    remove_config_values: List[str] = SimpleFrozenList(),
    model_log_interval: Optional[int] = None,
    log_dataset_dir: Optional[str] = None,
    entity: Optional[str] = None,
    run_name: Optional[str] = None,
    log_best_dir: Optional[str] = None,
    log_latest_dir: Optional[str] = None,
    log_custom_stats: Optional[List[str]] = None,
) -> LoggerT:
    """Creates a logger that interoperates with the Weights & Biases framework.

    Args:
        project_name (str):
            The name of the project in the Weights & Biases interface. The project will be created automatically if it doesn't exist yet.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to W&B. Defaults to [].
        model_log_interval (Optional[int]):
            Steps to wait between logging model checkpoints to the W&B dasboard. Defaults to None.
        log_dataset_dir (Optional[str]):
            Directory containing the dataset to be logged and versioned as a W&B artifact. Defaults to None.
        entity (Optional[str]):
            An entity is a username or team name where you're sending runs. If you don't specify an entity, the run will be sent to your default entity, which is usually your username. Defaults to None.
        run_name (Optional[str]):
            The name of the run. If you don't specify a run name, the name will be created by the `wandb` library. Defaults to None.
        log_best_dir (Optional[str]):
            Directory containing the best trained model as saved by spaCy, to be logged and versioned as a W&B artifact. Defaults to None.
        log_latest_dir (Optional[str]):
            Directory containing the latest trained model as saved by spaCy, to be logged and versioned as a W&B artifact. Defaults to None.
        log_custom_stats (Optional[List[str]]):
            A list of regular expressions that will be applied to the info dictionary passed to the logger. Statistics and metrics that match these regexps will be automatically logged. Defaults to None.

    Returns:
        LoggerT: Logger instance.
    """
    wandb = _import_wandb()

    def setup_logger(
        nlp: "Language", stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        match_stat = matcher_for_regex_patterns(log_custom_stats)
        run = _setup_wandb(
            wandb,
            nlp,
            project_name,
            remove_config_values=remove_config_values,
            entity=entity,
        )
        if run_name:
            wandb.run.name = run_name

        if log_dataset_dir:
            _log_dir_artifact(
                wandb, path=log_dataset_dir, name="dataset", type="dataset"
            )

        def log_step(info: Optional[Dict[str, Any]]):
            _log_scores(wandb, info)
            _log_model_artifact(wandb, info, run, model_log_interval)
            _log_custom_stats(wandb, info, match_stat)

        def finalize() -> None:
            if log_best_dir:
                _log_dir_artifact(
                    wandb,
                    path=log_best_dir,
                    name="model_best",
                    type="model",
                )

            if log_latest_dir:
                _log_dir_artifact(
                    wandb,
                    path=log_latest_dir,
                    name="model_last",
                    type="model",
                )
            wandb.join()

        return log_step, finalize

    return setup_logger


# entry point: spacy.WandbLogger.v4
def wandb_logger_v4(
    project_name: str,
    remove_config_values: List[str] = SimpleFrozenList(),
    model_log_interval: Optional[int] = None,
    log_dataset_dir: Optional[str] = None,
    entity: Optional[str] = None,
    run_name: Optional[str] = None,
    log_best_dir: Optional[str] = None,
    log_latest_dir: Optional[str] = None,
) -> LoggerT:
    """Creates a logger that interoperates with the Weights & Biases framework.

    Args:
        project_name (str):
            The name of the project in the Weights & Biases interface. The project will be created automatically if it doesn't exist yet.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to W&B. Defaults to [].
        model_log_interval (Optional[int]):
            Steps to wait between logging model checkpoints to the W&B dasboard. Defaults to None.
        log_dataset_dir (Optional[str]):
            Directory containing the dataset to be logged and versioned as a W&B artifact. Defaults to None.
        entity (Optional[str]):
            An entity is a username or team name where you're sending runs. If you don't specify an entity, the run will be sent to your default entity, which is usually your username. Defaults to None.
        run_name (Optional[str]):
            The name of the run. If you don't specify a run name, the name will be created by the `wandb` library. Defaults to None.
        log_best_dir (Optional[str]):
            Directory containing the best trained model as saved by spaCy, to be logged and versioned as a W&B artifact. Defaults to None.
        log_latest_dir (Optional[str]):
            Directory containing the latest trained model as saved by spaCy, to be logged and versioned as a W&B artifact. Defaults to None.

    Returns:
        LoggerT: Logger instance.
    """
    wandb = _import_wandb()

    def setup_logger(
        nlp: "Language", stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        console_log_step, console_finalize = setup_default_console_logger(
            nlp, stdout, stderr
        )
        run = _setup_wandb(
            wandb,
            nlp,
            project_name,
            remove_config_values=remove_config_values,
            entity=entity,
        )
        if run_name:
            wandb.run.name = run_name

        if log_dataset_dir:
            _log_dir_artifact(
                wandb, path=log_dataset_dir, name="dataset", type="dataset"
            )

        def log_step(info: Optional[Dict[str, Any]]):
            console_log_step(info)
            _log_scores(wandb, info)
            _log_model_artifact(wandb, info, run, model_log_interval)

        def finalize() -> None:
            if log_best_dir:
                _log_dir_artifact(
                    wandb,
                    path=log_best_dir,
                    name="model_best",
                    type="model",
                )

            if log_latest_dir:
                _log_dir_artifact(
                    wandb,
                    path=log_latest_dir,
                    name="model_last",
                    type="model",
                )
            console_finalize()
            wandb.join()

        return log_step, finalize

    return setup_logger


# entry point: spacy.WandbLogger.v3
def wandb_logger_v3(
    project_name: str,
    remove_config_values: List[str] = SimpleFrozenList(),
    model_log_interval: Optional[int] = None,
    log_dataset_dir: Optional[str] = None,
    entity: Optional[str] = None,
    run_name: Optional[str] = None,
) -> LoggerT:
    """Creates a logger that interoperates with the Weights & Biases framework.

    Args:
        project_name (str):
            The name of the project in the Weights & Biases interface. The project will be created automatically if it doesn't exist yet.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to W&B. Defaults to [].
        model_log_interval (Optional[int]):
            Steps to wait between logging model checkpoints to the W&B dasboard. Defaults to None.
        log_dataset_dir (Optional[str]):
            Directory containing the dataset to be logged and versioned as a W&B artifact. Defaults to None.
        entity (Optional[str]):
            An entity is a username or team name where you're sending runs. If you don't specify an entity, the run will be sent to your default entity, which is usually your username. Defaults to None.
        run_name (Optional[str]):
            The name of the run. If you don't specify a run name, the name will be created by the `wandb` library. Defaults to None.

    Returns:
        LoggerT: Logger instance.
    """
    wandb = _import_wandb()

    def setup_logger(
        nlp: "Language", stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        console_log_step, console_finalize = setup_default_console_logger(
            nlp, stdout, stderr
        )
        run = _setup_wandb(
            wandb,
            nlp,
            project_name,
            remove_config_values=remove_config_values,
            entity=entity,
        )
        if run_name:
            wandb.run.name = run_name

        if log_dataset_dir:
            _log_dir_artifact(
                wandb, path=log_dataset_dir, name="dataset", type="dataset"
            )

        def log_step(info: Optional[Dict[str, Any]]):
            console_log_step(info)
            _log_scores(wandb, info)
            _log_model_artifact(wandb, info, run, model_log_interval)

        def finalize() -> None:
            console_finalize()
            wandb.join()

        return log_step, finalize

    return setup_logger


# entry point: spacy.WandbLogger.v2
def wandb_logger_v2(
    project_name: str,
    remove_config_values: List[str] = SimpleFrozenList(),
    model_log_interval: Optional[int] = None,
    log_dataset_dir: Optional[str] = None,
) -> LoggerT:
    """Creates a logger that interoperates with the Weights & Biases framework.

    Args:
        project_name (str):
            The name of the project in the Weights & Biases interface. The project will be created automatically if it doesn't exist yet.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to W&B. Defaults to [].
        model_log_interval (Optional[int]):
            Steps to wait between logging model checkpoints to the W&B dasboard. Defaults to None.
        log_dataset_dir (Optional[str]):
            Directory containing the dataset to be logged and versioned as a W&B artifact. Defaults to None.

    Returns:
        LoggerT: Logger instance.
    """
    wandb = _import_wandb()

    def setup_logger(
        nlp: "Language", stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        console_log_step, console_finalize = setup_default_console_logger(
            nlp, stdout, stderr
        )
        run = _setup_wandb(
            wandb, nlp, project_name, remove_config_values=remove_config_values
        )

        if log_dataset_dir:
            _log_dir_artifact(
                wandb, path=log_dataset_dir, name="dataset", type="dataset"
            )

        def log_step(info: Optional[Dict[str, Any]]):
            console_log_step(info)
            _log_scores(wandb, info)
            _log_model_artifact(wandb, info, run, model_log_interval)

        def finalize() -> None:
            console_finalize()
            wandb.join()

        return log_step, finalize

    return setup_logger


# entry point: spacy.WandbLogger.v1
def wandb_logger_v1(
    project_name: str, remove_config_values: List[str] = SimpleFrozenList()
) -> LoggerT:
    """Creates a logger that interoperates with the Weights & Biases framework.

    Args:
        project_name (str):
            The name of the project in the Weights & Biases interface. The project will be created automatically if it doesn't exist yet.
        remove_config_values (List[str]):
            A list of values to exclude from the config before it is uploaded to W&B. Defaults to [].

    Returns:
        LoggerT: Logger instance.
    """
    wandb = _import_wandb()

    def setup_logger(
        nlp: "Language", stdout: IO = sys.stdout, stderr: IO = sys.stderr
    ) -> Tuple[Callable[[Dict[str, Any]], None], Callable[[], None]]:
        console_log_step, console_finalize = setup_default_console_logger(
            nlp, stdout, stderr
        )
        _setup_wandb(
            wandb, nlp, project_name, remove_config_values=remove_config_values
        )

        def log_step(info: Optional[Dict[str, Any]]):
            console_log_step(info)
            _log_scores(wandb, info)

        def finalize() -> None:
            console_finalize()
            wandb.join()

        return log_step, finalize

    return setup_logger


def _import_wandb() -> ModuleType:
    try:
        import wandb

        # test that these are available
        from wandb import init, log, join  # noqa: F401

        return wandb
    except ImportError:
        raise ImportError(
            "The 'wandb' library could not be found - did you install it? "
            "Alternatively, specify the 'ConsoleLogger' in the "
            "'training.logger' config section, instead of the 'WandbLogger'."
        )


def _setup_wandb(
    wandb: ModuleType,
    nlp: "Language",
    project: str,
    entity: Optional[str] = None,
    remove_config_values: List[str] = SimpleFrozenList(),
) -> Any:
    config = nlp.config.interpolate()
    config_dot = dict_to_dot(config)
    for field in remove_config_values:
        del config_dot[field]
    config = dot_to_dict(config_dot)
    run = wandb.init(project=project, config=config, entity=entity, reinit=True)
    return run


def _log_scores(wandb: ModuleType, info: Optional[Dict[str, Any]]):
    if info is not None:
        score = info["score"]
        other_scores = info["other_scores"]
        losses = info["losses"]
        wandb.log({"score": score})
        if losses:
            wandb.log({f"loss_{k}": v for k, v in losses.items()})
        if isinstance(other_scores, dict):
            wandb.log(other_scores)


def _log_model_artifact(
    wandb: ModuleType,
    info: Optional[Dict[str, Any]],
    run: Any,
    model_log_interval: Optional[int] = None,
):
    if info is not None:
        if model_log_interval and info.get("output_path"):
            if info["step"] % model_log_interval == 0 and info["step"] != 0:
                _log_dir_artifact(
                    wandb,
                    path=info["output_path"],
                    name="pipeline_" + run.id,
                    type="checkpoint",
                    metadata=info,
                    aliases=[
                        f"epoch {info['epoch']} step {info['step']}",
                        "latest",
                        "best" if info["score"] == max(info["checkpoints"])[0] else "",
                    ],
                )


def _log_dir_artifact(
    wandb: ModuleType,
    path: str,
    name: str,
    type: str,
    metadata: Optional[Dict[str, Any]] = None,
    aliases: Optional[List[str]] = None,
):
    dataset_artifact = wandb.Artifact(name, type=type, metadata=metadata)
    dataset_artifact.add_dir(path, name=name)
    wandb.log_artifact(dataset_artifact, aliases=aliases)


def _log_custom_stats(
    wandb: ModuleType, info: Optional[Dict[str, Any]], matcher: Callable[[str], bool]
):
    if info is not None:
        for k, v in info.items():
            if matcher(k):
                wandb.log({k: v})
