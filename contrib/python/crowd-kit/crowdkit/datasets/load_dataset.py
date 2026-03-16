__all__ = [
    "load_dataset",
    "get_datasets_list",
]

from typing import Any, Callable, Dict, List, Optional, Tuple, cast

import pandas as pd

from ._loaders import DATA_LOADERS


def load_dataset(
    dataset: str, data_dir: Optional[str] = None
) -> Tuple[pd.DataFrame, "pd.Series[Any]"]:
    """Downloads a dataset from remote and loads it into Pandas objects.
    If a dataset is already downloaded, loads it from cache.

    Parameters:
        dataset: str, a dataset name
        data_dir: Optional[str]
            Path to folder where to store downloaded dataset. If `None`, `~/crowdkit_data` is used.
            `default=None`. Alternatively, it can be set by the 'CROWDKIT_DATA' environment variable.
    Returns:
        data: Tuple[pd.DataFrame, pd.Series], a tuple of workers answers and ground truth labels.
    """

    if dataset not in DATA_LOADERS:
        raise ValueError("This dataset does not exist")

    return cast(
        Dict[str, Callable[[Optional[str]], Tuple[pd.DataFrame, "pd.Series[Any]"]]],
        DATA_LOADERS[dataset],
    )["loader"](data_dir)


def get_datasets_list() -> List[Tuple[str, str]]:
    """Returns a list of available datasets in format [(name, description)]."""
    return cast(
        List[Tuple[str, str]],
        [(dataset, info["description"]) for dataset, info in DATA_LOADERS.items()],
    )
