"""Container for metadata parsed from the output of a CmdStan run"""

import copy
import os
from typing import Any, Iterator, Union

import stanio

from cmdstanpy.utils import stancsv


class InferenceMetadata:
    """
    CmdStan configuration and contents of output file parsed out of
    the Stan CSV file header comments and column headers.
    Assumes valid CSV files.
    """

    def __init__(
        self, config: dict[str, Union[str, int, float, tuple[str, ...]]]
    ) -> None:
        """Initialize object from CSV headers"""
        self._cmdstan_config = config

        vars = stanio.parse_header(config['raw_header'])  # type: ignore

        self._method_vars = {
            k: v for (k, v) in vars.items() if k.endswith('__')
        }
        self._stan_vars = {
            k: v for (k, v) in vars.items() if not k.endswith('__')
        }

    @classmethod
    def from_csv(
        cls, stan_csv: Union[str, os.PathLike, Iterator[bytes]]
    ) -> 'InferenceMetadata':
        try:
            comments, header, _ = stancsv.parse_comments_header_and_draws(
                stan_csv
            )
            return cls(stancsv.construct_config_header_dict(comments, header))
        except Exception as exc:
            raise ValueError(
                f"An error occurred when parsing Stan csv {stan_csv}"
            ) from exc

    def __repr__(self) -> str:
        return 'Metadata:\n{}\n'.format(self._cmdstan_config)

    def __getitem__(self, key: str) -> Union[str, int, float, tuple[str, ...]]:
        return self._cmdstan_config[key]

    @property
    def cmdstan_config(self) -> dict[str, Any]:
        """
        Returns a dictionary containing a set of name, value pairs
        parsed out of the Stan CSV file header.  These include the
        command configuration and the CSV file header row information.
        Uses deepcopy for immutability.
        """
        return copy.deepcopy(self._cmdstan_config)

    @property
    def column_names(self) -> tuple[str, ...]:
        col_names = self['column_names']
        return col_names  # type: ignore

    @property
    def method_vars(self) -> dict[str, stanio.Variable]:
        """
        Method variable names always end in `__`, e.g. `lp__`.
        """
        return self._method_vars

    @property
    def stan_vars(self) -> dict[str, stanio.Variable]:
        """
        These are the user-defined variables in the Stan program.
        """
        return self._stan_vars
