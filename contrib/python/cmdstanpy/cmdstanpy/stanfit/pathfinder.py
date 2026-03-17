"""
Container for the result of running Pathfinder.
"""

from typing import Optional, Union

import numpy as np

from cmdstanpy.cmdstan_args import Method
from cmdstanpy.stanfit.metadata import InferenceMetadata
from cmdstanpy.stanfit.runset import RunSet
from cmdstanpy.utils import stancsv


class CmdStanPathfinder:
    """
    Container for outputs from the Pathfinder algorithm.
    Created by :meth:`CmdStanModel.pathfinder()`.
    """

    def __init__(self, runset: RunSet):
        """Initialize object."""
        if not runset.method == Method.PATHFINDER:
            raise ValueError(
                'Wrong runset method, expecting Pathfinder runset, '
                'found method {}'.format(runset.method)
            )
        self._runset = runset
        self._draws: np.ndarray = np.array(())
        self._metadata = InferenceMetadata.from_csv(self._runset.csv_files[0])

    def create_inits(
        self, seed: Optional[int] = None, chains: int = 4
    ) -> Union[list[dict[str, np.ndarray]], dict[str, np.ndarray]]:
        """
        Create initial values for the parameters of the model
        by randomly selecting draws from the Pathfinder approximation.

        :param seed: Used for random selection, defaults to None
        :param chains: Number of initial values to return, defaults to 4
        :return: The initial values for the parameters of the model.

        If ``chains`` is 1, a dictionary is returned, otherwise a list
        of dictionaries is returned, in the format expected for the
        ``inits`` argument of :meth:`CmdStanModel.sample`.
        """
        self._assemble_draws()
        rng = np.random.default_rng(seed)
        idxs = rng.choice(self._draws.shape[0], size=chains, replace=False)
        if chains == 1:
            draw = self._draws[idxs[0]]
            return {
                name: var.extract_reshape(draw)
                for name, var in self._metadata.stan_vars.items()
            }
        else:
            return [
                {
                    name: var.extract_reshape(self._draws[idx])
                    for name, var in self._metadata.stan_vars.items()
                }
                for idx in idxs
            ]

    def __repr__(self) -> str:
        rep = 'CmdStanPathfinder: model={}{}'.format(
            self._runset.model,
            self._runset._args.method_args.compose(0, cmd=[]),
        )
        rep = '{}\n csv_files:\n\t{}\n output_files:\n\t{}'.format(
            rep,
            '\n\t'.join(self._runset.csv_files),
            '\n\t'.join(self._runset.stdout_files),
        )
        return rep

    def _assemble_draws(self) -> None:
        if self._draws.shape != (0,):
            return

        csv_file = self._runset.csv_files[0]
        try:
            *_, draws = stancsv.parse_comments_header_and_draws(
                self._runset.csv_files[0]
            )
            self._draws = stancsv.csv_bytes_list_to_numpy(draws)
        except Exception as exc:
            raise ValueError(
                f"An error occurred when parsing Stan csv {csv_file}"
            ) from exc

    def stan_variable(self, var: str) -> np.ndarray:
        """
        Return a numpy.ndarray which contains the estimates for the
        for the named Stan program variable where the dimensions of the
        numpy.ndarray match the shape of the Stan program variable.

        This functionaltiy is also available via a shortcut using ``.`` -
        writing ``fit.a`` is a synonym for ``fit.stan_variable("a")``

        :param var: variable name

        See Also
        --------
        CmdStanPathfinder.stan_variables
        CmdStanMLE.stan_variable
        CmdStanMCMC.stan_variable
        CmdStanVB.stan_variable
        CmdStanGQ.stan_variable
        CmdStanLaplace.stan_variable
        """
        self._assemble_draws()
        try:
            out: np.ndarray = self._metadata.stan_vars[var].extract_reshape(
                self._draws
            )
            return out
        except KeyError:
            # pylint: disable=raise-missing-from
            raise ValueError(
                f'Unknown variable name: {var}\n'
                'Available variables are '
                + ", ".join(self._metadata.stan_vars.keys())
            )

    def stan_variables(self) -> dict[str, np.ndarray]:
        """
        Return a dictionary mapping Stan program variables names
        to the corresponding numpy.ndarray containing the inferred values.

        See Also
        --------
        CmdStanPathfinder.stan_variable
        CmdStanMCMC.stan_variables
        CmdStanMLE.stan_variables
        CmdStanVB.stan_variables
        CmdStanGQ.stan_variables
        CmdStanLaplace.stan_variables
        """
        result = {}
        for name in self._metadata.stan_vars:
            result[name] = self.stan_variable(name)
        return result

    def method_variables(self) -> dict[str, np.ndarray]:
        """
        Returns a dictionary of all sampler variables, i.e., all
        output column names ending in `__`.  Assumes that all variables
        are scalar variables where column name is variable name.
        Maps each column name to a numpy.ndarray (draws x chains x 1)
        containing per-draw diagnostic values.
        """
        self._assemble_draws()
        return {
            name: var.extract_reshape(self._draws)
            for name, var in self._metadata.method_vars.items()
        }

    def draws(self) -> np.ndarray:
        """
        Return a numpy.ndarray containing the draws from the
        approximate posterior distribution. This is a 2-D array
        of shape (draws, parameters).
        """
        self._assemble_draws()
        return self._draws

    def __getattr__(self, attr: str) -> np.ndarray:
        """Synonymous with ``fit.stan_variable(attr)"""
        if attr.startswith("_"):
            raise AttributeError(f"Unknown variable name {attr}")
        try:
            return self.stan_variable(attr)
        except ValueError as e:
            # pylint: disable=raise-missing-from
            raise AttributeError(*e.args)

    def __getstate__(self) -> dict:
        # This function returns the mapping of objects to serialize with pickle.
        # See https://docs.python.org/3/library/pickle.html#object.__getstate__
        # for details. We call _assemble_draws to ensure posterior samples have
        # been loaded prior to serialization.
        self._assemble_draws()
        return self.__dict__

    @property
    def metadata(self) -> InferenceMetadata:
        """
        Returns object which contains CmdStan configuration as well as
        information about the names and structure of the inference method
        and model output variables.
        """
        return self._metadata

    @property
    def column_names(self) -> tuple[str, ...]:
        """
        Names of all outputs from the sampler, comprising sampler parameters
        and all components of all model parameters, transformed parameters,
        and quantities of interest. Corresponds to Stan CSV file header row,
        with names munged to array notation, e.g. `beta[1]` not `beta.1`.
        """
        return self._metadata.column_names

    @property
    def is_resampled(self) -> bool:
        """
        Returns True if the draws were resampled from several Pathfinder
        approximations, False otherwise.
        """
        return (  # type: ignore
            self._metadata.cmdstan_config.get("num_paths", 4) > 1
            and self._metadata.cmdstan_config.get('psis_resample', 1)
            in (1, 'true')
            and self._metadata.cmdstan_config.get('calculate_lp', 1)
            in (1, 'true')
        )

    def save_csvfiles(self, dir: Optional[str] = None) -> None:
        """
        Move output CSV files to specified directory.  If files were
        written to the temporary session directory, clean filename.
        E.g., save 'bernoulli-201912081451-1-5nm6as7u.csv' as
        'bernoulli-201912081451-1.csv'.

        :param dir: directory path

        See Also
        --------
        stanfit.RunSet.save_csvfiles
        cmdstanpy.from_csv
        """
        self._runset.save_csvfiles(dir)
