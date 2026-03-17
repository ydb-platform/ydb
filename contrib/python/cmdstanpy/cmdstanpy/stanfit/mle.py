"""Container for the result of running optimization"""

from collections import OrderedDict
from typing import Optional, Union

import numpy as np
import pandas as pd

from cmdstanpy.cmdstan_args import Method, OptimizeArgs
from cmdstanpy.utils import get_logger, stancsv

from .metadata import InferenceMetadata
from .runset import RunSet


class CmdStanMLE:
    """
    Container for outputs from CmdStan optimization.
    Created by :meth:`CmdStanModel.optimize`.
    """

    def __init__(self, runset: RunSet) -> None:
        """Initialize object."""
        if not runset.method == Method.OPTIMIZE:
            raise ValueError(
                'Wrong runset method, expecting optimize runset, '
                'found method {}'.format(runset.method)
            )
        self.runset = runset
        # info from runset to be exposed
        self.converged = runset._check_retcodes()
        optimize_args = self.runset._args.method_args
        assert isinstance(
            optimize_args, OptimizeArgs
        )  # make the typechecker happy
        self._save_iterations: bool = optimize_args.save_iterations

        csv_file = self.runset.csv_files[0]
        try:
            (
                comment_lines,
                header,
                draws_lines,
            ) = stancsv.parse_comments_header_and_draws(
                self.runset.csv_files[0]
            )
            self._metadata = InferenceMetadata(
                stancsv.construct_config_header_dict(comment_lines, header)
            )
            all_draws = stancsv.csv_bytes_list_to_numpy(draws_lines)

        except Exception as exc:
            raise ValueError(
                f"An error occurred when parsing Stan csv {csv_file}"
            ) from exc
        self._mle: np.ndarray = all_draws[-1]
        if self._save_iterations:
            self._all_iters: np.ndarray = all_draws

    def create_inits(
        self, seed: Optional[int] = None, chains: int = 4
    ) -> dict[str, np.ndarray]:
        """
        Create initial values for the parameters of the model
        from the MLE.

        :param seed: Unused. Kept for compatibility with other
        create_inits methods.
        :param chains: Unused. Kept for compatibility with other
        create_inits methods.
        :return: The initial values for the parameters of the model.

        Returns a dictionary of MLE estimates in the format expected
        for the ``inits`` argument of :meth:`CmdStanModel.sample`.
        When running multi-chain sampling, all chains will be initialized
        at the same points.
        """
        # pylint: disable=unused-argument

        return {
            name: np.array(val) for name, val in self.stan_variables().items()
        }

    def __repr__(self) -> str:
        repr = 'CmdStanMLE: model={}{}'.format(
            self.runset.model, self.runset._args.method_args.compose(0, cmd=[])
        )
        repr = '{}\n csv_file:\n\t{}\n output_file:\n\t{}'.format(
            repr,
            '\n\t'.join(self.runset.csv_files),
            '\n\t'.join(self.runset.stdout_files),
        )
        if not self.converged:
            repr = '{}\n Warning: invalid estimate, '.format(repr)
            repr = '{} optimization failed to converge.'.format(repr)
        return repr

    def __getattr__(self, attr: str) -> Union[np.ndarray, float]:
        """Synonymous with ``fit.stan_variable(attr)"""
        if attr.startswith("_"):
            raise AttributeError(f"Unknown variable name {attr}")
        try:
            return self.stan_variable(attr)
        except ValueError as e:
            # pylint: disable=raise-missing-from
            raise AttributeError(*e.args)

    @property
    def column_names(self) -> tuple[str, ...]:
        """
        Names of estimated quantities, includes joint log probability,
        and all parameters, transformed parameters, and generated quantities.
        """
        return self.metadata.column_names

    @property
    def metadata(self) -> InferenceMetadata:
        """
        Returns object which contains CmdStan configuration as well as
        information about the names and structure of the inference method
        and model output variables.
        """
        return self._metadata

    @property
    def optimized_params_np(self) -> np.ndarray:
        """
        Returns all final estimates from the optimizer as a numpy.ndarray
        which contains all optimizer outputs, i.e., the value for `lp__`
        as well as all Stan program variables.
        """
        if not self.converged:
            get_logger().warning(
                'Invalid estimate, optimization failed to converge.'
            )
        return self._mle

    @property
    def optimized_iterations_np(self) -> Optional[np.ndarray]:
        """
        Returns all saved iterations from the optimizer and final estimate
        as a numpy.ndarray which contains all optimizer outputs, i.e.,
        the value for `lp__` as well as all Stan program variables.

        """
        if not self._save_iterations:
            get_logger().warning(
                'Intermediate iterations not saved to CSV output file. '
                'Rerun the optimize method with "save_iterations=True".'
            )
            return None
        if not self.converged:
            get_logger().warning(
                'Invalid estimate, optimization failed to converge.'
            )
        return self._all_iters

    @property
    def optimized_params_pd(self) -> pd.DataFrame:
        """
        Returns all final estimates from the optimizer as a pandas.DataFrame
        which contains all optimizer outputs, i.e., the value for `lp__`
        as well as all Stan program variables.
        """
        if not self.runset._check_retcodes():
            get_logger().warning(
                'Invalid estimate, optimization failed to converge.'
            )
        return pd.DataFrame([self._mle], columns=self.column_names)

    @property
    def optimized_iterations_pd(self) -> Optional[pd.DataFrame]:
        """
        Returns all saved iterations from the optimizer and final estimate
        as a pandas.DataFrame which contains all optimizer outputs, i.e.,
        the value for `lp__` as well as all Stan program variables.

        """
        if not self._save_iterations:
            get_logger().warning(
                'Intermediate iterations not saved to CSV output file. '
                'Rerun the optimize method with "save_iterations=True".'
            )
            return None
        if not self.converged:
            get_logger().warning(
                'Invalid estimate, optimization failed to converge.'
            )
        return pd.DataFrame(self._all_iters, columns=self.column_names)

    @property
    def optimized_params_dict(self) -> dict[str, np.float64]:
        """
        Returns all estimates from the optimizer, including `lp__` as a
        Python Dict.  Only returns estimate from final iteration.
        """
        if not self.runset._check_retcodes():
            get_logger().warning(
                'Invalid estimate, optimization failed to converge.'
            )
        return OrderedDict(zip(self.column_names, self._mle))

    def stan_variable(
        self,
        var: str,
        *,
        inc_iterations: bool = False,
        warn: bool = True,
    ) -> Union[np.ndarray, float]:
        """
        Return a numpy.ndarray which contains the estimates for the
        for the named Stan program variable where the dimensions of the
        numpy.ndarray match the shape of the Stan program variable.

        This functionaltiy is also available via a shortcut using ``.`` -
        writing ``fit.a`` is a synonym for ``fit.stan_variable("a")``

        :param var: variable name

        :param inc_iterations: When ``True`` and the intermediate estimates
            are included in the output, i.e., the optimizer was run with
            ``save_iterations=True``, then intermediate estimates are included.
            Default value is ``False``.

        See Also
        --------
        CmdStanMLE.stan_variables
        CmdStanMCMC.stan_variable
        CmdStanPathfinder.stan_variable
        CmdStanVB.stan_variable
        CmdStanGQ.stan_variable
        CmdStanLaplace.stan_variable
        """
        if var not in self._metadata.stan_vars:
            raise ValueError(
                f'Unknown variable name: {var}\n'
                'Available variables are ' + ", ".join(self._metadata.stan_vars)
            )
        if warn and inc_iterations and not self._save_iterations:
            get_logger().warning(
                'Intermediate iterations not saved to CSV output file. '
                'Rerun the optimize method with "save_iterations=True".'
            )
        if warn and not self.runset._check_retcodes():
            get_logger().warning(
                'Invalid estimate, optimization failed to converge.'
            )
        if inc_iterations and self._save_iterations:
            data = self._all_iters
        else:
            data = self._mle

        try:
            out: np.ndarray = self._metadata.stan_vars[var].extract_reshape(
                data
            )
            # TODO(2.0) remove
            if out.shape == () or out.shape == (1,):
                get_logger().warning(
                    "The default behavior of CmdStanMLE.stan_variable() "
                    "will change in a future release to always return a "
                    "numpy.ndarray, even for scalar variables."
                )
                return out.item()  # type: ignore
            return out
        except KeyError:
            # pylint: disable=raise-missing-from
            raise ValueError(
                f'Unknown variable name: {var}\n'
                'Available variables are '
                + ", ".join(self._metadata.stan_vars.keys())
            )

    def stan_variables(
        self, inc_iterations: bool = False
    ) -> dict[str, Union[np.ndarray, float]]:
        """
        Return a dictionary mapping Stan program variables names
        to the corresponding numpy.ndarray containing the inferred values.

        :param inc_iterations: When ``True`` and the intermediate estimates
            are included in the output, i.e., the optimizer was run with
            ``save_iterations=True``, then intermediate estimates are included.
            Default value is ``False``.


        See Also
        --------
        CmdStanMLE.stan_variable
        CmdStanMCMC.stan_variables
        CmdStanPathfinder.stan_variables
        CmdStanVB.stan_variables
        CmdStanGQ.stan_variables
        CmdStanLaplace.stan_variables
        """
        if not self.runset._check_retcodes():
            get_logger().warning(
                'Invalid estimate, optimization failed to converge.'
            )
        result = {}
        for name in self._metadata.stan_vars:
            result[name] = self.stan_variable(
                name, inc_iterations=inc_iterations, warn=False
            )
        return result

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
        self.runset.save_csvfiles(dir)
