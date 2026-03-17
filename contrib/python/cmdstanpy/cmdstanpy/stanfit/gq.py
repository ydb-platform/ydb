"""
Container for the result of running the
generate quantities (GQ) method
"""

from collections import Counter
from typing import (
    Any,
    Generic,
    Hashable,
    MutableMapping,
    NoReturn,
    Optional,
    TypeVar,
    Union,
    overload,
)

import numpy as np
import pandas as pd

try:
    import xarray as xr

    XARRAY_INSTALLED = True
except ImportError:
    XARRAY_INSTALLED = False


from cmdstanpy.cmdstan_args import Method
from cmdstanpy.utils import (
    build_xarray_data,
    flatten_chains,
    get_logger,
    stancsv,
)

from .mcmc import CmdStanMCMC
from .metadata import InferenceMetadata
from .mle import CmdStanMLE
from .runset import RunSet
from .vb import CmdStanVB

Fit = TypeVar('Fit', CmdStanMCMC, CmdStanMLE, CmdStanVB)


class CmdStanGQ(Generic[Fit]):
    """
    Container for outputs from CmdStan generate_quantities run.
    Created by :meth:`CmdStanModel.generate_quantities`.
    """

    def __init__(
        self,
        runset: RunSet,
        previous_fit: Fit,
    ) -> None:
        """Initialize object."""
        if not runset.method == Method.GENERATE_QUANTITIES:
            raise ValueError(
                'Wrong runset method, expecting generate_quantities runset, '
                'found method {}'.format(runset.method)
            )
        self.runset = runset

        self.previous_fit: Fit = previous_fit

        self._draws: np.ndarray = np.array(())
        self._metadata = self._validate_csv_files()

    def __repr__(self) -> str:
        repr = 'CmdStanGQ: model={} chains={}{}'.format(
            self.runset.model,
            self.chains,
            self.runset._args.method_args.compose(0, cmd=[]),
        )
        repr = '{}\n csv_files:\n\t{}\n output_files:\n\t{}'.format(
            repr,
            '\n\t'.join(self.runset.csv_files),
            '\n\t'.join(self.runset.stdout_files),
        )
        return repr

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
        # for details. We call _assemble_generated_quantities to ensure
        # the data are loaded prior to serialization.
        self._assemble_generated_quantities()
        return self.__dict__

    def _validate_csv_files(self) -> InferenceMetadata:
        """
        Checks that Stan CSV output files for all chains are consistent
        and returns InferenceMetadata object containing config and column names.

        Raises exception if inconsistencies are detected.
        """
        excluded_fields = {
            'id',
            'fitted_params',
            'diagnostic_file',
            'metric_file',
            'profile_file',
            'init',
            'seed',
            'start_datetime',
        }
        meta0 = InferenceMetadata.from_csv(self.runset.csv_files[0])
        for i in range(1, self.chains):
            meta = InferenceMetadata.from_csv(self.runset.csv_files[i])
            for key in set(meta._cmdstan_config.keys()) - excluded_fields:
                if meta0[key] != meta[key]:
                    raise ValueError(
                        'CmdStan config mismatch in Stan CSV file {}: '
                        'arg {} is {}, expected {}'.format(
                            self.runset.csv_files[i],
                            key,
                            meta0[key],
                            meta[key],
                        )
                    )
        return meta0

    @property
    def chains(self) -> int:
        """Number of chains."""
        return self.runset.chains

    @property
    def chain_ids(self) -> list[int]:
        """Chain ids."""
        return self.runset.chain_ids

    @property
    def column_names(self) -> tuple[str, ...]:
        """
        Names of generated quantities of interest.
        """
        return self._metadata.column_names

    @property
    def metadata(self) -> InferenceMetadata:
        """
        Returns object which contains CmdStan configuration as well as
        information about the names and structure of the inference method
        and model output variables.
        """
        return self._metadata

    def draws(
        self,
        *,
        inc_warmup: bool = False,
        inc_iterations: bool = False,
        concat_chains: bool = False,
        inc_sample: bool = False,
    ) -> np.ndarray:
        """
        Returns a numpy.ndarray over the generated quantities draws from
        all chains which is stored column major so that the values
        for a parameter are contiguous in memory, likewise all draws from
        a chain are contiguous.  By default, returns a 3D array arranged
        (draws, chains, columns); parameter ``concat_chains=True`` will
        return a 2D array where all chains are flattened into a single column,
        preserving chain order, so that given M chains of N draws,
        the first N draws are from chain 1, ..., and the the last N draws
        are from chain M.

        :param inc_warmup: When ``True`` and the warmup draws are present in
            the output, i.e., the sampler was run with ``save_warmup=True``,
            then the warmup draws are included.  Default value is ``False``.

        :param concat_chains: When ``True`` return a 2D array flattening all
            all draws from all chains.  Default value is ``False``.

        :param inc_sample: When ``True`` include all columns in the previous_fit
            draws array as well, excepting columns for variables already present
            in the generated quantities drawset. Default value is ``False``.

        See Also
        --------
        CmdStanGQ.draws_pd
        CmdStanGQ.draws_xr
        CmdStanMCMC.draws
        """
        self._assemble_generated_quantities()
        inc_warmup |= inc_iterations
        if inc_warmup:
            if (
                isinstance(self.previous_fit, CmdStanMCMC)
                and not self.previous_fit._save_warmup
            ):
                get_logger().warning(
                    "Sample doesn't contain draws from warmup iterations,"
                    ' rerun sampler with "save_warmup=True".'
                )
            elif (
                isinstance(self.previous_fit, CmdStanMLE)
                and not self.previous_fit._save_iterations
            ):
                get_logger().warning(
                    "MLE doesn't contain draws from pre-convergence iterations,"
                    ' rerun optimization with "save_iterations=True".'
                )
            elif isinstance(self.previous_fit, CmdStanVB):
                get_logger().warning(
                    "Variational fit doesn't make sense with argument "
                    '"inc_warmup=True"'
                )

        if inc_sample:
            cols_1 = self.previous_fit.column_names
            cols_2 = self.column_names
            dups = [
                item
                for item, count in Counter(cols_1 + cols_2).items()
                if count > 1
            ]
            drop_cols: list[int] = []
            for dup in dups:
                drop_cols.extend(
                    self.previous_fit._metadata.stan_vars[dup].columns()
                )

        start_idx, _ = self._draws_start(inc_warmup)
        previous_draws = self._previous_draws(True)
        if concat_chains and inc_sample:
            return flatten_chains(
                np.dstack(
                    (
                        np.delete(previous_draws, drop_cols, axis=1),
                        self._draws,
                    )
                )[start_idx:, :, :]
            )
        if concat_chains:
            return flatten_chains(self._draws[start_idx:, :, :])
        if inc_sample:
            return np.dstack(
                (
                    np.delete(previous_draws, drop_cols, axis=1),
                    self._draws,
                )
            )[start_idx:, :, :]
        return self._draws[start_idx:, :, :]

    def draws_pd(
        self,
        vars: Union[list[str], str, None] = None,
        inc_warmup: bool = False,
        inc_sample: bool = False,
    ) -> pd.DataFrame:
        """
        Returns the generated quantities draws as a pandas DataFrame.
        Flattens all chains into single column.  Container variables
        (array, vector, matrix) will span multiple columns, one column
        per element. E.g. variable 'matrix[2,2] foo' spans 4 columns:
        'foo[1,1], ... foo[2,2]'.

        :param vars: optional list of variable names.

        :param inc_warmup: When ``True`` and the warmup draws are present in
            the output, i.e., the sampler was run with ``save_warmup=True``,
            then the warmup draws are included.  Default value is ``False``.

        See Also
        --------
        CmdStanGQ.draws
        CmdStanGQ.draws_xr
        CmdStanMCMC.draws_pd
        """
        if vars is not None:
            if isinstance(vars, str):
                vars_list = [vars]
            else:
                vars_list = vars

            vars_list = list(dict.fromkeys(vars_list))

        if inc_warmup:
            if (
                isinstance(self.previous_fit, CmdStanMCMC)
                and not self.previous_fit._save_warmup
            ):
                get_logger().warning(
                    "Sample doesn't contain draws from warmup iterations,"
                    ' rerun sampler with "save_warmup=True".'
                )
            elif (
                isinstance(self.previous_fit, CmdStanMLE)
                and not self.previous_fit._save_iterations
            ):
                get_logger().warning(
                    "MLE doesn't contain draws from pre-convergence iterations,"
                    ' rerun optimization with "save_iterations=True".'
                )
            elif isinstance(self.previous_fit, CmdStanVB):
                get_logger().warning(
                    "Variational fit doesn't make sense with argument "
                    '"inc_warmup=True"'
                )

        self._assemble_generated_quantities()

        all_columns = ['chain__', 'iter__', 'draw__'] + list(self.column_names)

        gq_cols: list[str] = []
        mcmc_vars: list[str] = []
        if vars is not None:
            for var in vars_list:
                if var in self._metadata.stan_vars:
                    info = self._metadata.stan_vars[var]
                    gq_cols.extend(
                        self.column_names[info.start_idx : info.end_idx]
                    )
                elif (
                    inc_sample and var in self.previous_fit._metadata.stan_vars
                ):
                    info = self.previous_fit._metadata.stan_vars[var]
                    mcmc_vars.extend(
                        self.previous_fit.column_names[
                            info.start_idx : info.end_idx
                        ]
                    )
                elif var in ['chain__', 'iter__', 'draw__']:
                    gq_cols.append(var)
                else:
                    raise ValueError('Unknown variable: {}'.format(var))
        else:
            gq_cols = all_columns
            vars_list = gq_cols

        previous_draws_pd = self._previous_draws_pd(mcmc_vars, inc_warmup)

        draws = self.draws(inc_warmup=inc_warmup)
        # add long-form columns for chain, iteration, draw
        n_draws, n_chains, _ = draws.shape
        chains_col = (
            np.repeat(np.arange(1, n_chains + 1), n_draws)
            .reshape(1, n_chains, n_draws)
            .T
        )
        iter_col = (
            np.tile(np.arange(1, n_draws + 1), n_chains)
            .reshape(1, n_chains, n_draws)
            .T
        )
        draw_col = (
            np.arange(1, (n_draws * n_chains) + 1)
            .reshape(1, n_chains, n_draws)
            .T
        )
        draws = np.concatenate([chains_col, iter_col, draw_col, draws], axis=2)

        draws_pd = pd.DataFrame(
            data=flatten_chains(draws),
            columns=all_columns,
        )

        if inc_sample and mcmc_vars:
            if gq_cols:
                return pd.concat(
                    [
                        previous_draws_pd,
                        draws_pd[gq_cols],
                    ],
                    axis='columns',
                )[vars_list]
            else:
                return previous_draws_pd
        elif inc_sample and vars is None:
            cols_1 = list(previous_draws_pd.columns)
            cols_2 = list(draws_pd.columns)
            dups = [
                item
                for item, count in Counter(cols_1 + cols_2).items()
                if count > 1
            ]
            return pd.concat(
                [
                    previous_draws_pd.drop(columns=dups).reset_index(drop=True),
                    draws_pd,
                ],
                axis=1,
            )
        elif gq_cols:
            return draws_pd[gq_cols]

        return draws_pd

    @overload
    def draws_xr(
        self: Union["CmdStanGQ[CmdStanMLE]", "CmdStanGQ[CmdStanVB]"],
        vars: Union[str, list[str], None] = None,
        inc_warmup: bool = False,
        inc_sample: bool = False,
    ) -> NoReturn:
        ...

    @overload
    def draws_xr(
        self: "CmdStanGQ[CmdStanMCMC]",
        vars: Union[str, list[str], None] = None,
        inc_warmup: bool = False,
        inc_sample: bool = False,
    ) -> "xr.Dataset":
        ...

    def draws_xr(
        self,
        vars: Union[str, list[str], None] = None,
        inc_warmup: bool = False,
        inc_sample: bool = False,
    ) -> "xr.Dataset":
        """
        Returns the generated quantities draws as a xarray Dataset.

        This method can only be called when the underlying fit was made
        through sampling, it cannot be used on MLE or VB outputs.

        :param vars: optional list of variable names.

        :param inc_warmup: When ``True`` and the warmup draws are present in
            the MCMC sample, then the warmup draws are included.
            Default value is ``False``.

        See Also
        --------
        CmdStanGQ.draws
        CmdStanGQ.draws_pd
        CmdStanMCMC.draws_xr
        """
        if not XARRAY_INSTALLED:
            raise RuntimeError(
                'Package "xarray" is not installed, cannot produce draws array.'
            )
        if not isinstance(self.previous_fit, CmdStanMCMC):
            raise RuntimeError(
                'Method "draws_xr" is only available when '
                'original fit is done via Sampling.'
            )
        mcmc_vars_list = []
        dup_vars = []
        if vars is not None:
            if isinstance(vars, str):
                vars_list = [vars]
            else:
                vars_list = vars
            for var in vars_list:
                if var not in self._metadata.stan_vars:
                    if inc_sample and (
                        var in self.previous_fit._metadata.stan_vars
                    ):
                        mcmc_vars_list.append(var)
                        dup_vars.append(var)
                    else:
                        raise ValueError('Unknown variable: {}'.format(var))
        else:
            vars_list = list(self._metadata.stan_vars.keys())
            if inc_sample:
                for var in self.previous_fit._metadata.stan_vars.keys():
                    if var not in vars_list and var not in mcmc_vars_list:
                        mcmc_vars_list.append(var)
        for var in dup_vars:
            vars_list.remove(var)

        self._assemble_generated_quantities()

        num_draws = self.previous_fit.num_draws_sampling
        sample_config = self.previous_fit._metadata.cmdstan_config
        attrs: MutableMapping[Hashable, Any] = {
            "stan_version": f"{sample_config['stan_version_major']}."
            f"{sample_config['stan_version_minor']}."
            f"{sample_config['stan_version_patch']}",
            "model": sample_config["model"],
            "num_draws_sampling": num_draws,
        }
        if inc_warmup and sample_config['save_warmup']:
            num_draws += self.previous_fit.num_draws_warmup
            attrs["num_draws_warmup"] = self.previous_fit.num_draws_warmup

        data: MutableMapping[Hashable, Any] = {}
        coordinates: MutableMapping[Hashable, Any] = {
            "chain": self.chain_ids,
            "draw": np.arange(num_draws),
        }

        for var in vars_list:
            build_xarray_data(
                data,
                self._metadata.stan_vars[var],
                self.draws(inc_warmup=inc_warmup),
            )
        if inc_sample:
            for var in mcmc_vars_list:
                build_xarray_data(
                    data,
                    self.previous_fit._metadata.stan_vars[var],
                    self.previous_fit.draws(inc_warmup=inc_warmup),
                )

        return xr.Dataset(data, coords=coordinates, attrs=attrs).transpose(
            'chain', 'draw', ...
        )

    def stan_variable(self, var: str, **kwargs: bool) -> np.ndarray:
        """
        Return a numpy.ndarray which contains the set of draws
        for the named Stan program variable.  Flattens the chains,
        leaving the draws in chain order.  The first array dimension,
        corresponds to number of draws in the sample.
        The remaining dimensions correspond to
        the shape of the Stan program variable.

        Underlyingly draws are in chain order, i.e., for a sample with
        N chains of M draws each, the first M array elements are from chain 1,
        the next M are from chain 2, and the last M elements are from chain N.

        * If the variable is a scalar variable, the return array has shape
          ( draws * chains, 1).
        * If the variable is a vector, the return array has shape
          ( draws * chains, len(vector))
        * If the variable is a matrix, the return array has shape
          ( draws * chains, size(dim 1), size(dim 2) )
        * If the variable is an array with N dimensions, the return array
          has shape ( draws * chains, size(dim 1), ..., size(dim N))

        For example, if the Stan program variable ``theta`` is a 3x3 matrix,
        and the sample consists of 4 chains with 1000 post-warmup draws,
        this function will return a numpy.ndarray with shape (4000,3,3).

        This functionaltiy is also available via a shortcut using ``.`` -
        writing ``fit.a`` is a synonym for ``fit.stan_variable("a")``

        :param var: variable name

        :param kwargs: Additional keyword arguments are passed to the underlying
            fit's ``stan_variable`` method if the variable is not a generated
            quantity.

        See Also
        --------
        CmdStanGQ.stan_variables
        CmdStanMCMC.stan_variable
        CmdStanMLE.stan_variable
        CmdStanPathfinder.stan_variable
        CmdStanVB.stan_variable
        CmdStanLaplace.stan_variable
        """
        model_var_names = self.previous_fit._metadata.stan_vars.keys()
        gq_var_names = self._metadata.stan_vars.keys()
        if not (var in model_var_names or var in gq_var_names):
            raise ValueError(
                f'Unknown variable name: {var}\n'
                'Available variables are '
                + ", ".join(model_var_names | gq_var_names)
            )
        if var not in gq_var_names:
            # TODO(2.0) atleast1d may not be needed
            return np.atleast_1d(  # type: ignore
                self.previous_fit.stan_variable(var, **kwargs)
            )

        # is gq variable
        self._assemble_generated_quantities()

        draw1, _ = self._draws_start(
            inc_warmup=kwargs.get('inc_warmup', False)
            or kwargs.get('inc_iterations', False)
        )
        draws = flatten_chains(self._draws[draw1:])
        out: np.ndarray = self._metadata.stan_vars[var].extract_reshape(draws)
        return out

    def stan_variables(self, **kwargs: bool) -> dict[str, np.ndarray]:
        """
        Return a dictionary mapping Stan program variables names
        to the corresponding numpy.ndarray containing the inferred values.

        :param kwargs: Additional keyword arguments are passed to the underlying
            fit's ``stan_variable`` method if the variable is not a generated
            quantity.

        See Also
        --------
        CmdStanGQ.stan_variable
        CmdStanMCMC.stan_variables
        CmdStanMLE.stan_variables
        CmdStanPathfinder.stan_variables
        CmdStanVB.stan_variables
        CmdStanLaplace.stan_variables
        """
        result = {}
        sample_var_names = self.previous_fit._metadata.stan_vars.keys()
        gq_var_names = self._metadata.stan_vars.keys()
        for name in gq_var_names:
            result[name] = self.stan_variable(name, **kwargs)
        for name in sample_var_names:
            if name not in gq_var_names:
                result[name] = self.stan_variable(name, **kwargs)
        return result

    def _assemble_generated_quantities(self) -> None:
        if self._draws.shape != (0,):
            return
        # use numpy loadtxt
        _, num_draws = self._draws_start(inc_warmup=True)

        gq_sample: np.ndarray = np.empty(
            (num_draws, self.chains, len(self.column_names)),
            dtype=float,
            order='F',
        )
        for chain in range(self.chains):
            csv_file = self.runset.csv_files[chain]
            try:
                *_, draws = stancsv.parse_comments_header_and_draws(
                    self.runset.csv_files[chain]
                )
                gq_sample[:, chain, :] = stancsv.csv_bytes_list_to_numpy(draws)
            except Exception as exc:
                raise ValueError(
                    f"An error occurred when parsing Stan csv {csv_file}"
                    f" for chain {chain}"
                ) from exc
        self._draws = gq_sample

    def _draws_start(self, inc_warmup: bool) -> tuple[int, int]:
        draw1 = 0
        p_fit = self.previous_fit
        if isinstance(p_fit, CmdStanMCMC):
            num_draws = p_fit.num_draws_sampling
            if p_fit._save_warmup:
                if inc_warmup:
                    num_draws += p_fit.num_draws_warmup
                else:
                    draw1 = p_fit.num_draws_warmup

        elif isinstance(p_fit, CmdStanMLE):
            num_draws = 1
            if p_fit._save_iterations:
                opt_iters = len(p_fit.optimized_iterations_np)  # type: ignore
                if inc_warmup:
                    num_draws = opt_iters
                else:
                    draw1 = opt_iters - 1
        else:  # CmdStanVB:
            draw1 = 1  # skip mean
            num_draws = p_fit.variational_sample.shape[0]
            if inc_warmup:
                num_draws += 1

        return draw1, num_draws

    def _previous_draws(self, inc_warmup: bool) -> np.ndarray:
        """
        Extract the draws from self.previous_fit.
        Return is always 3-d
        """
        p_fit = self.previous_fit
        if isinstance(p_fit, CmdStanMCMC):
            return p_fit.draws(inc_warmup=inc_warmup)
        elif isinstance(p_fit, CmdStanMLE):
            if inc_warmup and p_fit._save_iterations:
                return p_fit.optimized_iterations_np[:, None]  # type: ignore

            return np.atleast_2d(  # type: ignore
                p_fit.optimized_params_np,
            )[:, None]
        else:  # CmdStanVB:
            if inc_warmup:
                return np.vstack(
                    [p_fit.variational_params_np, p_fit.variational_sample]
                )[:, None]
            return p_fit.variational_sample[:, None]

    def _previous_draws_pd(
        self, vars: list[str], inc_warmup: bool
    ) -> pd.DataFrame:
        if vars:
            sel: Union[list[str], slice] = vars
        else:
            sel = slice(None, None)

        p_fit = self.previous_fit
        if isinstance(p_fit, CmdStanMCMC):
            return p_fit.draws_pd(vars or None, inc_warmup=inc_warmup)

        elif isinstance(p_fit, CmdStanMLE):
            if inc_warmup and p_fit._save_iterations:
                return p_fit.optimized_iterations_pd[sel]  # type: ignore
            else:
                return p_fit.optimized_params_pd[sel]
        else:  # CmdStanVB:
            return p_fit.variational_sample_pd[sel]

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

    # TODO(2.0): remove
    @property
    def mcmc_sample(self) -> Union[CmdStanMCMC, CmdStanMLE, CmdStanVB]:
        get_logger().warning(
            "Property `mcmc_sample` is deprecated, use `previous_fit` instead"
        )
        return self.previous_fit
