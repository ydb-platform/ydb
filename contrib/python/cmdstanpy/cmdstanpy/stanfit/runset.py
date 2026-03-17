"""
Container for the information used in a generic CmdStan run,
such as file locations
"""

import os
import re
import shutil
import tempfile
from datetime import datetime
from time import time
from typing import Optional

from cmdstanpy import _TMPDIR
from cmdstanpy.cmdstan_args import CmdStanArgs, Method
from cmdstanpy.utils import get_logger


class RunSet:
    """
    Encapsulates the configuration and results of a call to any CmdStan
    inference method. Records the method return code and locations of
    all console, error, and output files.

    RunSet objects are instantiated by the CmdStanModel class inference methods
    which validate all inputs, therefore "__init__" method skips input checks.
    """

    def __init__(
        self,
        args: CmdStanArgs,
        chains: int = 1,
        *,
        chain_ids: Optional[list[int]] = None,
        time_fmt: str = "%Y%m%d%H%M%S",
        one_process_per_chain: bool = True,
    ) -> None:
        """Initialize object (no input arg checks)."""
        self._args = args
        self._chains = chains
        self._one_process_per_chain = one_process_per_chain
        if one_process_per_chain:
            self._num_procs = chains
        else:
            self._num_procs = 1
        self._retcodes = [-1 for _ in range(self._num_procs)]
        self._timeout_flags = [False for _ in range(self._num_procs)]
        if chain_ids is None:
            chain_ids = [i + 1 for i in range(chains)]
        self._chain_ids = chain_ids

        if args.output_dir is not None:
            self._output_dir = args.output_dir
        else:
            # make a per-run subdirectory of our master temp directory
            self._output_dir = tempfile.mkdtemp(
                prefix=args.model_name, dir=_TMPDIR
            )

        # output files prefix: ``<model_name>-<YYYYMMDDHHMM>_<chain_id>``
        self._base_outfile = (
            f'{args.model_name}-{datetime.now().strftime(time_fmt)}'
        )
        # per-process outputs
        self._stdout_files = [''] * self._num_procs
        self._profile_files = [''] * self._num_procs  # optional
        if one_process_per_chain:
            for i in range(chains):
                self._stdout_files[i] = self.file_path("-stdout.txt", id=i)
                if args.save_profile:
                    self._profile_files[i] = self.file_path(
                        ".csv", extra="-profile", id=chain_ids[i]
                    )
        else:
            self._stdout_files[0] = self.file_path("-stdout.txt")
            if args.save_profile:
                self._profile_files[0] = self.file_path(
                    ".csv", extra="-profile"
                )

        # per-chain output files
        self._csv_files: list[str] = [''] * chains
        self._diagnostic_files = [''] * chains  # optional

        if chains == 1:
            self._csv_files[0] = self.file_path(".csv")
            if args.save_latent_dynamics:
                self._diagnostic_files[0] = self.file_path(
                    ".csv", extra="-diagnostic"
                )
        else:
            for i in range(chains):
                self._csv_files[i] = self.file_path(".csv", id=chain_ids[i])
                if args.save_latent_dynamics:
                    self._diagnostic_files[i] = self.file_path(
                        ".csv", extra="-diagnostic", id=chain_ids[i]
                    )

    def __repr__(self) -> str:
        repr = 'RunSet: chains={}, chain_ids={}, num_processes={}'.format(
            self._chains, self._chain_ids, self._num_procs
        )
        repr = '{}\n cmd (chain 1):\n\t{}'.format(repr, self.cmd(0))
        repr = '{}\n retcodes={}'.format(repr, self._retcodes)
        repr = f'{repr}\n per-chain output files (showing chain 1 only):'
        repr = '{}\n csv_file:\n\t{}'.format(repr, self._csv_files[0])
        if self._args.save_latent_dynamics:
            repr = '{}\n diagnostics_file:\n\t{}'.format(
                repr, self._diagnostic_files[0]
            )
        if self._args.save_profile:
            repr = '{}\n profile_file:\n\t{}'.format(
                repr, self._profile_files[0]
            )
        repr = '{}\n console_msgs (if any):\n\t{}'.format(
            repr, self._stdout_files[0]
        )
        return repr

    @property
    def model(self) -> str:
        """Stan model name."""
        return self._args.model_name

    @property
    def method(self) -> Method:
        """CmdStan method used to generate this fit."""
        return self._args.method

    @property
    def num_procs(self) -> int:
        """Number of processes run."""
        return self._num_procs

    @property
    def one_process_per_chain(self) -> bool:
        """
        When True, for each chain, call CmdStan in its own subprocess.
        When False, use CmdStan's `num_chains` arg to run parallel chains.
        Always True if CmdStan < 2.28.
        For CmdStan 2.28 and up, `sample` method determines value.
        """
        return self._one_process_per_chain

    @property
    def chains(self) -> int:
        """Number of chains."""
        return self._chains

    @property
    def chain_ids(self) -> list[int]:
        """Chain ids."""
        return self._chain_ids

    def cmd(self, idx: int) -> list[str]:
        """
        Assemble CmdStan invocation.
        When running parallel chains from single process (2.28 and up),
        specify CmdStan arg `num_chains` and leave chain idx off CSV files.
        """
        if self._one_process_per_chain:
            return self._args.compose_command(
                idx,
                csv_file=self.csv_files[idx],
                diagnostic_file=self.diagnostic_files[idx]
                if self._args.save_latent_dynamics
                else None,
                profile_file=self.profile_files[idx]
                if self._args.save_profile
                else None,
            )
        else:
            return self._args.compose_command(
                idx,
                csv_file=self.file_path('.csv'),
                diagnostic_file=self.file_path(".csv", extra="-diagnostic")
                if self._args.save_latent_dynamics
                else None,
                profile_file=self.file_path(".csv", extra="-profile")
                if self._args.save_profile
                else None,
            )

    @property
    def csv_files(self) -> list[str]:
        """List of paths to CmdStan output files."""
        return self._csv_files

    @property
    def stdout_files(self) -> list[str]:
        """
        List of paths to transcript of CmdStan messages sent to the console.
        Transcripts include config information, progress, and error messages.
        """
        return self._stdout_files

    def _check_retcodes(self) -> bool:
        """Returns ``True`` when all chains have retcode 0."""
        for code in self._retcodes:
            if code != 0:
                return False
        return True

    @property
    def diagnostic_files(self) -> list[str]:
        """List of paths to CmdStan hamiltonian diagnostic files."""
        return self._diagnostic_files

    @property
    def profile_files(self) -> list[str]:
        """List of paths to CmdStan profiler files."""
        return self._profile_files

    # pylint: disable=invalid-name
    def file_path(
        self, suffix: str, *, extra: str = "", id: Optional[int] = None
    ) -> str:
        if id is not None:
            suffix = f"_{id}{suffix}"
        file = os.path.join(
            self._output_dir, f"{self._base_outfile}{extra}{suffix}"
        )
        return file

    def _retcode(self, idx: int) -> int:
        """Get retcode for process[idx]."""
        return self._retcodes[idx]

    def _set_retcode(self, idx: int, val: int) -> None:
        """Set retcode at process[idx] to val."""
        self._retcodes[idx] = val

    def _set_timeout_flag(self, idx: int, val: bool) -> None:
        """Set timeout_flag at process[idx] to val."""
        self._timeout_flags[idx] = val

    def get_err_msgs(self) -> str:
        """Checks console messages for each CmdStan run."""
        msgs = []
        for i in range(self._num_procs):
            if (
                os.path.exists(self._stdout_files[i])
                and os.stat(self._stdout_files[i]).st_size > 0
            ):
                if self._args.method == Method.OPTIMIZE:
                    msgs.append('console log output:\n')
                    with open(self._stdout_files[0], 'r') as fd:
                        msgs.append(fd.read())
                else:
                    with open(self._stdout_files[i], 'r') as fd:
                        contents = fd.read()
                        # pattern matches initial "Exception" or "Error" msg
                        pat = re.compile(r'^E[rx].*$', re.M)
                        errors = re.findall(pat, contents)
                        if len(errors) > 0:
                            msgs.append('\n\t'.join(errors))
        return '\n'.join(msgs)

    def save_csvfiles(self, dir: Optional[str] = None) -> None:
        """
        Moves CSV files to specified directory.

        :param dir: directory path

        See Also
        --------
        cmdstanpy.from_csv
        """
        if dir is None:
            dir = os.path.realpath('.')
        test_path = os.path.join(dir, str(time()))
        try:
            os.makedirs(dir, exist_ok=True)
            with open(test_path, 'w'):
                pass
            os.remove(test_path)  # cleanup
        except (IOError, OSError, PermissionError) as exc:
            raise RuntimeError('Cannot save to path: {}'.format(dir)) from exc

        for i in range(self.chains):
            if not os.path.exists(self._csv_files[i]):
                raise ValueError(
                    'Cannot access CSV file {}'.format(self._csv_files[i])
                )

            to_path = os.path.join(dir, os.path.basename(self._csv_files[i]))
            if os.path.exists(to_path):
                raise ValueError(
                    'File exists, not overwriting: {}'.format(to_path)
                )
            try:
                get_logger().debug(
                    'saving tmpfile: "%s" as: "%s"', self._csv_files[i], to_path
                )
                shutil.move(self._csv_files[i], to_path)
                self._csv_files[i] = to_path
            except (IOError, OSError, PermissionError) as e:
                raise ValueError(
                    'Cannot save to file: {}'.format(to_path)
                ) from e

    def raise_for_timeouts(self) -> None:
        if any(self._timeout_flags):
            raise TimeoutError(
                f"{sum(self._timeout_flags)} of {self.num_procs} "
                "processes timed out"
            )
