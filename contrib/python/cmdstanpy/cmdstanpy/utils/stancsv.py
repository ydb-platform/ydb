"""
Utility functions for reading the Stan CSV format
"""

import io
import json
import math
import os
import re
import warnings
from typing import Any, Iterator, Mapping, Optional, Union

import numpy as np
import numpy.typing as npt

from cmdstanpy import _CMDSTAN_SAMPLING, _CMDSTAN_THIN, _CMDSTAN_WARMUP


def parse_comments_header_and_draws(
    stan_csv: Union[str, os.PathLike, Iterator[bytes]],
) -> tuple[list[bytes], Optional[str], list[bytes]]:
    """Parses lines of a Stan CSV file into comment lines, the header line,
    and draws lines.

    Returns a (comment_lines, header, draws_lines) tuple.
    """

    def partition_csv(
        lines: Iterator[bytes],
    ) -> tuple[list[bytes], Optional[str], list[bytes]]:
        comment_lines: list[bytes] = []
        draws_lines: list[bytes] = []
        header = None
        for line in lines:
            if line.startswith(b"#"):  # is comment line
                comment_lines.append(line)
            elif header is None:  # Assumes the header is the first non-comment
                header = line.strip().decode()
            else:
                draws_lines.append(line)
        return comment_lines, header, draws_lines

    if isinstance(stan_csv, (str, os.PathLike)):
        with open(stan_csv, "rb") as f:
            return partition_csv(f)
    else:
        return partition_csv(stan_csv)


def filter_csv_bytes_by_columns(
    csv_bytes_list: list[bytes], indexes_to_keep: list[int]
) -> list[bytes]:
    """Given the list of bytes representing the lines of a CSV file
    and the indexes of columns to keep, will return a new list of bytes
    containing only those columns in the index order provided. Assumes
    column-delimited columns."""
    out = []
    for dl in csv_bytes_list:
        split = dl.strip().split(b",")
        out.append(b",".join(split[i] for i in indexes_to_keep) + b"\n")
    return out


def csv_bytes_list_to_numpy(
    csv_bytes_list: list[bytes],
) -> npt.NDArray[np.float64]:
    """Efficiently converts a list of bytes representing whose concatenation
    represents a CSV file into a numpy array.

    Returns a 2D numpy array with shape (n_rows, n_cols). If no data is found,
    returns an empty array with shape (0, 0)."""
    if not csv_bytes_list:
        return np.empty((0, 0))
    num_cols = csv_bytes_list[0].count(b",") + 1
    try:
        import polars as pl

        try:
            out: npt.NDArray[np.float64] = (
                pl.read_csv(
                    io.BytesIO(b"".join(csv_bytes_list)),
                    has_header=False,
                    schema_overrides=[pl.Float64] * num_cols,
                    infer_schema=False,
                )
                .to_numpy()
                .astype(np.float64)
            )
        except pl.exceptions.NoDataError:
            return np.empty((0, 0))
    except ImportError:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            out = np.loadtxt(
                csv_bytes_list,
                delimiter=",",
                dtype=np.float64,
                ndmin=2,
            )
        if out.shape[0] == 0:  # No data read
            out = np.empty((0, 0))

    return out


def parse_hmc_adaptation_lines(
    comment_lines: list[bytes],
) -> tuple[Optional[float], Optional[npt.NDArray[np.float64]]]:
    """Extracts step size/mass matrix information from the Stan CSV comment
    lines by parsing the adaptation section. If the diag_e metric is used,
    the returned mass matrix will be a 1D array of the diagnoal elements,
    if the dense_e metric is used, it will be a 2D array representing the
    entire matrix, and if unit_e is used then None will be returned.

    Returns a (step_size, mass_matrix) tuple"""
    step_size, mass_matrix = None, None

    cleaned_lines = (ln.lstrip(b"# ") for ln in comment_lines)
    in_matrix_block = False
    diag_e_metric = False
    matrix_lines = []
    for line in cleaned_lines:
        if in_matrix_block and line.strip():
            # Stop when we get to timing block
            if line.startswith(b"Elapsed Time"):
                break
            matrix_lines.append(line)
        elif line.startswith(b"Step size"):
            _, ss_str = line.split(b" = ")
            step_size = float(ss_str)
        elif line.startswith(b"Diagonal") or line.startswith(b"Elements"):
            in_matrix_block = True
        elif line.startswith(b"No free"):
            break
        elif b"diag_e" in line:
            diag_e_metric = True
    if matrix_lines:
        mass_matrix = csv_bytes_list_to_numpy(matrix_lines)
        if diag_e_metric and mass_matrix.shape[0] == 1:
            mass_matrix = mass_matrix[0]
    return step_size, mass_matrix


def extract_key_val_pairs(
    comment_lines: list[bytes], remove_default_text: bool = True
) -> Iterator[tuple[str, str]]:
    """Yields cleaned key = val pairs from stan csv comments.
    Removes '(Default)' text from values if remove_default_text is True."""
    cleaned_lines = (
        line.decode().lstrip("# ").strip() for line in comment_lines
    )
    for line in cleaned_lines:
        split_on_eq = line.split(" = ")
        # Only want lines with key = value
        if len(split_on_eq) != 2:
            continue

        key, val = split_on_eq
        if remove_default_text:
            val = val.replace("(Default)", "").strip()
        yield key, val


def parse_config(
    comment_lines: list[bytes],
) -> dict[str, Union[str, int, float]]:
    """Extracts the key=value config settings from Stan CSV comment
    lines and returns a dictionary."""
    out: dict[str, Union[str, int, float]] = {}
    for key, val in extract_key_val_pairs(comment_lines):
        if key == 'file':
            if not val.endswith('csv'):
                out['data_file'] = val
        else:
            if val == 'true':
                out[key] = 1
            elif val == 'false':
                out[key] = 0
            else:
                for cast in (int, float):
                    try:
                        out[key] = cast(val)
                        break
                    except ValueError:
                        pass
                else:
                    out[key] = val
    return out


def parse_header(header: str) -> tuple[str, ...]:
    """Returns munged variable names from a Stan csv header line"""
    return tuple(munge_varname(name) for name in header.split(","))


def construct_config_header_dict(
    comment_lines: list[bytes], header: Optional[str]
) -> dict[str, Union[str, int, float, tuple[str, ...]]]:
    """Extracts config and header info from comment/draws lines parsed
    from a Stan CSV file."""
    config = parse_config(comment_lines)
    out: dict[str, Union[str, int, float, tuple[str, ...]]] = {**config}
    if header:
        out["raw_header"] = header
        out["column_names"] = parse_header(header)
    return out


def parse_variational_eta(comment_lines: list[bytes]) -> float:
    """Extracts the variational eta parameter from stancsv comment lines"""
    for i, line in enumerate(comment_lines):
        if line.startswith(b"# Stepsize adaptation") and (
            i + 1 < len(comment_lines)  # Ensure i + 1 is in bounds
        ):
            eta_line = comment_lines[i + 1]
            break
    else:
        raise ValueError(
            "Unable to parse eta from Stan CSV, adaptation block not found"
        )

    _, val = eta_line.split(b" = ")
    return float(val)


def extract_max_treedepth_and_divergence_counts(
    header: str, draws_lines: list[bytes], max_treedepth: int, warmup_draws: int
) -> tuple[int, int]:
    """Extracts the max treedepth and divergence counts from the header
    and draw lines of the MCMC stan csv output."""
    if len(draws_lines) <= 1:  # Empty draws
        return 0, 0
    column_names = header.split(",")

    try:
        indexes_to_keep = [
            column_names.index("treedepth__"),
            column_names.index("divergent__"),
        ]
    except ValueError:
        # Throws if treedepth/divergent columns not recorded
        return 0, 0

    sampling_draws = draws_lines[1 + warmup_draws :]

    filtered = filter_csv_bytes_by_columns(sampling_draws, indexes_to_keep)
    arr = csv_bytes_list_to_numpy(filtered).astype(int)

    num_max_treedepth = np.sum(arr[:, 0] == max_treedepth)
    num_divergences = np.sum(arr[:, 1])
    return num_max_treedepth, num_divergences


# TODO: Remove after CmdStan 2.37 is the minimum version
def is_sneaky_fixed_param(header: str) -> bool:
    """Returns True if the header line indicates that the sampler
    ran with the fixed_param sampler automatically, despite the
    algorithm listed as 'hmc'.

    See issue #805"""
    num_dunder_cols = sum(col.endswith("__") for col in header.split(","))

    return (num_dunder_cols < 7) and "lp__" in header


def count_warmup_and_sampling_draws(
    stan_csv: Union[str, os.PathLike, Iterator[bytes]],
) -> tuple[int, int]:
    """Scans through a Stan CSV file to count the number of lines in the
    warmup/sampling blocks to determine counts for warmup and sampling draws.
    """

    def determine_draw_counts(lines: Iterator[bytes]) -> tuple[int, int]:
        is_fixed_param = False
        header_line_idx = None
        adaptation_block_idx = None
        sampling_block_idx = None
        timing_block_idx = None
        for i, line in enumerate(lines):
            if header_line_idx is None:
                if b"fixed_param" in line:
                    is_fixed_param = True
                if line.startswith(b"lp__"):
                    header_line_idx = i
                    if not is_fixed_param:
                        is_fixed_param = is_sneaky_fixed_param(
                            line.strip().decode()
                        )
                continue

            if not is_fixed_param and adaptation_block_idx is None:
                if line.startswith(b"#"):
                    adaptation_block_idx = i
            elif sampling_block_idx is None:
                if not line.startswith(b"#"):
                    sampling_block_idx = i
                elif line.startswith(b"#  Elapsed"):
                    sampling_block_idx = i
                    timing_block_idx = i
            elif timing_block_idx is None:
                if line.startswith(b"#"):
                    timing_block_idx = i
            else:
                break
        else:
            # Will raise if lines exhausts without all blocks being identified
            raise ValueError(
                "Unable to count warmup and sampling draws from Stan csv"
            )

        if is_fixed_param:
            num_warmup = 0
        else:
            num_warmup = (
                adaptation_block_idx - header_line_idx - 1  # type: ignore
            )
        num_sampling = timing_block_idx - sampling_block_idx
        return num_warmup, num_sampling

    if isinstance(stan_csv, (str, os.PathLike)):
        with open(stan_csv, "rb") as f:
            return determine_draw_counts(f)
    else:
        return determine_draw_counts(stan_csv)


def raise_on_inconsistent_draws_shape(
    header: str, draw_lines: list[bytes]
) -> None:
    """Throws a ValueError if any draws are found to have an inconsistent
    shape, i.e. too many/few columns compared to the header"""

    def column_count(ln: bytes) -> int:
        return ln.count(b",") + 1

    # Consider empty draws to be consistent
    if not draw_lines:
        return

    num_cols = column_count(header.encode())
    for i, draw in enumerate(draw_lines, start=1):
        if (draw_size := column_count(draw)) != num_cols:
            raise ValueError(
                f"line {i}: bad draw, expecting {num_cols} items, "
                f"found {draw_size}"
            )


def raise_on_invalid_adaptation_block(comment_lines: list[bytes]) -> None:
    """Throws ValueErrors if the parsed adaptation block is invalid, e.g.
    the metric information is not present, consistent with the rest of
    the file, or the step size info cannot be processed."""

    def column_count(ln: bytes) -> int:
        return ln.count(b",") + 1

    ln_iter = enumerate(comment_lines, start=2)
    metric = None
    for _, line in ln_iter:
        if b"metric =" in line:
            _, val = line.split(b" = ")
            metric = val.replace(b"(Default)", b"").strip().decode()
        if b"Adaptation terminated" in line:
            break
    else:  # No adaptation block found
        raise ValueError("No adaptation block found, expecting metric")

    if metric is None:
        raise ValueError("No reported metric found")
    # At this point iterator should be in the adaptation block

    # Ensure step size exists and is valid float
    num, line = next(ln_iter)
    if not line.startswith(b"# Step size"):
        raise ValueError(
            f"line {num}: expecting step size, found:\n\t \"{line.decode()}\""
        )
    _, step_size = line.split(b" = ")
    try:
        float(step_size.strip())
    except ValueError as exc:
        raise ValueError(
            f"line {num}: invalid step size: {step_size.decode()}"
        ) from exc

    # Ensure mass matrix valid
    num, line = next(ln_iter)
    if metric == "unit_e":
        return
    if not (
        (metric == "diag_e" and line.startswith(b"# Diagonal elements of "))
        or (metric == "dense_e" and line.startswith(b"# Elements of inverse"))
    ):
        raise ValueError(
            f"line {num}: invalid or missing mass matrix specification"
        )

    # Validating mass matrix shape
    _, line = next(ln_iter)
    num_unconstrained_params = column_count(line)
    if metric == "diag_e":
        return
    for (num, line), _ in zip(ln_iter, range(1, num_unconstrained_params)):
        if column_count(line) != num_unconstrained_params:
            raise ValueError(
                f"line {num}: invalid or missing mass matrix specification"
            )


def parse_timing_lines(
    comment_lines: list[bytes],
) -> dict[str, float]:
    """Parse the timing lines into a dictionary with key corresponding
    to the phase, e.g. Warm-up, Sampling, Total, and value the elapsed seconds
    """
    out: dict[str, float] = {}

    cleaned_lines = (ln.lstrip(b"# ") for ln in comment_lines)
    in_timing_block = False
    for line in cleaned_lines:
        if line.startswith(b"Elapsed Time") and not in_timing_block:
            in_timing_block = True

        if not in_timing_block:
            continue
        match = re.findall(r"([\d\.]+) seconds \((.+)\)", str(line))
        if match:
            seconds = float(match[0][0])
            phase = match[0][1]
            out[phase] = seconds
    return out


def check_sampler_csv(
    path: Union[str, os.PathLike],
    iter_sampling: int = _CMDSTAN_SAMPLING,
    iter_warmup: int = _CMDSTAN_WARMUP,
    save_warmup: bool = False,
    thin: int = _CMDSTAN_THIN,
) -> dict[str, Any]:
    """Capture essential config, shape from stan_csv file."""
    meta = parse_sampler_metadata_from_csv(path)
    if thin > _CMDSTAN_THIN:
        if 'thin' not in meta:
            raise ValueError(
                f'Bad Stan CSV file {path}, config error, '
                f'expected thin = {thin}'
            )
        if meta['thin'] != thin:
            raise ValueError(
                f'Bad Stan CSV file {path}, '
                f'config error, expected thin = {thin}, found {meta["thin"]}'
            )
    draws_warmup = int(math.ceil(iter_warmup / thin))
    draws_sampling = int(math.ceil(iter_sampling / thin))
    if meta['draws_sampling'] != draws_sampling:
        raise ValueError(
            f'Bad Stan CSV file {path}, expected {draws_sampling} draws, '
            f'found {meta["draws_sampling"]}'
        )
    if save_warmup:
        if not ('save_warmup' in meta and meta['save_warmup'] == 1):
            raise ValueError(
                f'Bad Stan CSV file {path}, config error, expected '
                'save_warmup = 1'
            )
        if meta['draws_warmup'] != draws_warmup:
            raise ValueError(
                f'Bad Stan CSV file {path}, expected {draws_warmup} '
                f'warmup draws, found {meta["draws_warmup"]}'
            )
    return meta


def parse_sampler_metadata_from_csv(
    path: Union[str, os.PathLike],
) -> dict[str, Union[int, float, str, tuple[str, ...], dict[str, float]]]:
    """Parses sampling metadata from a given Stan CSV path for a sample run"""
    try:
        comments, header, draws = parse_comments_header_and_draws(path)
        if header is None:
            raise ValueError("No header line found in stan csv")
        raise_on_inconsistent_draws_shape(header, draws)
        config = construct_config_header_dict(comments, header)
        num_warmup, num_sampling = count_warmup_and_sampling_draws(path)
        timings = parse_timing_lines(comments)
        if (
            (config['algorithm'] != 'fixed_param')
            and header
            and not is_sneaky_fixed_param(header)
        ):
            raise_on_invalid_adaptation_block(comments)
            max_depth: int = config["max_depth"]  # type: ignore
            max_tree_hits, divs = extract_max_treedepth_and_divergence_counts(
                header, draws, max_depth, num_warmup
            )
        else:
            max_tree_hits, divs = 0, 0
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Error in reading csv file: {path}") from exc

    key_renames = {
        "Warm-up": "warmup",
        "Sampling": "sampling",
        "Total": "total",
    }
    addtl: dict[str, Union[int, dict[str, float]]] = {
        "draws_warmup": num_warmup,
        "draws_sampling": num_sampling,
        "ct_divergences": divs,
        "ct_max_treedepth": max_tree_hits,
        "time": {key_renames[k]: v for k, v in timings.items()},
    }
    return config | addtl


def munge_varname(name: str) -> str:
    if '.' not in name and ':' not in name:
        return name

    tuple_parts = name.split(':')
    for i, part in enumerate(tuple_parts):
        if '.' not in part:
            continue
        part = part.replace('.', '[', 1)
        part = part.replace('.', ',')
        part += ']'
        tuple_parts[i] = part

    return '.'.join(tuple_parts)


def read_metric(path: str) -> list[int]:
    """
    Read metric file in JSON or Rdump format.
    Return dimensions of entry "inv_metric".
    """
    if path.endswith('.json'):
        with open(path, 'r') as fd:
            metric_dict = json.load(fd)
        if 'inv_metric' in metric_dict:
            dims_np: np.ndarray = np.asarray(metric_dict['inv_metric'])
            return list(dims_np.shape)
        else:
            raise ValueError(
                'metric file {}, bad or missing entry "inv_metric"'.format(path)
            )
    else:
        dims = list(read_rdump_metric(path))
        if dims is None:
            raise ValueError(
                'metric file {}, bad or missing entry "inv_metric"'.format(path)
            )
        return dims


def read_rdump_metric(path: str) -> list[int]:
    """
    Find dimensions of variable named 'inv_metric' in Rdump data file.
    """
    metric_dict = rload(path)
    if metric_dict is None or not (
        'inv_metric' in metric_dict
        and isinstance(metric_dict['inv_metric'], np.ndarray)
    ):
        raise ValueError(
            'metric file {}, bad or missing entry "inv_metric"'.format(path)
        )
    return list(metric_dict['inv_metric'].shape)


def rload(fname: str) -> Optional[dict[str, Union[int, float, np.ndarray]]]:
    """Parse data and parameter variable values from an R dump format file.
    This parser only supports the subset of R dump data as described
    in the "Dump Data Format" section of the CmdStan manual, i.e.,
    scalar, vector, matrix, and array data types.
    """
    data_dict = {}
    with open(fname, 'r') as fd:
        lines = fd.readlines()
    # Variable data may span multiple lines, parse accordingly
    idx = 0
    while idx < len(lines) and '<-' not in lines[idx]:
        idx += 1
    if idx == len(lines):
        return None
    start_idx = idx
    idx += 1
    while True:
        while idx < len(lines) and '<-' not in lines[idx]:
            idx += 1
        next_var = idx
        var_data = ''.join(lines[start_idx:next_var]).replace('\n', '')
        lhs, rhs = [item.strip() for item in var_data.split('<-')]
        lhs = lhs.replace('"', '')  # strip optional Jags double quotes
        rhs = rhs.replace('L', '')  # strip R long int qualifier
        data_dict[lhs] = parse_rdump_value(rhs)
        if idx == len(lines):
            break
        start_idx = next_var
        idx += 1
    return data_dict


def parse_rdump_value(rhs: str) -> Union[int, float, np.ndarray]:
    """Process right hand side of Rdump variable assignment statement.
    Value is either scalar, vector, or multi-dim structure.
    Use regex to capture structure values, dimensions.
    """
    pat = re.compile(
        r'structure\(\s*c\((?P<vals>[^)]*)\)'
        r'(,\s*\.Dim\s*=\s*c\s*\((?P<dims>[^)]*)\s*\))?\)'
    )
    val: Union[int, float, np.ndarray]
    try:
        if rhs.startswith('structure'):
            parse = pat.match(rhs)
            if parse is None or parse.group('vals') is None:
                raise ValueError(rhs)
            vals = [float(v) for v in parse.group('vals').split(',')]
            val = np.array(vals, order='F')
            if parse.group('dims') is not None:
                dims = [int(v) for v in parse.group('dims').split(',')]
                val = np.array(vals).reshape(dims, order='F')
        elif rhs.startswith('c(') and rhs.endswith(')'):
            val = np.array([float(item) for item in rhs[2:-1].split(',')])
        elif '.' in rhs or 'e' in rhs:
            val = float(rhs)
        else:
            val = int(rhs)
    except TypeError as e:
        raise ValueError('bad value in Rdump file: {}'.format(rhs)) from e
    return val


def try_deduce_metric_type(
    inv_metric: Union[
        str,
        np.ndarray,
        Mapping[str, Any],
        list[Union[str, np.ndarray, Mapping[str, Any]]],
    ],
) -> Optional[str]:
    """Given a user-supplied metric, try to infer the correct metric type."""
    if isinstance(inv_metric, list):
        if inv_metric:
            inv_metric = inv_metric[0]

    if isinstance(inv_metric, Mapping):
        if (metric_type := inv_metric.get("metric_type")) in (
            'diag_e',
            'dense_e',
        ):
            return metric_type  # type: ignore
        inv_metric = inv_metric.get('inv_metric', None)

    if isinstance(inv_metric, np.ndarray):
        if len(inv_metric.shape) == 1:
            return 'diag_e'
        else:
            return 'dense_e'

    if isinstance(inv_metric, str):
        dims = read_metric(inv_metric)
        if len(dims) == 1:
            return 'diag_e'
        else:
            return 'dense_e'

    return None
