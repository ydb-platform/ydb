"""Helpers used to serialize/deserialize Atop statistics directly from log files.

The basic layout of an Atop log file can be described as the following:
1. Raw Header:
    - Single instance at the beginning of the file.
    - Contains information about all the following raw records.
2. Raw Record:
    - Single instance repeated in a loop with remaining stats until end of file.
    - Contains information about the *Stat structs that immediately follow it.
3. Raw SStat:
    - Single instance repeated in a loop with remaining stats until end of file.
    - Always found directly after a Record.
    - Always found directly before a TStat array.
    - Contains statistics about overall system activity.
4. Raw TStats:
    - Array of TStat instances repeated in a loop with remaining stats until end of file.
    - Always found directly after a SStat.
    - Always found directly before the next Record (if <= 2.11) or before a CStat array (if 2.11+).
    - Contains statistics about every task/process on the system.
4. Raw CStats/CGroups:
    - Array of CStat/CGroup instances repeated in a loop with other stats until end of file.
    - Always found directly after a TStat.
    - Always found directly before the next Record if not the end of file.
    - Contains statistics about every CGroup on the system.
"""

from __future__ import annotations

import ctypes
import io
import zlib
from typing import Union

from atoparser.structs import atop_1_26
from atoparser.structs import atop_2_3
from atoparser.structs import atop_2_4
from atoparser.structs import atop_2_5
from atoparser.structs import atop_2_6
from atoparser.structs import atop_2_7
from atoparser.structs import atop_2_8
from atoparser.structs import atop_2_9
from atoparser.structs import atop_2_10
from atoparser.structs import atop_2_11
from atoparser.structs import atop_2_12
from atoparser.structs.shared import pid_t

_VERSIONS = [
    atop_1_26,
    atop_2_3,
    atop_2_4,
    atop_2_5,
    atop_2_6,
    atop_2_7,
    atop_2_8,
    atop_2_9,
    atop_2_10,
    atop_2_11,
    atop_2_12,
]
_CSTAT_VERSIONS = _VERSIONS[9:]
# Fallback to latest if there is no custom class provided to attempt backwards compatibility.
_DEFAULT_VERSION = _VERSIONS[-1].Header.supported_version
_HEADER_BY_VERSION: dict[str, type[Header]] = {module.Header.supported_version: module.Header for module in _VERSIONS}

Header = Union[tuple(header for header in _HEADER_BY_VERSION.values())]
Record = Union[tuple(header.Record for header in _HEADER_BY_VERSION.values())]
SStat = Union[tuple(header.SStat for header in _HEADER_BY_VERSION.values())]
TStat = Union[tuple(header.TStat for header in _HEADER_BY_VERSION.values())]  # pylint: disable=invalid-name
CStat = Union[tuple(header.CStat for header in _HEADER_BY_VERSION.values())]
CGChainer = Union[tuple(header.CGChainer for header in _HEADER_BY_VERSION.values())]


# Default Atop sample is once per minute, but it can be manually increased/decreased.
# Additionally, logs may not rollover as expected, combining multiple hours into 1 log.
# Limit the amount of read attempts to 1 per second for 1 day to ensure we do not get stuck in an infinite loop.
MAX_SAMPLES_PER_FILE = 86400

# Definition from rawlog.c
MAGIC = 0xFEEDBEEF


def generate_statistics(
    raw_file: io.BytesIO,
    header: Header = None,
    raise_on_truncation: bool = True,
    max_samples: int = MAX_SAMPLES_PER_FILE,
) -> tuple[Record, SStat, list[TStat], list[CGChainer]]:
    """Read statistics groups from an open Atop log file.

    Args:
        raw_file: An open Atop file capable of reading as bytes.
        header: The header from the file containing metadata about records to read. If not provided, one will be read.
        raise_on_truncation: Raise compression exceptions after header is read. e.g. Software restarts
        max_samples: Maximum number of samples read from a file.

    Yields:
        The next record, sstat, tstat list, and cstat list statistic groups after reading in raw bytes to objects.
    """
    if header is None:
        # If a header was not provided, read up to the proper length and discard to ensure the correct starting offset.
        header = get_header(raw_file)

    try:
        header_version = header.semantic_version
        major, minor = header_version.split(".")[:2]
        major, minor = int(major), int(minor)
        for _ in range(max_samples):
            # Read the repeating structured information until the end of the file.
            # Atop log files consist of the following after the header, repeated until the end:
            # 1. Record: Metadata about statistics.
            # 2. SStats: System statistics.
            # 3. TStats: Task/process statistics.
            # 4. CGChain/CStats: CGroup statistics.
            record = get_record(raw_file, header)
            if record.scomplen <= 0:
                # Natural end-of-file, no further bytes were found to populate another record.
                break
            sstat = get_sstat(raw_file, header, record)
            tstats = get_tstat(raw_file, header, record)
            if major >= 2 and minor >= 11:
                cgroups = get_cstat(raw_file, header, record)
            else:
                cgroups = []
            yield record, sstat, tstats, cgroups
    except zlib.error:
        # End of readable data reached. This is common during software restarts.
        # All errors after the header are squashable errors, since that means the file is valid, but was not closed.
        if raise_on_truncation:
            raise


def get_header(raw_file: io.BytesIO, check_compatibility: bool = True) -> Header:
    """Get the raw file header from an open Atop file.

    Args:
        raw_file: An open Atop file capable of reading as bytes.
        check_compatibility: Whether to enforce compatibility check against supported versions on header creation.

    Returns:
        The header at the beginning of an Atop file.

    Raises:
        ValueError if there are not enough bytes to read the header, or the bytes were invalid.
    """
    # Read the header directly into the struct, there is no padding to consume or add.
    # Use default Header as the baseline in order to check the version. It can be transferred without re-reading.
    header = _HEADER_BY_VERSION[_DEFAULT_VERSION]()
    raw_file.readinto(header)

    if header.magic != MAGIC:
        msg = f"File does not contain raw atop output (wrong magic number): {hex(header.magic)}"
        raise ValueError(msg)

    header_version = header.semantic_version
    if header_version != _DEFAULT_VERSION and header_version in _HEADER_BY_VERSION:
        # Header byte length is consistent across versions. Transfer the initial read into the versioned header.
        header = _HEADER_BY_VERSION[header_version].from_buffer(header)

    if check_compatibility:
        # Ensure all struct lengths match the lengths specified in the header. If not, we cannot read the file further.
        header.check_compatibility()

    return header


def get_record(
    raw_file: io.BytesIO,
    header: Header,
) -> Record:
    """Get the next raw record from an open Atop file.

    Args:
        raw_file: An open Atop file capable of reading as bytes.
        header: The header from the file containing metadata about records to read.

    Returns:
        A single record representing the data before an SStat struct.

    Raises:
        ValueError if there are not enough bytes to read a single record.
    """
    record = header.Record()
    raw_file.readinto(record)
    return record


def get_sstat(
    raw_file: io.BytesIO,
    header: Header,
    record: Record,
) -> SStat:
    """Get the next raw sstat from an open Atop file.

    Args:
        raw_file: An open Atop file capable of reading as bytes.
        header: The header from the file containing metadata about records to read.
        record: The preceding record containing metadata about the SStat to read.

    Returns:
        A single struct representing the data after a raw record, but before an array of TStat structs.

    Raises:
        ValueError if there are not enough bytes to read a single stat.
    """
    # Read the requested length instead of the length of the struct.
    # The data is compressed and must be decompressed before it will fill the struct.
    buffer = raw_file.read(record.scomplen)
    decompressed = zlib.decompress(buffer)
    sstat = header.SStat.from_buffer_copy(decompressed)
    return sstat


def get_tstat(
    raw_file: io.BytesIO,
    header: Header,
    record: Record,
) -> list[TStat]:
    """Get the next raw tstat array from an open Atop file.

    Args:
        raw_file: An open Atop file capable of reading as bytes.
        header: The header from the file containing metadata about records to read.
        record: The preceding record containing metadata about the TStats to read.

    Returns:
        All TStat structs after a raw SStat, but before the next raw record.

    Raises:
        ValueError if there are not enough bytes to read a stat array.
    """
    # Read the requested length instead of the length of the struct.
    # The data is compressed and must be decompressed before it will fill the final list of structs.
    buffer = raw_file.read(record.pcomplen)
    decompressed = zlib.decompress(buffer)

    record_count = record.nlist if isinstance(record, atop_1_26.Record) else record.ndeviat
    tstatlen = header.tstatlen if header.major_version >= 2 and header.minor_version >= 3 else header.pstatlen
    tstats = []
    for index in range(record_count):
        # Reconstruct one TStat struct for every possible byte chunk, incrementing the offset each pass. For example:
        # First pass: 0 - 21650
        # Second pass: 21651 - 43300
        tstat = header.TStat.from_buffer_copy(decompressed[index * tstatlen : tstatlen * (index + 1)])
        tstats.append(tstat)
    return tstats


def get_cstat(
    raw_file: io.BytesIO,
    header: Header,
    record: Record,
) -> list[CGChainer]:
    """Get the next raw cstat array from an open Atop file.

    Args:
        raw_file: An open Atop file capable of reading as bytes.
        header: The header from the file containing metadata about records to read.
        record: The preceding record containing metadata about the CStats to read.

    Returns:
        All CGroup CStat structs and PID lists after a raw TStat, but before the next raw record.

    Raises:
        ValueError if there are not enough bytes to read a stat array.
    """
    # Read the requested length instead of the length of the struct.
    # The data is compressed and must be decompressed before it will fill the final list of structs.
    buffer_cstats = raw_file.read(record.ccomplen)
    decompressed_cstats = zlib.decompress(buffer_cstats)
    buffer_pidlist = raw_file.read(record.icomplen)
    decompressed_pidlist = zlib.decompress(buffer_pidlist)

    cgroups = []
    cstat_start = 0
    cstatlen = header.cstatlen
    pidlist_start = 0
    for _ in range(record.ncgroups):
        # Reconstruct one CStat struct and pidlist for every possible byte chunk, incrementing the offset each pass.
        # For example:
        # First pass: 0 - 21650
        # Second pass: 21651 - 43300
        # N.B. The variable length cgname is currently unsupported. In order to properly skip the remaining bytes,
        # Use the final structlen from the nested gen struct to update the starting point.
        cstat = header.CStat.from_buffer_copy(decompressed_cstats[cstat_start : cstat_start + cstatlen])
        cstat_start += cstat.gen.structlen

        pid_array = pid_t * cstat.gen.nprocs
        pid_array_size = ctypes.sizeof(pid_array)
        pidlist = pid_array.from_buffer_copy(decompressed_pidlist[pidlist_start : pidlist_start + pid_array_size])
        pidlist_start += pid_array_size

        cgroups.append(header.CGChainer(cstat, pidlist))
    return cgroups


def struct_to_dict(struct: ctypes.Structure) -> dict:
    """Convert C struct, and all nested structs, into a Python dictionary.

    Skips any "future" named fields since they are empty placeholders for potential future versions.

    Args:
        struct: C struct loaded from raw Atop file.

    Returns:
        C struct converted into a dictionary using the names of the struct's fields as keys.
    """
    struct_dict = {}
    for field in struct._fields_:  # pylint: disable=protected-access
        field_name = field[0]
        field_data = getattr(struct, field_name)
        if isinstance(field_data, ctypes.Structure):
            struct_dict[field_name] = struct_to_dict(field_data)
        elif "future" not in field_name:
            if isinstance(field_data, ctypes.Array):
                struct_dict[field_name] = []
                limiters = getattr(struct, "fields_limiters", {})
                limiter = limiters.get(field_name)
                if limiter:
                    field_data = field_data[: getattr(struct, limiter)]
                for sub_data in field_data:
                    if isinstance(sub_data, ctypes.Structure):
                        struct_dict[field_name].append(struct_to_dict(sub_data))
                    else:
                        struct_dict[field_name].append(sub_data)
            elif isinstance(field_data, bytes):
                struct_dict[field_name] = field_data.decode(errors="ignore")
            else:
                struct_dict[field_name] = field_data
    return struct_dict
