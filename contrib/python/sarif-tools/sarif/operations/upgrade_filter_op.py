"""
Code for `sarif upgrade-filter` command.
"""

import os
import yaml


def _load_blame_filter_file(file_path):
    filter_description = os.path.basename(file_path)
    include_patterns = []
    exclude_patterns = []
    try:
        with open(file_path, encoding="utf-8") as file_in:
            for line in file_in.readlines():
                if line.startswith("\ufeff"):
                    # Strip byte order mark
                    line = line[1:]
                lstrip = line.strip()
                if lstrip.startswith("#"):
                    # Ignore comment lines
                    continue
                pattern_spec = None
                is_include = True
                if lstrip.startswith("description:"):
                    filter_description = lstrip[12:].strip()
                elif lstrip.startswith("+: "):
                    is_include = True
                    pattern_spec = lstrip[3:].strip()
                elif lstrip.startswith("-: "):
                    is_include = False
                    pattern_spec = lstrip[3:].strip()
                else:
                    is_include = True
                    pattern_spec = lstrip
                if pattern_spec:
                    (include_patterns if is_include else exclude_patterns).append(
                        pattern_spec
                    )
    except UnicodeDecodeError as error:
        raise IOError(
            f"Cannot read blame filter file {file_path}: not UTF-8 encoded?"
        ) from error
    return (
        filter_description,
        include_patterns,
        exclude_patterns,
    )


def upgrade_filter_file(old_filter_file, output_file):
    """Convert blame filter file to general filter file."""
    (
        filter_description,
        include_patterns,
        exclude_patterns,
    ) = _load_blame_filter_file(old_filter_file)
    new_filter_definition = {
        "description": (
            filter_description
            if filter_description
            else f"Migrated from {os.path.basename(old_filter_file)}"
        ),
        "configuration": {"default-include": True, "check-line-number": True},
    }
    if include_patterns:
        new_filter_definition["include"] = [
            {"author-mail": include_pattern} for include_pattern in include_patterns
        ]
    if exclude_patterns:
        new_filter_definition["exclude"] = [
            {"author-mail": exclude_pattern} for exclude_pattern in exclude_patterns
        ]
    with open(output_file, "w", encoding="utf8") as yaml_out:
        yaml.dump(new_filter_definition, yaml_out)
    print("Wrote", output_file)
