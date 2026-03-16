import argparse
import logging
import sys
from pathlib import Path
import importlib

import orjson

from . import components_whitelist

logger = logging.getLogger(__name__)


def configure_logging(verbosity: int) -> None:
    level = logging.DEBUG if verbosity >= 1 else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr, format="%(message)s")


def clean_component_index(input_file: Path, output_file: Path, dry_run: bool = False) -> None:
    """
    Clean the component index by applying whitelist filtering.

    Args:
        input_file: Path to the input component index JSON file
        output_file: Path to write the cleaned component index
        dry_run: If True, allow all components (skip whitelist filtering)
    """
    # Read the component index
    with open(input_file, "rb") as f:
        data = orjson.loads(f.read())

    # Handle both old format (list) and new format (dict with entries)
    if isinstance(data, dict) and "entries" in data:
        entries = data["entries"]
        metadata = data.get("metadata", {})
        version = data.get("version", "")
    else:
        logger.error("Can't parse current index, aborting...")
        exit(1)

    logger.debug(f"Loaded {len(entries)} bundles from source index: {input_file}\n")
    logger.debug(f"Metadata of source index: \nVersion: {version}\nMetadata: {metadata}\n")

    # Bundle is collection of components (i.e. bundle 'utilities' contains component 'python_repl')
    allowed_bundles = components_whitelist.get_allowed_bundles()
    banned_components = components_whitelist.get_banned_components()

    logger.debug(f"Allowed bundles ({len(allowed_bundles)}):\n  - {"\n  - ".join(sorted(allowed_bundles))}\n")
    logger.debug(f"Banned components ({len(banned_components)}):\n  - {"\n  - ".join(sorted(banned_components))}\n")

    # Linear traversal through existing index
    cleaned_entries = []
    removed_bundles = set()
    removed_components = []
    total_components_before = 0
    total_components_after = 0

    for entry in entries:
        if len(entry) != 2:
            logger.warning(f"Warning: Skipping malformed entry: {entry}")
            continue

        bundle_name, components_dict = entry
        total_components_before += len(components_dict)

        # Remove non-whitelisted bundles
        if bundle_name not in allowed_bundles and not dry_run:
            removed_bundles.add(bundle_name)
            continue

        # Remove banned components from allowed bundles
        filtered_components = {}
        for comp_name, comp_data in components_dict.items():
            if isinstance(comp_data, dict) and "metadata" in comp_data:
                component_path = comp_data["metadata"].get("module", "")
            else:
                logger.warning(f"Warning: Skipping malformed component: {comp_data}")
                continue

            if components_whitelist.is_allowed(bundle_name, component_path, dry_run=dry_run):
                filtered_components[comp_name] = comp_data
                total_components_after += 1
            else:
                removed_components.append(component_path)

        if len(filtered_components) > 0:
            cleaned_entries.append([bundle_name, filtered_components])

    if removed_bundles:
        logger.debug(
            f"Removed {len(removed_bundles)} non-whitelisted bundles:\n  - {"\n  - ".join(sorted(removed_bundles))}\n"
        )
    if removed_components:
        logger.debug(
            f"Removed {len(removed_components)} banned components:\n  - {"\n  - ".join(sorted(removed_components))}\n"
        )

    new_metadata = {
        "num_components": total_components_after,
        "num_modules": len(cleaned_entries),
        "cleaned": True,
        "original_components": metadata.get("num_components"),
        "original_modules": metadata.get("num_modules"),
    }
    metadata.update(new_metadata)
    cleaned_data = {
        "entries": cleaned_entries,
        "metadata": metadata,  # prioritise new data in case of conflicts
        "version": importlib.metadata.version("lfx"),
    }

    # Recalculate SHA256 for the cleaned data
    import hashlib

    sha256_payload = orjson.dumps(cleaned_data, option=orjson.OPT_SORT_KEYS)
    cleaned_data["sha256"] = hashlib.sha256(sha256_payload).hexdigest()

    with open(output_file, "wb") as f:
        f.write(orjson.dumps(cleaned_data, option=orjson.OPT_INDENT_2))

    logger.info(f"Cleaned component index written to {output_file}")
    logger.info(f"Total bundles: {len(entries)} -> {len(cleaned_entries)}")
    logger.info(f"Total components: {total_components_before} -> {total_components_after}")
    logger.info(f"Removed: {total_components_before - total_components_after} components")

    logger.debug(f"Metadata of new index: \nVersion: {cleaned_data['version']}\nMetadata: {cleaned_data['metadata']}\n")


def main():
    parser = argparse.ArgumentParser(description="Clean component index by applying whitelist filtering")
    parser.add_argument("input_file", type=Path, help="Path to the input component index JSON file")
    parser.add_argument(
        "-o", "--output", type=Path, default=Path('index.json'), help="Path to write the cleaned component index"
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Enable debug logging")
    parser.add_argument("--dry-run", action="store_true", help="Allow all components (skip whitelist filtering)")

    args = parser.parse_args()

    configure_logging(args.verbose)

    clean_component_index(args.input_file, args.output, dry_run=args.dry_run)
