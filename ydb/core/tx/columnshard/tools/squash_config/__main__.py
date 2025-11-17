#!/usr/bin/env python3
"""
Config squashing tool for YDB YAML configurations.

This script takes a YAML config file and consolidates selector_config items
for tenants matching a given regex pattern. For each matching tenant, it merges
all applicable config parameters into a single consolidated entry.
"""

import argparse
import logging
import re
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Set, Union

import yaml


class InheritTag:
    """Custom YAML tag handler for !inherit"""

    def __init__(self, data):
        self.data = data


class AppendTag:
    """Custom YAML tag handler for !append"""

    def __init__(self, data):
        self.data = data


class UnknownTag:
    """Custom YAML tag handler for unknown tags"""

    def __init__(self, tag_name, data):
        self.tag_name = tag_name
        self.data = data


def inherit_constructor(loader, node):
    """Constructor for !inherit YAML tag"""
    if isinstance(node, yaml.ScalarNode):
        return InheritTag(loader.construct_scalar(node))
    elif isinstance(node, yaml.SequenceNode):
        return InheritTag(loader.construct_sequence(node))
    elif isinstance(node, yaml.MappingNode):
        return InheritTag(loader.construct_mapping(node))
    else:
        return InheritTag(None)


def append_constructor(loader, node):
    """Constructor for !append YAML tag"""
    if isinstance(node, yaml.SequenceNode):
        return AppendTag(loader.construct_sequence(node))
    else:
        # !append should only be used with sequences
        raise yaml.constructor.ConstructorError(
            None, None,
            f"!append tag can only be applied to sequences, got {type(node).__name__}",
            node.start_mark
        )


def inherit_representer(dumper, data):
    """Representer for !inherit YAML tag"""
    if isinstance(data.data, dict):
        return dumper.represent_mapping("!inherit", data.data)
    elif isinstance(data.data, list):
        return dumper.represent_sequence("!inherit", data.data)
    else:
        return dumper.represent_scalar("!inherit", str(data.data))


def append_representer(dumper, data):
    """Representer for !append YAML tag"""
    if isinstance(data.data, list):
        return dumper.represent_sequence("!append", data.data)
    else:
        raise yaml.representer.RepresenterError(f"!append tag can only represent lists, got {type(data.data).__name__}")


def unknown_constructor(loader, suffix, node):
    """Constructor for unknown YAML tags"""
    tag_name = f"!{suffix}" if suffix else "!"
    
    if isinstance(node, yaml.ScalarNode):
        return UnknownTag(tag_name, loader.construct_scalar(node))
    elif isinstance(node, yaml.SequenceNode):
        return UnknownTag(tag_name, loader.construct_sequence(node))
    elif isinstance(node, yaml.MappingNode):
        return UnknownTag(tag_name, loader.construct_mapping(node))
    else:
        return UnknownTag(tag_name, None)


def unknown_representer(dumper, data):
    """Representer for unknown YAML tags"""
    if isinstance(data.data, dict):
        return dumper.represent_mapping(data.tag_name, data.data)
    elif isinstance(data.data, list):
        return dumper.represent_sequence(data.tag_name, data.data)
    else:
        return dumper.represent_scalar(data.tag_name, str(data.data))


# Setup YAML loader/dumper with custom tag support
class CustomLoader(yaml.SafeLoader):
    pass


class CustomDumper(yaml.SafeDumper):
    pass


CustomLoader.add_constructor("!inherit", inherit_constructor)
CustomLoader.add_constructor("!append", append_constructor)
CustomLoader.add_multi_constructor("!", unknown_constructor)
CustomDumper.add_representer(InheritTag, inherit_representer)
CustomDumper.add_representer(AppendTag, append_representer)
CustomDumper.add_representer(UnknownTag, unknown_representer)


def extract_tenants_from_selector(selector: Dict[str, Any]) -> Set[str]:
    """
    Extract tenant names from a selector configuration.

    Args:
        selector: The selector dictionary

    Returns:
        Set of tenant names found in the selector
    """
    tenants = set()

    if "tenant" in selector:
        tenant_spec = selector["tenant"]
        if isinstance(tenant_spec, str):
            tenants.add(tenant_spec)
        elif isinstance(tenant_spec, dict) and "in" in tenant_spec:
            if isinstance(tenant_spec["in"], list):
                tenants.update(tenant_spec["in"])
            else:
                tenants.add(tenant_spec["in"])

    return tenants


def matches_regex(tenant: str, pattern: re.Pattern) -> bool:
    """
    Check if a tenant matches the given regex pattern.

    Args:
        tenant: The tenant string to check
        pattern: Compiled regex pattern

    Returns:
        True if the tenant matches the pattern
    """
    return pattern.search(tenant) is not None


def deep_merge_configs(
    base_config: Dict[str, Any], new_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Deep merge two configuration dictionaries with tag-aware behavior.
    
    Merge behavior based on tags:
    - !inherit: Deep merge (merge new values with old values, override specified fields)
    - !append: For lists, append new items to existing list
    - No tag: Complete override (new value replaces old value)

    Args:
        base_config: The base configuration dictionary
        new_config: The new configuration to merge in

    Returns:
        Merged configuration dictionary
    """
    result = deepcopy(base_config)

    if isinstance(new_config, InheritTag):
        logging.warning("Encountered !inherit tag on selector config root")
        new_config = new_config.data

    for key, value in new_config.items():
        if isinstance(value, InheritTag):
            # !inherit tag: deep merge behavior
            if key in result:
                if isinstance(result[key], dict) and isinstance(value.data, dict):
                    result[key] = deep_merge_configs(result[key], value.data)
                elif isinstance(result[key], list) and isinstance(value.data, list):
                    # For !inherit with lists, merge by overriding
                    result[key] = deepcopy(value.data)
                else:
                    # Override with the new value
                    result[key] = deepcopy(value.data)
            else:
                # Key doesn't exist in base, add it
                result[key] = deepcopy(value.data)
        elif isinstance(value, AppendTag):
            # !append tag: append to existing list
            if isinstance(value.data, list):
                if key in result and isinstance(result[key], list):
                    # Append to existing list
                    result[key] = result[key] + deepcopy(value.data)
                else:
                    # No existing list, treat as normal assignment
                    result[key] = deepcopy(value.data)
            else:
                raise ValueError(f"!append tag can only be used with lists, got {type(value.data).__name__} for key '{key}'")
        elif isinstance(value, UnknownTag):
            logging.error(f"Merging unknown tag: {value.tag_name}")
            sys.exit(1)
        else:
            # No tag: complete override behavior
            if key in result:
                if isinstance(result[key], dict) and isinstance(value, dict):
                    # For dictionaries without tags, still do deep merge to handle nested tags
                    result[key] = deep_merge_configs(result[key], value)
                else:
                    # Complete override
                    result[key] = deepcopy(value)
            else:
                # Key doesn't exist in base, add it
                result[key] = deepcopy(value)

    return result


def consolidate_tenant_configs(
    selector_configs: List[Dict[str, Any]], target_tenants: Set[str]
) -> Dict[str, Any]:
    """
    Consolidate all config items that apply to the target tenants.

    Args:
        selector_configs: List of all selector config items
        target_tenants: Set of tenant names to consolidate

    Returns:
        Consolidated configuration dictionary
    """
    consolidated_config = {}

    # Process selector configs in order to maintain precedence
    for selector_config in selector_configs:
        if "selector" not in selector_config or "config" not in selector_config:
            continue

        selector_tenants = extract_tenants_from_selector(selector_config["selector"])

        # Check if this selector applies to any of our target tenants
        if target_tenants.intersection(selector_tenants):
            consolidated_config = deep_merge_configs(
                consolidated_config, selector_config["config"]
            )

    return consolidated_config


def _normalize_config_for_comparison(config: Any) -> Any:
    """
    Normalize config objects for comparison by converting custom tags to consistent representations.
    """
    if isinstance(config, InheritTag):
        return {'__tag__': 'inherit', 'data': _normalize_config_for_comparison(config.data)}
    elif isinstance(config, AppendTag):
        return {'__tag__': 'append', 'data': _normalize_config_for_comparison(config.data)}
    elif isinstance(config, UnknownTag):
        return {'__tag__': config.tag_name, 'data': _normalize_config_for_comparison(config.data)}
    elif isinstance(config, dict):
        return {k: _normalize_config_for_comparison(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_normalize_config_for_comparison(item) for item in config]
    else:
        return config


def deduplicate_selector_configs(selector_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate selector config items by merging those with identical configurations.
    
    Items with the same config will be merged into a single item with 'in:' syntax
    for the tenant selector.
    
    Args:
        selector_configs: List of selector config items
        
    Returns:
        Deduplicated list of selector config items
    """
    import json
    from collections import defaultdict
    
    # Group configs by their content (serialized for comparison)
    config_groups = defaultdict(list)
    
    deduplicated_configs = []
    
    for selector_config in selector_configs:
        if 'description' not in selector_config:
            logging.warning("No description found in the a selector config")
            deduplicated_configs.append(selector_config)
            continue

        if not isinstance(selector_config['description'], str):
            logging.warning("Description is not a string")
            deduplicated_configs.append(selector_config)
            continue

        if 'Consolidated' not in selector_config['description']:
            deduplicated_configs.append(selector_config)
            continue

        if 'config' not in selector_config:
            # Items without config go directly to result
            config_groups['__no_config__'].append(selector_config)
            continue
            
        # Normalize and serialize config for comparison (excluding description which may vary)
        normalized_config = _normalize_config_for_comparison(selector_config['config'])
        config_key = json.dumps(normalized_config, sort_keys=True)
        config_groups[config_key].append(selector_config)
    
    for config_key, items in config_groups.items():
        if config_key == '__no_config__':
            # Add items without config as-is
            deduplicated_configs.extend(items)
            continue
            
        if len(items) == 1:
            # Single item, keep as-is
            deduplicated_configs.append(items[0])
            continue
            
        # Multiple items with same config - merge them
        tenants = []
        descriptions = []
        
        for item in items:
            if 'selector' in item and 'tenant' in item['selector']:
                tenant_spec = item['selector']['tenant']
                if isinstance(tenant_spec, str):
                    tenants.append(tenant_spec)
                elif isinstance(tenant_spec, dict) and 'in' in tenant_spec:
                    if isinstance(tenant_spec['in'], list):
                        tenants.extend(tenant_spec['in'])
                    else:
                        tenants.append(tenant_spec['in'])
                        
            if 'description' in item:
                descriptions.append(item['description'])
        
        # Remove duplicates and sort
        tenants = sorted(set(tenants))
        
        if tenants:
            # Create merged item
            merged_item = {
                'description': 'Consolidated config',
                'selector': {
                    'tenant': {
                        'in': tenants
                    } if len(tenants) > 1 else tenants[0]
                },
                'config': items[0]['config']  # All items have the same config
            }
            deduplicated_configs.append(merged_item)
        else:
            # No tenants found, keep original items
            deduplicated_configs.extend(items)
    
    items_before = len(selector_configs)
    items_after = len(deduplicated_configs)
    if items_before != items_after:
        logging.info(f"Deduplicated selector_config from {items_before} to {items_after} items")
    
    return deduplicated_configs

def process_config(config_data: Dict[str, Any], tenant_regex: str) -> Dict[str, Any]:
    """
    Process the configuration file and consolidate matching tenant configs.

    Args:
        config_data: The parsed YAML configuration
        tenant_regex: Regular expression to match tenant names

    Returns:
        Processed configuration with consolidated tenant entries
    """
    if "selector_config" not in config_data:
        logging.warning("No selector_config found in the configuration file")
        return config_data

    pattern = re.compile(tenant_regex)
    selector_configs = config_data["selector_config"]

    # Find all tenants that match the regex
    all_matching_tenants = set()
    for selector_config in selector_configs:
        if "selector" not in selector_config:
            logging.warning("No 'selector' found in a selector config")
            continue

        if not selector_config["selector"]:
            matching_tenants = all_matching_tenants
        else:
            selector_tenants = extract_tenants_from_selector(
                selector_config["selector"]
            )
            matching_tenants = {
                t for t in selector_tenants if matches_regex(t, pattern)
            }
            if not matching_tenants:
                continue
            if selector_tenants != matching_tenants:
                logging.error(f"Selector contains both matched and not matched tenants: {', '.join(selector_tenants)}")
                sys.exit(1)
            all_matching_tenants.update(matching_tenants)

    if not all_matching_tenants:
        logging.warning(f"No tenants found matching regex pattern: {tenant_regex}")
        return config_data

    # Create consolidated entries for each matching tenant
    new_selector_configs = []
    processed_tenants = set()

    # Keep non-matching selector configs
    for selector_config in selector_configs:
        if "selector" not in selector_config:
            new_selector_configs.append(selector_config)
            continue

        selector_tenants = extract_tenants_from_selector(selector_config["selector"])
        matching_tenants = {t for t in selector_tenants if matches_regex(t, pattern)}

        # If this selector doesn't match any of our target tenants, keep it
        if not matching_tenants:
            new_selector_configs.append(selector_config)

    # Create consolidated entries for each matching tenant
    for tenant in sorted(all_matching_tenants):
        if tenant in processed_tenants:
            continue

        # Find all configs that apply to this tenant
        consolidated_config = consolidate_tenant_configs(selector_configs, {tenant})

        if consolidated_config:
            # Create new consolidated selector config entry
            new_entry = {
                "description": f"Consolidated config for tenant {tenant}",
                "selector": {"tenant": tenant},
                "config": consolidated_config,
            }
            new_selector_configs.append(new_entry)
            processed_tenants.add(tenant)
    # Deduplicate configs with identical content
    new_selector_configs = deduplicate_selector_configs(new_selector_configs)

    # Update the configuration
    result_config = deepcopy(config_data)
    result_config["selector_config"] = new_selector_configs

    logging.info(
        f"Consolidated {len(all_matching_tenants)} matching tenants: {sorted(all_matching_tenants)}"
    )
    logging.info(
        f"Reduced selector_config from {len(selector_configs)} to {len(new_selector_configs)} items"
    )

    return result_config


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Consolidate YDB YAML config selector entries for matching tenants"
    )
    parser.add_argument(
        "config_file", type=Path, help="Path to the YAML configuration file"
    )
    parser.add_argument("tenant_regex", help="Regular expression to match tenant names")

    args = parser.parse_args()

    # Setup logging to stderr
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stderr
    )

    # Validate input file
    if not args.config_file.exists():
        logging.error(f"Configuration file '{args.config_file}' does not exist")
        sys.exit(1)

    # Validate regex
    try:
        re.compile(args.tenant_regex)
    except re.error as e:
        logging.error(f"Invalid regex pattern '{args.tenant_regex}': {e}")
        sys.exit(1)

    # Load and process the configuration
    try:
        with open(args.config_file, "r", encoding="utf-8") as f:
            config_data = yaml.load(f, Loader=CustomLoader)

        if config_data is None:
            logging.error("Configuration file is empty or invalid")
            sys.exit(1)

        processed_config = process_config(config_data, args.tenant_regex)

        # Output the result
        yaml_output = yaml.dump(
            processed_config,
            Dumper=CustomDumper,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

        print(yaml_output)

    except yaml.YAMLError as e:
        logging.error(f"Error: Failed to parse YAML file: {e}")
        sys.exit(1)
    except IOError as e:
        logging.error(f"Error: Failed to read/write file: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
