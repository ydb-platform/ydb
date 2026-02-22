"""Load and filter pattern rules from pattern_rules.yaml."""

import os
import yaml


def _rules_path():
    dir = os.path.dirname(__file__)
    repo = os.path.normpath(os.path.join(dir, "../../.."))
    return os.path.join(repo, ".github/config/pattern_rules.yaml")


def load_rules(rules_path=None):
    """Load rules from YAML. Returns list of rule dicts."""
    path = rules_path or _rules_path()
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("rules", [])


def get_rules_for_build(rules, build_type, reaction=None):
    """Filter rules by build_type and optionally by reaction."""
    result = []
    for r in rules:
        if r.get("enabled") is False:
            continue
        bt = r.get("build_types")
        if bt is not None and build_type not in bt:
            continue
        if reaction is not None and r.get("reaction") != reaction:
            continue
        result.append(r)
    return result


def get_mute_rule(rules, build_type="relwithdebinfo"):
    """Get first mute rule for build_type."""
    matched = get_rules_for_build(rules, build_type, reaction="mute")
    return matched[0] if matched else None


def get_unmute_rule(rules, build_type="relwithdebinfo"):
    """Get first unmute rule for build_type."""
    matched = get_rules_for_build(rules, build_type, reaction="unmute")
    return matched[0] if matched else None


def get_delete_rule(rules, build_type="relwithdebinfo"):
    """Get first delete rule for build_type."""
    matched = get_rules_for_build(rules, build_type, reaction="delete")
    return matched[0] if matched else None


def get_quarantine_graduation_rule(rules, build_type="relwithdebinfo"):
    """Get quarantine graduation rule."""
    matched = get_rules_for_build(rules, build_type, reaction="remove_from_quarantine")
    return matched[0] if matched else None


def get_rule_params(rule, defaults=None):
    """Get params from rule with defaults."""
    if rule is None:
        return dict(defaults) if defaults else {}
    params = rule.get("params", {}) or {}
    if defaults:
        for k, v in defaults.items():
            if k not in params:
                params[k] = v
    return params
