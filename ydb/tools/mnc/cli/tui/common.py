import os
from dataclasses import dataclass
from typing import List, Optional

from rich.text import Text

from ydb.tools.mnc.lib import config
import ydb.tools.mnc.scheme as scheme


COMMON_OPTION_GROUP = "Common options"

SKIPPED_OPTION_DESTS = {
    "config_name",
    "config_path",
    "__breaker__",
}


@dataclass
class ConfigCandidate:
    name: str
    path: str


def discover_config_candidates() -> List[ConfigCandidate]:
    candidates = []
    roots = [config.local_config_dir_path]
    try:
        mnc_config = config.get_mnc_config()
        roots.append(os.path.join(mnc_config['git_ydb_root'], 'junk', config.user, config.multinode_configure_config_dir))
    except Exception:
        pass
    seen = set()
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        for name in sorted(os.listdir(root)):
            path = os.path.join(root, name)
            if not os.path.isfile(path):
                continue
            base, ext = os.path.splitext(name)
            if ext not in ('.yaml', '.yml', ''):
                continue
            candidate_name = base if ext else name
            key = (candidate_name, path)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(ConfigCandidate(candidate_name, path))
    return candidates


def config_preview(candidate: ConfigCandidate, command_scheme=None) -> Text:
    preview = Text()
    preview.append(candidate.name + "\n")
    preview.append(candidate.path + "\n\n")
    try:
        with open(candidate.path) as file:
            content = file.read()
    except Exception as error:
        content = f"Failed to read config: {error}"
    preview.append(content)
    errors = config_validation_errors(candidate, command_scheme)
    if errors:
        preview.append("\n\n")
        preview.append("Config is not compatible with selected command:\n", style="bold red")
        preview.append("\n".join(f"- {error}" for error in errors), style="red")
    return preview


def config_validation_errors(candidate: ConfigCandidate, command_scheme) -> List[str]:
    if not command_scheme:
        return []
    cfg = config.read_config(scheme.multinode, candidate.name, candidate.path)
    if cfg is None:
        return ["failed to parse config"]
    validated, errors = scheme.apply_scheme(cfg, command_scheme)
    if validated is None:
        return errors
    return []


def primary_alias(option) -> str:
    long_aliases = [alias for alias in option.aliases if alias.startswith("--")]
    hyphenated = [alias for alias in long_aliases if "_" not in alias]
    if hyphenated:
        return hyphenated[0]
    return long_aliases[0] if long_aliases else option.aliases[0]


def option_label(option) -> str:
    aliases = ", ".join(option.aliases)
    if option.dest:
        return f"{aliases} ({option.dest})"
    return aliases


def value_to_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(map(str, value))
    return str(value)


def field_id(prefix: str, dest: Optional[str]) -> str:
    safe_dest = (dest or "value").replace("_", "-")
    return f"{prefix}-{safe_dest}"
