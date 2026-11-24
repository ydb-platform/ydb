"""Translate a named build profile to an argument vector for ya make.

The CLI writes one argument per line to ``args`` and the sanitizer to ``sanitize``.
"""

import argparse
from pathlib import Path

import yaml

PROFILE_PATH = Path("ydb/ci/build_profiles.yaml")


def load_profiles(path=PROFILE_PATH):
    with Path(path).open() as stream:
        return yaml.safe_load(stream)


def resolve_profile(name, fallback=None):
    # Absence of the file is expected on release branches predating profiles.
    # A missing profile in an existing file is a configuration error.
    profiles = load_profiles() if PROFILE_PATH.is_file() else fallback
    if profiles is None:
        raise ValueError(f"Build profiles file is missing: {PROFILE_PATH}")
    if name not in profiles:
        raise ValueError(f"Unknown build profile: {name}")
    profile = profiles[name]
    args = [f"--build={profile['build_type']}"]
    if profile.get("thinlto", False):
        args += ["--thinlto"]
    sanitizer = profile.get("sanitize", "")
    if sanitizer:
        args += [f"--sanitize={sanitizer}"]
    for key, value in profile.get("definition_flags", {}).items():
        args.append(f"-D{key}={value}")
    return {"args": args, "sanitize": sanitizer}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile")
    parser.add_argument("--output-dir", required=True)
    options = parser.parse_args()
    profile = resolve_profile(options.profile)
    output_dir = Path(options.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if any("\n" in arg or "\r" in arg for arg in profile["args"]):
        raise ValueError("Build profile arguments must not contain line breaks")
    (output_dir / "args").write_text("\n".join(profile["args"]) + "\n")
    (output_dir / "sanitize").write_text(profile["sanitize"] + "\n")
