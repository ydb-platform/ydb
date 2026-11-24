"""Translate a named build profile to an argument vector for ya make."""

import argparse
import logging
import shlex
from pathlib import Path

import yaml

PROFILE_PATH = Path("ydb/ci/build_profiles.yaml")


def load_profiles(path=None):
    with Path(path or PROFILE_PATH).open() as stream:
        return yaml.safe_load(stream)


def resolve_profile(name, fallback=None):
    # Absence of the file is expected on release branches predating profiles.
    # The head snapshot also supplies names newly introduced by a PR.
    profiles = load_profiles() if PROFILE_PATH.is_file() else fallback
    if profiles is None:
        raise ValueError(f"Build profiles file is missing: {PROFILE_PATH}")
    if not isinstance(profiles, dict):
        raise ValueError("Build profiles must be a mapping")
    if name not in profiles and fallback is not None and name in fallback:
        profiles = fallback
    if name not in profiles:
        raise ValueError(f"Unknown build profile: {name}")
    profile = profiles[name]
    if not isinstance(profile, dict) or not isinstance(profile.get("build_type"), str) or not profile["build_type"]:
        raise ValueError(f"Build profile {name!r} requires a nonempty build_type")
    args = [f"--build={profile['build_type']}"]
    if profile.get("thinlto", False):
        args += ["--thinlto"]
    sanitizer = profile.get("sanitize", "")
    if sanitizer:
        args += [f"--sanitize={sanitizer}"]
    for key, value in profile.get("definition_flags", {}).items():
        args.append(f"-D{key}={value}")
    return {"args": args, "sanitize": sanitizer}


def reproduction_args(name):
    # A missing/invalid profile must not hide the test results themselves.
    try:
        return shlex.join(resolve_profile(name)["args"])
    except (OSError, ValueError, TypeError, KeyError, yaml.YAMLError) as error:
        logging.warning("Cannot generate a reproduction command for profile %r: %s", name, error)
        return ""


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
