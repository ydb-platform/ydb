# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Environment variable carriers for text map propagators.

Use :class:`EnvironmentGetter` with the environment mapping to extract from,
usually ``os.environ`` during application or child-process initialization.
Use :class:`EnvironmentSetter` with a mutable environment copy when preparing
the environment for a child process.
"""

import re
from collections.abc import Mapping, MutableMapping

from opentelemetry.propagators.textmap import Getter, Setter


def _normalize_key(key: str) -> str:
    if not key:
        return "_"
    result = re.sub(r"[^A-Za-z0-9_]", "_", key).upper()
    if result and result[0].isdigit():
        result = "_" + result
    return result


def _is_normalized_key(key: str) -> bool:
    if not key:
        return False
    if "0" <= key[0] <= "9":
        return False
    return all(
        "A" <= char <= "Z" or "0" <= char <= "9" or char == "_" for char in key
    )


class EnvironmentGetter(Getter[Mapping[str, str]]):
    """Getter implementation for extracting context and baggage from environment variables.

    EnvironmentGetter reads from the mapping provided as the carrier, normalizes
    requested keys, and provides simple data access without validation.

    Example usage:
        getter = EnvironmentGetter()
        traceparent = getter.get(os.environ, "traceparent")
    """

    def get(self, carrier: Mapping[str, str], key: str) -> list[str] | None:
        """Get a value from the environment carrier for the given key.

        Args:
            carrier: Mapping to read environment variables from
            key: The key to look up (will be normalized)

        Returns:
            A list with a single string value if the key exists, None otherwise.
        """
        val = carrier.get(_normalize_key(key))
        if val is None:
            return None
        return [val]

    def keys(self, carrier: Mapping[str, str]) -> list[str]:
        """Get all keys from the environment carrier.

        Args:
            carrier: Mapping to read environment variable keys from

        Returns:
            List of all already-normalized environment variable keys.
        """
        return [key for key in carrier.keys() if _is_normalized_key(key)]


class EnvironmentSetter(Setter[MutableMapping[str, str]]):
    """Setter implementation for building environment variable dictionaries.

    EnvironmentSetter builds a dictionary of environment variables that
    can be passed to utilities like subprocess.run()

    Example usage:
        setter = EnvironmentSetter()
        env_vars = {}
        setter.set(env_vars, "traceparent", "00-trace-id-span-id-01")
        subprocess.run(myCommand, env=env_vars)
    """

    def set(
        self, carrier: MutableMapping[str, str], key: str, value: str
    ) -> None:
        """Set a value in the carrier dictionary for the given key.

        Args:
            carrier: Dictionary to store environment variables
            key: The key to set (normalized)
            value: The value to set
        """
        carrier[_normalize_key(key)] = value
