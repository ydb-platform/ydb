"""
Reusable utility functions for handling the SARIF format.

Primarily interrogating the `result` JSON defined at
https://docs.oasis-open.org/sarif/sarif/v2.1.0/cs01/sarif-v2.1.0-cs01.html#_Toc16012594
"""

import textwrap
from typing import Literal, Tuple, Union

# SARIF severity levels as per
# https://docs.oasis-open.org/sarif/sarif/v2.1.0/sarif-v2.1.0.html#_Toc141790898
SARIF_SEVERITIES_WITHOUT_NONE = ["error", "warning", "note"]
SARIF_SEVERITIES_WITH_NONE = SARIF_SEVERITIES_WITHOUT_NONE + ["none"]


def combine_code_and_description(code: str, description: str) -> str:
    """
    Combine code and description into one string, keeping total length under 120 characters.
    """
    length_budget = 120
    if code:
        code = code.strip()
        length_budget -= len(code) + 1  # Allow issue code and space character
    continuation_placeholder = " ..."
    # Allow extra space when truncating for continuation characters
    length_budget_pre_continuation = length_budget - len(continuation_placeholder)
    if length_budget_pre_continuation < 10:
        # Don't include description if it would be very short due to long code
        return code
    if description:
        if "\n" in description:
            description = description[: description.index("\n")]
        if description.startswith(code):
            # Don't duplicate the code
            description = description[len(code) :]
        description = description.strip()
    if description:
        if len(description) > length_budget:
            shorter_description = textwrap.shorten(
                description,
                width=length_budget_pre_continuation,
                placeholder=continuation_placeholder,
            )
            if len(shorter_description) < length_budget_pre_continuation - 40:
                # Word wrap shortens the description significantly, so truncate mid-word instead
                description = (
                    description[:length_budget_pre_continuation]
                    + continuation_placeholder
                )
            else:
                description = shorter_description
        if code:
            return f"{code.strip()} {description}"
        return description
    if code:
        return code
    return "<NONE>"


def combine_record_code_and_description(record: dict) -> str:
    """
    Combine code and description fields into one string.
    """
    return combine_code_and_description(record["Code"], record["Description"])


def read_result_location(result) -> Tuple[str, str]:
    """
    Extract the file path and line number strings from the Result.

    Tools store this in different ways, so this function tries a few different JSON locations.
    """
    file_path = None
    line_number = None
    locations = result.get("locations", [])
    if locations and isinstance(locations, list):
        location = locations[0]
        physical_location = location.get("physicalLocation", {})
        # SpotBugs has some errors with no line number so deal with them by just leaving it at 1
        line_number = physical_location.get("region", {}).get("startLine", None)
        # For file name, first try the location written by DevSkim
        file_path = (
            location.get("physicalLocation", {})
            .get("address", {})
            .get("fullyQualifiedName", None)
        )
        if not file_path:
            # Next try the physical location written by MobSF and by SpotBugs (for some errors)
            file_path = (
                location.get("physicalLocation", {})
                .get("artifactLocation", {})
                .get("uri", None)
            )
        if not file_path:
            logical_locations = location.get("logicalLocations", None)
            if logical_locations:
                # Finally, try the logical location written by SpotBugs for some errors
                file_path = logical_locations[0].get("fullyQualifiedName", None)
    return (file_path, line_number)


def read_result_rule(result, run) -> Tuple[Union[dict, None], int]:
    """
    Returns the corresponding rule object for the specified result, plus its index
    in the rules array. Follows the rules at
    https://docs.oasis-open.org/sarif/sarif/v2.1.0/sarif-v2.1.0.html#_Toc141790895
    """
    ruleIndex = result.get("ruleIndex")
    ruleId = result.get("ruleId")
    rule = result.get("rule")

    if rule:
        if ruleIndex is None:
            ruleIndex = rule.get("index")

        if ruleId is None:
            ruleId = rule.get("id")

    rules = run.get("tool", {}).get("driver", {}).get("rules", [])

    if ruleIndex is not None and ruleIndex >= 0 and ruleIndex < len(rules):
        return (rules[ruleIndex], ruleIndex)

    if ruleId:
        for i, rule in enumerate(rules):
            if rule.get("id") == ruleId:
                return (rule, i)

    return (None, -1)


def read_result_invocation(result, run):
    """
    Extract the invocation metadata for the result, following the rules at
    https://docs.oasis-open.org/sarif/sarif/v2.1.0/sarif-v2.1.0.html#_Toc141790917
    """
    invocationIndex = result.get("provenance", {}).get("invocationIndex")
    if invocationIndex is None:
        return None

    invocations = run.get("invocations")

    if invocations and invocationIndex >= 0 and invocationIndex < len(invocations):
        return invocations[invocationIndex]

    return None


def read_result_severity(result, run) -> Literal["none", "note", "warning", "error"]:
    """
    Extract the severity level from the result following the rules at
    https://docs.oasis-open.org/sarif/sarif/v2.1.0/sarif-v2.1.0.html#_Toc141790898
    """
    severity = result.get("level")
    if severity:
        return severity

    # If kind has any value other than "fail", then if level is absent,
    # it SHALL default to "none"
    kind = result.get("kind", "fail")
    if kind and kind != "fail":
        return "none"

    # If kind has the value "fail" and level is absent, then...
    rule, ruleIndex = read_result_rule(result, run)
    if rule:
        # Honor the invocation's configuration override if present...
        invocation = read_result_invocation(result, run)
        if invocation:
            ruleConfigurationOverrides = invocation.get(
                "ruleConfigurationOverrides", []
            )
            override = next(
                (
                    override
                    for override in ruleConfigurationOverrides
                    if override.get("descriptor", {}).get("id") == rule.get("id")
                    or override.get("descriptor", {}).get("index") == ruleIndex
                ),
                None,
            )

            if override:
                overrideLevel = override.get("configuration", {}).get("level")
                if overrideLevel:
                    return overrideLevel

        # Otherwise, use the rule's default configuraiton if present...
        defaultConfiguration = rule.get("defaultConfiguration")
        if defaultConfiguration:
            severity = defaultConfiguration.get("level")
            if severity:
                return severity

    # Otherwise, fall back to warning
    return "warning"


def record_sort_key(record: dict) -> str:
    """Get a sort key for the record."""
    return (
        combine_record_code_and_description(record)
        + record["Location"]
        + str(record["Line"]).zfill(6)
    )
