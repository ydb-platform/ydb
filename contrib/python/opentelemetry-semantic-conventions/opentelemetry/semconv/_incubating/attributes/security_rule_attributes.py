# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

SECURITY_RULE_CATEGORY: Final = "security_rule.category"
"""
A categorization value keyword used by the entity using the rule for detection of this event.
"""

SECURITY_RULE_DESCRIPTION: Final = "security_rule.description"
"""
The description of the rule generating the event.
"""

SECURITY_RULE_LICENSE: Final = "security_rule.license"
"""
Name of the license under which the rule used to generate this event is made available.
"""

SECURITY_RULE_NAME: Final = "security_rule.name"
"""
The name of the rule or signature generating the event.
"""

SECURITY_RULE_REFERENCE: Final = "security_rule.reference"
"""
Reference URL to additional information about the rule used to generate this event.
Note: The URL can point to the vendor’s documentation about the rule. If that’s not available, it can also be a link to a more general page describing this type of alert.
"""

SECURITY_RULE_RULESET_NAME: Final = "security_rule.ruleset.name"
"""
Name of the ruleset, policy, group, or parent category in which the rule used to generate this event is a member.
"""

SECURITY_RULE_UUID: Final = "security_rule.uuid"
"""
A rule ID that is unique within the scope of a set or group of agents, observers, or other entities using the rule for detection of this event.
"""

SECURITY_RULE_VERSION: Final = "security_rule.version"
"""
The version / revision of the rule being used for analysis.
"""
