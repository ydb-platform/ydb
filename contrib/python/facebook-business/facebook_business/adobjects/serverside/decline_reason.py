# Copyright 2014 Facebook, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# Facebook.

# As with any software that integrates with the Facebook platform, your use
# of this software is subject to the Facebook Developer Principles and
# Policies [http://developers.facebook.com/policy/]. This copyright notice
# shall be included in all copies or substantial portions of the software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from enum import Enum


class DeclineReason(Enum):
    ATTRIBUTE_TO_OTHER_SOURCE = "attribute_to_other_source"
    OUT_OF_LOOKBACK_WINDOW = "out_of_lookback_window"
    VIEW_THROUGH_DISABLED = "view_through_disabled"
    WITHIN_INACTIVE_WINDOW = "within_inactive_window"
    INACTIVE = "inactive"
    FRAUD_DETECTED = "fraud_detected"
    UNKNOWN = "unknown"
    REINSTALL_ATTRIBUTION_DISABLED = "reinstall_attribution_disabled"
    LOOKBACK = "lookback"
    NOT_PMOD_MATCH = "not_pmod_match"
    VALIDATION_RULE_DETECTED = "validation_rule_detected"
    PRELOAD_INSTALL = "preload_install"
    MIN_TIME_BETWEEN_RE_ENGAGEMENTS = "min_time_between_re_engagements"
    DUPLICATED = "duplicated"
    PMOD_DISABLED = "pmod_disabled"
