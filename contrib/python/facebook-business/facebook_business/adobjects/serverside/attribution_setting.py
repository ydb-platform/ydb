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

import six


class AttributionSetting(object):
    param_types = {
        "inactivity_window_hours": "int",
        "reattribution_window_hours": "int",
    }

    def __init__(self, inactivity_window_hours=None, reattribution_window_hours=None):
        """Attribution Setting"""
        self._inactivity_window_hours = None
        self._reattribution_window_hours = None

        if inactivity_window_hours is not None:
            self.inactivity_window_hours = inactivity_window_hours
        if reattribution_window_hours is not None:
            self.reattribution_window_hours = reattribution_window_hours

    @property
    def inactivity_window_hours(self):
        """Gets the inactivity_window_hours of Attribution Setting.

        The inactivity window in hours.

        :return: The inactivity_window_hours of Attribution Setting.
        :rtype: int
        """
        return self._inactivity_window_hours

    @inactivity_window_hours.setter
    def inactivity_window_hours(self, inactivity_window_hours):
        """Sets the inactivity_window_hours of Attribution Setting.

        The inactivity window in hours.

        :param inactivity_window_hours: The inactivity_window_hours of Attribution Setting.
        :type: int
        """
        self._inactivity_window_hours = inactivity_window_hours

    @property
    def reattribution_window_hours(self):
        """Gets the reattribution_window_hours of Attribution Setting.

        The reattribution window in hours.

        :return: The reattribution_window_hours of Attribution Setting.
        :rtype: int
        """
        return self._reattribution_window_hours

    @reattribution_window_hours.setter
    def reattribution_window_hours(self, reattribution_window_hours):
        """Sets the reattribution_window_hours of Attribution Setting.

        The reattribution window in hours.

        :param reattribution_window_hours: The reattribution_window_hours of Attribution Setting.
        :type: int
        """
        self._reattribution_window_hours = reattribution_window_hours

    def normalize(self):
        normalized_payload = {
            "inactivity_window_hours": self.inactivity_window_hours,
            "reattribution_window_hours": self.reattribution_window_hours,
        }
        normalized_payload = {
            k: v for k, v in normalized_payload.items() if v is not None
        }
        return normalized_payload

    def to_dict(self):
        return self.normalize()
