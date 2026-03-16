# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations


import proto  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "VanityPharmaTextEnum",
    },
)


class VanityPharmaTextEnum(proto.Message):
    r"""The text that will be displayed in display URL of the text ad
    when website description is the selected display mode for vanity
    pharma URLs.

    """

    class VanityPharmaText(proto.Enum):
        r"""Enum describing possible text.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            PRESCRIPTION_TREATMENT_WEBSITE_EN (2):
                Prescription treatment website with website
                content in English.
            PRESCRIPTION_TREATMENT_WEBSITE_ES (3):
                Prescription treatment website with website
                content in Spanish (Sitio de tratamientos con
                receta).
            PRESCRIPTION_DEVICE_WEBSITE_EN (4):
                Prescription device website with website
                content in English.
            PRESCRIPTION_DEVICE_WEBSITE_ES (5):
                Prescription device website with website
                content in Spanish (Sitio de dispositivos con
                receta).
            MEDICAL_DEVICE_WEBSITE_EN (6):
                Medical device website with website content
                in English.
            MEDICAL_DEVICE_WEBSITE_ES (7):
                Medical device website with website content
                in Spanish (Sitio de dispositivos médicos).
            PREVENTATIVE_TREATMENT_WEBSITE_EN (8):
                Preventative treatment website with website
                content in English.
            PREVENTATIVE_TREATMENT_WEBSITE_ES (9):
                Preventative treatment website with website
                content in Spanish (Sitio de tratamientos
                preventivos).
            PRESCRIPTION_CONTRACEPTION_WEBSITE_EN (10):
                Prescription contraception website with
                website content in English.
            PRESCRIPTION_CONTRACEPTION_WEBSITE_ES (11):
                Prescription contraception website with
                website content in Spanish (Sitio de
                anticonceptivos con receta).
            PRESCRIPTION_VACCINE_WEBSITE_EN (12):
                Prescription vaccine website with website
                content in English.
            PRESCRIPTION_VACCINE_WEBSITE_ES (13):
                Prescription vaccine website with website
                content in Spanish (Sitio de vacunas con
                receta).
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        PRESCRIPTION_TREATMENT_WEBSITE_EN = 2
        PRESCRIPTION_TREATMENT_WEBSITE_ES = 3
        PRESCRIPTION_DEVICE_WEBSITE_EN = 4
        PRESCRIPTION_DEVICE_WEBSITE_ES = 5
        MEDICAL_DEVICE_WEBSITE_EN = 6
        MEDICAL_DEVICE_WEBSITE_ES = 7
        PREVENTATIVE_TREATMENT_WEBSITE_EN = 8
        PREVENTATIVE_TREATMENT_WEBSITE_ES = 9
        PRESCRIPTION_CONTRACEPTION_WEBSITE_EN = 10
        PRESCRIPTION_CONTRACEPTION_WEBSITE_ES = 11
        PRESCRIPTION_VACCINE_WEBSITE_EN = 12
        PRESCRIPTION_VACCINE_WEBSITE_ES = 13


__all__ = tuple(sorted(__protobuf__.manifest))
