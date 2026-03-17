#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#

#
# Preconfigured sets of parser options.
# Individual options could be used in certain combinations.
#
import warnings


smi_v2 = {}  # TODO: move strict mode here.

smi_v1 = smi_v2.copy()
smi_v1.update(supportSmiV1Keywords=True, supportIndex=True)

smi_v1_relaxed = smi_v1.copy()
smi_v1_relaxed.update(
    commaAtTheEndOfImport=True,
    commaAtTheEndOfSequence=True,
    mixOfCommasAndSpaces=True,
    uppercaseIdentifier=True,
    lowcaseIdentifier=True,
    curlyBracesAroundEnterpriseInTrap=True,
    noCells=True,
)

# Compatibility API
deprecated_attributes = {
    "smiV1Relaxed": "smi_v1_relaxed",
    "smiV1": "smi_v1",
    "smiV2": "smi_v2",
}


def __getattr__(attr: str):
    if new_attr := deprecated_attributes.get(attr):
        warnings.warn(
            f"{attr} is deprecated. Please use {new_attr} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[new_attr]
    raise AttributeError(f"module '{__name__}' has no attribute '{attr}'")
