#pragma once

#include <library/cpp/lwtrace/all.h>

#define SCHLAB_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(AddJob, GROUPS("SchLab"), \
        TYPES(ui64,        ui64,     ui64,        ui64), \
        NAMES("ownerGate", "timeNs", "queueSize", "queueCost")) \
    PROBE(ChangeJob, GROUPS("SchLab"), \
        TYPES(ui64,        ui64,     ui64,        ui64), \
        NAMES("ownerGate", "timeNs", "queueSize", "queueCost")) \
    PROBE(PopJob, GROUPS("SchLab"), \
        TYPES(ui64,        ui64,     ui64,        ui64), \
        NAMES("ownerGate", "timeNs", "queueSize", "queueCost")) \
    PROBE(CompleteJobInActiveCbs, GROUPS("SchLab"), \
        TYPES(ui64,        ui64,     double,     double, ui64,      i64,   i64,         i64,      i64), \
        NAMES("ownerGate", "timeNs", "prevUact", "uact", "jobCost", "dqi", "curBudget", "weight", "totalWeight")) \
    PROBE(CompleteJobInDepletedCbs, GROUPS("SchLab"), \
        TYPES(ui64,        ui64,     double,     double, ui64,      i64,   i64,         i64,      i64), \
        NAMES("ownerGate", "timeNs", "prevUact", "uact", "jobCost", "dqi", "curBudget", "weight", "totalWeight")) \
    PROBE(ActivateTask, GROUPS("SchLab"), \
        TYPES(ui64,        ui64,     double,     double, ui64,      i64,         i64,      i64), \
        NAMES("ownerGate", "timeNs", "prevUact", "uact", "jobCost", "curBudget", "weight", "totalWeight")) \
    PROBE(AddCbs, GROUPS("SchLab"), \
        TYPES(ui64,        ui64,     double,     double, i64,      i64), \
        NAMES("ownerGate", "timeNs", "prevUact", "uact", "weight", "totalWeight")) \
    PROBE(UpdateTotalWeight, GROUPS("SchLab"), \
        TYPES(double,     double, i64,               i64), \
        NAMES("prevUact", "uact", "prevTotalWeight", "totalWeight")) \
    /**/

LWTRACE_DECLARE_PROVIDER(SCHLAB_PROVIDER)
