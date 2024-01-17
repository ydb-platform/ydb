#pragma once

#include <library/cpp/lwtrace/all.h>

#define DNSCACHELIB_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)           \
    PROBE(Created, GROUPS(), TYPES(), NAMES())                             \
    PROBE(Destroyed, GROUPS(), TYPES(), NAMES())                           \
    PROBE(AresInitFailed, GROUPS(), TYPES(), NAMES())                      \
    PROBE(FamilyMismatch,                                                  \
          GROUPS(),                                                        \
          TYPES(int, bool, bool),                                          \
          NAMES("family", "allowIpV4", "allowIpV6"))                       \
    PROBE(ResolveNullHost,                                                 \
          GROUPS(),                                                        \
          TYPES(TString, int),                                             \
          NAMES("hostname", "family"))                                     \
    PROBE(ResolveFromCache,                                                \
          GROUPS(),                                                        \
          TYPES(TString, int, TString, TString, ui64),                     \
          NAMES("hostname", "family", "addrsV4", "addrsV6", "aCacheHits")) \
    PROBE(ResolveDone,                                                     \
          GROUPS(),                                                        \
          TYPES(TString, int, TString, TString),                           \
          NAMES("hostname", "family", "addrsV4", "addrsV6"))               \
    PROBE(ResolveCacheTimeout,                                             \
          GROUPS(),                                                        \
          TYPES(TString),                                                  \
          NAMES("hostname"))                                               \
    PROBE(ResolveCacheNew,                                                 \
          GROUPS(),                                                        \
          TYPES(TString),                                                  \
          NAMES("hostname"))                                               \
    /**/

LWTRACE_DECLARE_PROVIDER(DNSCACHELIB_PROVIDER)
