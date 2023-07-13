#pragma once
#include <ydb/core/quoter/public/quoter.h>

#include <library/cpp/lwtrace/all.h>

#include <util/string/builder.h>

#include <type_traits>
#include <limits>

#define QUOTER_SERVICE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)     \
    PROBE(StartRequest, GROUPS("QuoterService", "ClientRequest", "Orbit"), \
          TYPES(NLWTrace::TEnumParamWithSerialization<NKikimr::TEvQuota::EResourceOperator>, TDuration, ui64), \
          NAMES("operator", "deadlineMs", "cookie"))                      \
    PROBE(RequestResource, GROUPS("QuoterService", "ClientRequest"),    \
          TYPES(ui64, TString, TString, ui64, ui64),                    \
          NAMES("amount", "quoter", "resource", "quoterId", "resourceId")) \
    PROBE(RequestDone, GROUPS("QuoterService", "ClientRequest"),        \
          TYPES(NLWTrace::TEnumParamWithSerialization<NKikimr::TEvQuota::TEvClearance::EResult>, ui64), \
          NAMES("result", "cookie"))                                    \
    PROBE(ResourceQueueState, GROUPS("QuoterService", "ClientRequest"), \
          TYPES(TString, TString, ui64, ui64, ui64, double),            \
          NAMES("quoter", "resource", "quoterId", "resourceId", "queueSize", "queueWeight")) \
    PROBE(StartCharging, GROUPS("QuoterService", "ClientRequest"),      \
          TYPES(TString, TString, ui64, ui64),                          \
          NAMES("quoter", "resource", "quoterId", "resourceId"))        \
    PROBE(Charge, GROUPS("QuoterService", "ClientRequest"),             \
          TYPES(TString, TString, ui64, ui64),                          \
          NAMES("quoter", "resource", "quoterId", "resourceId"))        \
                                                                        \
    PROBE(AllocateResource, GROUPS("QuoterService", "Resource"),        \
          TYPES(TString, TString, ui64, ui64, ui64, double, ui64, double, double, double), \
          NAMES("quoter", "resource", "quoterId", "resourceId", "requestsProcessed", "amountConsumed", "queueSize", "queueWeight", "balance", "freeBalance")) \
    PROBE(FeedResource, GROUPS("QuoterService", "Resource"),            \
          TYPES(TString, TString, ui64, ui64, double, double),          \
          NAMES("quoter", "resource", "quoterId", "resourceId", "balance", "freeBalance")) \
    /**/

LWTRACE_DECLARE_PROVIDER(QUOTER_SERVICE_PROVIDER)
