#pragma once
#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/lwtrace/all.h>

#include <util/string/builder.h>

#include <type_traits>
#include <limits>

struct TActorIdParam {
    using TStoreType = TString;
    using TFuncParam = typename TTypeTraits<NActors::TActorId>::TFuncParam;

    inline static void ToString(typename TTypeTraits<TStoreType>::TFuncParam stored, TString* out) {
        *out = stored;
    }

    inline static TStoreType ToStoreType(TFuncParam v) {
        return TStringBuilder() << v;
    }
};

#define KESUS_QUOTER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)       \
    PROBE(ResourceProcess, GROUPS("QuoterResource"),                    \
          TYPES(TString, TString, TInstant, bool, size_t),              \
          NAMES("quoter", "resource", "timestampSec", "active", "activeChildren")) \
    PROBE(ResourceAccumulateResource, GROUPS("QuoterResource"),         \
          TYPES(TString, TString, TInstant, bool, double),              \
          NAMES("quoter", "resource", "timestampSec", "active", "spent")) \
    PROBE(ResourceActivate, GROUPS("QuoterResource"),                   \
          TYPES(TString, TString),                                      \
          NAMES("quoter", "resource"))                                  \
    PROBE(ResourceDeactivate, GROUPS("QuoterResource"),                 \
          TYPES(TString, TString),                                      \
          NAMES("quoter", "resource"))                                  \
    PROBE(ResourceGiveToChild, GROUPS("QuoterResource"),                \
          TYPES(TString, TString, TInstant, double, ui32),              \
          NAMES("quoter", "resource", "timestampSec", "giveAmount", "childWeight")) \
                                                                        \
    PROBE(ResourceBillSend, GROUPS("QuoterResource", "RateAccounting"), \
          TYPES(TString, TString, TString, ui64, TInstant, TInstant, TString, TString, TString, TString, TString, TString), \
          NAMES("quoter", "resource", "category", "quantity", "billStartSec", "billEndSec", "version", "schema", "cloudId", "folderId", "resourceId", "sourceId")) \
    PROBE(ResourceBillAdd, GROUPS("QuoterResource", "RateAccounting"), \
          TYPES(TString, TString, TString, TInstant, TInstant, TDuration, double, double), \
          NAMES("quoter", "resource", "category", "accumulatedSec", "accountSec", "accDeltaMs", "quantity", "value")) \
    PROBE(ResourceAccountConfigure, GROUPS("QuoterResource", "RateAccounting"), \
          TYPES(TString, TString, double, double, double, TInstant, double, double, double, TInstant), \
          NAMES("quoter", "resource", "provisionedRate", "provisionedBurst", "provisionedAvail", "provisionedLastSec", "onDemandRate", "onDemandBurst", "onDemandAvail", "onDemandLastSec")) \
    PROBE(ResourceAccount, GROUPS("QuoterResource", "RateAccounting"), \
          TYPES(TString, TString, TInstant, double, double, double, double, double, double, double, double, double, double), \
          NAMES("quoter", "resource", "accountSec", "provisionedAndOnDemand", "provisioned", "onDemand", "overshoot", "provisionedRate", "provisionedBurst", "provisionedAvail", "onDemandRate", "onDemandBurst", "onDemandAvail")) \
    PROBE(ResourceReportDedup, GROUPS("QuoterResource", "RateAccounting", "RateAccountingReport"), \
          TYPES(TString, TString, ui64, ui64, TInstant), \
          NAMES("quoter", "resource", "skip", "size", "reportSec")) \
    PROBE(ResourceReportDedupFull, GROUPS("QuoterResource", "RateAccounting", "RateAccountingReport"), \
          TYPES(TString, TString, ui64, ui64, TInstant), \
          NAMES("quoter", "resource", "skip", "size", "reportSec")) \
    PROBE(ResourceReportAdd, GROUPS("QuoterResource", "RateAccounting", "RateAccountingReport"), \
          TYPES(TString, TString, ui64, ui64, TInstant, TInstant), \
          NAMES("quoter", "resource", "added", "size", "reportSec", "accountedSec")) \
    PROBE(ResourceAccountingTooEarly, GROUPS("QuoterResource", "RateAccounting"), \
          TYPES(TString, TString, TInstant, TInstant, TDuration), \
          NAMES("quoter", "resource", "accountTillSec", "accountedSec", "accountPeriodMs")) \
    PROBE(ResourceAccountingNoData, GROUPS("QuoterResource", "RateAccounting"), \
          TYPES(TString, TString, TInstant, TInstant, TDuration), \
          NAMES("quoter", "resource", "historyEndSec", "accountedSec", "maxBillingPeriodMs")) \
    PROBE(ResourceAccountingSend, GROUPS("QuoterResource", "RateAccounting"), \
          TYPES(TString, TString, TInstant, TInstant), \
          NAMES("quoter", "resource", "accountTillSec", "accountedSec")) \
                                                                        \
    PROBE(SessionProcess, GROUPS("QuoterSession"),                      \
          TYPES(TString, TString, TActorIdParam, TInstant, bool),       \
          NAMES("quoter", "resource", "session", "timestampSec", "active")) \
    PROBE(SessionAccumulateResource, GROUPS("QuoterSession"),           \
          TYPES(TString, TString, TActorIdParam, TInstant, bool, double), \
          NAMES("quoter", "resource", "session", "timestampSec", "active", "spent")) \
    PROBE(SessionActivate, GROUPS("QuoterSession"),                     \
          TYPES(TString, TString, TActorIdParam),                       \
          NAMES("quoter", "resource", "session"))                       \
    PROBE(SessionDeactivate, GROUPS("QuoterSession"),                   \
          TYPES(TString, TString, TActorIdParam),                       \
          NAMES("quoter", "resource", "session"))                       \
    PROBE(SessionUpdateConsumptionState, GROUPS("QuoterSession"),       \
          TYPES(TString, TString, TActorIdParam, bool, double),         \
          NAMES("quoter", "resource", "session", "consume", "amount"))  \
    PROBE(SessionSend, GROUPS("QuoterSession"),                         \
          TYPES(TString, TString, TActorIdParam, double),               \
          NAMES("quoter", "resource", "session", "amount"))             \
    /**/

LWTRACE_DECLARE_PROVIDER(KESUS_QUOTER_PROVIDER)
