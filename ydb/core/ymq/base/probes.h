#pragma once

#include <library/cpp/lwtrace/all.h>

#define SQS_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                \
    PROBE(CreateLeader, GROUPS("SqsLeadersLifeTime"),                   \
          TYPES(TString, TString, TString),                             \
          NAMES("user", "queue", "reason"))                             \
    PROBE(DestroyLeader, GROUPS("SqsLeadersLifeTime"),                  \
          TYPES(TString, TString, TString),                             \
          NAMES("user", "queue", "reason"))                             \
    PROBE(IncLeaderRef, GROUPS("SqsLeadersLifeTime"),                   \
          TYPES(TString, TString, TString),                             \
          NAMES("user", "queue", "referer"))                            \
    PROBE(DecLeaderRef, GROUPS("SqsLeadersLifeTime"),                   \
          TYPES(TString),                                               \
          NAMES("rerefer"))                                             \
    PROBE(IncLeaderRefAlreadyHasRef, GROUPS("SqsLeadersLifeTime"),      \
          TYPES(TString, TString, TString),                             \
          NAMES("user", "queue", "referer"))                            \
    PROBE(DecLeaderRefNotInRefSet, GROUPS("SqsLeadersLifeTime"),        \
          TYPES(TString),                                               \
          NAMES("rerefer"))                                             \
                                                                        \
    PROBE(QueueAttributesCacheMiss, GROUPS("SqsPerformance"),           \
          TYPES(TString, TString, TString),                             \
          NAMES("user", "queue", "requestId"))                          \
    PROBE(QueueRequestCacheMiss, GROUPS("SqsPerformance"),              \
          TYPES(TString, TString, TString, TString),                    \
          NAMES("user", "queue", "requestId", "requestType"))           \
    PROBE(LoadInfly, GROUPS("SqsPerformance"),                          \
          TYPES(TString, TString, ui64, ui64),                          \
          NAMES("user", "queue", "shard", "count"))                     \
    PROBE(AddMessagesToInfly, GROUPS("SqsPerformance"),                 \
          TYPES(TString, TString, ui64, ui64),                          \
          NAMES("user", "queue", "shard", "count"))                     \
    PROBE(InflyInvalidation, GROUPS("SqsPerformance"),                  \
          TYPES(TString, TString, ui64, ui64, TString),                 \
          NAMES("user", "queue", "shard", "count", "reason"))           \
    /**/

LWTRACE_DECLARE_PROVIDER(SQS_PROVIDER)
