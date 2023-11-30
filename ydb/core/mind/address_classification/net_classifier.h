#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/util/address_classifier.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>

#include <vector>

namespace NKikimr::NNetClassifier {

using NActors::TActorId;

inline TActorId MakeNetClassifierID() {
    static const char x[12] = "net_classvc";
    return TActorId(0, TStringBuf(x, 12));
}

NActors::IActor* CreateNetClassifier();

struct TEvNetClassifier {
    enum EEv {
        EvClassifierUpdate = EventSpaceBegin(TKikimrEvents::ES_NET_CLASSIFIER),
        EvSubscribe,
        EvUpdateTimedCounters,
        EvEnd
    };

    struct TEvClassifierUpdate : public NActors::TEventLocal<TEvClassifierUpdate, EvClassifierUpdate> {
        NAddressClassifier::TLabeledAddressClassifier::TConstPtr Classifier;
        TMaybe<TInstant> NetDataUpdateTimestamp;
    };

    struct TEvSubscribe : public NActors::TEventLocal<TEvSubscribe, EvSubscribe> {
    };

    struct TEvUpdateTimedCounters : public NActors::TEventLocal<TEvUpdateTimedCounters, EvUpdateTimedCounters> {
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_NET_CLASSIFIER), "Unexpected TEvNetClassifier event range");
};

enum ENetDataSourceType {
    None = 0,
    File,
    DistributableConfig
};

} // namespace NKikimr::NNetClassifier
