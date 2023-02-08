#pragma once

#include "defs.h"
#include "test_load_actor.h"

#include <ydb/core/base/events.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NDataShardLoad {

TString GetKey(size_t n);

static const TString Value = TString(100, 'x');

struct TEvPrivate {
    enum EEv {
        EvKeys = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvPointTimes,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE));

    struct TEvKeys : public TEventLocal<TEvKeys, EvKeys> {
        TVector<TOwnedCellVec> Keys;

        TEvKeys(TVector<TOwnedCellVec>&& keys)
            : Keys(std::move(keys))
        {
        }
    };

    struct TEvPointTimes : public TEventLocal<TEvPointTimes, EvPointTimes> {
        TVector<TDuration> RequestTimes;

        TEvPointTimes(TVector<TDuration>&& requestTime)
            : RequestTimes(std::move(requestTime))
        {
        }
    };
};

TVector<TCell> ToCells(const std::vector<TString> &keys);

void AddRangeQuery(TEvDataShard::TEvRead &request,
                   const std::vector<TString> &from, bool fromInclusive,
                   const std::vector<TString> &to, bool toInclusive);

void AddKeyQuery(TEvDataShard::TEvRead &request, const TOwnedCellVec &key);

IActor *CreateReadIteratorScan(
    TEvDataShard::TEvRead* request,
    ui64 tablet,
    const TActorId& parent,
    const TSubLoadId& id,
    ui64 sample);

} // NKikimr::NDataShardLoad
