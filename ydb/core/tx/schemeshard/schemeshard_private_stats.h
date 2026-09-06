#pragma once

#include "schemeshard_private.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NSchemeShard::TEvPrivate {

// Sent by TStatsParserActor back to the schemeshard after it parsed a raw
// TEvDataShard::TEvPeriodicTableStats. Carries the same handle (not just the record) so the
// original datashard Sender survives for VerifySplitAndRequestStats, and the schemeshard's
// Get() is a cache hit rather than a second parse.
struct TEvPeriodicTableStatsParsed : public TEventLocal<TEvPeriodicTableStatsParsed, EvPeriodicTableStatsParsed> {
    TEvDataShard::TEvPeriodicTableStats__HandlePtr Ev;

    explicit TEvPeriodicTableStatsParsed(TEvDataShard::TEvPeriodicTableStats__HandlePtr&& ev)
        : Ev(std::move(ev))
    {}
};

} // namespace NKikimr::NSchemeShard::TEvPrivate
