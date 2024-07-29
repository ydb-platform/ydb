#pragma once

#include <ydb/core/protos/table_stats.pb.h>
#include <util/datetime/base.h>
#include <ydb/core/base/appdata_fwd.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {

class TColumnTablesCounters {
private:
    YDB_READONLY(TInstant, LastAccessTime, TInstant::Zero());
    YDB_READONLY(TInstant, LastUpdateTime, TInstant::Zero());

    THashMap<ui64, class TSingleColumnTableCounters> PathIdCounters;

    friend class TSingleColumnTableCounters;

public:
    void FillStats(::NKikimrTableStats::TTableStats& output) const {
        output.SetLastAccessTime(LastAccessTime.MilliSeconds());
        output.SetLastUpdateTime(LastUpdateTime.MilliSeconds());
    }

    TSingleColumnTableCounters& GetPathIdCounter(ui64 pathId);
};

class TSingleColumnTableCounters {
private:
    YDB_READONLY(TInstant, PathIdLastAccessTime, TInstant::Zero());
    YDB_READONLY(TInstant, PathIdLastUpdateTime, TInstant::Zero());

    TInstant& TotalLastAccessTime;
    TInstant& TotalLastUpdateTime;

public:
    TSingleColumnTableCounters(TColumnTablesCounters& owner)
        : TotalLastAccessTime(owner.LastAccessTime)
        , TotalLastUpdateTime(owner.LastUpdateTime) {
    }

    void OnAccess() {
        UpdateLastAccessTime(TAppData::TimeProvider->Now());
    }

    void OnUpdate() {
        TInstant now = TAppData::TimeProvider->Now();
        UpdateLastUpdateTime(now);
        UpdateLastAccessTime(now);
    }

    void FillStats(::NKikimrTableStats::TTableStats& output) const {
        output.SetLastAccessTime(PathIdLastAccessTime.MilliSeconds());
        output.SetLastUpdateTime(PathIdLastUpdateTime.MilliSeconds());
    }

private:
    void UpdateLastAccessTime(TInstant value) {
        if (PathIdLastAccessTime < value) {
            PathIdLastAccessTime = value;
        }
        if (TotalLastAccessTime < value) {
            TotalLastAccessTime = value;
        }
    }

    void UpdateLastUpdateTime(TInstant value) {
        if (PathIdLastUpdateTime < value) {
            PathIdLastUpdateTime = value;
        }
        if (TotalLastUpdateTime < value) {
            TotalLastUpdateTime = value;
        }
    }
};

} // namespace NKikimr::NColumnShard
