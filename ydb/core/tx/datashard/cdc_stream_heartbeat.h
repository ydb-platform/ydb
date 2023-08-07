#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/queue.h>

namespace NKikimr::NDataShard {

class TCdcStreamHeartbeatManager {
    struct THeartbeatInfo {
        TPathId TablePathId;
        TDuration Interval;
        TRowVersion Last;
    };

    struct TNextHeartbeat {
        TPathId StreamPathId;
        TRowVersion Version;

        explicit TNextHeartbeat(const TPathId& streamPathId, const TRowVersion& version)
            : StreamPathId(streamPathId)
            , Version(version)
        {}

        friend bool operator>(const TNextHeartbeat& a, const TNextHeartbeat& b) {
            return a.Version > b.Version;
        }
    };

    void PersistUpdate(NIceDb::TNiceDb& db, const TPathId& tablePathId, const TPathId& streamPathId, const THeartbeatInfo& info);
    void PersistRemove(NIceDb::TNiceDb& db, const TPathId& tablePathId, const TPathId& streamPathId);

public:
    void Reset();
    bool Load(NIceDb::TNiceDb& db);

    void AddCdcStream(NTable::TDatabase& db, const TPathId& tablePathId, const TPathId& streamPathId, TDuration heartbeatInterval);
    void DropCdcStream(NTable::TDatabase& db, const TPathId& tablePathId, const TPathId& streamPathId);

    TRowVersion LowestVersion() const;
    bool ShouldEmitHeartbeat(const TRowVersion& edge) const;
    THashMap<TPathId, THeartbeatInfo> EmitHeartbeats(NTable::TDatabase& db, const TRowVersion& edge);

private:
    THashMap<TPathId, THeartbeatInfo> CdcStreams;
    TPriorityQueue<TNextHeartbeat, TVector<TNextHeartbeat>, TGreater<TNextHeartbeat>> Schedule;
};

}
