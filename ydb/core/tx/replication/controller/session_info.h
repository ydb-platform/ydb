#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/tx/replication/common/worker_id.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NKikimr::NReplication::NController {

class TSessionInfo {
public:
    TSessionInfo();

    void SetReady();
    bool IsReady() const;
    bool SupportsSourceOffsets = false;

    void AttachWorker(const TWorkerId& id);
    void DetachWorker(const TWorkerId& id);
    const THashSet<TWorkerId>& GetWorkers() const;
    bool HasWorker(const TWorkerId& id) const;

private:
    bool Ready;
    THashSet<TWorkerId> Workers;
};

class TWorkerInfo {
public:
    void AttachSession(ui32 nodeId);
    void ClearSession();
    bool HasSession() const;
    ui32 GetSession() const;

    bool IsDataEnded() const;
    void SetDataEnded(bool value);

    void SetChildPartitionIds(TVector<ui64>&& value);
    const TVector<ui64>& GetChildPartitionIds() const;

    void SetHeartbeat(const TRowVersion& value);
    void ClearHeartbeat();
    bool HasHeartbeat() const;
    const TRowVersion& GetHeartbeat() const;

private:
    TMaybe<ui32> Session;
    bool DataEnded = false;
    TVector<ui64> ChildPartitionIds;
    TMaybe<TRowVersion> Heartbeat;
};

}
