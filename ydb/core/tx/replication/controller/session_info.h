#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/tx/replication/common/worker_id.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NKikimrReplication {
    class TRunWorkerCommand;
}

namespace NKikimr::NReplication::NController {

class TSessionInfo {
public:
    TSessionInfo();

    void SetReady();
    bool IsReady() const;

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
    explicit TWorkerInfo(NKikimrReplication::TRunWorkerCommand* cmd = nullptr);

    void SetCommand(NKikimrReplication::TRunWorkerCommand* cmd);
    bool HasCommand() const;
    const NKikimrReplication::TRunWorkerCommand* GetCommand() const;

    void AttachSession(ui32 nodeId);
    void ClearSession();
    bool HasSession() const;
    ui32 GetSession() const;

    bool IsDataEnded() const;
    void SetDataEnded(bool value);

    void SetHeartbeat(const TRowVersion& value);
    bool HasHeartbeat() const;
    const TRowVersion& GetHeartbeat() const;

private:
    THolder<NKikimrReplication::TRunWorkerCommand> Command;
    TMaybe<ui32> Session;
    bool DataEnded = false;
    TMaybe<TRowVersion> Heartbeat;
};

}
