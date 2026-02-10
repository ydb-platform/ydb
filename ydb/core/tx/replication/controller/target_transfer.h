#pragma once

#include "target_with_stream.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/replication/service/worker.h>


namespace NKikimr::NReplication::NController {


class TTargetTransfer: public TTargetWithStream {
    using TBase = TTargetWithStream;

public:
    struct TTransferConfig: public TConfigBase {
        using TPtr = std::shared_ptr<TTransferConfig>;

        TTransferConfig(const TString& srcPath, const TString& dstPath, const TString& transformLambda, const TString& runAsUser, const TString& directoryPath);

        const TString& GetTransformLambda() const;
        const TString& GetRunAsUser() const;
        const TString& GetDirectoryPath() const;

    private:
        TString TransformLambda;
        TString RunAsUser;
        TString DirectoryPath;
    };

    explicit TTargetTransfer(TReplication* replication,
        ui64 id, const IConfig::TPtr& config);

    void UpdateConfig(const NKikimrReplication::TReplicationConfig&) override;

    void Progress(const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

    TString GetStreamPath() const override;
    void EnsureCounters();

    void UpdateStats(ui64 workerId, const NKikimrReplication::TWorkerStats& stats) override;
    void WorkerStatusChanged(ui64 workerId, ui64 status) override;
    void RemoveWorker(ui64 id) override;

private:
    TActorId StreamConsumerRemover;
    ui64 MetricsLevel = 0;
};
}
