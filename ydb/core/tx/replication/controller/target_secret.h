#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTargetSecret: public TTargetWithStream {
public:
    struct TSecretConfig: TConfigBase {
        TSecretConfig(const TString& srcPath, const TString& dstPath)
            : TConfigBase(ETargetKind::Secret, srcPath, dstPath)
        {}
    };

    TTargetSecret(TReplication* replication, ui64 id, const IConfig::TPtr& config)
        : TTargetWithStream(replication, ETargetKind::Secret, id, config)
    {
        SetStreamConsumerName(ReplicationConsumerName);
    }

    TString GetStreamPath() const override { return GetStreamName(); }
    bool UpdateStats(ui64, const NKikimrReplication::TWorkerStats&) override { return false; }
    void WorkerStatusChanged(ui64, ui64) override {}
    const TReplication::ITargetStats* GetStats() override { return nullptr; }

protected:
    void ProgressDestination(const TActorContext& ctx) override;
    IActor* CreateStreamRemoverActor(const TActorContext& ctx) const override;
    bool WaitForStreamCreatorOnRemoval() const override { return true; }
    IActor* CreateWorkerRegistar(const TActorContext&) const override {
        Y_ABORT("Secret targets use initial copying");
    }

};

}
