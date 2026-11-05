#pragma once

#include "target_base.h"

namespace NKikimr::NReplication::NController {

class TTargetSecret: public TTargetBase {
public:
    struct TSecretConfig: TConfigBase {
        TSecretConfig(const TString& srcPath, const TString& dstPath)
            : TConfigBase(ETargetKind::Secret, srcPath, dstPath)
        {}
    };

    TTargetSecret(TReplication* replication, ui64 id, const IConfig::TPtr& config)
        : TTargetBase(replication, ETargetKind::Secret, id, config)
    {}

    bool IsExpectedStreamCreator(const TActorId&) const override { return false; }
    TString GetStreamPath() const override { return GetStreamName(); }
    bool UpdateStats(ui64, const NKikimrReplication::TWorkerStats&) override { return false; }
    void WorkerStatusChanged(ui64, ui64) override {}
    const TReplication::ITargetStats* GetStats() override { return nullptr; }
    void Progress(const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

protected:
    IActor* CreateWorkerRegistar(const TActorContext&) const override {
        Y_ABORT("Secret targets use initial copying");
    }

private:
    TActorId TopicRemover;
};

}
