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
    {}

    ui64 GetCreateTxId() const { return CreateTxId; }
    void SetCreateTxId(ui64 txId) { CreateTxId = txId; }

protected:
    TString BuildStreamPath() const override;
    void ProgressDestination(const TActorContext& ctx) override;
    bool WaitForStreamCreatorOnRemoval() const override { return true; }
    IActor* CreateWorkerRegistar(const TActorContext&) const override {
        Y_ABORT("Secret targets use initial copying");
    }

private:
    ui64 CreateTxId = 0;

};

}
