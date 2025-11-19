#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTargetTransfer: public TTargetWithStream {
public:
    struct TTransferConfig : public TConfigBase {
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
        
private:
    TActorId StreamConsumerRemover;
};

}
