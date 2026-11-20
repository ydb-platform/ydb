#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTargetTopic: public TTargetWithStream {
public:
    struct TTopicConfig: public TConfigBase {
        TTopicConfig(const TString& srcPath, const TString& dstPath)
            : TConfigBase(ETargetKind::Topic, srcPath, dstPath)
        {}
    };

    explicit TTargetTopic(TReplication* replication, ui64 id, const IConfig::TPtr& config);

    TString BuildStreamConsumerName() const override;
    TString GetStreamPath() const override;

protected:
    IActor* CreateWorkerRegistar(const TActorContext& ctx) const override;
};

}
