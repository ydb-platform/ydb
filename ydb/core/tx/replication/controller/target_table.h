#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTableTarget: public TTargetWithStream {
public:
    explicit TTableTarget(TReplication* replication,
        ui64 id, const TString& srcPath, const TString& dstPath);

protected:
    IActor* CreateWorkerRegistar(const TActorContext& ctx) const override;

}; // TTableTarget

}
