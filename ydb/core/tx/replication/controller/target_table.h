#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTargetTableBase: public TTargetWithStream {
public:
    explicit TTargetTableBase(TReplication* replication, ETargetKind finalKind,
        ui64 id, const TString& srcPath, const TString& dstPath);

protected:
    IActor* CreateWorkerRegistar(const TActorContext& ctx) const override;
    virtual TString BuildStreamPath() const = 0;
};

class TTargetTable: public TTargetTableBase {
public:
    explicit TTargetTable(TReplication* replication,
        ui64 id, const TString& srcPath, const TString& dstPath);

protected:
    TString BuildStreamPath() const override;
};

class TTargetIndexTable: public TTargetTableBase {
public:
    explicit TTargetIndexTable(TReplication* replication,
        ui64 id, const TString& srcPath, const TString& dstPath);

protected:
    TString BuildStreamPath() const override;
};

}
