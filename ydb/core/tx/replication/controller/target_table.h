#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTargetTableBase: public TTargetWithStream {
public:
    explicit TTargetTableBase(TReplication* replication, ETargetKind finalKind,
        ui64 id, const TString& srcPath, const IProperties::TPtr& dstProperties);

    TString GetStreamPath() const override;

protected:
    IActor* CreateWorkerRegistar(const TActorContext& ctx) const override;
    virtual TString BuildStreamPath() const = 0;
};

class TTargetTable: public TTargetTableBase {
public:
    struct TTableProperties : public TPropertiesBase {
        using TPtr = std::shared_ptr<TTableProperties>;

        TTableProperties(const TString& dstPath)
            : TPropertiesBase(ETargetKind::Table, dstPath)
        {}        
    };

    explicit TTargetTable(TReplication* replication,
        ui64 id, const TString& srcPath, const IProperties::TPtr& dstProperties);

protected:
    TString BuildStreamPath() const override;
};

class TTargetIndexTable: public TTargetTableBase {
public:
    struct TIndexTableProperties : public TPropertiesBase {
        using TPtr = std::shared_ptr<TIndexTableProperties>;

        TIndexTableProperties(const TString& dstPath)
            : TPropertiesBase(ETargetKind::IndexTable, dstPath)
        {}        
    };

    explicit TTargetIndexTable(TReplication* replication,
        ui64 id, const TString& srcPath, const IProperties::TPtr& dstProperties);

protected:
    TString BuildStreamPath() const override;
};

class TTargetTransfer: public TTargetTableBase {
public:
    struct TTransferProperties : public TPropertiesBase {
        using TPtr = std::shared_ptr<TTransferProperties>;

        TTransferProperties(const TString& dstPath, const TString& transformLambda);

        const TString& GetTransformLambda() const;

    private:
        TString TransformLambda;
    };

    explicit TTargetTransfer(TReplication* replication,
        ui64 id, const TString& srcPath, const IProperties::TPtr& dstProperties);


protected:
    TString BuildStreamPath() const override;
};

}
