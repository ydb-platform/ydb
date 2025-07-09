#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTargetTableBase: public TTargetWithStream {
public:
    explicit TTargetTableBase(TReplication* replication, ETargetKind finalKind,
        ui64 id, const IConfig::TPtr& config);

    TString GetStreamPath() const override;

protected:
    virtual TString BuildStreamPath() const = 0;
};

class TTargetTable: public TTargetTableBase {
public:
    struct TTableConfig : public TConfigBase {
        using TPtr = std::shared_ptr<TTableConfig>;

        TTableConfig(const TString& srcPath, const TString& dstPath)
            : TConfigBase(ETargetKind::Table, srcPath, dstPath)
        {}        
    };

    explicit TTargetTable(TReplication* replication,
        ui64 id, const IConfig::TPtr& config);

protected:
    TString BuildStreamPath() const override;
};

class TTargetIndexTable: public TTargetTableBase {
public:
    struct TIndexTableConfig : public TConfigBase {
        using TPtr = std::shared_ptr<TIndexTableConfig>;

        TIndexTableConfig(const TString& srcPath, const TString& dstPath)
            : TConfigBase(ETargetKind::IndexTable, srcPath, dstPath)
        {}        
    };

    explicit TTargetIndexTable(TReplication* replication,
        ui64 id, const IConfig::TPtr& config);

protected:
    TString BuildStreamPath() const override;
};

}
