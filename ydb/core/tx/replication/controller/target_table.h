#pragma once

#include "target_with_stream.h"

namespace NKikimr::NReplication::NController {

class TTargetTable: public TTargetWithStream {
public:
    struct TTableConfig: public TConfigBase {
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

class TTargetIndexTable: public TTargetWithStream {
public:
    struct TIndexTableConfig: public TConfigBase {
        using TPtr = std::shared_ptr<TIndexTableConfig>;

        TIndexTableConfig(const TString& srcPath, const TString& dstPath, bool srcIndexTable = false, bool rowIdIndex = false)
            : TConfigBase(ETargetKind::IndexTable, srcPath, dstPath)
            , SrcIndexTable(srcIndexTable)
            , RowIdIndex(rowIdIndex)
        {}

        bool IsSrcIndexTable() const override { return SrcIndexTable; }
        bool IsRowIdIndex() const override { return RowIdIndex; }

    private:
        const bool SrcIndexTable;
        const bool RowIdIndex;
    };

    explicit TTargetIndexTable(TReplication* replication,
        ui64 id, const IConfig::TPtr& config);

protected:
    TString BuildStreamPath() const override;
};

}
