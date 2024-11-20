#pragma once
#include "read_metadata.h"
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/program/program.h>

namespace NKikimr::NOlap::NReader {

class IScannerConstructor {
protected:
    const TSnapshot Snapshot;
    const ui64 ItemsLimit;
    const bool IsReverse;
    TConclusionStatus ParseProgram(const TVersionedIndex* vIndex, const NKikimrSchemeOp::EOlapProgramType programType,
        const TString& serializedProgram, TReadDescription& read, const IColumnResolver& columnResolver) const;
private:
    virtual TConclusion<std::shared_ptr<TReadMetadataBase>> DoBuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const = 0;
public:
    using TFactory = NObjectFactory::TParametrizedObjectFactory<IScannerConstructor, TString, NKikimrTxDataShard::TEvKqpScan>;
    virtual ~IScannerConstructor() = default;

    IScannerConstructor(const NKikimrTxDataShard::TEvKqpScan& request)
        : Snapshot(snapshot)
        , ItemsLimit(request.HasItemsLimit() ? request.GetItemsLimit() : 0)
        , IsReverse(request.GetReverse())
    {

    }

    virtual TConclusionStatus ParseProgram(const TVersionedIndex* vIndex, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const = 0;
    virtual std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const = 0;
    TConclusion<std::shared_ptr<TReadMetadataBase>> BuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const;
};

}