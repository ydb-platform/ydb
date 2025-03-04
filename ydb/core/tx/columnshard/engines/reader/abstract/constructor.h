#pragma once
#include "read_metadata.h"

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/program/program.h>

namespace NKikimr::NOlap::NReader {

class TScannerConstructorContext {
private:
    YDB_READONLY(TSnapshot, Snapshot, TSnapshot::Zero());
    YDB_READONLY(ui32, ItemsLimit, 0);
    YDB_READONLY(bool, Reverse, false);

public:
    TScannerConstructorContext(const TSnapshot& snapshot, const ui32 itemsLimit, const bool reverse)
        : Snapshot(snapshot)
        , ItemsLimit(itemsLimit)
        , Reverse(reverse) {
    }
};

class IScannerConstructor {
protected:
    const TSnapshot Snapshot;
    const ui64 ItemsLimit;
    const bool IsReverse;
    TConclusionStatus ParseProgram(const TVersionedIndex* vIndex, const NKikimrSchemeOp::EOlapProgramType programType,
        const TString& serializedProgram, TReadDescription& read, const NArrow::NSSA::IColumnResolver& columnResolver) const noexcept;

private:
    virtual TConclusion<std::shared_ptr<TReadMetadataBase>> DoBuildReadMetadata(
        const NColumnShard::TColumnShard* self, const TReadDescription& read) const = 0;
    virtual std::shared_ptr<IScanCursor> DoBuildCursor() const = 0;

public:
    using TFactory = NObjectFactory::TParametrizedObjectFactory<IScannerConstructor, TString, TScannerConstructorContext>;
    virtual ~IScannerConstructor() = default;

    IScannerConstructor(const TScannerConstructorContext& context)
        : Snapshot(context.GetSnapshot())
        , ItemsLimit(context.GetItemsLimit())
        , IsReverse(context.GetReverse()) {
    }

    TConclusion<std::shared_ptr<IScanCursor>> BuildCursorFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) const;
    virtual TConclusionStatus ParseProgram(
        const TVersionedIndex* vIndex, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const = 0;
    virtual std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const = 0;
    TConclusion<std::shared_ptr<TReadMetadataBase>> BuildReadMetadata(
        const NColumnShard::TColumnShard* self, const TReadDescription& read) const;
};

}   // namespace NKikimr::NOlap::NReader
