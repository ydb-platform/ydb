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
    YDB_READONLY(TReadMetadataBase::ESorting, Sorting, TReadMetadataBase::ESorting::NONE);

public:
    TScannerConstructorContext(const TSnapshot& snapshot, const ui32 itemsLimit, const TReadMetadataBase::ESorting sorting)
        : Snapshot(snapshot)
        , ItemsLimit(itemsLimit)
        , Sorting(sorting) {
    }
};

class TProgramParsingContext {
private:
    const TVersionedPresetSchemas& VersionedSchemas;

public:
    const TVersionedPresetSchemas& GetVersionedSchemas() const {
        return VersionedSchemas;
    }

    TProgramParsingContext(const TVersionedPresetSchemas& schemas)
        : VersionedSchemas(schemas) {
    }
};

class IScannerConstructor {
protected:
    const TSnapshot Snapshot;
    const ui64 ItemsLimit;
    const TReadMetadataBase::ESorting Sorting;
    TConclusionStatus ParseProgram(const TProgramParsingContext& context, const NKikimrSchemeOp::EOlapProgramType programType,
        const TString& serializedProgram, TReadDescription& read, const NArrow::NSSA::IColumnResolver& columnResolver) const;

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
        , Sorting(context.GetSorting()) {
    }

    TConclusion<std::shared_ptr<IScanCursor>> BuildCursorFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) const;
    virtual TConclusionStatus ParseProgram(
        const TProgramParsingContext& context, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const = 0;
    virtual std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const = 0;
    TConclusion<std::shared_ptr<TReadMetadataBase>> BuildReadMetadata(
        const NColumnShard::TColumnShard* self, const TReadDescription& read) const;
};

}   // namespace NKikimr::NOlap::NReader
