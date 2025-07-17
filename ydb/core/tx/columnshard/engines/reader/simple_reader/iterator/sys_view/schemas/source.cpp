#include "source.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/system/hostname.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {
std::shared_ptr<arrow::Array> TSourceData::BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const {
    //        PrimaryIndexSchemaStats
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexSchemaStats::TabletId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexSchemaStats::PresetId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Schemas) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetIndexInfo().GetPresetId());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexSchemaStats::SchemaVersion::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Schemas) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetVersion());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexSchemaStats::SchemaSnapshotPlanStep::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Schemas) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetSnapshot().GetPlanStep());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexSchemaStats::SchemaSnapshotTxId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Schemas) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetSnapshot().GetTxId());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexSchemaStats::SchemaDetails::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : Schemas) {
            const TString jsonDescription = i->DebugJson().GetStringRobust();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(jsonDescription.data(), jsonDescription.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    AFL_VERIFY(false)("column_id", columnId);
    return nullptr;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
