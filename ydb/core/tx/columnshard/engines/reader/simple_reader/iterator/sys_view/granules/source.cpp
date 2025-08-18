#include "source.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/system/hostname.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules {
std::shared_ptr<arrow::Array> TSourceData::BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const {
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexGranuleStats::PathId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : ExternalPathIds) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRawValue());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexGranuleStats::TabletId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexGranuleStats::PortionsCount::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : PortionsCount) {
            NArrow::Append<arrow::UInt64Type>(*builder, i);
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexGranuleStats::HostName::ColumnId) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::StringScalar(::HostName()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexGranuleStats::NodeId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(
            arrow::MakeArrayFromScalar(arrow::UInt64Scalar(NActors::TActivationContext::AsActorContext().SelfID.NodeId()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexGranuleStats::InternalPathId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Granules) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetPathId().GetRawValue());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    AFL_VERIFY(false)("column_id", columnId);
    return nullptr;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules
