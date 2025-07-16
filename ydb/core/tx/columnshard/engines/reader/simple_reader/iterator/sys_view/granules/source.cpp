#include "source.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/system/hostname.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules {
std::shared_ptr<arrow::Array> TSourceData::BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const {
    //        PrimaryIndexGranuleStats
    if (columnId == 1) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : ExternalPathIds) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRawValue());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 2) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
    }
    if (columnId == 3) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : PortionsCount) {
            NArrow::Append<arrow::UInt64Type>(*builder, i);
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 4) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::StringScalar(::HostName()), recordsCount));
    }
    if (columnId == 5) {
        return NArrow::TStatusValidator::GetValid(
            arrow::MakeArrayFromScalar(arrow::UInt64Scalar(NActors::TActivationContext::AsActorContext().SelfID.NodeId()), recordsCount));
    }
    if (columnId == 6) {
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
