#include "source.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/system/hostname.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {
std::shared_ptr<arrow::Array> TSourceData::BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const {
    //        PrimaryIndexOptimizerStats
    if (columnId == 1) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(ExternalPathId.GetRawValue()), recordsCount));
    }
    if (columnId == 2) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
    }
    if (columnId == 3) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : OptimizerTasks) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetTaskId());
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
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : OptimizerTasks) {
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(i.GetStart().data(), i.GetStart().size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    if (columnId == 7) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : OptimizerTasks) {
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(i.GetFinish().data(), i.GetFinish().size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    if (columnId == 8) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : OptimizerTasks) {
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(i.GetDetails().data(), i.GetDetails().size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    if (columnId == 9) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : OptimizerTasks) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetWeightCategory());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    if (columnId == 10) {
        auto builder = NArrow::MakeBuilder(arrow::int64());
        for (auto&& i : OptimizerTasks) {
            NArrow::Append<arrow::Int64Type>(*builder, i.GetWeight());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    AFL_VERIFY(false)("column_id", columnId);
    return nullptr;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
