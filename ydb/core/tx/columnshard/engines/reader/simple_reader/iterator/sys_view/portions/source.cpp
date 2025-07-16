#include "source.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {
std::shared_ptr<arrow::Array> TSourceData::BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const {
    //        PrimaryIndexPortionStats
    if (columnId == 1) {
        return NArrow::TStatusValidator::GetValid(
            arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetUnifiedPathId().GetSchemeShardLocalPathId().GetRawValue()), recordsCount));
    }
    if (columnId == 2) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : Portions) {
            auto kindString = ::ToString(i->GetProduced());
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(kindString.data(), kindString.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 3) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
    }
    if (columnId == 4) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetRecordsCount());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 5) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetMeta().GetColumnRawBytes());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 6) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetMeta().GetIndexRawBytes());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 7) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetMeta().GetColumnBlobBytes());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 8) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetMeta().GetIndexBlobBytes());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 9) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetPortionId());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 10) {
        auto builder = NArrow::MakeBuilder(arrow::uint8());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt8Type>(*builder, i->HasRemoveSnapshot() ? 0 : 1);
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 11) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : Portions) {
            const auto tierName = i->GetTierNameDef(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(tierName.data(), tierName.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 12) {
        const TString statInfo = Default<TString>();
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::StringScalar(statInfo), recordsCount));
    }
    if (columnId == 13) {
        auto builder = NArrow::MakeBuilder(arrow::uint8());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt8Type>(*builder, i->HasRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized) ? 1 : 0);
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    if (columnId == 14) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : Portions) {
            NArrow::Append<arrow::UInt64Type>(*builder, i->GetMeta().GetCompactionLevel());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    if (columnId == 15) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : Portions) {
            NJson::TJsonValue details = NJson::JSON_MAP;
            details.InsertValue("snapshot_min", i->RecordSnapshotMin().SerializeToJson());
            details.InsertValue("snapshot_max", i->RecordSnapshotMax().SerializeToJson());
            details.InsertValue("primary_key_min", i->IndexKeyStart().DebugString());
            details.InsertValue("primary_key_max", i->IndexKeyEnd().DebugString());
            const auto detailsInfo = details.GetStringRobust();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(detailsInfo.data(), detailsInfo.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }

    AFL_VERIFY(false)("column_id", columnId);
    return nullptr;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
