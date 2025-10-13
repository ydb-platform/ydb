#include "info.h"

#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap {

TConclusionStatus TSimpleColumnInfo::DeserializeFromProto(const NKikimrSchemeOp::TOlapColumnDescription& columnInfo) {
    AFL_VERIFY(columnInfo.GetId() == ColumnId);
    if (columnInfo.HasSerializer()) {
        AFL_VERIFY(Serializer.DeserializeFromProto(columnInfo.GetSerializer()));
    } else if (columnInfo.HasCompression()) {
        Serializer.DeserializeFromProto(columnInfo.GetCompression()).Validate();
    }
    if (columnInfo.HasDefaultValue()) {
        DefaultValue.DeserializeFromProto(columnInfo.GetDefaultValue()).Validate();
    }
    if (columnInfo.HasDataAccessorConstructor()) {
        AFL_VERIFY(DataAccessorConstructor.DeserializeFromProto(columnInfo.GetDataAccessorConstructor()));
    }
    IsNullable = columnInfo.HasNotNull() ? !columnInfo.GetNotNull() : true;
    AFL_VERIFY(Serializer);
    Loader = std::make_shared<TColumnLoader>(Serializer, DataAccessorConstructor, ArrowField, DefaultValue.GetValue(), ColumnId);
    return TConclusionStatus::Success();
}

TSimpleColumnInfo::TSimpleColumnInfo(const ui32 columnId, const std::shared_ptr<arrow::Field>& arrowField,
    const NArrow::NSerialization::TSerializerContainer& serializer, const bool needMinMax, const bool isSorted, const bool isNullable,
    const std::shared_ptr<arrow::Scalar>& defaultValue, const std::optional<ui32>& pkColumnIndex)
    : ColumnId(columnId)
    , PKColumnIndex(pkColumnIndex)
    , ArrowField(arrowField)
    , Serializer(serializer)
    , NeedMinMax(needMinMax)
    , IsSorted(isSorted)
    , IsNullable(isNullable)
    , DefaultValue(defaultValue) {
    ColumnName = ArrowField->name();
    Loader = std::make_shared<TColumnLoader>(Serializer, DataAccessorConstructor, ArrowField, DefaultValue.GetValue(), ColumnId);
}

std::vector<std::shared_ptr<NKikimr::NOlap::IPortionDataChunk>> TSimpleColumnInfo::ActualizeColumnData(
    const std::vector<std::shared_ptr<IPortionDataChunk>>& source, const TSimpleColumnInfo& sourceColumnFeatures) const {
    AFL_VERIFY(Loader);
    const auto checkNeedActualize = [&]() {
        if (!Serializer.IsEqualTo(sourceColumnFeatures.Serializer)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "actualization")("reason", "serializer")(
                "from", sourceColumnFeatures.Serializer.SerializeToProto().DebugString())("to", Serializer.SerializeToProto().DebugString());
            return true;
        }
        if (!Loader->IsEqualTo(*sourceColumnFeatures.Loader)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "actualization")("reason", "loader");
            return true;
        }
        return false;
    };
    if (!checkNeedActualize()) {
        return source;
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> result;
    for (auto&& s : source) {
        TString data;
        ui32 rawBytes = s->GetRawBytesVerified();
        const auto loadContext = Loader->BuildAccessorContext(s->GetRecordsCountVerified());
        if (!DataAccessorConstructor.IsEqualTo(sourceColumnFeatures.DataAccessorConstructor)) {
            auto chunkedArray = sourceColumnFeatures.Loader->ApplyVerified(s->GetData(), s->GetRecordsCountVerified());
            auto newArray = DataAccessorConstructor->Construct(chunkedArray, loadContext).DetachResult();
            rawBytes = newArray->GetRawSizeVerified();
            data = DataAccessorConstructor.SerializeToString(DataAccessorConstructor->Construct(chunkedArray, loadContext).DetachResult(), loadContext);
        } else {
            data = DataAccessorConstructor.SerializeToString(
                DataAccessorConstructor.DeserializeFromString(s->GetData(), loadContext).DetachResult(), loadContext);
        }
        result.emplace_back(s->CopyWithAnotherBlob(std::move(data), rawBytes, *this));
    }
    return result;
}

}   // namespace NKikimr::NOlap
