#include "constructor.h"
#include "operator.h"

namespace NKikimr::NOlap::NStatistics::NVariability {

NKikimr::TConclusion<std::shared_ptr<IOperator>> TConstructor::DoCreateOperator(const NSchemeShard::TOlapSchema& currentSchema) const {
    auto column = currentSchema.GetColumns().GetByName(ColumnName);
    if (!TOperator::IsAvailableType(column->GetType())) {
        return TConclusionStatus::Fail("incorrect type for stat calculation");
    }
    return std::make_shared<TOperator>(column->GetId());
}

bool TConstructor::DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) {
    if (!proto.HasVariability()) {
        return false;
    }
    ColumnName = proto.GetVariability().GetColumnName();
    if (!ColumnName) {
        return false;
    }
    return true;
}

void TConstructor::DoSerializeToProto(NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) const {
    AFL_VERIFY(!!ColumnName);
    proto.MutableVariability()->SetColumnName(ColumnName);
}

NKikimr::TConclusionStatus TConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonData) {
    if (!jsonData.Has("column_name")) {
        return TConclusionStatus::Fail("no column_name field in json description");
    }
    TString columnNameLocal;
    if (!jsonData["column_name"].GetString(&columnNameLocal)) {
        return TConclusionStatus::Fail("incorrect column_name field in json description (no string)");
    }
    if (!columnNameLocal) {
        return TConclusionStatus::Fail("empty column_name field in json description");
    }
    ColumnName = columnNameLocal;
    return TConclusionStatus::Success();
}

}