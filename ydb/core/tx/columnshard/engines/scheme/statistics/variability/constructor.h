#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/statistics/abstract/constructor.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NOlap::NStatistics::NVariability {

class TConstructor: public IConstructor {
private:
    using TBase = IConstructor;
    static inline const auto Registrator = TFactory::TRegistrator<TConstructor>(::ToString(EType::Variability));
    YDB_READONLY(TString, ColumnName, 0);
protected:
    virtual TConclusion<TOperatorContainer> DoCreateOperator(const NSchemeShard::TOlapSchema& currentSchema) const override;
    virtual bool DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) override;
    virtual void DoSerializeToProto(NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonData) override;
public:
    TConstructor(const TString& columnName)
        : TBase(EType::Max)
        , ColumnName(columnName)
    {

    }

    TConstructor()
        :TBase(EType::Variability) {

    }
};

}