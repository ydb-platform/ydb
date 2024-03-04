#pragma once
#include "common.h"
#include "portion_storage.h"
#include "operator.h"

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {
class TOlapSchema;
}

namespace NKikimrColumnShardStatisticsProto {
class TOperatorContainer;
}

namespace NKikimr::NOlap::NStatistics {

class IConstructor {
private:
    YDB_READONLY(EType, Type, EType::Undefined);
    IConstructor() = default;
protected:
    virtual TConclusion<TOperatorContainer> DoCreateOperator(const NSchemeShard::TOlapSchema& currentSchema) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) = 0;
    virtual void DoSerializeToProto(NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonData) = 0;
public:
    using TProto = NKikimrColumnShardStatisticsProto::TConstructorContainer;
    using TFactory = NObjectFactory::TObjectFactory<IConstructor, TString>;

    virtual ~IConstructor() = default;

    IConstructor(const EType type)
        :Type(type) {

    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonData) {
        return DoDeserializeFromJson(jsonData);
    }

    TConclusion<TOperatorContainer> CreateOperator(const NSchemeShard::TOlapSchema& currentSchema) const {
        return DoCreateOperator(currentSchema);
    }

    TString GetClassName() const {
        return ::ToString(Type);
    }

    bool DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) {
        if (!TryFromString(proto.GetClassName(), Type)) {
            return false;
        }
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(NKikimrColumnShardStatisticsProto::TConstructorContainer& proto) const {
        return DoSerializeToProto(proto);
    }
};

class TConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<IConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IConstructor>;
public:
    using TBase::TBase;
};

}