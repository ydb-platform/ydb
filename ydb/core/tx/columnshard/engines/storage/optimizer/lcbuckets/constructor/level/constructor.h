#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/level/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/level/counters.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class ILevelConstructor {
private:
    YDB_READONLY(TString, DefaultSelectorName, "default");
    virtual std::shared_ptr<IPortionsLevel> DoBuildLevel(const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel,
        const std::shared_ptr<TSimplePortionsGroupInfo>& portionsInfo, const TLevelCounters& counters,
        const std::vector<std::shared_ptr<IPortionsSelector>>& selectors) const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<ILevelConstructor, TString>;
    using TProto = NKikimrSchemeOp::TCompactionLevelConstructorContainer;

    virtual ~ILevelConstructor() = default;

    bool IsEqualTo(const ILevelConstructor& item) const {
        return SerializeToProto().SerializeAsString() == item.SerializeToProto().SerializeAsString();
    }

    std::shared_ptr<IPortionsLevel> BuildLevel(const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel,
        const std::shared_ptr<TSimplePortionsGroupInfo>& portionsInfo, const TLevelCounters& counters,
        const std::vector<std::shared_ptr<IPortionsSelector>>& selectors) const {
        return DoBuildLevel(nextLevel, indexLevel, portionsInfo, counters, selectors);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& json) {
        return DoDeserializeFromJson(json);
    }

    bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }
    void SerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const {
        return DoSerializeToProto(proto);
    }
    NKikimrSchemeOp::TCompactionLevelConstructorContainer SerializeToProto() const {
        NKikimrSchemeOp::TCompactionLevelConstructorContainer result;
        SerializeToProto(result);
        return result;
    }
    virtual TString GetClassName() const = 0;
};

class TLevelConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<ILevelConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ILevelConstructor>;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
