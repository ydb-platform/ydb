#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector/abstract.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class ISelectorConstructor {
private:
    YDB_READONLY_DEF(TString, Name);
    virtual std::shared_ptr<IPortionsSelector> DoBuildSelector() const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<ISelectorConstructor, TString>;
    using TProto = NKikimrSchemeOp::TCompactionSelectorConstructorContainer;

    virtual ~ISelectorConstructor() = default;

    bool IsEqualTo(const ISelectorConstructor& item) const {
        return SerializeToProto().SerializeAsString() == item.SerializeToProto().SerializeAsString();
    }

    std::shared_ptr<IPortionsSelector> BuildSelector() const {
        return DoBuildSelector();
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& json) {
        if (json.Has("name")) {
            if (json["name"].IsString()) {
                Name = json["name"].GetStringSafe();
            } else {
                return TConclusionStatus::Fail("name field have to be string");
            }
        } else {
            Name = "default";
        }
        return DoDeserializeFromJson(json);
    }

    bool DeserializeFromProto(const TProto& proto) {
        Name = proto.GetName();
        if (!Name) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot parse proto selector constructor")("reason", "empty name");
            return false;
        }
        return DoDeserializeFromProto(proto);
    }
    void SerializeToProto(NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) const {
        proto.SetName(Name);
        return DoSerializeToProto(proto);
    }
    NKikimrSchemeOp::TCompactionSelectorConstructorContainer SerializeToProto() const {
        NKikimrSchemeOp::TCompactionSelectorConstructorContainer result;
        SerializeToProto(result);
        return result;
    }
    virtual TString GetClassName() const = 0;
};

class TSelectorConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<ISelectorConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ISelectorConstructor>;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
