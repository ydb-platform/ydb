#pragma once
#include "manager.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TManagerConstructionContext {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    const bool IsUpdateFlag = false;

public:
    bool IsUpdate() const {
        return IsUpdateFlag;
    }

    TManagerConstructionContext(const NActors::TActorId& tabletActorId, const bool isUpdate)
        : TabletActorId(tabletActorId)
        , IsUpdateFlag(isUpdate)
    {
    }
};

class IManagerConstructor {
public:
    using TFactory = NObjectFactory::TObjectFactory<IManagerConstructor, TString>;
    using TProto = NKikimrSchemeOp::TMetadataManagerConstructorContainer;

private:
    virtual TConclusion<std::shared_ptr<IMetadataMemoryManager>> DoBuild(const TManagerConstructionContext& context) const = 0;
    virtual bool DoDeserializeFromProto(const TProto& proto) = 0;
    virtual void DoSerializeToProto(TProto& proto) const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;
    virtual bool IsEqualToWithSameClassName(const IManagerConstructor& /*item*/) const {
        return false;
    }

public:
    static std::shared_ptr<IManagerConstructor> BuildDefault();

    virtual ~IManagerConstructor() = default;

    bool IsEqualTo(const IManagerConstructor& item) const {
        if (GetClassName() != item.GetClassName()) {
            return false;
        }
        return IsEqualToWithSameClassName(item);
    }
    virtual TString GetClassName() const = 0;

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        return DoDeserializeFromJson(jsonInfo);
    }

    bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }
    void SerializeToProto(TProto& proto) const {
        DoSerializeToProto(proto);
    }

    TConclusion<std::shared_ptr<IMetadataMemoryManager>> Build(const TManagerConstructionContext& context) {
        return DoBuild(context);
    }
};

class TMetadataManagerConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<IManagerConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IManagerConstructor>;

public:
    using TBase::TBase;

    bool IsEqualTo(const TMetadataManagerConstructorContainer& item) {
        if (TBase::HasObject() != item.HasObject()) {
            return false;
        }
        if (!TBase::HasObject()) {
            return true;
        }
        return TBase::GetObjectPtr()->IsEqualTo(*item.GetObjectPtr());
    }

    static TConclusion<TMetadataManagerConstructorContainer> BuildFromProto(const NKikimrSchemeOp::TMetadataManagerConstructorContainer& proto) {
        TMetadataManagerConstructorContainer result;
        if (!result.DeserializeFromProto(proto)) {
            return TConclusionStatus::Fail("cannot parse interface from proto: " + proto.DebugString());
        }
        return result;
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
