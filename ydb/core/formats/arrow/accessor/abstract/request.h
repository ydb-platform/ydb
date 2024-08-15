#pragma once
#include "constructor.h"

#include <ydb/core/formats/arrow/protos/accessor.pb.h>

#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/services/metadata/abstract/request_features.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NArrow::NAccessor {

class IRequestedConstructor {
public:
    using TFactory = NObjectFactory::TObjectFactory<IRequestedConstructor, TString>;
    using TProto = NKikimrArrowAccessorProto::TRequestedConstructor;
private:
    virtual TConclusion<NArrow::NAccessor::TConstructorContainer> DoBuildConstructor() const = 0;
    virtual NKikimrArrowAccessorProto::TRequestedConstructor DoSerializeToProto() const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TRequestedConstructor& proto) = 0;
    virtual TConclusionStatus DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) = 0;

public:
    virtual ~IRequestedConstructor() = default;

    NKikimrArrowAccessorProto::TRequestedConstructor SerializeToProto() const {
        return DoSerializeToProto();
    }

    void SerializeToProto(NKikimrArrowAccessorProto::TRequestedConstructor& proto) const {
        proto = DoSerializeToProto();
    }

    bool DeserializeFromProto(const NKikimrArrowAccessorProto::TRequestedConstructor& proto) {
        return DoDeserializeFromProto(proto);
    }

    TConclusionStatus DeserializeFromRequest(NYql::TFeaturesExtractor& features) {
        return DoDeserializeFromRequest(features);
    }

    TConclusion<TConstructorContainer> BuildConstructor() const {
        return DoBuildConstructor();
    }

    virtual TString GetClassName() const = 0;
};

class TRequestedConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<IRequestedConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IRequestedConstructor>;

public:
    using TBase::TBase;
    TConclusionStatus DeserializeFromRequest(NYql::TFeaturesExtractor& features);
};

}   // namespace NKikimr::NArrow::NAccessor
