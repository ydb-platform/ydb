#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NSchemeShard {

class IMetadataObjectProperties {
public:
    using TPtr = std::shared_ptr<IMetadataObjectProperties>;
    using TFactory = NObjectFactory::TObjectFactory<IMetadataObjectProperties, NKikimrSchemeOp::EPathType>;
    using TProto = NKikimrSchemeOp::TMetadataObjectProperties;

public:
    [[nodiscard]] virtual bool DeserializeFromProto(const TProto& proto) = 0;
    [[nodiscard]] virtual bool ApplyPatch(const TProto& patch) = 0;
    virtual TProto SerializeToProto() const = 0;
    virtual void CopyFromVerified(const IMetadataObjectProperties::TPtr& other) = 0;

    static TPtr Create(NKikimrSchemeOp::EPathType pathType) {
        return TPtr(TFactory::Construct(pathType));
    }

    virtual ~IMetadataObjectProperties() = default;
};

template <typename TDerived>
class TMetadataObjectPropertiesBase: public IMetadataObjectProperties {
public:
    void CopyFromVerified(const IMetadataObjectProperties::TPtr& other) override {
        std::shared_ptr<TDerived> converted = std::dynamic_pointer_cast<TDerived>(other);
        AFL_VERIFY(converted)("other", other->SerializeToProto());
        *static_cast<TDerived*>(this) = *converted;
    }
};

}   // namespace NKikimr::NSchemeShard
