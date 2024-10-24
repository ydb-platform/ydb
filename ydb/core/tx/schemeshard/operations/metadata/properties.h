#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

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

    static TPtr Create(NKikimrSchemeOp::EPathType pathType) {
        return TPtr(TFactory::Construct(pathType));
    }

    virtual ~IMetadataObjectProperties() = default;
};

}   // namespace NKikimr::NSchemeShard
