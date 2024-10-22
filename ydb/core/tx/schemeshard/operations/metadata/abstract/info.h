#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/accessor/accessor.h>

#include <library/cpp/object_factory/object_factory.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSchemeShard {

template <class TProperties>
class TMetadataObjectInfoImpl;

class TMetadataObjectInfo: public TSimpleRefCount<TMetadataObjectInfo> {
public:
    using TPtr = TIntrusivePtr<TMetadataObjectInfo>;
    using TFactory = NObjectFactory::TObjectFactory<TMetadataObjectInfo, NKikimrSchemeOp::EPathType>;

private:
    YDB_ACCESSOR(ui64, AlterVersion, 0);

private:
    template <class TProperties>
    friend class TMetadataObjectInfoImpl;

    TMetadataObjectInfo() = default;
    TMetadataObjectInfo(ui64 alterVersion)
        : AlterVersion(alterVersion) {
    }

public:
    static TPtr Create(NKikimrSchemeOp::EPathType pathType) {
        return TPtr(TFactory::Construct(pathType));
    }

    template <typename T>
    const T& GetPropertiesVerified() const {
        const TMetadataObjectInfoImpl<T>* convertedInfo = dynamic_cast<const TMetadataObjectInfoImpl<T>*>(this);
        AFL_VERIFY(convertedInfo);
        return convertedInfo->GetProperties();
    }

    template <typename T>
    T& MutablePropertiesVerified() {
        TMetadataObjectInfoImpl<T>* convertedInfo = dynamic_cast<TMetadataObjectInfoImpl<T>*>(this);
        AFL_VERIFY(convertedInfo);
        return convertedInfo->MutableProperties();
    }

    [[nodiscard]] virtual bool DeserializePropertiesFromProto(const NKikimrSchemeOp::TMetadataObjectProperties& proto) = 0;
    [[nodiscard]] virtual bool ApplyPatch(const NKikimrSchemeOp::TMetadataObjectProperties& patch) = 0;
    virtual NKikimrSchemeOp::TMetadataObjectProperties SerializePropertiesToProto() const = 0;

    virtual ~TMetadataObjectInfo() = default;
};

template <class TProperties>
class TMetadataObjectInfoImpl: public TMetadataObjectInfo {
private:
    using TSelf = TMetadataObjectInfoImpl<TProperties>;

    YDB_ACCESSOR_DEF(TProperties, Properties);

protected:
    virtual std::optional<TProperties> DoDeserializeFromProto(const NKikimrSchemeOp::TMetadataObjectProperties& proto) const = 0;
    virtual bool DoApplyPatch(TProperties& object, const NKikimrSchemeOp::TMetadataObjectProperties& patch) const = 0;
    virtual NKikimrSchemeOp::TMetadataObjectProperties DoSerializeToProto(const TProperties& object) const = 0;

public:
    bool DeserializePropertiesFromProto(const NKikimrSchemeOp::TMetadataObjectProperties& proto) override {
        auto parseResult = DoDeserializeFromProto(proto);
        if (!parseResult) {
            return false;
        }
        Properties = *parseResult;
        return true;
    }

    virtual bool ApplyPatch(const NKikimrSchemeOp::TMetadataObjectProperties& patch) override {
        TProperties result = Properties;
        if (!DoApplyPatch(result, patch)) {
            return false;
        }
        Properties = result;
        return true;
    }

    NKikimrSchemeOp::TMetadataObjectProperties SerializePropertiesToProto() const override {
        return DoSerializeToProto(Properties);
    }

    TMetadataObjectInfoImpl() {
    }
    TMetadataObjectInfoImpl(const ui64 alterVersion)
        : TMetadataObjectInfo(alterVersion) {
    }
};

}   // namespace NKikimr::NSchemeShard
