#pragma once
#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/object_factory/object_factory.h>
#include <library/cpp/string_utils/base64/base64.h>

namespace NKikimr::NBackgroundTasks {

class IStringSerializable {
protected:
    virtual bool DoDeserializeFromString(const TString& data) = 0;
    virtual TString DoSerializeToString() const = 0;
public:
    bool DeserializeFromString(const TString& data) {
        return DoDeserializeFromString(data);
    }

    TString SerializeToString() const {
        return DoSerializeToString();
    }
    virtual ~IStringSerializable() = default;
};

template <class TBaseClass = IStringSerializable>
class IJsonStringSerializable: public TBaseClass {
protected:
    virtual bool DoDeserializeFromString(const TString& data) override final {
        std::optional<NJson::TJsonValue> jsonData = ParseStringToStorageObject(data);
        if (!jsonData) {
            return false;
        }
        return DeserializeFromJson(*jsonData);
    }
    virtual TString DoSerializeToString() const override final {
        NJson::TJsonValue jsonData = SerializeToJson();
        return jsonData.GetStringRobust();
    }
    virtual NJson::TJsonValue DoSerializeToJson() const = 0;
    virtual bool DoDeserializeFromJson(const NJson::TJsonValue& jsonData) = 0;
public:
    static std::optional<NJson::TJsonValue> ParseStringToStorageObject(const TString& data) {
        NJson::TJsonValue jsonData;
        if (!NJson::ReadJsonFastTree(data, &jsonData)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse string as json: " << Base64Encode(data);
            return {};
        }
        return jsonData;
    }
    static NJson::TJsonValue SerializeToStorageObject(const IJsonStringSerializable& object) {
        return object.SerializeToJson();
    }
    NJson::TJsonValue SerializeToJson() const {
        return DoSerializeToJson();
    }
    bool DeserializeFromJson(const NJson::TJsonValue& jsonData) {
        return DoDeserializeFromJson(jsonData);
    }
};

template <class TProtoClass, class TBaseClass = IStringSerializable>
class IProtoStringSerializable: public TBaseClass {
private:
    using TSelf = IProtoStringSerializable<TProtoClass, TBaseClass>;
protected:
    virtual bool DoDeserializeFromString(const TString& data) override final {
        std::optional<TProtoClass> protoData = ParseStringToStorageObject(data);
        if (!protoData) {
            return false;
        }
        return DeserializeFromProto(*protoData);
    }
    virtual TString DoSerializeToString() const override final {
        TProtoClass protoData = SerializeToProto();
        return protoData.SerializeAsString();
    }
    virtual TProtoClass DoSerializeToProto() const = 0;
    virtual bool DoDeserializeFromProto(const TProtoClass& protoData) = 0;
public:
    static std::optional<TProtoClass> ParseStringToStorageObject(const TString& data) {
        TProtoClass protoData;
        if (!protoData.ParseFromArray(data.data(), data.size())) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse string as proto: " << Base64Encode(data);
            return {};
        }
        return protoData;
    }
    static TProtoClass SerializeToStorageObject(const IProtoStringSerializable& object) {
        return object.SerializeToProto();
    }
    TProtoClass SerializeToProto() const {
        return DoSerializeToProto();
    }
    bool DeserializeFromProto(const TProtoClass& protoData) {
        return DoDeserializeFromProto(protoData);
    }
};

template <class IInterface>
class TCommonInterfaceContainer {
protected:
    std::shared_ptr<IInterface> Object;
    using TFactory = typename IInterface::TFactory;
public:
    TCommonInterfaceContainer() = default;
    TCommonInterfaceContainer(std::shared_ptr<IInterface> object)
        : Object(object) {
    }

    template <class TDerived>
    TCommonInterfaceContainer(std::shared_ptr<TDerived> object)
        : Object(object) {
        static_assert(std::is_base_of<IInterface, TDerived>::value);
    }

    bool Initialize(const TString& className, const bool maybeExists = false) {
        AFL_VERIFY(maybeExists || !Object)("problem", "initialize for not-empty-object");
        Object.reset(TFactory::Construct(className));
        if (!Object) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "incorrect class name: " << className << " for " << typeid(IInterface).name();
            return false;
        }
        return true;
    }

    TString GetClassName() const {
        return Object ? Object->GetClassName() : "UNDEFINED";
    }

    bool HasObject() const {
        return !!Object;
    }

    template <class T>
    const T& GetAsSafe() const {
        return *GetObjectPtrVerifiedAs<T>();
    }

    template <class T>
    T& GetAsSafe() {
        return *GetObjectPtrVerifiedAs<T>();
    }

    std::shared_ptr<IInterface> GetObjectPtr() const {
        return Object;
    }

    std::shared_ptr<IInterface> GetObjectPtrVerified() const {
        AFL_VERIFY(Object);
        return Object;
    }

    template <class T>
    std::shared_ptr<T> GetObjectPtrVerifiedAs() const {
        auto result = std::dynamic_pointer_cast<T>(Object);
        Y_ABORT_UNLESS(!!result);
        return result;
    }

    const IInterface& GetObjectVerified() const {
        AFL_VERIFY(Object);
        return *Object;
    }

    const IInterface* operator->() const {
        AFL_VERIFY(Object);
        return Object.get();
    }

    IInterface* operator->() {
        AFL_VERIFY(Object);
        return Object.get();
    }

    bool operator!() const {
        return !Object;
    }

    operator bool() const {
        return !!Object;
    }

};

class TStringContainerProcessor {
public:
    static bool DeserializeFromContainer(const TString& data, TString& className, TString& binary);

    static TString SerializeToContainer(const TString& className, const TString& binary);
};

template <class IInterface>
class TInterfaceStringContainer: public TCommonInterfaceContainer<IInterface> {
protected:
    using TBase = TCommonInterfaceContainer<IInterface>;
    using TFactory = typename TBase::TFactory;
    using TBase::Object;
public:
    using TBase::TBase;
    TString SerializeToStringBase64() const {
        return Base64Encode(SerializeToString());
    }

    TString SerializeToString() const {
        if (!Object) {
            return TStringContainerProcessor::SerializeToContainer("__UNDEFINED", "");
        } else {
            return TStringContainerProcessor::SerializeToContainer(Object->GetClassName(), Object->SerializeToString());
        }
    }

    bool DeserializeFromString(const TString& data) {
        if (!data) {
            Object = nullptr;
            return true;
        }
        TString className;
        TString binaryData;
        if (!TStringContainerProcessor::DeserializeFromContainer(data, className, binaryData)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse string as proto: " << Base64Encode(data);
            return false;
        }
        if (className == "__UNDEFINED") {
            return true;
        }
        std::shared_ptr<IInterface> object(TFactory::Construct(className));
        if (!object) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "incorrect class name: " << className << " for " << typeid(IInterface).name();
            return false;
        }

        if (!object->DeserializeFromString(binaryData)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse class instance: " << className << " for " << typeid(IInterface).name();
            return false;
        }
        Object = object;
        return true;
    }
};

template <class TProto, class IBaseInterface>
class TInterfaceProtoAdapter: public IBaseInterface {
private:
    using TBase = IBaseInterface;
    virtual TConclusionStatus DoDeserializeFromProto(const TProto& proto) = 0;
    virtual TProto DoSerializeToProto() const = 0;
protected:
    using TProtoStorage = TProto;
    virtual TConclusionStatus DoDeserializeFromString(const TString& data) override final {
        TProto proto;
        if (!proto.ParseFromArray(data.data(), data.size())) {
            return TConclusionStatus::Fail("cannot parse proto string as " + TypeName<TProto>());
        }
        return DoDeserializeFromProto(proto);
    }
    virtual TString DoSerializeToString() const override final {
        TProto proto = DoSerializeToProto();
        return proto.SerializeAsString();
    }
public:
    using TBase::TBase;

    TConclusionStatus DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }
    TProto SerializeToProto() const {
        return DoSerializeToProto();
    }
};


class TDefaultJsonContainerPolicy {
public:
    static TString GetClassName(const NJson::TJsonValue& jsonInfo) {
        return jsonInfo["className"].GetStringRobust();
    }
    static void SetClassName(NJson::TJsonValue& jsonInfo, const TString& className) {
        jsonInfo["className"] = className;
    }
};

template <class IInterface, class TOperatorPolicy = TDefaultJsonContainerPolicy>
class TInterfaceJsonContainer: public TCommonInterfaceContainer<IInterface> {
protected:
    using TBase = TCommonInterfaceContainer<IInterface>;
    using TFactory = typename TBase::TFactory;
    using TBase::Object;
public:
    using TBase::TBase;
    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        if (!Object) {
            TOperatorPolicy::SetClassName(result, "__UNDEFINED");
            return result;
        }
        TOperatorPolicy::SetClassName(result, Object->GetClassName());
        result.InsertValue("objectData", Object->SerializeToJson());
        return result;
    }

    bool DeserializeFromJson(const NJson::TJsonValue& data) {
        const TString& className = TOperatorPolicy::GetClassName(data);
        if (className == "__UNDEFINED") {
            return true;
        }
        std::shared_ptr<IInterface> object(TFactory::Construct(className));
        if (!object) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "incorrect class name: " << className << " for " << typeid(IInterface).name();
            return false;
        }
        if (!object->DeserializeFromJson(data["objectData"])) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse class instance: " << className << " for " << typeid(IInterface).name();
            return false;
        }
        Object = object;
        return true;
    }
};

class TDefaultProtoContainerPolicy {
public:
    template <class TProto>
    static TString GetClassName(const TProto& protoInfo) {
        return protoInfo.GetClassName();
    }
    template <class TProto>
    static void SetClassName(TProto& protoInfo, const TString& className) {
        protoInfo.SetClassName(className);
    }
};

template <class IInterface, class TOperatorPolicy = TDefaultProtoContainerPolicy>
class TInterfaceProtoContainer: public TCommonInterfaceContainer<IInterface> {
private:
    using TProto = typename IInterface::TProto;
    using TSelf = TInterfaceProtoContainer<IInterface, TOperatorPolicy>;
protected:
    using TBase = TCommonInterfaceContainer<IInterface>;
    using TFactory = typename TBase::TFactory;
    using TBase::Object;
public:
    using TBase::TBase;
    bool DeserializeFromProto(const TProto& data) {
        const TString& className = TOperatorPolicy::GetClassName(data);
        if (className == "__UNDEFINED") {
            return true;
        }
        std::shared_ptr<IInterface> object(TFactory::Construct(className));
        if (!object) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "incorrect class name: " << className << " for " << typeid(IInterface).name();
            return false;
        }
        if (!object->DeserializeFromProto(data)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse class instance: " << className << " for " << typeid(IInterface).name();
            return false;
        }
        Object = object;
        return true;
    }

    static TConclusion<TSelf> BuildFromProto(const TProto& data) {
        TSelf result;
        if (!result.DeserializeFromProto(data)) {
            return TConclusionStatus::Fail("cannot parse interface from proto: " + data.DebugString());
        }
        return result;
    }

    TProto SerializeToProto() const {
        TProto result;
        if (!Object) {
            return result;
        }
        Object->SerializeToProto(result);
        TOperatorPolicy::SetClassName(result, Object->GetClassName());
        return result;
    }

    template <class TProto>
    void SerializeToProto(TProto& result) const {
        if (!Object) {
            TOperatorPolicy::SetClassName(result, "__UNDEFINED");
            return;
        }
        Object->SerializeToProto(result);
        TOperatorPolicy::SetClassName(result, Object->GetClassName());
    }

    TString SerializeToString() const {
        return SerializeToProto().SerializeAsString();
    }

    TConclusionStatus DeserializeFromString(const TString& data) {
        TProto proto;
        if (!proto.ParseFromArray(data.data(), data.size())) {
            return TConclusionStatus::Fail("cannot parse string as proto");
        }
        if (!DeserializeFromProto(proto)) {
            return TConclusionStatus::Fail("cannot parse proto in container");
        }
        return TConclusionStatus::Success();
    }
};

}
