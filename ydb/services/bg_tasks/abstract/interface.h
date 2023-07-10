#pragma once
#include <ydb/core/base/events.h>
#include <ydb/services/bg_tasks/protos/container.pb.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/actors/core/events.h>
#include <library/cpp/object_factory/object_factory.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/actors/core/log.h>

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

    bool HasObject() const {
        return !!Object;
    }

    template <class T>
    const T& GetAsSafe() const {
        auto result = std::dynamic_pointer_cast<T>(Object);
        Y_VERIFY(!!result);
        return *result;
    }

    template <class T>
    T& GetAsSafe() {
        auto result = std::dynamic_pointer_cast<T>(Object);
        Y_VERIFY(!!result);
        return *result;
    }

    std::shared_ptr<IInterface> GetObjectPtr() const {
        return Object;
    }

    const IInterface* operator->() const {
        return Object.get();
    }

    IInterface* operator->() {
        return Object.get();
    }

    bool operator!() const {
        return !Object;
    }

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
        NKikimrProto::TStringContainer result;
        if (!Object) {
            return result.SerializeAsString();
        }
        result.SetClassName(Object->GetClassName());
        result.SetBinaryData(Object->SerializeToString());
        return result.SerializeAsString();
    }

    bool DeserializeFromString(const TString& data) {
        if (!data) {
            Object = nullptr;
            return true;
        }
        NKikimrProto::TStringContainer protoData;
        if (!protoData.ParseFromArray(data.data(), data.size())) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse string as proto: " << Base64Encode(data);
            return {};
        }
        const TString& className = protoData.GetClassName();
        std::shared_ptr<IInterface> object(TFactory::Construct(protoData.GetClassName()));
        if (!object) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "incorrect class name: " << className << " for " << typeid(IInterface).name();
            return false;
        }

        if (!object->DeserializeFromString(protoData.GetBinaryData())) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse class instance: " << className << " for " << typeid(IInterface).name();
            return false;
        }
        Object = object;
        return true;
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
            return result;
        }
        TOperatorPolicy::SetClassName(result, Object->GetClassName());
        result.InsertValue("objectData", Object->SerializeToJson());
        return result;
    }

    bool DeserializeFromJson(const NJson::TJsonValue& data) {
        const TString& className = TOperatorPolicy::GetClassName(data);
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
protected:
    using TBase = TCommonInterfaceContainer<IInterface>;
    using TFactory = typename TBase::TFactory;
    using TBase::Object;
public:
    using TBase::TBase;
    bool DeserializeFromProto(const TProto& data) {
        const TString& className = TOperatorPolicy::GetClassName(data);
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

    TProto SerializeToProto() const {
        TProto result;
        if (!Object) {
            return result;
        }
        result = Object->SerializeToProto();
        TOperatorPolicy::SetClassName(result, Object->GetClassName());
        return result;
    }
};

}
