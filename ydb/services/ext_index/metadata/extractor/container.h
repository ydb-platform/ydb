#pragma once
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NMetadata::NCSIndex {

template <class TInterface>
class TInterfaceContainer {
private:
    using TPtr = typename TInterface::TPtr;
    using TFactory = typename TInterface::TFactory;
    TPtr Object;
public:
    TInterfaceContainer() = default;

    explicit TInterfaceContainer(TPtr object)
        : Object(object)
    {

    }

    const TInterface* operator->() const {
        return Object.get();
    }

    TInterface* operator->() {
        return Object.get();
    }

    TString DebugString() const {
        return SerializeToJson().GetStringRobust();
    }

    bool DeserializeFromJson(const TString& jsonString) {
        NJson::TJsonValue jsonInfo;
        if (!NJson::ReadJsonFastTree(jsonString, &jsonInfo)) {
            return false;
        }
        return DeserializeFromJson(jsonInfo);
    }

    bool DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        TString className;
        if (!jsonInfo["class_name"].GetString(&className)) {
            return false;
        }
        TPtr result(TFactory::Construct(className));
        if (!result) {
            return false;
        }
        if (!result->DeserializeFromJson(jsonInfo["object"])) {
            return false;
        }
        Object = result;
        return true;
    }

    NJson::TJsonValue SerializeToJson() const {
        if (!Object) {
            return NJson::JSON_NULL;
        }
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("class_name", Object->GetClassName());
        result.InsertValue("object", Object->SerializeToJson());
        return result;
    }
};

}
