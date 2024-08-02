#pragma once
#include "viewer.h"
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/viewer/yaml/yaml.h>

namespace NKikimr::NViewer {

class TJsonHandlerBase {
public:
    virtual ~TJsonHandlerBase() = default;
    virtual IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) = 0;
    virtual YAML::Node GetRequestSwagger() = 0;
};

template <typename ActorRequestType>
class TJsonHandler : public TJsonHandlerBase {
public:
    YAML::Node Swagger;

    TJsonHandler(YAML::Node swagger)
        : Swagger(swagger)
    {}

    IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) override {
        return new ActorRequestType(viewer, event);
    }

    YAML::Node GetRequestSwagger() override {
        return Swagger;
    }
};

struct TJsonHandlers {
    std::vector<TString> JsonHandlersList;
    THashMap<TString, std::shared_ptr<TJsonHandlerBase>> JsonHandlersIndex;
    std::map<TString, int> Capabilities;

    void AddHandler(const TString& name, TJsonHandlerBase* handler, int version = 1) {
        JsonHandlersList.push_back(name);
        JsonHandlersIndex[name] = std::shared_ptr<TJsonHandlerBase>(handler);
        Capabilities[name] = version;
    }

    TJsonHandlerBase* FindHandler(const TString& name) const {
        auto it = JsonHandlersIndex.find(name);
        if (it == JsonHandlersIndex.end()) {
            return nullptr;
        }
        return it->second.get();
    }
};

class TSimpleYamlBuilder {
public:
    struct TInitializer {
        TStringBuf Method;
        TStringBuf Tag;
        TStringBuf Url;
        TStringBuf Summary;
        TStringBuf Description;
    };

    struct TParameter {
        TStringBuf Name;
        TStringBuf Description;
        TStringBuf Type;
        TStringBuf Default;
        bool Required = false;
    };

    YAML::Node Root;
    YAML::Node Method;

    TSimpleYamlBuilder(TInitializer initializer);
    void SetParameters(YAML::Node parameters);
    void AddParameter(TParameter parameter);
    void SetResponseSchema(YAML::Node schema);

    operator YAML::Node() {
        return Root;
    }
};

} // namespace NKikimr::NViewer
