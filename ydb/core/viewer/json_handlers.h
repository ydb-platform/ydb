#pragma once

#include "viewer.h"
#include <library/cpp/yaml/as/tstring.h>

namespace NKikimr::NViewer {

class TJsonHandlerBase {
public:
    virtual ~TJsonHandlerBase() = default;
    virtual IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) = 0;
    virtual YAML::Node GetResponseJsonSchema() = 0;
    virtual TString GetRequestSummary() = 0;
    virtual TString GetRequestDescription() = 0;
    virtual YAML::Node GetRequestParameters() = 0;
    virtual YAML::Node GetRequestSwagger() = 0;
};

template <typename ActorRequestType>
class TJsonHandler : public TJsonHandlerBase {
public:
    IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) override {
        return new ActorRequestType(viewer, event);
    }

    YAML::Node GetResponseJsonSchema() override {
        static YAML::Node jsonSchema = TJsonRequestSchema<ActorRequestType>::GetSchema();
        return jsonSchema;
    }

    TString GetRequestSummary() override {
        static TString summary = TJsonRequestSummary<ActorRequestType>::GetSummary();
        return summary;
    }

    TString GetRequestDescription() override {
        static TString description = TJsonRequestDescription<ActorRequestType>::GetDescription();
        return description;
    }

    YAML::Node GetRequestParameters() override {
        static YAML::Node parameters = TJsonRequestParameters<ActorRequestType>::GetParameters();
        return parameters;
    }

    YAML::Node GetRequestSwagger() override {
        static YAML::Node swagger = TJsonRequestSwagger<ActorRequestType>::GetSwagger();
        return swagger;
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

} // namespace NKikimr::NViewer
