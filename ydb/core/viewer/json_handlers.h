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
};

struct TJsonHandlers {
    std::vector<TString> JsonHandlersList;
    THashMap<TString, TAutoPtr<TJsonHandlerBase>> JsonHandlersIndex;

    void AddHandler(const TString& name, TAutoPtr<TJsonHandlerBase> handler) {
        JsonHandlersList.push_back(name);
        JsonHandlersIndex[name] = std::move(handler);
    }

    TJsonHandlerBase* FindHandler(const TString& name) const {
        auto it = JsonHandlersIndex.find(name);
        if (it == JsonHandlersIndex.end()) {
            return nullptr;
        }
        return it->second.Get();
    }
};

} // namespace NKikimr::NViewer
