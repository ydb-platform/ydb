#pragma once

#include "viewer.h"
#include <ydb/core/viewer/json/json.h>

namespace NKikimr::NViewer {

class TJsonHandlerBase {
public:
    virtual ~TJsonHandlerBase() = default;
    virtual IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) = 0;
    virtual TString GetResponseJsonSchema() = 0;
    virtual TString GetRequestSummary() { return TString(); }
    virtual TString GetRequestDescription() { return TString(); }
    virtual TString GetRequestParameters() { return TString(); }
};

template <typename ActorRequestType>
class TJsonHandler : public TJsonHandlerBase {
public:
    IActor* CreateRequestActor(IViewer* viewer, NMon::TEvHttpInfo::TPtr& event) override {
        return new ActorRequestType(viewer, event);
    }

    TString GetResponseJsonSchema() override {
        static TString jsonSchema = TJsonRequestSchema<ActorRequestType>::GetSchema();
        return jsonSchema;
    }

    TString GetRequestSummary() override {
        static TString jsonSummary = TJsonRequestSummary<ActorRequestType>::GetSummary();
        return jsonSummary;
    }

    TString GetRequestDescription() override {
        static TString jsonDescription = TJsonRequestDescription<ActorRequestType>::GetDescription();
        return jsonDescription;
    }

    TString GetRequestParameters() override {
        static TString jsonParameters = TJsonRequestParameters<ActorRequestType>::GetParameters();
        return jsonParameters;
    }
};

template <typename TTagInfo>
struct TJsonHandlers {
    THashMap<TString, TAutoPtr<TJsonHandlerBase>> JsonHandlers;

    void Init();

    void Handle(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        NMon::TEvHttpInfo* msg = ev->Get();
        auto itJson = JsonHandlers.find(msg->Request.GetPage()->Path + msg->Request.GetPathInfo());
        if (itJson == JsonHandlers.end()) {
            itJson = JsonHandlers.find(msg->Request.GetPathInfo());
        }
        if (itJson != JsonHandlers.end()) {
            try {
                ctx.ExecutorThread.RegisterActor(itJson->second->CreateRequestActor(viewer, ev));
            }
            catch (const std::exception& e) {
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + e.what(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                return;
            }
        } else {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND));
        }
    }

    void PrintForSwagger(TStringStream &json) {
        for (auto itJson = JsonHandlers.begin(); itJson != JsonHandlers.end(); ++itJson) {
            if (itJson != JsonHandlers.begin()) {
                json << ',';
            }
            TString name = itJson->first;
            json << "\"/" << name << '"' << ":{";
                json << "\"get\":{";
                    json << "\"tags\":[\"" << TTagInfo::TagName << "\"],";
                    json << "\"produces\":[\"application/json\"],";
                    TString summary = itJson->second->GetRequestSummary();
                    if (!summary.empty()) {
                        json << "\"summary\":" << summary << ',';
                    }
                    TString description = itJson->second->GetRequestDescription();
                    if (!description.empty()) {
                        json << "\"description\":" << description << ',';
                    }
                    TString parameters = itJson->second->GetRequestParameters();
                    if (!parameters.empty()) {
                        json << "\"parameters\":" << parameters << ',';
                    }
                    json << "\"responses\":{";
                        json << "\"200\":{";
                            TString schema = itJson->second->GetResponseJsonSchema();
                            if (!schema.empty()) {
                                json << "\"schema\":" << schema;
                            }
                        json << "}";
                    json << "}";
                json << "}";
            json << "}";
        }
    }
};

struct TViewerTagInfo {
    static constexpr auto TagName = "viewer";
};
struct TVDiskTagInfo {
    static constexpr auto TagName = "vdisk";
};

using TViewerJsonHandlers = TJsonHandlers<TViewerTagInfo>;
using TVDiskJsonHandlers = TJsonHandlers<TVDiskTagInfo>;


}
