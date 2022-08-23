#pragma once

#include "http_router.h"
#include "viewer.h"

namespace NKikimr::NViewer {

template <typename ActorRequestType>
class TJsonHandler : public TJsonHandlerBase {
public:
    IActor* CreateRequestActor(IViewer* viewer, const TRequest& request) override {
        return new ActorRequestType(viewer, request);
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
    THttpRequestRouter Router;

    void Init();

    void Handle(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        NMon::TEvHttpInfo* msg = ev->Get();
        auto handlerWithParamsO = Router.ResolveHandler(msg->Request.GetMethod(), msg->Request.GetPage()->Path + msg->Request.GetPathInfo());
        if (!handlerWithParamsO) {
            // for legendary /viewer handlers
            handlerWithParamsO = Router.ResolveHandler(msg->Request.GetMethod(), msg->Request.GetPathInfo());
            if (!handlerWithParamsO) {
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(NMonitoring::HTTPNOTFOUND));
                return;
            }
        }
        try {
            TRequest request;
            request.Event = ev;
            request.PathParams = handlerWithParamsO->PathParams;
            ctx.ExecutorThread.RegisterActor(handlerWithParamsO->Handler->CreateRequestActor(viewer, request));
        } catch (const std::exception& e) {
            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + e.what(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return;
        }
    }

    void PrintForSwagger(TStringStream &json) const {
        std::map<TString, std::map<HTTP_METHOD, TJsonHandlerBase*>> allHandlers;

        Router.ForEach([&](HTTP_METHOD method, const TString& pathPattern, TJsonHandlerBase::TPtr handler) {
            allHandlers[pathPattern][method] = handler.get();
        });

        char sep = ' ';
        for (const auto& [path, method2handler] : allHandlers) {
            json << sep;
            sep = ',';

            json << '"' << path << '"' << ":{";
                char methodSep = ' ';
                for (const auto& [method, handler] : method2handler) {
                    json << methodSep;
                    methodSep = ',';

                    auto methodName = ResolveMethodName(method);
                    json << "\"" << methodName << "\":{";
                        json << "\"tags\":[\"" << TTagInfo::TagName << "\"],";
                        json << "\"produces\":[\"application/json\"],";
                        TString summary = handler->GetRequestSummary();
                        if (!summary.empty()) {
                            json << "\"summary\":" << summary << ',';
                        }
                        TString description = handler->GetRequestDescription();
                        if (!description.empty()) {
                            json << "\"description\":" << description << ',';
                        }
                        TString parameters = handler->GetRequestParameters();
                        if (!parameters.empty()) {
                            json << "\"parameters\":" << parameters << ',';
                        }
                        json << "\"responses\":{";
                            json << "\"200\":{";
                                TString schema = handler->GetResponseJsonSchema();
                                if (!schema.empty()) {
                                    json << "\"schema\":" << schema;
                                }
                            json << "}";
                        json << "}";
                    json << "}";
                }
            json << "}";
        }
    }

    static std::string_view ResolveMethodName(HTTP_METHOD method) {
        switch (method) {
        case HTTP_METHOD_GET:
            return "get"sv;
        case HTTP_METHOD_POST:
            return "post"sv;
        case HTTP_METHOD_PUT:
            return "put"sv;
        case HTTP_METHOD_DELETE:
            return "delete"sv;
        default:
            return "unknown http method"sv;
        }
    }

};

struct TViewerTagInfo {
    static constexpr auto TagName = "viewer";
};
struct TVDiskTagInfo {
    static constexpr auto TagName = "vdisk";
};
struct FQTagInfo {
    static constexpr auto TagName = "fq";
};

using TViewerJsonHandlers = TJsonHandlers<TViewerTagInfo>;
using TVDiskJsonHandlers = TJsonHandlers<TVDiskTagInfo>;
using TFQJsonHandlers = TJsonHandlers<FQTagInfo>;

}
