#pragma once
#include <unordered_map>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include "viewer.h"
#include "browse.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using ::google::protobuf::FieldDescriptor;

class TJsonContent : public TActorBootstrapped<TJsonContent> {
    using TThis = TJsonContent;
    using TBase = TActorBootstrapped<TJsonContent>;

    IViewer* Viewer;
    TActorId Initiator;
    NMon::TEvHttpInfo::TPtr Event;

    IViewer::TContentRequestContext ContentRequestContext;
    TInstant BrowseStarted;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonContent(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Initiator(ev->Sender)
        , Event(ev)
    {}

    STFUNC(StateWaitingBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NViewerEvents::TEvBrowseResponse, HandleBrowseResponse);
            CFunc(TEvents::TSystem::Wakeup, HandleBrowseTimeout);
        }
    }

public:
    void Bootstrap(const TActorContext& ctx) {
        BuildRequestContext(&Event->Get()->Request, ContentRequestContext);
        if (!Event->Get()->UserToken.empty()) {
            ContentRequestContext.UserToken = Event->Get()->UserToken;
        }
        BrowseStarted = ctx.Now();
        ctx.RegisterWithSameMailbox(new TBrowse(Viewer, ctx.SelfID, ContentRequestContext.Path, Event->Get()->UserToken));

        TBase::Become(
            &TThis::StateWaitingBrowse,
            ctx,
            ContentRequestContext.Timeout,
            new TEvents::TEvWakeup());
    }

private:
    static void BuildRequestContext(
        const NMonitoring::IMonHttpRequest* httpRequest,
        IViewer::TContentRequestContext& reqCtx) {
        if (!httpRequest) {
            return;
        }

        const auto& params = httpRequest->GetParams();
        auto post = httpRequest->GetPostContent();

        reqCtx.JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(
            params.Get("enums"),
            !reqCtx.JsonSettings.EnumAsNumbers); // defaults to false
        reqCtx.JsonSettings.UI64AsString = !FromStringWithDefault<bool>(
            params.Get("ui64"),
            !reqCtx.JsonSettings.UI64AsString); // defaults to false

        ui32 timeoutMillis = FromStringWithDefault<ui32>(
            params.Get("timeout"),
            (ui32)reqCtx.Timeout.MilliSeconds());
        reqCtx.Timeout = TDuration::MilliSeconds(timeoutMillis);

        reqCtx.Limit = FromStringWithDefault<ui32>(params.Get("limit"), reqCtx.Limit);
        reqCtx.Offset = FromStringWithDefault<ui32>(params.Get("offset"), reqCtx.Offset);
        reqCtx.Key = post;

        if (params.Has("key")) {
            reqCtx.Key = params.Get("key");
        }

        reqCtx.Path = params.Get("path");
    }

    void HandleBrowseResponse(NViewerEvents::TEvBrowseResponse::TPtr &ev, const TActorContext &ctx) {
        NViewerEvents::TEvBrowseResponse& event = *ev->Get();

        if (!event.Error.empty()) {
            return SendErrorReplyAndDie(event.Error, ctx);
        }

        auto type = event.BrowseInfo.GetType();
        auto contentHandler = Viewer->GetContentHandler(type);
        if (!contentHandler) {
            return SendErrorReplyAndDie(TStringBuilder()
                 << "HTTP/1.1 500 Internal Server Error\r\n"
                     "Connection: Close\r\n"
                     "\r\n"
                      "No content can be retrieved from "
                 << (NKikimrViewer::EObjectType_IsValid((int)type) ? NKikimrViewer::EObjectType_Name(type) : TString("unknown"))
                 << " object\r\n",
                 ctx);
        }

        ContentRequestContext.Type = event.BrowseInfo.GetType();
        ContentRequestContext.ObjectName = event.BrowseInfo.GetName();
        ContentRequestContext.Timeout -= (ctx.Now() - BrowseStarted);

        // spawn content retrieval actor
        ctx.RegisterWithSameMailbox(contentHandler(Initiator, ContentRequestContext));
        Die(ctx);
    }

    void HandleBrowseTimeout(const TActorContext& ctx) {
        return SendErrorReplyAndDie(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), ctx);
    }

    void SendErrorReplyAndDie(const TString& error, const TActorContext& ctx) {
        ctx.Send(
            Initiator,
            new NMon::TEvHttpInfoRes(
                error,
                0,
                NMon::IEvHttpInfoRes::EContentType::Custom));

        Die(ctx);
    }
};

template <>
struct TJsonRequestParameters<TJsonContent> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: path
              in: query
              description: schema path
              required: true
              type: string
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: key
              in: query
              description: key for positioning
              required: false
              type: string
            - name: limit
              in: query
              description: rows limit
              required: false
              type: integer
            - name: offset
              in: query
              description: offset in rows
              required: false
              type: integer
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
        )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonContent> {
    static TString GetSummary() {
        return "Schema content preview";
    }
};

template <>
struct TJsonRequestDescription<TJsonContent> {
    static TString GetDescription() {
        return "Return schema preview";
    }
};


}
}
