#pragma once

#include "defs.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/kesus.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {
namespace NKesus {

struct TEvKesusProxy {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_KESUS_PROXY),

        // Request actors
        EvResolveKesusProxy = EvBegin + 0,
        EvAttachProxyActor,
        EvCancelRequest,
        EvProxyError,

        EvEnd
    };

    static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_KESUS_PROXY),
        "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_KESUS_PROXY)");

    struct TEvResolveKesusProxy : public TEventLocal<TEvResolveKesusProxy, EvResolveKesusProxy> {
        const TString KesusPath;

        explicit TEvResolveKesusProxy(const TString& kesusPath)
            : KesusPath(kesusPath)
        {}
    };

    struct TEvAttachProxyActor : public TEventLocal<TEvAttachProxyActor, EvAttachProxyActor> {
        const TActorId ProxyActor;
        const TIntrusivePtr<TSecurityObject> SecurityObject;

        TEvAttachProxyActor(const TActorId& proxyActor, TIntrusivePtr<TSecurityObject> securityObject)
            : ProxyActor(proxyActor)
            , SecurityObject(std::move(securityObject))
        {}
    };

    struct TEvCancelRequest : public TEventLocal<TEvCancelRequest, EvCancelRequest> {
        // nothing yet
    };

    struct TEvProxyError : public TEventLocal<TEvProxyError, EvProxyError> {
        NKikimrKesus::TKesusError Error;

        explicit TEvProxyError(const NKikimrKesus::TKesusError& error)
            : Error(error)
        {}

        TEvProxyError(Ydb::StatusIds::StatusCode status, const TString& reason) {
            Error.SetStatus(status);
            Error.AddIssues()->set_message(reason);
        }
    };
};

}
}
