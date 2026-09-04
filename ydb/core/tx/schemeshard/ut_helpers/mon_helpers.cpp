#include "mon_helpers.h"

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/http/http.h>

#include <util/string/builder.h>

namespace NSchemeShardUT_Private {

using namespace NKikimr;
using namespace NActors;

// Note on queries below: they omit the `TabletID` cgi param. The SchemeShard mon
// handler never reads it (it uses Self->TabletID() for self-links), and the request
// is delivered straight to the target tablet via SendToPipe rather than through the
// tablet-monitoring HTTP proxy that would route by TabletID. So the param is inert here.
TSchemeShardMonResponse SendSchemeShardMonRequest(TTestActorRuntime& runtime, ui64 schemeShard, const TString& query, HTTP_METHOD method) {
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(schemeShard, sender, new NMon::TEvRemoteHttpInfo(query, method), 0, {});

    TAutoPtr<IEventHandle> handle;
    auto [http, json, bin] = runtime.GrabEdgeEventsRethrow<NMon::TEvRemoteHttpInfoRes, NMon::TEvRemoteJsonInfoRes, NMon::TEvRemoteBinaryInfoRes>(handle);

    if (http) {
        return {TSchemeShardMonResponse::EKind::Http, http->Html};
    }
    if (json) {
        return {TSchemeShardMonResponse::EKind::Json, json->Json};
    }
    return {TSchemeShardMonResponse::EKind::Binary, bin->Blob};
}

TString PostSwitchAction(TTestActorRuntime& runtime, ui64 schemeShard, TPathId pathId, TStringBuf format) {
    const TString query = TStringBuilder()
        << "/app?Action=TablePartitionsFormatSwitch"
        << "&OwnerPathId=" << pathId.OwnerId
        << "&LocalPathId=" << pathId.LocalPathId
        << "&format=" << format
    ;
    return SendSchemeShardMonRequest(runtime, schemeShard, query, HTTP_METHOD_POST).Body;
}

TString PostSweepAction(TTestActorRuntime& runtime, ui64 schemeShard, const TString& params) {
    const TString query = TStringBuilder()
        << "/app?Action=TablePartitionsFormatSweep" << params;
    auto reply = SendSchemeShardMonRequest(runtime, schemeShard, query, HTTP_METHOD_POST);

    // The sweep action answers with a redirect; its `Location` is relative to the
    // original query base, so follow it (prefixed with `/`) with a GET.
    NHttp::THttpParser<NHttp::THttpResponse> parser(reply.Body);
    NHttp::THeaders headers(parser.Headers);
    const TString location = TStringBuilder() << "/" << headers.Get("Location");
    return SendSchemeShardMonRequest(runtime, schemeShard, location, HTTP_METHOD_GET).Body;
}

TSchemeShardMonResponse PostMoveToStoragePoolAction(TTestActorRuntime& runtime, ui64 schemeShard, ui64 tabletId, const TString& storagePool, bool confirmSamePool) {
    TStringBuilder query;
    query << "/app?Action=MoveToStoragePool"
        << "&ShardID=" << tabletId
        << "&StoragePool=" << storagePool
    ;
    if (confirmSamePool) {
        query << "&ConfirmSamePool=1";
    }
    return SendSchemeShardMonRequest(runtime, schemeShard, query, HTTP_METHOD_POST);
}

}  // namespace NSchemeShardUT_Private
