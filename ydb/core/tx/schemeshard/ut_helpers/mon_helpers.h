#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/http/fetch/httpheader.h>  // for HTTP_METHOD

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NSchemeShardUT_Private {

using namespace NKikimr;

// Reply to a SchemeShard DevUI monitoring request. The SchemeShard answers with one
// of three event types depending on the request: an HTML page, a JSON action result,
// or a plain-text/binary blob (errors, redirects).
struct TSchemeShardMonResponse {
    enum class EKind {
        Http,    // TEvRemoteHttpInfoRes   — page HTML
        Json,    // TEvRemoteJsonInfoRes   — action success payload
        Binary,  // TEvRemoteBinaryInfoRes — errors, redirects, plain text
    };

    EKind Kind = EKind::Binary;
    TString Body;

    bool IsHttp() const { return Kind == EKind::Http; }
    bool IsJson() const { return Kind == EKind::Json; }
    bool IsBinary() const { return Kind == EKind::Binary; }
};

// Send a SchemeShard DevUI monitoring request (query is the full "/app?..." string)
// to the given SchemeShard tablet and return whichever reply it produced.
TSchemeShardMonResponse SendSchemeShardMonRequest(NActors::TTestActorRuntime& runtime, ui64 schemeShard, const TString& query, HTTP_METHOD method);

// POST the TablePartitionsFormatSwitch admin action; returns the raw response body.
TString PostSwitchAction(NActors::TTestActorRuntime& runtime, ui64 schemeShard, TPathId pathId, TStringBuf format);

// POST the TablePartitionsFormatSweep admin action; the action replies with a redirect,
// so this follows the Location with a GET and returns the resulting page HTML.
TString PostSweepAction(NActors::TTestActorRuntime& runtime, ui64 schemeShard, const TString& params);

// POST the MoveToStoragePool admin action for a shard's tablet; returns the reply
// (JSON on success, plain text on error). Set confirmSamePool to force a Hive re-notify
// even when the shard is already on the target pool.
TSchemeShardMonResponse PostMoveToStoragePoolAction(NActors::TTestActorRuntime& runtime, ui64 schemeShard, ui64 tabletId, const TString& storagePool, bool confirmSamePool = false);

}  // namespace NSchemeShardUT_Private
