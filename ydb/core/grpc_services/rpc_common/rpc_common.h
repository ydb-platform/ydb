#pragma once


#include <ydb/core/base/path.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/proto_duration.h>
#include "ydb/core/grpc_services/base/base.h"

namespace NKikimr {
namespace NGRpcService {
class IRequestCtx;

// Only the database identity in DatabaseConfig metadata is a schema operand.
bool ResolveDatabaseConfigMetadata(IRequestCtxBase& request, TString& config);

// Topic protocols historically accept database-relative paths even with a
// leading slash. An explicit absolute alias wins; only a miss tries the legacy
// logical candidate. Identity matches are winners, not misses.
TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveTopicSchemaPath(
    const IRequestCtxBaseMtSafe& request, const TString& path);
TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveTopicSchemaPath(
    const NPathAliasing::TPathContext& context, const TString& logicalDatabase,
    const TString& physicalDatabase, const TString& path);

// Preserve GetFullTopicPath's optional-database and lexical semantics.
TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveFullTopicSchemaPath(
    const IRequestCtxBaseMtSafe& request, const TString& path);

// Converter-owned protocols have a different legacy prefix grammar. Keep it
// separate from schema APIs while sharing the one-pass candidate matcher.
TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveFstClassTopicSchemaPath(
    const IRequestCtxBaseMtSafe& request, const TString& path, const TString& defaultDatabase = {});
TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveFstClassTopicSchemaPath(
    const NPathAliasing::TPathContext& context, const TString& logicalDatabase,
    const TString& physicalDatabase, const TString& path);
// The owner supplies a validated complete legacy candidate and retains its
// existing database-containment and ACL checks after resolving the target.
TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveConvertedTopicSchemaPath(
    const NPathAliasing::TPathContext& context, const TString& path,
    const TString& completeLegacyCandidate);

// Native schema APIs interpret these paths from the root, including the legacy
// no-leading-slash spelling. Relative topic paths need their owner's adapter.
// SQL operands are not rewritten.
// Do not canonicalize unmatched inputs: their existing validator owns syntax.
template <class TRequest>
inline bool ResolveRootSchemaPath(TRequest& request, const TString& logicalPath, TString& resolvedPath) {
    resolvedPath = logicalPath;
    if (!request.HasActivePathRewriting() || logicalPath.empty()) {
        return true;
    }
    const auto candidate = CanonizePath(logicalPath);
    auto resolved = request.NormalizePath(candidate.empty() ? TString("/") : candidate);
    if (resolved.IsFail()) {
        request.RaiseIssue(NYql::TIssue(resolved.GetErrorMessage()));
        return false;
    }
    if (resolved.GetResult().Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten) {
        resolvedPath = resolved.GetResult().Path;
    }
    return true;
}

// SplitPath(database, path) also checks containment. Keep its lexical contract,
// but check database containment only after resolving the target. This adapter
// intentionally throws only for call sites with the existing BAD_REQUEST catch.
inline std::pair<TString, TString> SplitRootSchemaPath(IRequestCtxBase& request, const TString& path, bool checkDatabase = false) {
    if (!request.HasActivePathRewriting()) {
        return checkDatabase ? SplitPath(request.GetDatabaseName(), path) : SplitPath(path);
    }
    if (!path.StartsWith('/')) {
        if (checkDatabase) {
            SplitPath(TMaybe<TString>{}, path);
        } else {
            SplitPath(path);
        }
    }
    const auto candidate = CanonizePath(path);
    auto resolved = request.NormalizePath(candidate.empty() ? TString("/") : candidate);
    if (resolved.IsFail()) {
        ythrow yexception() << resolved.GetErrorMessage();
    }
    const auto& effectivePath = resolved.GetResult().Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten
        ? resolved.GetResult().Path : path;
    return checkDatabase ? SplitPath(request.GetDatabaseName(), effectivePath) : SplitPath(effectivePath);
}

template<typename TEv>
inline void SetRlPath(TEv& ev, const IRequestCtx& ctx) {
    if (const auto& path = ctx.GetRlPath()) {
        auto rl = ev->Record.MutableRlPath();
        rl->SetCoordinationNode(path->CoordinationNode);
        rl->SetResourcePath(path->ResourcePath);
    }
}

template<typename TEv>
inline void SetAuthToken(TEv& ev, const IRequestCtx& ctx) {
    if (ctx.GetSerializedToken()) {
        ev->Record.SetUserToken(ctx.GetSerializedToken());
    }
}

inline std::optional<TString> GetUserSID(const IRequestCtx& ctx) {
    if (const auto& serializedToken = ctx.GetSerializedToken()) {
        if (NACLibProto::TUserToken userToken; userToken.ParseFromString(serializedToken)) {
            return userToken.GetUserSID();
        }
    }

    return std::nullopt;
}

template<typename TEv>
inline void SetClientIdentitySettings(TEv& ev, const IRequestCtx& ctx) {
    ev->Record.SetClientAddress(ctx.GetPeerName());
    const auto& token = ctx.GetInternalToken();
    if (token && !token->GetSerializedToken().empty()) {
        ev->Record.SetUserSID(token->GetUserSID());
    } else {
        ev->Record.SetUserSID("<anonymous>");
    }

    const auto& userAgent = ctx.GetPeerMetaValues(NYdbGrpc::GRPC_USER_AGENT_HEADER);
    ev->Record.SetClientUserAgent(userAgent.GetOrElse("<empty>"));
    const auto& sdkBuildInfo = ctx.GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    ev->Record.SetClientSdkBuildInfo(sdkBuildInfo.GetOrElse("<empty>"));
    const auto& appName = ctx.GetPeerMetaValues(NYdb::YDB_APPLICATION_NAME);
    ev->Record.SetApplicationName(appName.GetOrElse("<empty>"));
    const auto& pid = ctx.GetPeerMetaValues(NYdb::YDB_CLIENT_PID);
    ev->Record.SetClientPID(pid.GetOrElse("<empty>"));
}

template<typename TEv>
inline void SetDatabase(TEv& ev, const IRequestCtx& ctx) {
    // Empty database in case of absent header
    ev->Record.MutableRequest()->SetDatabase(CanonizePath(ctx.GetDatabaseName().GetOrElse("")));
}

inline void SetDatabase(TEvTxUserProxy::TEvProposeTransaction* ev, const IRequestCtx& ctx) {
    // Empty database in case of absent header
    ev->Record.SetDatabaseName(CanonizePath(ctx.GetDatabaseName().GetOrElse("")));
}

inline void SetDatabase(TEvTxUserProxy::TEvNavigate* ev, const IRequestCtx& ctx) {
    // Empty database in case of absent header
    ev->Record.SetDatabaseName(CanonizePath(ctx.GetDatabaseName().GetOrElse("")));
}

inline void SetRequestType(TEvTxUserProxy::TEvProposeTransaction* ev, const IRequestCtx& ctx) {
    ev->Record.SetRequestType(ctx.GetRequestType().GetOrElse(""));
}

inline void SetPeerName(TEvTxUserProxy::TEvProposeTransaction* ev, const IRequestCtx& ctx) {
    ev->Record.SetPeerName(ctx.GetPeerName());
}

inline bool CheckSession(const TString& sessionId, IRequestCtxBase* ctx) {
    static const auto err = TString("Empty session id");
    if (sessionId.empty()) {
        ctx->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, err));
        return false;
    }

    return true;
}

// Both row and column TTL descriptions use the same owned external-storage
// operand. Other tier settings are data, not namespace paths.
template<class TTtlSettings>
bool ResolveTtlSchemaPaths(IRequestCtxBase& request, TTtlSettings& settings) {
    if (!request.HasActivePathRewriting()) {
        return true;
    }
    for (auto& tier : *settings.MutableTiers()) {
        if (tier.HasEvictToExternalStorage()) {
            TString storage;
            if (!ResolveRootSchemaPath(request, tier.GetEvictToExternalStorage().GetStorage(), storage)) {
                return false;
            }
            tier.MutableEvictToExternalStorage()->SetStorage(storage);
        }
    }
    return true;
}

inline bool CheckQuery(const TString& query, IRequestCtxBase* ctx) {
    static const auto err = TString("Empty query text");
    if (query.empty()) {
        ctx->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, err));
        return false;
    }

    return true;
}

template<typename TKqpResponse>
void FillCommonKqpRespFields(const TKqpResponse& kqpResponse, IRequestCtx* ctx) {
    if (kqpResponse.GetWorkerIsClosing()) {
        ctx->AddServerHint(TString(NYdb::YDB_SESSION_CLOSE));
    }
    ctx->SetRuHeader(kqpResponse.GetConsumedRu());
}

} // namespace NGRpcService
} // namespace NKikimr
