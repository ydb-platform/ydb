#include "base_auth_actors.h"

#include <ydb/core/protos/auth.pb.h>

#include <ydb/library/login/sasl/saslprep.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SASL_AUTH

namespace NKikimr::NSasl {

using namespace NLogin;
using namespace NLogin::NSasl;
using namespace NSchemeShard;

TAuthActorBase::TAuthActorBase(TActorId sender, const std::string& database, const std::string& peerName)
    : Sender(sender)
    , Database(database)
    , PeerName(peerName)
{
}

void TAuthActorBase::HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext &ctx) {
    YDB_LOG_CTX_TRACE(ctx, "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
        {"DerivedActorName", DerivedActorName},
        {"ctx.SelfID", ctx.SelfID.ToString()},
        {"errors", ev->Get()->Request.Get()->ErrorCount});

    const auto& resultSet = ev->Get()->Request.Get()->ResultSet;
    if (resultSet.size() == 1 && resultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        const auto domainInfo = resultSet.front().DomainInfo;
        if (domainInfo != nullptr) {
            IActor* pipe = NTabletPipe::CreateClient(SelfId(), domainInfo->ExtractSchemeShard(), GetPipeClientConfig());
            PipeClient = RegisterWithSameMailbox(pipe);

            return ProceedWithAuthentication(ctx, domainInfo);
        }
    }

    std::string error = "Unknown database";
    YDB_LOG_CTX_INFO(ctx, "Authentication",
        {"DerivedActorName", DerivedActorName},
        {"ctx.SelfID", ctx.SelfID.ToString()},
        {"failed", error});
    SendError(NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST, error);
    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext &ctx) {
    std::string error = "SchemeShard is unreachable";
    YDB_LOG_CTX_ERROR(ctx, "",
        {"DerivedActorName", DerivedActorName},
        {"ctx.SelfID", ctx.SelfID.ToString()},
        {"error", error});
    SendError(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx) {
    std::string error = "SchemeShard is unreachable";
    YDB_LOG_CTX_ERROR(ctx, "",
        {"DerivedActorName", DerivedActorName},
        {"ctx.SelfID", ctx.SelfID.ToString()},
        {"error", error});
    SendError(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext &ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        std::string error = "SchemeShard is unreachable";
        YDB_LOG_CTX_ERROR(ctx, "",
            {"DerivedActorName", DerivedActorName},
            {"ctx.SelfID", ctx.SelfID.ToString()},
            {"error", error});
        SendError(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
        return CleanupAndDie(ctx);
    }
}

void TAuthActorBase::HandleLoginResult(TEvSchemeShard::TEvLoginResult::TPtr& ev, const TActorContext &ctx) {
    YDB_LOG_CTX_DEBUG(ctx, "Handle TEvSchemeShard::TEvLoginResult",
        {"DerivedActorName", DerivedActorName},
        {"ctx.SelfID", ctx.SelfID.ToString()},
        {"DebugString", ev->Get()->Record.DebugString()});

    const NKikimrScheme::TEvLoginResult& loginResult = ev->Get()->Record;
    if (loginResult.HasError()) { // explicit error takes precedence
        YDB_LOG_CTX_INFO(ctx, "Authentication",
            {"DerivedActorName", DerivedActorName},
            {"ctx.SelfID", ctx.SelfID.ToString()},
            {"failed", loginResult.GetError()});
        SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, loginResult.GetError(), EScramServerError::InvalidProof);
    } else if (!loginResult.HasToken()) { // empty token is still an error
        std::string error = "Failed to produce a token";
        YDB_LOG_CTX_ERROR(ctx, "",
            {"DerivedActorName", DerivedActorName},
            {"ctx.SelfID", ctx.SelfID.ToString()},
            {"error", error});
        SendError(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, error);
    } else { // success = token + no errors
        YDB_LOG_CTX_DEBUG(ctx, "Authentication completed for ' '",
            {"DerivedActorName", DerivedActorName},
            {"ctx.SelfID", ctx.SelfID.ToString()},
            {"AuthcId", AuthcId});

        SendIssuedToken(loginResult);
    }

    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleTimeout(const TActorContext &ctx) {
    std::string error = "SchemeShard response timeout";
    YDB_LOG_CTX_ERROR(ctx, "",
        {"DerivedActorName", DerivedActorName},
        {"ctx.SelfID", ctx.SelfID.ToString()},
        {"error", error});
    SendError(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
    return CleanupAndDie(ctx);
}

void TAuthActorBase::ResolveSchemeShard(const TActorContext &ctx) {
    auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = Database;

    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = SplitPath(TString(Database));
    entry.RedirectRequired = false;
    entry.SyncVersion = true;

    request->ResultSet.emplace_back(std::move(entry));

    ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
    Become(&TThis::StateNavigate);
}

void TAuthActorBase::SendLoginRequest() {
    auto request = std::make_unique<TEvSchemeShard::TEvLogin>();
    request.get()->Record = CreateLoginRequest();
    NTabletPipe::SendData(SelfId(), PipeClient, request.release());
    Become(&TThis::StateLogin, Timeout, new TEvents::TEvWakeup());
}

NTabletPipe::TClientConfig TAuthActorBase::GetPipeClientConfig() const {
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    return clientConfig;
}

void TAuthActorBase::CleanupAndDie(const TActorContext &ctx) {
    if (PipeClient) {
        NTabletPipe::CloseClient(SelfId(), PipeClient);
    }
    return Die(ctx);
}

const TDuration TAuthActorBase::Timeout = TDuration::MilliSeconds(30000);

TPlainAuthActorBase::TPlainAuthActorBase(TActorId sender, const std::string& database,
    const std::string& authMsg, const std::string& peerName
)
    : TAuthActorBase(sender, database, peerName)
    , AuthMsg(authMsg)
{
}

bool TPlainAuthActorBase::ProcessAuthMsg(const TActorContext &ctx) {
    std::vector<std::string> authMsgParts = StringSplitter(AuthMsg).Split('\0');
    if (authMsgParts.size() != 3) {
        std::string error = "Wrong SASL PLAIN auth message format";
        YDB_LOG_CTX_WARN(ctx, "",
            {"DerivedActorName", DerivedActorName},
            {"ctx.SelfID", ctx.SelfID.ToString()},
            {"error", error});
        SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
        CleanupAndDie(ctx);
        return false;
    }

    AuthzId = authMsgParts[0];
    AuthcId = authMsgParts[1];
    Passwd = authMsgParts[2];

    if (!AuthzId.empty()) {
        std::string prepAuthzId;
        auto saslPrepRC = SaslPrep(AuthzId, prepAuthzId);
        if (saslPrepRC != ESaslPrepReturnCodes::Success) {
            std::string error = "Unsupported characters in the authorization identity";
            YDB_LOG_CTX_INFO(ctx, "",
                {"DerivedActorName", DerivedActorName},
                {"ctx.SelfID", ctx.SelfID.ToString()},
                {"error", error});
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            CleanupAndDie(ctx);
            return false;
        }

        AuthzId = std::move(prepAuthzId);
    }

    if (AuthcId.empty()) {
        std::string error = "Empty authentication identity";
        YDB_LOG_CTX_INFO(ctx, "",
            {"DerivedActorName", DerivedActorName},
            {"ctx.SelfID", ctx.SelfID.ToString()},
            {"error", error});
        SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
        CleanupAndDie(ctx);
        return false;
    } else {
        std::string prepAuthcId;
        auto saslPrepRC = SaslPrep(AuthcId, prepAuthcId);
        if (saslPrepRC != ESaslPrepReturnCodes::Success) {
            std::string error = "Unsupported characters in the authentication identity";
            YDB_LOG_CTX_INFO(ctx, "",
                {"DerivedActorName", DerivedActorName},
                {"ctx.SelfID", ctx.SelfID.ToString()},
                {"error", error});
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            CleanupAndDie(ctx);
            return false;
        }

        AuthcId = std::move(prepAuthcId);
    }

    return true;
}

}
