#include "base_auth_actors.h"

#include <ydb/core/protos/auth.pb.h>

#include <ydb/library/login/sasl/saslprep.h>

namespace NKikimr::NSasl {

using namespace NLogin;
using namespace NLogin::NSasl;
using namespace NSchemeShard;

TAuthActorBase::TAuthActorBase(TActorId sender, const std::string& database, const std::string& peerName
)
    : Sender(sender)
    , Database(database)
    , PeerName(peerName)
{
}

void TAuthActorBase::HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::SASL_AUTH,
        DerivedActorName << "# " << ctx.SelfID.ToString() <<
        ", " << "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult" <<
        ", errors# " << ev->Get()->Request.Get()->ErrorCount);

    const auto& resultSet = ev->Get()->Request.Get()->ResultSet;
    if (resultSet.size() == 1 && resultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        const auto domainInfo = resultSet.front().DomainInfo;
        if (domainInfo != nullptr) {
            IActor* pipe = NTabletPipe::CreateClient(SelfId(), domainInfo->ExtractSchemeShard(), GetPipeClientConfig());
            PipeClient = RegisterWithSameMailbox(pipe);

            auto request = std::make_unique<TEvSchemeShard::TEvLogin>();
            request.get()->Record = CreateLoginRequest();
            NTabletPipe::SendData(SelfId(), PipeClient, request.release());
            Become(&TThis::StateLogin, Timeout, new TEvents::TEvWakeup());
            return;
        }
    }

    std::string error = "Unknown database";
    LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
        DerivedActorName << "# " << ctx.SelfID.ToString() <<
        ", " << error
    );
    SendError(NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST, error);
    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext &ctx) {
    std::string error = "SchemeShard is unreachable";
    LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
        DerivedActorName << "# " << ctx.SelfID.ToString() <<
        ", " << error
    );
    SendError(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx) {
    std::string error = "SchemeShard is unreachable";
    LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
        DerivedActorName << "# " << ctx.SelfID.ToString() <<
        ", " << error
    );
    SendError(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext &ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        std::string error = "SchemeShard is unreachable";
        LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
            DerivedActorName << "# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
        return CleanupAndDie(ctx);
    }
}

void TAuthActorBase::HandleLoginResult(TEvSchemeShard::TEvLoginResult::TPtr& ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::SASL_AUTH,
        DerivedActorName << "# " << ctx.SelfID.ToString() <<
        " Handle TEvSchemeShard::TEvLoginResult" <<
        ", " << ev->Get()->Record.DebugString()
    );

    const NKikimrScheme::TEvLoginResult& loginResult = ev->Get()->Record;
    if (loginResult.HasError()) { // explicit error takes precedence
        LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
            DerivedActorName << "# " << ctx.SelfID.ToString() <<
            ", " << "Authentication failed: " << loginResult.GetError()
        );
        SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, loginResult.GetError(), EScramServerError::InvalidProof);
    } else if (!loginResult.HasToken()) { // empty token is still an error
        std::string error = "Failed to produce a token";
        LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
            DerivedActorName << "# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, error);
    } else { // success = token + no errors
        LOG_DEBUG_S(ctx, NKikimrServices::SASL_AUTH,
            DerivedActorName << "# " << ctx.SelfID.ToString() <<
            ", " << "Authentication completed for '" << AuthcId << "'"
        );

        SendIssuedToken(loginResult);
    }

    return CleanupAndDie(ctx);
}

void TAuthActorBase::HandleTimeout(const TActorContext &ctx) {
    std::string error = "SchemeShard response timeout";
    LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
        DerivedActorName << "# " << ctx.SelfID.ToString() <<
        ", " << error
    );
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

    request->ResultSet.emplace_back(std::move(entry));

    ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
    Become(&TThis::StateNavigate);
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

void TPlainAuthActorBase::ProcessAuthMsg(const TActorContext &ctx) {
    std::vector<std::string> authMsgParts = StringSplitter(AuthMsg).Split('\0');
    if (authMsgParts.size() != 3) {
        std::string error = "Wrong SASL PLAIN auth message format";
        LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
            DerivedActorName << "# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
        return CleanupAndDie(ctx);
    }

    AuthzId = authMsgParts[0];
    AuthcId = authMsgParts[1];
    Passwd = authMsgParts[2];

    if (!AuthzId.empty()) {
        std::string prepAuthzId;
        auto saslPrepRC = NLogin::NSasl::SaslPrep(AuthzId, prepAuthzId);
        if (saslPrepRC != NLogin::NSasl::ESaslPrepReturnCodes::Success) {
            std::string error = "Unsupported characters in the authorization identity";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                DerivedActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        AuthzId = std::move(prepAuthzId);
    }

    if (AuthcId.empty()) {
        std::string error = "Empty authentication identity";
        LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
            DerivedActorName << "# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
        return CleanupAndDie(ctx);
    } else {
        std::string prepAuthcId;
        auto saslPrepRC = NLogin::NSasl::SaslPrep(AuthcId, prepAuthcId);
        if (saslPrepRC != NLogin::NSasl::ESaslPrepReturnCodes::Success) {
            std::string error = "Unsupported characters in the authentication identity";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                DerivedActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        AuthcId = std::move(prepAuthcId);
    }
}

}
