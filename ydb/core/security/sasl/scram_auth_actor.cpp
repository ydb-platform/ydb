#include "scram_auth_actor.h"

#include <openssl/rand.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/login_shared_func.h>
#include <ydb/core/security/sasl/events.h>
#include <ydb/core/security/sasl/static_credentials_provider.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/login/hashes_checker/hashes_checker.h>
#include <ydb/library/login/sasl/saslprep.h>
#include <ydb/library/login/sasl/scram.h>

namespace NKikimr::NSasl {

using namespace NActors;
using namespace NLogin::NSasl;
using namespace NSchemeShard;

class TScramAuthActor : public TActorBootstrapped<TScramAuthActor> {
public:
    TScramAuthActor(TActorId sender, const std::string& database, NLoginProto::EHashType::HashType hashType,
        const std::string& clientFirstMsg, const std::string& peerName)
        : Sender(sender)
        , Database(database)
        , AuthHashType(hashType)
        , ClientFirstMsg(clientFirstMsg)
        , PeerName(peerName)
    {
    }

    STATEFN(StateFinalMsgAwait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSasl::TEvSaslScramFinalClientRequest, HandleClientFinalMsg);
            CFunc(TEvents::TEvPoison::EventType, CleanupAndDie);
        }
    }

    STATEFN(StateNavigate) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            CFunc(TEvents::TEvPoison::EventType, CleanupAndDie);
        }
    }

    STATEFN(StateLogin) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
            HFunc(TEvSchemeShard::TEvLoginResult, HandleLoginResult);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            CFunc(TEvents::TEvPoison::EventType, CleanupAndDie);
        }
    }

    void Bootstrap(const TActorContext &ctx) {
        if (!AppData(ctx)->AuthConfig.GetEnableLoginAuthentication()) {
            std::string error = "Login authentication is disabled";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        TFirstClientMsg parsedFirstClientMsg;
        auto parsingRes = ParseFirstClientMsg(ClientFirstMsg, parsedFirstClientMsg);
        if (parsingRes != EParseMsgReturnCodes::Success) {
            std::string error = "Malformed SASL SCRAM first client message";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );

            EScramServerError serverError;
            switch (parsingRes) {
            case EParseMsgReturnCodes::InvalidEncoding:
                serverError = EScramServerError::InvalidEncoding;
                break;
            case EParseMsgReturnCodes::InvalidUsernameEncoding:
                serverError = EScramServerError::InvalidUsernameEncoding;
                break;
            default:
                serverError = EScramServerError::OtherError;
                break;
            }
            SendError(serverError, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        GS2Header = parsedFirstClientMsg.GS2Header;
        if (GS2Header.ChannelBindingFlag == EClientChannelBindingFlag::Required) {
            std::string error = "Channel binding isn't supported by server";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::ChannelBindingNotSupported, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        AuthcId = std::move(parsedFirstClientMsg.AuthcId);

        Nonce = std::move(parsedFirstClientMsg.Nonce);
        AuthMsg = std::move(parsedFirstClientMsg.ClientFirstMessageBare);

        if (!GS2Header.AuthzId.empty()) {
            std::string prepAuthzId;
            auto saslPrepRC = SaslPrep(GS2Header.AuthzId, prepAuthzId);
            if (saslPrepRC != ESaslPrepReturnCodes::Success) {
                std::string error = "Unsupported characters in the authorization identity";
                LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                    "TScramAuthActor# " << ctx.SelfID.ToString() <<
                    ", " << error
                );
                SendError(EScramServerError::InvalidUsernameEncoding, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
                return CleanupAndDie(ctx);
            }

            GS2Header.AuthzId = std::move(prepAuthzId);
        }

        if (!parsedFirstClientMsg.Mext.empty()) {
            std::string error = "Unsupported extensions in SASL SCRAM first client message";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::ExtensionsNoSupported, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        std::string prepAuthcId;
        auto saslPrepRC = SaslPrep(AuthcId, prepAuthcId);
        if (saslPrepRC != ESaslPrepReturnCodes::Success) {
            std::string error = "Unsupported characters in the authentication identity";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::InvalidUsernameEncoding, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        AuthcId = std::move(prepAuthcId);

        const auto [credsLookupResult, userHashInitParams] = TStaticCredentialsProvider::GetInstance()
            .GetUserHashInitParams(Database, AuthcId);

        // it can happen if SchemeShard works on a old version and doesn't pass hashes params
        // in this case we didn't have any user hashes in scram format
        if (credsLookupResult == TStaticCredentialsProvider::UnknownDatabase) {
            std::string error = "UnknownDatabase or SchemeShard works on old version";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            // Needs change to NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST after migration
            SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        } else if (credsLookupResult == TStaticCredentialsProvider::UnknownUser) {
            std::stringstream error;
            error << "Cannot find user '" << AuthcId << "'";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << "Authentication failed: " << error.str();
            );
            SendError(EScramServerError::UnknownUser, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error.str());
            return CleanupAndDie(ctx);
        }

        const auto itHashesInitParams = userHashInitParams.find(AuthHashType);
        if (itHashesInitParams == userHashInitParams.end()) {
            std::stringstream error;
            error << "Missing hash value for specified hash type";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << "Authentication failed: " << error.str();
            );
            error << ". Needed password change to use SASL SCRAM";
            SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::WARNING, error.str());
            return CleanupAndDie(ctx);
        }

        const auto scramInitParams = NLogin::ParseScramHashInitParams(itHashesInitParams->second);
        AddServerNonce();

        const std::string firstServerMsg = BuildFirstServerMsg(Nonce, scramInitParams.Salt, scramInitParams.IterationsCount);
        ConcatenateAuthMsg(firstServerMsg);
        SendFirstServerMsg(firstServerMsg);
        return;
    }

    void HandleClientFinalMsg(TEvSasl::TEvSaslScramFinalClientRequest::TPtr& ev, const TActorContext &ctx) {
        TFinalClientMsg parsedFinalClientMsg;
        auto parsingRes = ParseFinalClientMsg(ev->Get()->Msg, parsedFinalClientMsg);
        if (parsingRes != EParseMsgReturnCodes::Success) {
            std::string error = "Malformed SASL SCRAM final client message";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );

            EScramServerError serverError;
            switch (parsingRes) {
            case EParseMsgReturnCodes::InvalidEncoding:
                serverError = EScramServerError::InvalidEncoding;
                break;
            case EParseMsgReturnCodes::InvalidUsernameEncoding:
                serverError = EScramServerError::InvalidUsernameEncoding;
                break;
            default:
                serverError = EScramServerError::OtherError;
                break;
            }
            SendError(serverError, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        if (parsedFinalClientMsg.GS2Header.ChannelBindingFlag != GS2Header.ChannelBindingFlag
            || parsedFinalClientMsg.GS2Header.RequestedChannelBinding != GS2Header.RequestedChannelBinding)
        {
            std::string error = "Client channel bindings don't match";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::ChannelBindingsDontMatch, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        if (!parsedFinalClientMsg.GS2Header.AuthzId.empty()) {
            std::string prepAuthzId;
            auto saslPrepRC = SaslPrep(GS2Header.AuthzId, prepAuthzId);
            if (saslPrepRC != ESaslPrepReturnCodes::Success) {
                std::string error = "Unsupported characters in the authorization identity";
                LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                    "TScramAuthActor# " << ctx.SelfID.ToString() <<
                    ", " << error
                );
                SendError(EScramServerError::InvalidUsernameEncoding, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
                return CleanupAndDie(ctx);
            }

            parsedFinalClientMsg.GS2Header.AuthzId = std::move(prepAuthzId);
        }

        if (parsedFinalClientMsg.GS2Header.AuthzId != GS2Header.AuthzId) {
            std::string error = "Authorization identities don't match";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        if (parsedFinalClientMsg.Nonce != Nonce) {
            std::string error = "Nonces don't match";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        ConcatenateAuthMsg(parsedFinalClientMsg.ClientFinalMessageWithoutProof);
        ClientProof = std::move(parsedFinalClientMsg.Proof);
        ResolveSchemeShard(ctx);
        return;
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext &ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::SASL_AUTH,
            "TScramAuthActor# " << ctx.SelfID.ToString() <<
            ", " << "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult" <<
            ", errors# " << ev->Get()->Request.Get()->ErrorCount);

        const auto& resultSet = ev->Get()->Request.Get()->ResultSet;
        if (resultSet.size() == 1 && resultSet.front().Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            const auto domainInfo = resultSet.front().DomainInfo;
            if (domainInfo != nullptr) {
                IActor* pipe = NTabletPipe::CreateClient(SelfId(), domainInfo->ExtractSchemeShard(), GetPipeClientConfig());
                PipeClient = RegisterWithSameMailbox(pipe);

                auto request = std::make_unique<TEvSchemeShard::TEvLogin>();
                request.get()->Record = CreateScramLoginRequest(TString(AuthcId), AuthHashType, TString(ClientProof),
                    TString(AuthMsg), TString(PeerName), AppData()->AuthConfig);

                NTabletPipe::SendData(SelfId(), PipeClient, request.release());
                Become(&TThis::StateLogin, Timeout, new TEvents::TEvWakeup());
                return;
            }
        }

        std::string error = "Unknown database";
        LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
            "TScramAuthActor# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST, error);
        return CleanupAndDie(ctx);
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&, const TActorContext &ctx) {
        std::string error = "SchemeShard is unreachable";
        LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
            "TScramAuthActor# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
        return CleanupAndDie(ctx);
    }

    void HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx) {
        std::string error = "SchemeShard is unreachable";
        LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
            "TScramAuthActor# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
        return CleanupAndDie(ctx);
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext &ctx) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            std::string error = "SchemeShard is unreachable";
            LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
            return CleanupAndDie(ctx);
        }
    }

    void HandleLoginResult(TEvSchemeShard::TEvLoginResult::TPtr& ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::SASL_AUTH,
            "TScramAuthActor# " << ctx.SelfID.ToString() <<
            " Handle TEvSchemeShard::TEvLoginResult" <<
            ", " << ev->Get()->Record.DebugString()
        );

        const NKikimrScheme::TEvLoginResult& loginResult = ev->Get()->Record;
        if (loginResult.HasError()) { // explicit error takes precedence
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << "Authentication failed: " << loginResult.GetError()
            );
            SendError(EScramServerError::InvalidProof, NKikimrIssues::TIssuesIds::ACCESS_DENIED, loginResult.GetError());
        } else if (!loginResult.HasToken()) { // empty token is still an error
            std::string error = "Failed to produce a token";
            LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, error);
        } else { // success = token + no errors
            auto response = std::make_unique<TEvSasl::TEvSaslScramFinalServerResponse>();

            response->Issue = MakeIssue(NKikimrIssues::TIssuesIds::SUCCESS);
            response->Msg = BuildFinalServerMsg(loginResult.GetServerSignature());
            response->Token = loginResult.GetToken();
            response->SanitizedToken = loginResult.GetSanitizedToken();
            response->IsAdmin = loginResult.GetIsAdmin();

            LOG_DEBUG_S(ctx, NKikimrServices::SASL_AUTH,
                "TScramAuthActor# " << ctx.SelfID.ToString() <<
                ", " << "Authentication completed for '" << AuthcId << "'"
            );
            SendFinalResponse(std::move(response));
        }

        return CleanupAndDie(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        std::string error = "SchemeShard response timeout";
        LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
            "TScramAuthActor# " << ctx.SelfID.ToString() <<
            ", " << error
        );
        SendError(EScramServerError::OtherError, NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, error);
        return CleanupAndDie(ctx);
    }

private:

    void AddServerNonce() {
        std::string nonce;
        nonce.resize(ServerNonceSize);
        RAND_bytes(reinterpret_cast<unsigned char*>(nonce.data()), nonce.size());
        Nonce += Base64Encode(nonce);
    }

    void ConcatenateAuthMsg(const std::string& msg) {
        AuthMsg += "," + msg;
    }

    void ResolveSchemeShard(const TActorContext &ctx) {
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

    NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        return clientConfig;
    }

    void CleanupAndDie(const TActorContext &ctx) {
        if (PipeClient) {
            NTabletPipe::CloseClient(SelfId(), PipeClient);
        }
        return Die(ctx);
    }

    void SendFirstResponse(std::unique_ptr<TEvSasl::TEvSaslScramFirstServerResponse> response) {
        Send(Sender, response.release());
    }

    void SendFinalResponse(std::unique_ptr<TEvSasl::TEvSaslScramFinalServerResponse> response) {
        response->AuthcId = AuthcId;
        Send(Sender, response.release());
    }

    void SendFirstServerMsg(const std::string& msg) {
        auto response = std::make_unique<TEvSasl::TEvSaslScramFirstServerResponse>();
        response->Msg = msg;
        SendFirstResponse(std::move(response));
        Become(&TThis::StateFinalMsgAwait);
    }

    void SendError(EScramServerError scramErrorCode, NKikimrIssues::TIssuesIds::EIssueCode issueCode, const std::string& message) {
        auto response = std::make_unique<TEvSasl::TEvSaslScramFinalServerResponse>();
        response->Msg = BuildErrorMsg(scramErrorCode);
        response->Issue = MakeIssue(issueCode, TString(message));
        SendFinalResponse(std::move(response));
    }

private:
    const TActorId Sender;
    const std::string Database;
    const NLoginProto::EHashType::HashType AuthHashType;
    const std::string ClientFirstMsg;
    const std::string PeerName;

    GS2Header GS2Header;
    std::string AuthcId;

    std::string Nonce;
    std::string AuthMsg;
    std::string ClientProof;

    TActorId PipeClient;

    static const TDuration Timeout;
    static const size_t ServerNonceSize;
};

const TDuration TScramAuthActor::Timeout = TDuration::MilliSeconds(30000);
const size_t TScramAuthActor::ServerNonceSize = 12;

std::unique_ptr<IActor> CreateScramAuthActor(
    TActorId sender, const std::string& database, NLoginProto::EHashType::HashType hashType,
    const std::string& clientFirstMsg, const std::string& peerName)
{
    return std::make_unique<TScramAuthActor>(sender, database, hashType, clientFirstMsg, peerName);
}

} // namespace NKikimr::NSasl
