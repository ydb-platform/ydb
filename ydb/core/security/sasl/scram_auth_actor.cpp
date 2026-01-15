#include "scram_auth_actor.h"

#include <openssl/rand.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/login_shared_func.h>
#include <ydb/core/security/sasl/base_auth_actors.h>
#include <ydb/core/security/sasl/events.h>
#include <ydb/core/security/sasl/static_credentials_provider.h>

#include <ydb/library/login/hashes_checker/hashes_checker.h>
#include <ydb/library/login/sasl/saslprep.h>

namespace NKikimr::NSasl {

using namespace NActors;
using namespace NLogin::NSasl;
using namespace NSchemeShard;

class TScramAuthActor : public TAuthActorBase {
public:
    TScramAuthActor(TActorId sender, const std::string& database, NLoginProto::EHashType::HashType hashType,
        const std::string& clientFirstMsg, const std::string& peerName)
        : TAuthActorBase(sender, database, peerName)
        , AuthHashType(hashType)
        , ClientFirstMsg(clientFirstMsg)
    {
        DerivedActorName = ActorName;
    }

    STATEFN(StateFinalMsgAwait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSasl::TEvSaslScramFinalClientRequest, HandleClientFinalMsg);
            CFunc(TEvents::TEvPoison::EventType, CleanupAndDie);
        }
    }

    virtual void Bootstrap(const TActorContext &ctx) override final {
        if (!AppData(ctx)->AuthConfig.GetEnableLoginAuthentication()) {
            std::string error = "Login authentication is disabled";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        TFirstClientMsg parsedFirstClientMsg;
        auto parsingRes = ParseFirstClientMsg(ClientFirstMsg, parsedFirstClientMsg);
        if (parsingRes != EParseMsgReturnCodes::Success) {
            std::string error = "Malformed SASL SCRAM first client message";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
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
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, serverError);
            return CleanupAndDie(ctx);
        }

        GS2Header = parsedFirstClientMsg.GS2Header;
        if (GS2Header.ChannelBindingFlag == EClientChannelBindingFlag::Required) {
            std::string error = "Channel binding isn't supported by server";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, EScramServerError::ChannelBindingNotSupported);
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
                    ActorName << "# " << ctx.SelfID.ToString() <<
                    ", " << error
                );
                SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, EScramServerError::InvalidUsernameEncoding);
                return CleanupAndDie(ctx);
            }

            GS2Header.AuthzId = std::move(prepAuthzId);
        }

        if (!parsedFirstClientMsg.Mext.empty()) {
            std::string error = "Unsupported extensions in SASL SCRAM first client message";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, EScramServerError::ExtensionsNoSupported);
            return CleanupAndDie(ctx);
        }

        std::string prepAuthcId;
        auto saslPrepRC = SaslPrep(AuthcId, prepAuthcId);
        if (saslPrepRC != ESaslPrepReturnCodes::Success) {
            std::string error = "Unsupported characters in the authentication identity";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, EScramServerError::InvalidUsernameEncoding);
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
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            // Needs change to NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST after migration
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        } else if (credsLookupResult == TStaticCredentialsProvider::UnknownUser) {
            std::stringstream error;
            error << "Cannot find user '" << AuthcId << "'";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << "Authentication failed: " << error.str();
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error.str(), EScramServerError::UnknownUser);
            return CleanupAndDie(ctx);
        }

        const auto itHashesInitParams = userHashInitParams.find(AuthHashType);
        if (itHashesInitParams == userHashInitParams.end()) {
            std::stringstream error;
            error << "Missing hash value for specified hash type";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << "Authentication failed: " << error.str();
            );
            error << ". Needed password change to use SASL SCRAM";
            SendError(NKikimrIssues::TIssuesIds::WARNING, error.str());
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
                ActorName << "# " << ctx.SelfID.ToString() <<
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
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, serverError);
            return CleanupAndDie(ctx);
        }

        if (parsedFinalClientMsg.GS2Header.ChannelBindingFlag != GS2Header.ChannelBindingFlag
            || parsedFinalClientMsg.GS2Header.RequestedChannelBinding != GS2Header.RequestedChannelBinding)
        {
            std::string error = "Client channel bindings don't match";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, EScramServerError::ChannelBindingsDontMatch);
            return CleanupAndDie(ctx);
        }

        if (!parsedFinalClientMsg.GS2Header.AuthzId.empty()) {
            std::string prepAuthzId;
            auto saslPrepRC = SaslPrep(GS2Header.AuthzId, prepAuthzId);
            if (saslPrepRC != ESaslPrepReturnCodes::Success) {
                std::string error = "Unsupported characters in the authorization identity";
                LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                    ActorName << "# " << ctx.SelfID.ToString() <<
                    ", " << error
                );
                SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error, EScramServerError::InvalidUsernameEncoding);
                return CleanupAndDie(ctx);
            }

            parsedFinalClientMsg.GS2Header.AuthzId = std::move(prepAuthzId);
        }

        if (parsedFinalClientMsg.GS2Header.AuthzId != GS2Header.AuthzId) {
            std::string error = "Authorization identities don't match";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        if (parsedFinalClientMsg.Nonce != Nonce) {
            std::string error = "Nonces don't match";
            LOG_WARN_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << error
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return CleanupAndDie(ctx);
        }

        ConcatenateAuthMsg(parsedFinalClientMsg.ClientFinalMessageWithoutProof);
        ClientProof = std::move(parsedFinalClientMsg.Proof);
        ResolveSchemeShard(ctx);
        return;
    }

private:
    virtual NKikimrScheme::TEvLogin CreateLoginRequest() const override final {
        return CreateScramLoginRequest(TString(AuthcId), AuthHashType, TString(ClientProof), TString(AuthMsg),
                                        TString(PeerName), AppData()->AuthConfig);
    }

    virtual void SendIssuedToken(const NKikimrScheme::TEvLoginResult& loginResult) const override final {
        auto response = std::make_unique<TEvSasl::TEvSaslScramFinalServerResponse>();
        response->Issue = MakeIssue(NKikimrIssues::TIssuesIds::SUCCESS);
        response->Msg = BuildFinalServerMsg(loginResult.GetServerSignature());
        response->Token = loginResult.GetToken();
        response->SanitizedToken = loginResult.GetSanitizedToken();
        response->IsAdmin = loginResult.GetIsAdmin();

        SendFinalResponse(std::move(response));
    }

    virtual void SendError(NKikimrIssues::TIssuesIds::EIssueCode issueCode, const std::string& message,
        NLogin::NSasl::EScramServerError scramErrorCode = NLogin::NSasl::EScramServerError::OtherError,
        [[maybe_unused]] const std::string& reason = "") const override final
    {
        auto response = std::make_unique<TEvSasl::TEvSaslScramFinalServerResponse>();
        response->Msg = BuildErrorMsg(scramErrorCode);
        response->Issue = MakeIssue(issueCode, TString(message));

        SendFinalResponse(std::move(response));
    }

    void AddServerNonce() {
        std::string nonce;
        nonce.resize(ServerNonceSize);
        RAND_bytes(reinterpret_cast<unsigned char*>(nonce.data()), nonce.size());
        Nonce += Base64Encode(nonce);
    }

    void ConcatenateAuthMsg(const std::string& msg) {
        AuthMsg += "," + msg;
    }

    void SendFirstResponse(std::unique_ptr<TEvSasl::TEvSaslScramFirstServerResponse> response) const {
        Send(Sender, response.release());
    }

    void SendFinalResponse(std::unique_ptr<TEvSasl::TEvSaslScramFinalServerResponse> response) const {
        response->AuthcId = AuthcId;
        Send(Sender, response.release());
    }

    void SendFirstServerMsg(const std::string& msg) {
        auto response = std::make_unique<TEvSasl::TEvSaslScramFirstServerResponse>();
        response->Msg = msg;

        SendFirstResponse(std::move(response));
        Become(&TScramAuthActor::StateFinalMsgAwait);
    }

private:
    const NLoginProto::EHashType::HashType AuthHashType;
    const std::string ClientFirstMsg;

    GS2Header GS2Header;

    std::string Nonce;
    std::string AuthMsg;
    std::string ClientProof;

    static const size_t ServerNonceSize;

    static constexpr std::string_view ActorName = "TScramAuthActor";
};

const size_t TScramAuthActor::ServerNonceSize = 12;

std::unique_ptr<IActor> CreateScramAuthActor(
    TActorId sender, const std::string& database, NLoginProto::EHashType::HashType hashType,
    const std::string& clientFirstMsg, const std::string& peerName)
{
    return std::make_unique<TScramAuthActor>(sender, database, hashType, clientFirstMsg, peerName);
}

} // namespace NKikimr::NSasl
