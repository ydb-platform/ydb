#include "plain_auth_actor.h"

#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/login_shared_func.h>
#include <ydb/core/security/sasl/base_auth_actors.h>
#include <ydb/core/security/sasl/events.h>
#include <ydb/core/security/sasl/static_credentials_provider.h>

#include <ydb/library/login/hashes_checker/hash_types.h>
#include <ydb/library/login/hashes_checker/hashes_checker.h>
#include <ydb/library/login/sasl/scram.h>


using namespace NLoginProto;

namespace {

const std::vector<EHashType::HashType> ALLOWED_HASHES_TO_AUTH = {
    // Ordered by priority
    EHashType::ScramSha256,
    EHashType::Argon,
};

bool IsBase64(const std::string& value) {
    try {
        Base64StrictDecode(value);
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

namespace NKikimr::NSasl {

using namespace NActors;
using namespace NLogin;

class TPlainAuthActor : public TPlainAuthActorBase {
public:
    TPlainAuthActor(TActorId sender, const std::string& database, const std::string& authMsg, const std::string& peerName)
        : TPlainAuthActorBase(sender, database, authMsg, peerName)
    {
        DerivedActorName = ActorName;
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

        ProcessAuthMsg(ctx);

        const auto [credsLookupResult, userHashInitParams] = TStaticCredentialsProvider::GetInstance()
            .GetUserHashInitParams(Database, AuthcId);
        CredsLookupResult = credsLookupResult;

        // it can happen if SchemeShard works on a old version and doesn't pass hashes params
        // after migration it has to become an error
        if (CredsLookupResult == TStaticCredentialsProvider::UnknownDatabase) {
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << "Unknown database or SchemeShard works on old version"
            );
            ResolveSchemeShard(ctx);
            return;
        } else if (CredsLookupResult == TStaticCredentialsProvider::UnknownUser) {
            std::stringstream error;
            error << "Cannot find user '" << AuthcId << "'";
            LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << "Authentication failed: " << error.str();
            );
            SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error.str());
            return CleanupAndDie(ctx);
        }

        ComputeHash(ctx, userHashInitParams);
        ResolveSchemeShard(ctx);
        return;
    }

private:
    virtual NKikimrScheme::TEvLogin CreateLoginRequest() const override final {
        if (CredsLookupResult == TStaticCredentialsProvider::UnknownDatabase) { // for backward compatibility
            return NKikimr::CreatePlainLoginRequestOldFormat(TString(AuthcId), TString(Passwd), TString(PeerName),
                AppData()->AuthConfig);
        } else {
            return NKikimr::CreatePlainLoginRequest(TString(AuthcId), ChosenAuthHashType, TString(ComputedHash),
                TString(PeerName), AppData()->AuthConfig);
        }
    }

    virtual void SendIssuedToken(const NKikimrScheme::TEvLoginResult& loginResult) const override final {
        auto response = std::make_unique<TEvSasl::TEvSaslPlainLoginResponse>();
        response->Issue = MakeIssue(NKikimrIssues::TIssuesIds::SUCCESS);
        response->Token = loginResult.GetToken();
        response->SanitizedToken = loginResult.GetSanitizedToken();
        response->IsAdmin = loginResult.GetIsAdmin();

        SendResponse(std::move(response));
    }

    virtual void SendError(NKikimrIssues::TIssuesIds::EIssueCode issueCode, const std::string& message,
        [[maybe_unused]] NLogin::NSasl::EScramServerError scramErrorCode = NLogin::NSasl::EScramServerError::OtherError,
        [[maybe_unused]] const std::string& reason = "") const override final
    {
        auto response = std::make_unique<TEvSasl::TEvSaslPlainLoginResponse>();
        response->Issue = MakeIssue(issueCode, TString(message));
        SendResponse(std::move(response));
    }

    std::string ComputeArgonHash(const THashTypeDescription& hashParams, const std::string& salt) const {
        std::string hash;
        hash.resize(hashParams.HashSize);
        ArgonHasher->Hash(
            reinterpret_cast<const ui8*>(Passwd.data()),
            Passwd.size(),
            reinterpret_cast<const ui8*>(salt.data()),
            salt.size(),
            reinterpret_cast<ui8*>(hash.data()),
            hash.size());

        return Base64Encode(hash);
    }

    std::string ComputeScramHash(const THashTypeDescription& hashParams,
        ui32 iterationsCount, const std::string& salt, std::string& error) const
    {
        std::string serverKey;
        if (!NLogin::NSasl::ComputeServerKey(hashParams.Name, Passwd, salt, iterationsCount,
            serverKey, error))
        {
            return "";
        };

        return Base64Encode(serverKey);
    }

    void ComputeHash(const TActorContext &ctx,
        const std::unordered_map<NLoginProto::EHashType::HashType, std::string>& hashesInitParams)
    {
        std::string computedHash;
        for (const auto& allowedHashType : ALLOWED_HASHES_TO_AUTH) {
            const auto itHashesInitParams = hashesInitParams.find(allowedHashType);
            if (itHashesInitParams == hashesInitParams.end()) {
                continue;
            }

            ChosenAuthHashType = allowedHashType;
            const auto& hashTypeDescr = HashesRegistry.HashTypesMap.at(ChosenAuthHashType);
            switch (hashTypeDescr.Class) {
            case EHashClass::Argon: {
                if (!IsBase64(itHashesInitParams->second)) {
                    LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
                        ActorName << "# " << ctx.SelfID.ToString() <<
                        ", " << "Authentication failed: " <<
                        "'" << AuthcId << "' has broken Argon hash";
                    );
                    SendError(NKikimrIssues::TIssuesIds::UNEXPECTED, "");
                    return CleanupAndDie(ctx);
                }

                const auto argonSalt = Base64StrictDecode(itHashesInitParams->second);
                auto argonHash = ComputeArgonHash(hashTypeDescr, argonSalt);
                ComputedHash = std::move(argonHash);
                break;
            }
            case EHashClass::Scram: {
                if (Passwd.empty()) {
                    std::string error = "Empty password";
                    LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                        ActorName << "# " << ctx.SelfID.ToString() <<
                        ", " << error
                    );
                    SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
                    return CleanupAndDie(ctx);
                }

                const auto scramInitParams = ParseScramHashInitParams(itHashesInitParams->second);
                ui32 iterationsCount;
                if (!TryFromString(scramInitParams.IterationsCount, iterationsCount)
                    || (iterationsCount == 0) || !IsBase64(scramInitParams.Salt)) {
                    LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
                        ActorName << "# " << ctx.SelfID.ToString() <<
                        ", " << "Authentication failed: " <<
                        "'" << AuthcId << "' has broken Scram hash";
                    );
                    SendError(NKikimrIssues::TIssuesIds::UNEXPECTED, "");
                    return CleanupAndDie(ctx);
                }

                const auto scramSalt = Base64StrictDecode(scramInitParams.Salt);

                std::string error;
                auto scramHash = ComputeScramHash(hashTypeDescr, iterationsCount, scramSalt, error);
                if (!error.empty()) {
                    std::string error = "Unsupported characters in the password";
                    LOG_INFO_S(ctx, NKikimrServices::SASL_AUTH,
                        ActorName << "# " << ctx.SelfID.ToString() <<
                        ", " << error
                    );
                    SendError(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
                    return CleanupAndDie(ctx);
                }

                ComputedHash = std::move(scramHash);
                break;
            }
            }

            break;
        }

        if (ChosenAuthHashType == EHashType::Unknown) {
            LOG_ERROR_S(ctx, NKikimrServices::SASL_AUTH,
                ActorName << "# " << ctx.SelfID.ToString() <<
                ", " << "Authentication failed: " <<
                "'" << AuthcId << "' has no hashes";
            );
            SendError(NKikimrIssues::TIssuesIds::UNEXPECTED, "");
            return CleanupAndDie(ctx);
        }
    }

    void SendResponse(std::unique_ptr<TEvSasl::TEvSaslPlainLoginResponse> response) const {
        Send(Sender, response.release());
    }

private:
    TStaticCredentialsProvider::ELookupResultCode CredsLookupResult;
    NLoginProto::EHashType::HashType ChosenAuthHashType = EHashType::Unknown;
    std::string ComputedHash;

    static const std::unique_ptr<const NArgonish::IArgon2Base> ArgonHasher;
    static constexpr std::string_view ActorName = "TPlainAuthActor";
};

const std::unique_ptr<const NArgonish::IArgon2Base> TPlainAuthActor::ArgonHasher(Default<NArgonish::TArgon2Factory>().Create(
    NArgonish::EArgon2Type::Argon2id, // Mixed version of Argon2
    2, // 2-pass computation
    (1<<11), // 2 mebibytes memory usage (in KiB)
    1 // number of threads and lanes
).Release());

std::unique_ptr<IActor> CreatePlainAuthActor(
    TActorId sender, const std::string& database, const std::string& saslPlainAuthMsg, const std::string& peerName)
{
    return std::make_unique<TPlainAuthActor>(sender, database, saslPlainAuthMsg, peerName);
}

} // namespace NKikimr::NSasl
