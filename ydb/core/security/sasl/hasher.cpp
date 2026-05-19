#include "hasher.h"

#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <openssl/rand.h>

#include <ydb/core/security/sasl/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/login/hashes_checker/hash_types.h>
#include <ydb/library/login/hashes_checker/hashes_checker.h>
#include <ydb/library/login/sasl/saslprep.h>
#include <ydb/library/login/sasl/scram.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SASL_AUTH


namespace NKikimr::NSasl {

using namespace NActors;
using namespace NLogin;
using namespace NLoginProto;

class THasher : public TActorBootstrapped<THasher> {
public:
    THasher(TActorId sender, const TStaticCredentials& creds,
        const std::vector<EHashType::HashType>& hashTypes, const TPasswordComplexity& passwordComplexity)
        : Sender(sender)
        , StaticCreds(creds)
        , HashTypes(hashTypes)
        , PasswordChecker(passwordComplexity)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        auto response = std::make_unique<TEvSasl::TEvComputedHashes>();

        std::string prepUsername;
        auto saslPrepRC = NLogin::NSasl::SaslPrep(StaticCreds.Username, prepUsername);
        if (saslPrepRC != NLogin::NSasl::ESaslPrepReturnCodes::Success) {
            response->Error = "Unsupported characters in username";
            YDB_LOG_CTX_ERROR(ctx, ", username check failed",
                {"Hasher", ctx.SelfID.ToString()},
                {"reason", response->Error});

            YDB_LOG_CTX_DEBUG(ctx, ", Send TEvComputedHashes:",
                {"Hasher", ctx.SelfID.ToString()},
                {"error", response->Error});

            Send(Sender, response.release());
            return Die(ctx);
        }

        response->PreparedUsername = std::move(prepUsername);

        auto passwordCheckResult = PasswordChecker.Check(StaticCreds.Username, StaticCreds.Password);
        if (!passwordCheckResult.Success) {
            response->Error = passwordCheckResult.Error;

            YDB_LOG_CTX_ERROR(ctx, ", password check failed",
                {"Hasher", ctx.SelfID.ToString()},
                {"reason", response->Error});

            YDB_LOG_CTX_DEBUG(ctx, ", Send TEvComputedHashes:",
                {"Hasher", ctx.SelfID.ToString()},
                {"error", response->Error});

            Send(Sender, response.release());
            return Die(ctx);
        }

        NJson::TJsonValue hashes;
        for (const auto& hashType : HashTypes) {
            if (HashesRegistry.HashTypesMap.contains(hashType)) {
                const auto& hashTypeDescription = HashesRegistry.HashTypesMap.at(hashType);
                switch (hashTypeDescription.Class) {
                case EHashClass::Argon: {
                    response->ArgonHash = GenerateArgonHash(hashTypeDescription);
                    hashes[hashTypeDescription.Name] = *ArgonHashToNewFormat(response->ArgonHash);
                    break;
                }
                case EHashClass::Scram: {
                    if (StaticCreds.Password.empty()) {
                        break;
                    }

                    auto scramHash = GenerateScramHash(hashTypeDescription, response->Error);
                    if (!response->Error.empty()) {
                        response->Error = "Error in generating '" + hashTypeDescription.Name + "' hash: " + response->Error;
                        break;
                    }

                    hashes[hashTypeDescription.Name] = std::move(scramHash);
                    break;
                }
                }

                if (!response->Error.empty()) {
                    break;
                }

            } else {
                response->Error = "Unavailable hash type";
                break;
            }
        }

        if (!response->Error.empty()) {
            YDB_LOG_CTX_ERROR(ctx, "",
                {"Hasher", ctx.SelfID.ToString()},
                {"Error", response->Error});

            hashes = NJson::TJsonValue();
            response->ArgonHash.clear();
        }

        if (hashes.IsDefined()) {
            hashes["version"] = HASHES_JSON_SCHEMA_VERSION;
            response->Hashes = Base64Encode(NJson::WriteJson(hashes, false));
        }

        YDB_LOG_CTX_DEBUG(ctx, ", Send TEvComputedHashes:, argon",
            {"Hasher", ctx.SelfID.ToString()},
            {"error", response->Error},
            {"username", response->PreparedUsername},
            {"hashes", Base64StrictDecode(response->Hashes)},
            {"hash", response->ArgonHash});

        Send(Sender, response.release());
        return Die(ctx);
    }

    std::string GenerateArgonHash(const THashTypeDescription& hashParams) const {
        char salt[hashParams.SaltSize];
        char hash[hashParams.HashSize];
        RAND_bytes(reinterpret_cast<unsigned char*>(salt), hashParams.SaltSize);
        ArgonHasher->Hash(
            reinterpret_cast<const ui8*>(StaticCreds.Password.data()),
            StaticCreds.Password.size(),
            reinterpret_cast<ui8*>(salt),
            hashParams.SaltSize,
            reinterpret_cast<ui8*>(hash),
            hashParams.HashSize);
        NJson::TJsonValue json;
        json["type"] = hashParams.Name;
        json["salt"] = Base64Encode(std::string_view(salt, hashParams.SaltSize));
        json["hash"] = Base64Encode(std::string_view(hash, hashParams.HashSize));
        return NJson::WriteJson(json, false);
    }

    std::string GenerateScramHash(const THashTypeDescription& hashParams, std::string& error) const {
        std::string salt;
        salt.resize(hashParams.SaltSize);
        RAND_bytes(reinterpret_cast<unsigned char*>(salt.data()), salt.size());

        std::string storedKey;
        std::string serverKey;
        if (!NLogin::NSasl::GenerateScramSecrets(hashParams.Name, StaticCreds.Password,
            salt, hashParams.IterationsCount,
            storedKey, serverKey, error))
        {
            return "";
        };

        std::stringstream secret;
        secret << hashParams.IterationsCount << ':' << Base64Encode(salt) << '$'
            << Base64Encode(storedKey) << ':' << Base64Encode(serverKey);
        return secret.str();
    }

private:
    const TActorId Sender;
    const TStaticCredentials StaticCreds;
    const std::vector<EHashType::HashType> HashTypes;

    const TPasswordChecker PasswordChecker;
    static const std::unique_ptr<const NArgonish::IArgon2Base> ArgonHasher;
};

const std::unique_ptr<const NArgonish::IArgon2Base> THasher::ArgonHasher(Default<NArgonish::TArgon2Factory>().Create(
    NArgonish::EArgon2Type::Argon2id, // Mixed version of Argon2
    2, // 2-pass computation
    (1<<11), // 2 mebibytes memory usage (in KiB)
    1 // number of threads and lanes
).Release());

std::unique_ptr<IActor> CreateHasher(
    TActorId sender, const TStaticCredentials& creds,
   const std::vector<EHashType::HashType>& hashTypes, TPasswordComplexity passwordComplexity
) {
    return std::make_unique<THasher>(sender, creds, hashTypes, passwordComplexity);
}

} // namespace NKikimr::NSasl
