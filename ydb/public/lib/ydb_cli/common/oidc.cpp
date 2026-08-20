#include "oidc.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/yaml/as/tstring.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/spinlock.h>

#include <filesystem>
#include <mutex>

namespace NYdb::NConsoleClient {

    namespace {

        constexpr i64 CACHE_VERSION = 1;

        std::optional<TInstant> ReadExpiresAt(const NJson::TJsonValue::TMapType& object, std::string_view name) {
            const auto it = object.find(std::string(name));
            if (it == object.end()) {
                return std::nullopt;
            }
            TInstant result;
            if (!TInstant::TryParseIso8601(it->second.GetStringSafe(), result)) {
                throw yexception() << "Invalid OIDC token cache field '" << name << "'";
            }
            return result;
        }

        class TOidcFileTokenCache final: public IOidcTokenCache {
        public:
            explicit TOidcFileTokenCache(std::string path)
                : Path_(std::move(path))
            {
            }

            std::optional<TOidcTokenSet> Read() const override {
                with_lock (Lock_) {
                    const TFsPath path(Path_);
                    if (!path.Exists()) {
                        return std::nullopt;
                    }
#ifndef _win32_
                    const TFileStat stat(path.GetPath());
                    if (stat.Mode & (S_IRWXG | S_IRWXO)) {
                        throw yexception() << "OIDC token cache must be accessible only by its owner: " << Path_;
                    }
#endif
                    NJson::TJsonValue json;
                    NJson::ReadJsonTree(TFileInput(path.GetPath()).ReadAll(), &json, true);
                    const auto& object = json.GetMapSafe();
                    const auto version = object.find("version");
                    if (version == object.end() || version->second.GetIntegerRobust() != CACHE_VERSION) {
                        return std::nullopt;
                    }

                    const auto access = object.find("access_token");
                    if (access == object.end() || access->second.GetStringSafe().empty()) {
                        return std::nullopt;
                    }

                    TOidcTokenSet tokens;
                    tokens.AccessToken.Token = access->second.GetStringSafe();
                    tokens.AccessToken.ExpiresAt = ReadExpiresAt(object, "access_token_expires_at");
                    if (const auto refresh = object.find("refresh_token"); refresh != object.end()) {
                        const std::string value = refresh->second.GetStringSafe();
                        if (!value.empty()) {
                            tokens.RefreshToken = TOidcToken{
                                .Token = value,
                                .ExpiresAt = ReadExpiresAt(object, "refresh_token_expires_at"),
                            };
                        }
                    }
                    return tokens;
                }
            }

            void Write(const TOidcTokenSet& tokens) override {
                with_lock (Lock_) {
                    TFsPath path(Path_);
                    path.Fix();
                    if (!path.Parent().Exists()) {
                        path.Parent().MkDirs();
                    }
#ifndef _win32_
                    if (Chmod(path.Parent().GetPath().c_str(), S_IRWXU) != 0) {
                        throw yexception() << "Couldn't protect OIDC token cache directory: " << path.Parent().GetPath();
                    }
#endif

                    TStringStream content;
                    NJsonWriter::TBuf json(NJsonWriter::HEM_RELAXED, &content);
                    json.BeginObject();
                    json.WriteKey("version").WriteLongLong(CACHE_VERSION);
                    json.WriteKey("access_token").WriteString(tokens.AccessToken.Token);
                    if (tokens.AccessToken.ExpiresAt.has_value()) {
                        json.WriteKey("access_token_expires_at").WriteString(tokens.AccessToken.ExpiresAt->ToString());
                    }
                    if (tokens.RefreshToken.has_value()) {
                        json.WriteKey("refresh_token").WriteString(tokens.RefreshToken->Token);
                        if (tokens.RefreshToken->ExpiresAt.has_value()) {
                            json.WriteKey("refresh_token_expires_at").WriteString(tokens.RefreshToken->ExpiresAt->ToString());
                        }
                    }
                    json.EndObject();

                    const TString tempPath = TStringBuilder()
                                             << path.GetPath() << ".tmp." << TInstant::Now().NanoSeconds() << '.' << reinterpret_cast<uintptr_t>(this);
                    try {
                        TFileOutput(TFile(tempPath, CreateAlways | WrOnly | AWUser | ARUser)).Write(content.Str());
#ifndef _win32_
                        if (Chmod(tempPath.c_str(), S_IRUSR | S_IWUSR) != 0) {
                            throw yexception() << "Couldn't protect temporary OIDC token cache: " << tempPath;
                        }
#endif
                        TFsPath(tempPath).RenameTo(path);
                    } catch (...) {
                        TFsPath(tempPath).DeleteIfExists();
                        throw;
                    }
                }
            }

        private:
            std::string Path_;
            mutable TAdaptiveLock Lock_;
        };

        class TOidcConsoleAcceptor final: public IOidcAuthorizationAcceptor {
        public:
            void Accept(const TOidcDeviceAuthorizationInfo& info) override {
                Cout << "Open " << info.VerificationUri << " and enter code: " << info.UserCode << Endl;
            }
        };

        std::string ReadCachePath(const std::string& configPath) {
            const YAML::Node root = YAML::LoadFile(configPath);
            const YAML::Node cachePath = root["cache_path"];
            if (!cachePath) {
                return {};
            }
            if (!cachePath.IsScalar()) {
                throw yexception() << "OIDC config field 'cache_path' must be scalar";
            }

            std::filesystem::path result(cachePath.as<std::string>());
            if (result.is_relative()) {
                result = std::filesystem::path(configPath).parent_path() / result;
            }
            return result.lexically_normal().string();
        }

    } // namespace

    std::shared_ptr<IOidcTokenCache> CreateOidcFileTokenCache(const std::string& path) {
        return std::make_shared<TOidcFileTokenCache>(path);
    }

    std::shared_ptr<IOidcAuthorizationAcceptor> CreateOidcConsoleAcceptor() {
        return std::make_shared<TOidcConsoleAcceptor>();
    }

    TOidcConfig ReadOidcConfig(const std::string& configPath) {
        TOidcConfig config = TOidcConfig::Parse(configPath);
        if (const std::string cachePath = ReadCachePath(configPath); !cachePath.empty()) {
            config.TokenCache(CreateOidcFileTokenCache(cachePath));
        }
        if (std::holds_alternative<TDeviceOidcConfig>(config.Flow_)) {
            config.Acceptor(CreateOidcConsoleAcceptor());
        }
        return config;
    }

} // namespace NYdb::NConsoleClient
