#include "meta_info.h"

#include <library/cpp/json/json_reader.h>

#include <util/string/builder.h>

namespace NTvmAuth::NTvmTool {
    TString TMetaInfo::TConfig::ToString() const {
        TStringStream s;
        s << "self_tvm_id=" << SelfTvmId << ", "
          << "bb_env=" << BbEnv << ", "
          << "dsts=[";

        for (const auto& pair : DstAliases) {
            s << "(" << pair.first << ":" << pair.second << ")";
        }

        s << "]";

        return std::move(s.Str());
    }

    TMetaInfo::TMetaInfo(TLoggerPtr logger)
        : Logger_(std::move(logger))
    {
    }

    TMetaInfo::TConfigPtr TMetaInfo::Init(TKeepAliveHttpClient& client,
                                          const TClientSettings& settings) {
        ApplySettings(settings);

        TryPing(client);
        const TString metaString = Fetch(client);
        if (Logger_) {
            TStringStream s;
            s << "Meta info fetched from " << settings.GetHostname() << ":" << settings.GetPort();
            Logger_->Debug(s.Str());
        }

        try {
            Config_.Set(ParseMetaString(metaString, SelfAlias_));
        } catch (const yexception& e) {
            ythrow TNonRetriableException() << "Malformed json from tvmtool: " << e.what();
        }
        TConfigPtr cfg = Config_.Get();
        Y_ENSURE_EX(cfg, TNonRetriableException() << "Alias '" << SelfAlias_ << "' not found in meta info");

        if (Logger_) {
            Logger_->Info("Meta: " + cfg->ToString());
        }

        return cfg;
    }

    TString TMetaInfo::GetRequestForTickets(const TConfig& config) {
        Y_ENSURE(!config.DstAliases.empty());

        TStringStream s;
        s << "/tvm/tickets"
          << "?src=" << config.SelfTvmId
          << "&dsts=";

        for (const auto& pair : config.DstAliases) {
            s << pair.second << ","; // avoid aliases - url-encoding required
        }
        s.Str().pop_back();

        return s.Str();
    }

    bool TMetaInfo::TryUpdateConfig(TKeepAliveHttpClient& client) {
        const TString metaString = Fetch(client);

        TConfigPtr config;
        try {
            config = ParseMetaString(metaString, SelfAlias_);
        } catch (const yexception& e) {
            ythrow TNonRetriableException() << "Malformed json from tvmtool: " << e.what();
        }
        Y_ENSURE_EX(config, TNonRetriableException() << "Alias '" << SelfAlias_ << "' not found in meta info");

        TConfigPtr oldConfig = Config_.Get();
        if (*config == *oldConfig) {
            return false;
        }

        if (Logger_) {
            Logger_->Info(TStringBuilder()
                          << "Meta was updated. Old: (" << oldConfig->ToString()
                          << "). New: (" << config->ToString() << ")");
        }

        Config_ = config;
        return true;
    }

    void TMetaInfo::TryPing(TKeepAliveHttpClient& client) {
        try {
            TStringStream s;
            TKeepAliveHttpClient::THttpCode code = client.DoGet("/tvm/ping", &s);
            if (code < 200 || 300 <= code) {
                throw yexception() << "(" << code << ") " << s.Str();
            }
        } catch (const std::exception& e) {
            ythrow TNonRetriableException() << "Failed to connect to tvmtool: " << e.what();
        }
    }

    TString TMetaInfo::Fetch(TKeepAliveHttpClient& client) const {
        TStringStream res;
        TKeepAliveHttpClient::THttpCode code;
        try {
            code = client.DoGet("/tvm/private_api/__meta__", &res, AuthHeader_);
        } catch (const std::exception& e) {
            ythrow TRetriableException() << "Failed to fetch meta data from tvmtool: " << e.what();
        }

        if (code != 200) {
            Y_ENSURE_EX(code != 404,
                        TNonRetriableException() << "Library does not support so old tvmtool. You need tvmtool>=1.1.0");

            TStringStream err;
            err << "Failed to fetch meta from tvmtool: " << client.GetHost() << ":" << client.GetPort()
                << " (" << code << "): " << res.Str();
            Y_ENSURE_EX(!(500 <= code && code < 600), TRetriableException() << err.Str());
            ythrow TNonRetriableException() << err.Str();
        }

        return res.Str();
    }

    static TMetaInfo::TDstAliases::value_type ParsePair(const NJson::TJsonValue& val, const TString& meta) {
        NJson::TJsonValue jAlias;
        Y_ENSURE(val.GetValue("alias", &jAlias), meta);
        Y_ENSURE(jAlias.IsString(), meta);

        NJson::TJsonValue jClientId;
        Y_ENSURE(val.GetValue("client_id", &jClientId), meta);
        Y_ENSURE(jClientId.IsInteger(), meta);

        return {jAlias.GetString(), jClientId.GetInteger()};
    }

    TMetaInfo::TConfigPtr TMetaInfo::ParseMetaString(const TString& meta, const TString& self) {
        NJson::TJsonValue jDoc;
        Y_ENSURE(NJson::ReadJsonTree(meta, &jDoc), meta);

        NJson::TJsonValue jEnv;
        Y_ENSURE(jDoc.GetValue("bb_env", &jEnv), meta);

        NJson::TJsonValue jTenants;
        Y_ENSURE(jDoc.GetValue("tenants", &jTenants), meta);
        Y_ENSURE(jTenants.IsArray(), meta);

        for (const NJson::TJsonValue& jTen : jTenants.GetArray()) {
            NJson::TJsonValue jSelf;
            Y_ENSURE(jTen.GetValue("self", &jSelf), meta);
            auto selfPair = ParsePair(jSelf, meta);
            if (selfPair.first != self) {
                continue;
            }

            TConfigPtr config = std::make_shared<TConfig>();
            config->SelfTvmId = selfPair.second;
            config->BbEnv = BbEnvFromString(jEnv.GetString(), meta);

            NJson::TJsonValue jDsts;
            Y_ENSURE(jTen.GetValue("dsts", &jDsts), meta);
            Y_ENSURE(jDsts.IsArray(), meta);
            for (const NJson::TJsonValue& jDst : jDsts.GetArray()) {
                config->DstAliases.insert(ParsePair(jDst, meta));
            }

            return config;
        }

        return {};
    }

    void TMetaInfo::ApplySettings(const TClientSettings& settings) {
        AuthHeader_ = {{"Authorization", settings.GetAuthToken()}};
        SelfAlias_ = settings.GetSelfAlias();
    }

    EBlackboxEnv TMetaInfo::BbEnvFromString(const TString& env, const TString& meta) {
        if (env == "Prod") {
            return EBlackboxEnv::Prod;
        } else if (env == "Test") {
            return EBlackboxEnv::Test;
        } else if (env == "ProdYaTeam") {
            return EBlackboxEnv::ProdYateam;
        } else if (env == "TestYaTeam") {
            return EBlackboxEnv::TestYateam;
        } else if (env == "Stress") {
            return EBlackboxEnv::Stress;
        }

        ythrow yexception() << "'bb_env'=='" << env << "'. " << meta;
    }
}
