#pragma once 
 
#include "settings.h" 
 
#include <library/cpp/tvmauth/client/misc/utils.h> 
 
#include <library/cpp/tvmauth/client/logger.h> 
 
#include <library/cpp/http/simple/http_client.h> 
 
namespace NTvmAuth::NTvmTool { 
    class TMetaInfo { 
    public: 
        using TDstAliases = THashMap<TClientSettings::TAlias, TTvmId>; 
 
        struct TConfig { 
            TTvmId SelfTvmId = 0; 
            EBlackboxEnv BbEnv = EBlackboxEnv::Prod; 
            TDstAliases DstAliases; 
 
            bool AreTicketsRequired() const { 
                return !DstAliases.empty(); 
            } 
 
            TString ToString() const; 
 
            bool operator==(const TConfig& c) const { 
                return SelfTvmId == c.SelfTvmId && 
                       BbEnv == c.BbEnv && 
                       DstAliases == c.DstAliases; 
            } 
        }; 
        using TConfigPtr = std::shared_ptr<TConfig>; 
 
    public: 
        TMetaInfo(TLoggerPtr logger); 
 
        TConfigPtr Init(TKeepAliveHttpClient& client, 
                        const TClientSettings& settings); 
 
        static TString GetRequestForTickets(const TMetaInfo::TConfig& config); 
 
        const TKeepAliveHttpClient::THeaders& GetAuthHeader() const { 
            return AuthHeader_; 
        } 
 
        TConfigPtr GetConfig() const { 
            return Config_.Get(); 
        } 
 
        bool TryUpdateConfig(TKeepAliveHttpClient& client); 
 
    protected: 
        void TryPing(TKeepAliveHttpClient& client); 
        TString Fetch(TKeepAliveHttpClient& client) const; 
        static TConfigPtr ParseMetaString(const TString& meta, const TString& self); 
        void ApplySettings(const TClientSettings& settings); 
        static EBlackboxEnv BbEnvFromString(const TString& env, const TString& meta); 
 
    protected: 
        NUtils::TProtectedValue<TConfigPtr> Config_; 
        TKeepAliveHttpClient::THeaders AuthHeader_; 
 
        TLoggerPtr Logger_; 
        TString SelfAlias_; 
    }; 
} 
