#pragma once 
 
#include "retry_settings.h" 
 
#include <library/cpp/tvmauth/client/misc/fetch_result.h> 
#include <library/cpp/tvmauth/client/misc/proc_info.h> 
#include <library/cpp/tvmauth/client/misc/utils.h> 
#include <library/cpp/tvmauth/client/misc/roles/roles.h> 
 
#include <library/cpp/tvmauth/client/logger.h> 
 
#include <library/cpp/http/simple/http_client.h> 
 
namespace NTvmAuth::NTvmApi { 
    struct TRolesFetcherSettings { 
        TString TiroleHost; 
        ui16 TirolePort = 0; 
        TString CacheDir; 
        NUtils::TProcInfo ProcInfo; 
        TTvmId SelfTvmId = 0; 
        TString IdmSystemSlug; 
        TDuration Timeout = TDuration::Seconds(30); 
    }; 
 
    class TRolesFetcher { 
    public: 
        TRolesFetcher(const TRolesFetcherSettings& settings, TLoggerPtr logger); 
 
        TInstant ReadFromDisk(); 
 
        bool AreRolesOk() const; 
        static bool IsTimeToUpdate(const TRetrySettings& settings, TDuration sinceUpdate); 
        static bool ShouldWarn(const TRetrySettings& settings, TDuration sinceUpdate); 
 
        NUtils::TFetchResult FetchActualRoles(const TString& serviceTicket); 
        void Update(NUtils::TFetchResult&& fetchResult, TInstant now = TInstant::Now()); 
 
        NTvmAuth::NRoles::TRolesPtr GetCurrentRoles() const; 
 
        void ResetConnection(); 
 
    public: 
        static std::pair<TString, TString> ParseDiskFormat(TStringBuf filebody); 
        static TString PrepareDiskFormat(TStringBuf roles, TStringBuf slug); 
 
        struct TRequest { 
            TString Url; 
            TKeepAliveHttpClient::THeaders Headers; 
        }; 
        TRequest CreateTiroleRequest(const TString& serviceTicket) const; 
 
    private: 
        const TRolesFetcherSettings Settings_; 
        const TLoggerPtr Logger_; 
        const TString CacheFilePath_; 
        const TString XYaServiceTicket_ = "X-Ya-Service-Ticket"; 
        const TString IfNoneMatch_ = "If-None-Match"; 
 
        NUtils::TProtectedValue<NTvmAuth::NRoles::TRolesPtr> CurrentRoles_; 
 
        std::unique_ptr<TKeepAliveHttpClient> Client_; 
    }; 
} 
