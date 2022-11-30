#pragma once

#include "retry_settings.h"
#include "roles_fetcher.h"
#include "settings.h"

#include <library/cpp/tvmauth/client/misc/async_updater.h>
#include <library/cpp/tvmauth/client/misc/threaded_updater.h>

#include <util/generic/set.h>
#include <util/random/fast.h>

#include <mutex>

namespace NTvmAuth::NTvmApi {
    using TDstSet = TSet<TClientSettings::TDst>;
    using TDstSetPtr = std::shared_ptr<const TDstSet>;

    class TThreadedUpdater: public TThreadedUpdaterBase {
    public:
        /*!
         * Starts thread for updating of in-memory cache in background
         * Reads cache from disk if specified
         * @param settings
         * @param logger is usefull for monitoring and debuging
         */
        static TAsyncUpdaterPtr Create(const TClientSettings& settings, TLoggerPtr logger);
        ~TThreadedUpdater();

        TClientStatus GetStatus() const override;
        NRoles::TRolesPtr GetRoles() const override;

    protected: // for tests
        TClientStatus::ECode GetState() const;

        TThreadedUpdater(const TClientSettings& settings, TLoggerPtr logger);
        void Init();

        void UpdateServiceTickets();
        void UpdateAllServiceTickets();
        TServiceTicketsPtr UpdateMissingServiceTickets(const TDstSet& required);
        void UpdatePublicKeys();
        void UpdateRoles();

        TServiceTicketsPtr UpdateServiceTicketsCachePartly(TPairTicketsErrors&& tickets, size_t got);
        void UpdateServiceTicketsCache(TPairTicketsErrors&& tickets, TInstant time);
        void UpdatePublicKeysCache(const TString& publicKeys, TInstant time);

        void ReadStateFromDisk();

        struct TServiceTicketsFromDisk {
            TPairTicketsErrors TicketsWithErrors;
            TInstant BornDate;
            TString FileBody;
        };

        std::pair<TServiceTicketsFromDisk, TServiceTicketsFromDisk> ReadServiceTicketsFromDisk() const;

        std::pair<TString, TInstant> ReadPublicKeysFromDisk() const;
        TString ReadRetrySettingsFromDisk() const;

        struct THttpResult {
            TPairTicketsErrors TicketsWithErrors;
            TSmallVec<TString> Responses;
        };

        template <class Dsts>
        THttpResult GetServiceTicketsFromHttp(const Dsts& dsts, const size_t dstLimit) const;
        TString GetPublicKeysFromHttp() const;
        TServiceTickets::TMapIdStr GetRequestedTicketsFromStartUpCache(const TDstSet& dsts) const;

        virtual NUtils::TFetchResult FetchServiceTicketsFromHttp(const TString& body) const;
        virtual NUtils::TFetchResult FetchPublicKeysFromHttp() const;

        bool UpdateRetrySettings(const TString& header) const;

        template <typename Func>
        NUtils::TFetchResult FetchWithRetries(Func func, EScope scope) const;
        void RandomSleep() const;

        static TString PrepareRequestForServiceTickets(TTvmId src,
                                                       const TServiceContext& ctx,
                                                       const TClientSettings::TDstVector& dsts,
                                                       const NUtils::TProcInfo& procInfo,
                                                       time_t now = time(nullptr));
        template <class Dsts>
        void ParseTicketsFromResponse(TStringBuf resp,
                                      const Dsts& dsts,
                                      TPairTicketsErrors& out) const;

        void ParseTicketsFromDiskCache(TStringBuf cache,
                                       TPairTicketsErrors& out) const;

        static TString PrepareTicketsForDisk(TStringBuf tvmResponse, TTvmId selfId);
        static std::pair<TStringBuf, TTvmId> ParseTicketsFromDisk(TStringBuf data);

        TDstSetPtr GetDsts() const;
        void SetDsts(TDstSetPtr dsts);

        TInstant GetStartUpCacheBornDate() const;

        bool IsTimeToUpdateServiceTickets(TInstant lastUpdate) const;
        bool IsTimeToUpdatePublicKeys(TInstant lastUpdate) const;

        bool AreServicesTicketsOk() const;
        bool IsServiceContextOk() const;
        bool IsUserContextOk() const;

        void Worker() override;

        static TServiceTickets::TMapAliasId MakeAliasMap(const TClientSettings& settings);
        static TClientSettings::TDstVector FindMissingDsts(TServiceTicketsPtr available, const TDstSet& required);
        static TClientSettings::TDstVector FindMissingDsts(const TDstSet& available, const TDstSet& required);

        static TString CreateJsonArray(const TSmallVec<TString>& responses);
        static TString AppendToJsonArray(const TString& json, const TSmallVec<TString>& responses);

    private:
        TRetrySettings RetrySettings_;

    protected:
        mutable TExponentialBackoff ExpBackoff_;
        std::unique_ptr<std::mutex> ServiceTicketBatchUpdateMutex_;

    private:
        const TClientSettings Settings_;

        const NUtils::TProcInfo ProcInfo_;

        const TString PublicKeysUrl_;

        const TServiceTickets::TMapAliasId DstAliases_;

        const TKeepAliveHttpClient::THeaders Headers_;
        TMaybe<TServiceContext> SigningContext_;

        NUtils::TProtectedValue<TDstSetPtr> Destinations_;

        TString DiskCacheServiceTickets_;
        TServiceTicketsFromDisk StartUpCache_;
        bool NeedFetchMissingServiceTickets_ = true;

        TString PublicKeysFilepath_;
        TString ServiceTicketsFilepath_;
        TString RetrySettingsFilepath_;

        std::unique_ptr<TRolesFetcher> RolesFetcher_;

        mutable TReallyFastRng32 Random_;

        bool Inited_ = false;
    };
}
