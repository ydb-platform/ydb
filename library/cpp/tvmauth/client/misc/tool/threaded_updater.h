#pragma once

#include "meta_info.h"
#include "roles_fetcher.h"

#include <library/cpp/tvmauth/client/misc/async_updater.h>
#include <library/cpp/tvmauth/client/misc/threaded_updater.h>

#include <atomic>

namespace NTvmAuth::NTvmTool {
    class TThreadedUpdater: public TThreadedUpdaterBase {
    public:
        static TAsyncUpdaterPtr Create(const TClientSettings& settings, TLoggerPtr logger);
        ~TThreadedUpdater();

        TClientStatus GetStatus() const override;
        NRoles::TRolesPtr GetRoles() const override;

    protected: // for tests
        TClientStatus::ECode GetState() const;

        TThreadedUpdater(const TString& host, ui16 port, TDuration socketTimeout, TDuration connectTimeout, TLoggerPtr logger);

        void Init(const TClientSettings& settings);
        void UpdateState();

        TInstant UpdateServiceTickets(const TMetaInfo::TConfig& config);
        std::pair<TString, TInstant> FetchServiceTickets(const TMetaInfo::TConfig& config) const;
        TPairTicketsErrors ParseFetchTicketsResponse(const TString& resp,
                                                     const TMetaInfo::TDstAliases& dsts) const;

        TInstant UpdateKeys(const TMetaInfo::TConfig& config);
        std::pair<TString, TInstant> FetchPublicKeys() const;

        static TInstant GetBirthTimeFromResponse(const THttpHeaders& headers, TStringBuf errMsg);

        bool IsTimeToUpdateServiceTickets(const TMetaInfo::TConfig& config, TInstant lastUpdate) const;
        bool IsTimeToUpdatePublicKeys(TInstant lastUpdate) const;

        bool IsEverythingOk(const TMetaInfo::TConfig& config) const;
        bool AreServiceTicketsOk(const TMetaInfo::TConfig& config) const;
        bool AreServiceTicketsOk(size_t requiredCount) const;
        bool ArePublicKeysOk() const;
        bool IsConfigWarnTime() const;

    private:
        void Worker() override;

    protected:
        TMetaInfo MetaInfo_;
        TInstant LastVisitForServiceTickets_;
        TInstant LastVisitForPublicKeys_;
        TInstant LastVisitForConfig_;
        TDuration ConfigWarnDelay_;
        std::unique_ptr<TRolesFetcher> RolesFetcher_;
    };
}
