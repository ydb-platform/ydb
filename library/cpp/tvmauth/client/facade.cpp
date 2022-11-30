#include "facade.h"

#include "misc/checker.h"
#include "misc/default_uid_checker.h"
#include "misc/getter.h"
#include "misc/src_checker.h"
#include "misc/api/threaded_updater.h"
#include "misc/tool/threaded_updater.h"

namespace NTvmAuth {
    TTvmClient::TTvmClient(const NTvmTool::TClientSettings& settings, TLoggerPtr logger)
        : Updater_(NTvmTool::TThreadedUpdater::Create(settings, std::move(logger)))
        , NeedService_(true)
        , NeedUser_(true)
    {
        Y_ENSURE_EX(Updater_->GetCachedServiceContext(), TInternalException() << "Unable to get cached service context");
        Y_ENSURE_EX(Updater_->GetCachedUserContext(), TInternalException() << "Unable to get cached user context");

        if (Updater_->GetCachedServiceTickets()) {
            NeedTickets_ = true;
        }

        try {
            if (settings.ShouldCheckSrc && Updater_->GetRoles()) {
                NeedSrcChecker_ = true;
            }

            if (settings.ShouldCheckDefaultUid && Updater_->GetRoles()) {
                NeedDefaultUidChecker_ = true;
            }
        } catch (const TBrokenTvmClientSettings&) {
            // Roles are not configured
        }
    }

    TTvmClient::TTvmClient(const NTvmApi::TClientSettings& settings, TLoggerPtr logger)
        : Updater_(NTvmApi::TThreadedUpdater::Create(settings, std::move(logger)))
        , NeedService_(settings.CheckServiceTickets)
        , NeedUser_(settings.CheckUserTicketsWithBbEnv)
        , NeedTickets_(settings.IsServiceTicketFetchingRequired())
        , NeedSrcChecker_(settings.FetchRolesForIdmSystemSlug && settings.ShouldCheckSrc)
        , NeedDefaultUidChecker_(settings.FetchRolesForIdmSystemSlug && settings.ShouldCheckDefaultUid)
    {
        ServiceTicketCheckFlags_.NeedDstCheck = settings.ShouldCheckDst;
        if (NeedService_) {
            Y_ENSURE_EX(Updater_->GetCachedServiceContext(), TInternalException() << "Unable to get cached service context");
        }
        if (NeedUser_) {
            Y_ENSURE_EX(Updater_->GetCachedUserContext(), TInternalException() << "Unable to get cached user context");
        }
        if (NeedTickets_) {
            Y_ENSURE_EX(Updater_->GetCachedServiceTickets(), TInternalException() << "Unable to get cached service tickets");
        }
        if (NeedSrcChecker_) {
            GetRoles();
        }
        if (NeedDefaultUidChecker_) {
            GetRoles();
        }
    }

    TTvmClient::TTvmClient(
        TAsyncUpdaterPtr updater,
        const TServiceContext::TCheckFlags& serviceTicketCheckFlags)
        : Updater_(std::move(updater))
        , ServiceTicketCheckFlags_(serviceTicketCheckFlags)
        , NeedService_(Updater_->GetCachedServiceContext())
        , NeedUser_(Updater_->GetCachedUserContext())
        , NeedTickets_(Updater_->GetCachedServiceTickets())
    {
        try {
            if (Updater_->GetRoles()) {
                NeedSrcChecker_ = true;
                NeedDefaultUidChecker_ = true;
            }
        } catch (const TIllegalUsage&) {
            // it is a test probably
        }
    }

    TTvmClient::TTvmClient(TTvmClient&& o) = default;
    TTvmClient::~TTvmClient() = default;
    TTvmClient& TTvmClient::operator=(TTvmClient&& o) = default;

    TClientStatus TTvmClient::GetStatus() const {
        Y_ENSURE(Updater_);
        return Updater_->GetStatus();
    }

    TInstant TTvmClient::GetUpdateTimeOfPublicKeys() const {
        Y_ENSURE(Updater_);
        return Updater_->GetUpdateTimeOfPublicKeys();
    }

    TInstant TTvmClient::GetUpdateTimeOfServiceTickets() const {
        Y_ENSURE(Updater_);
        return Updater_->GetUpdateTimeOfServiceTickets();
    }

    TInstant TTvmClient::GetInvalidationTimeOfPublicKeys() const {
        Y_ENSURE(Updater_);
        return Updater_->GetInvalidationTimeOfPublicKeys();
    }

    TInstant TTvmClient::GetInvalidationTimeOfServiceTickets() const {
        Y_ENSURE(Updater_);
        return Updater_->GetInvalidationTimeOfServiceTickets();
    }

    TString TTvmClient::GetServiceTicketFor(const TClientSettings::TAlias& dst) const {
        Y_ENSURE_EX(NeedTickets_, TBrokenTvmClientSettings()
                                      << "Need to enable ServiceTickets fetching");

        TServiceTicketsPtr c = Updater_->GetCachedServiceTickets();
        return TServiceTicketGetter::GetTicket(dst, c);
    }

    TString TTvmClient::GetServiceTicketFor(const TTvmId dst) const {
        Y_ENSURE_EX(NeedTickets_, TBrokenTvmClientSettings()
                                      << "Need to enable ServiceTickets fetching");
        TServiceTicketsPtr c = Updater_->GetCachedServiceTickets();
        return TServiceTicketGetter::GetTicket(dst, c);
    }

    TCheckedServiceTicket TTvmClient::CheckServiceTicket(TStringBuf ticket) const {
        Y_ENSURE_EX(NeedService_, TBrokenTvmClientSettings()
                                      << "Need to use TClientSettings::EnableServiceTicketChecking()");

        TServiceContextPtr c = Updater_->GetCachedServiceContext();
        TCheckedServiceTicket res = TServiceTicketChecker::Check(ticket, c, ServiceTicketCheckFlags_);
        if (NeedSrcChecker_ && res) {
            NRoles::TRolesPtr roles = Updater_->GetRoles();
            return TSrcChecker::Check(std::move(res), roles);
        }
        return res;
    }

    TCheckedUserTicket TTvmClient::CheckUserTicket(TStringBuf ticket, TMaybe<EBlackboxEnv> overridenEnv) const {
        Y_ENSURE_EX(NeedUser_, TBrokenTvmClientSettings()
                                   << "Need to use TClientSettings::EnableUserTicketChecking()");

        auto c = Updater_->GetCachedUserContext(overridenEnv);
        TCheckedUserTicket res = TUserTicketChecker::Check(ticket, c);
        if (NeedDefaultUidChecker_ && res) {
            NRoles::TRolesPtr roles = Updater_->GetRoles();
            return TDefaultUidChecker::Check(std::move(res), roles);
        }
        return res;
    }

    NRoles::TRolesPtr TTvmClient::GetRoles() const {
        Y_ENSURE(Updater_);
        return Updater_->GetRoles();
    }
}
