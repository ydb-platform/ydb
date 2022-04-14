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
        , Service_(MakeHolder<TServiceTicketChecker>(Updater_))
        , User_(MakeHolder<TUserTicketChecker>(Updater_))
    {
        if (Updater_->GetCachedServiceTickets()) {
            Tickets_ = MakeHolder<TServiceTicketGetter>(Updater_);
        }
    }

    TTvmClient::TTvmClient(const NTvmApi::TClientSettings& settings, TLoggerPtr logger)
        : Updater_(NTvmApi::TThreadedUpdater::Create(settings, std::move(logger)))
    {
        if (settings.IsServiceTicketFetchingRequired()) {
            Tickets_ = MakeHolder<TServiceTicketGetter>(Updater_);
        }
        if (settings.IsServiceTicketCheckingRequired()) {
            Service_ = MakeHolder<TServiceTicketChecker>(Updater_);
        }
        if (settings.IsUserTicketCheckingRequired()) {
            User_ = MakeHolder<TUserTicketChecker>(Updater_);
        }
        if (settings.IsRolesFetchingEnabled() && settings.ShouldCheckSrc) {
            SrcChecker_ = MakeHolder<TSrcChecker>(Updater_);
        }
        if (settings.IsRolesFetchingEnabled() && settings.ShouldCheckDefaultUid) {
            DefaultUidChecker_ = MakeHolder<TDefaultUidChecker>(Updater_);
        }
    }

    TTvmClient::TTvmClient(TAsyncUpdaterPtr updater)
        : Updater_(std::move(updater))
    {
        if (Updater_->GetCachedServiceTickets()) {
            Tickets_ = MakeHolder<TServiceTicketGetter>(Updater_);
        }
        if (Updater_->GetCachedServiceContext()) {
            Service_ = MakeHolder<TServiceTicketChecker>(Updater_);
        }
        if (Updater_->GetCachedUserContext()) {
            User_ = MakeHolder<TUserTicketChecker>(Updater_);
        }

        try {
            if (Updater_->GetRoles()) {
                SrcChecker_ = MakeHolder<TSrcChecker>(Updater_);
                DefaultUidChecker_ = MakeHolder<TDefaultUidChecker>(Updater_);
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
        Y_ENSURE_EX(Tickets_, TBrokenTvmClientSettings()
                                  << "Need to enable ServiceTickets fetching");
        return Tickets_->GetTicket(dst);
    }

    TString TTvmClient::GetServiceTicketFor(const TTvmId dst) const {
        Y_ENSURE_EX(Tickets_, TBrokenTvmClientSettings()
                                  << "Need to enable ServiceTickets fetching");
        return Tickets_->GetTicket(dst);
    }

    TCheckedServiceTicket TTvmClient::CheckServiceTicket(TStringBuf ticket) const {
        Y_ENSURE_EX(Service_, TBrokenTvmClientSettings()
                                  << "Need to use TClientSettings::EnableServiceTicketChecking()");

        TCheckedServiceTicket res = Service_->Check(ticket);
        if (SrcChecker_ && res) {
            return SrcChecker_->Check(std::move(res));
        }
        return res;
    }

    TCheckedUserTicket TTvmClient::CheckUserTicket(TStringBuf ticket, TMaybe<EBlackboxEnv> overrideEnv) const {
        Y_ENSURE_EX(User_, TBrokenTvmClientSettings()
                               << "Need to use TClientSettings::EnableUserTicketChecking()");

        TCheckedUserTicket res = User_->Check(ticket, overrideEnv);
        if (DefaultUidChecker_ && res) {
            return DefaultUidChecker_->Check(std::move(res));
        }
        return User_->Check(ticket, overrideEnv);
    }

    NRoles::TRolesPtr TTvmClient::GetRoles() const {
        Y_ENSURE(Updater_);
        return Updater_->GetRoles();
    }
}
