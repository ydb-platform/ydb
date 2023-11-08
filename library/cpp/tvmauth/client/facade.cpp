#include "facade.h"

#include "misc/api/threaded_updater.h"
#include "misc/tool/threaded_updater.h"

namespace NTvmAuth {
    TTvmClient::TTvmClient(const NTvmTool::TClientSettings& settings, TLoggerPtr logger)
        : Updater_(NTvmTool::TThreadedUpdater::Create(settings, std::move(logger)))
    {
        ServiceTicketCheckFlags_.NeedDstCheck = settings.ShouldCheckDst;
    }

    TTvmClient::TTvmClient(const NTvmApi::TClientSettings& settings, TLoggerPtr logger)
        : Updater_(NTvmApi::TThreadedUpdater::Create(settings, std::move(logger)))
    {
        ServiceTicketCheckFlags_.NeedDstCheck = settings.ShouldCheckDst;
    }

    TTvmClient::TTvmClient(
        TAsyncUpdaterPtr updater,
        const TServiceContext::TCheckFlags& serviceTicketCheckFlags)
        : Updater_(std::move(updater))
        , ServiceTicketCheckFlags_(serviceTicketCheckFlags)
    {
        try {
            if (Updater_->GetRoles()) {
            }
        } catch (const TIllegalUsage&) {
            // it is a test probably
        } catch (const TNotInitializedException&) {
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
        Y_ENSURE(Updater_);
        return Updater_->GetServiceTicketFor(dst);
    }

    TString TTvmClient::GetServiceTicketFor(const TTvmId dst) const {
        Y_ENSURE(Updater_);
        return Updater_->GetServiceTicketFor(dst);
    }

    TCheckedServiceTicket TTvmClient::CheckServiceTicket(TStringBuf ticket) const {
        Y_ENSURE(Updater_);
        return Updater_->CheckServiceTicket(ticket, ServiceTicketCheckFlags_);
    }

    TCheckedUserTicket TTvmClient::CheckUserTicket(TStringBuf ticket, TMaybe<EBlackboxEnv> overridenEnv) const {
        Y_ENSURE(Updater_);
        return Updater_->CheckUserTicket(ticket, overridenEnv);
    }

    NRoles::TRolesPtr TTvmClient::GetRoles() const {
        Y_ENSURE(Updater_);
        return Updater_->GetRoles();
    }
}
