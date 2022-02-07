#pragma once

#include "async_updater.h"

#include <library/cpp/tvmauth/client/exception.h>

#include <library/cpp/tvmauth/checked_user_ticket.h>
#include <library/cpp/tvmauth/src/user_impl.h>

namespace NTvmAuth {
    class TDefaultUidChecker {
    public:
        TDefaultUidChecker(TAsyncUpdaterPtr updater)
            : Updater_(std::move(updater))
        {
            Y_ENSURE(Updater_);
            GetCache();
        }

        /*!
         * Checking must be enabled in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         * @param ticket
         */
        TCheckedUserTicket Check(TCheckedUserTicket ticket) const {
            NRoles::TConsumerRolesPtr roles = GetCache()->GetRolesForUser(ticket);
            if (roles) {
                return ticket;
            }

            TUserTicketImplPtr impl = THolder(NInternal::TCanningKnife::GetU(ticket));
            impl->SetStatus(ETicketStatus::NoRoles);
            return TCheckedUserTicket(std::move(impl));
        }

    private:
        NRoles::TRolesPtr GetCache() const {
            NRoles::TRolesPtr c = Updater_->GetRoles();
            Y_ENSURE_EX(c, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableRolesFetching()");
            return c;
        }

    private:
        TAsyncUpdaterPtr Updater_;
    };
}
