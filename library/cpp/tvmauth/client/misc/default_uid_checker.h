#pragma once

#include "roles/roles.h"

#include <library/cpp/tvmauth/client/exception.h>

#include <library/cpp/tvmauth/checked_user_ticket.h>
#include <library/cpp/tvmauth/src/user_impl.h>
#include <library/cpp/tvmauth/src/utils.h>

namespace NTvmAuth {
    class TDefaultUidChecker {
    public:
        /*!
         * Checking must be enabled in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         * @param ticket
         */
        static TCheckedUserTicket Check(TCheckedUserTicket ticket, NRoles::TRolesPtr r) {
            Y_ENSURE_EX(r, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableRolesFetching()");
            NRoles::TConsumerRolesPtr roles = r->GetRolesForUser(ticket);
            if (roles) {
                return ticket;
            }

            TUserTicketImplPtr impl = THolder(NInternal::TCanningKnife::GetU(ticket));
            impl->SetStatus(ETicketStatus::NoRoles);
            return TCheckedUserTicket(std::move(impl));
        }
    };
}
