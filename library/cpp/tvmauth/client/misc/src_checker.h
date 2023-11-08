#pragma once

#include "roles/roles.h"

#include <library/cpp/tvmauth/client/exception.h>

#include <library/cpp/tvmauth/checked_service_ticket.h>
#include <library/cpp/tvmauth/src/service_impl.h>
#include <library/cpp/tvmauth/src/utils.h>

namespace NTvmAuth {
    class TSrcChecker {
    public:
        /*!
         * Checking must be enabled in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         * @param ticket
         */
        static TCheckedServiceTicket Check(TCheckedServiceTicket ticket, NRoles::TRolesPtr r) {
            Y_ENSURE_EX(r, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableRolesFetching()");
            NRoles::TConsumerRolesPtr roles = r->GetRolesForService(ticket);
            if (roles) {
                return ticket;
            }

            TServiceTicketImplPtr impl = THolder(NInternal::TCanningKnife::GetS(ticket));
            impl->SetStatus(ETicketStatus::NoRoles);
            return TCheckedServiceTicket(std::move(impl));
        }
    };
}
