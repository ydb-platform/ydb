#pragma once

#include <library/cpp/tvmauth/client/exception.h>

#include <library/cpp/tvmauth/checked_service_ticket.h>
#include <library/cpp/tvmauth/checked_user_ticket.h>
#include <library/cpp/tvmauth/deprecated/service_context.h>
#include <library/cpp/tvmauth/deprecated/user_context.h>

namespace NTvmAuth {
    class TServiceTicketChecker {
    public:
        /*!
         * Checking must be enabled in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         * @param ticket
         */
        static TCheckedServiceTicket Check(
            TStringBuf ticket,
            TServiceContextPtr c,
            const TServiceContext::TCheckFlags& flags = {}) {
            Y_ENSURE_EX(c, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableServiceTicketChecking()");
            return c->Check(ticket, flags);
        }
    };

    class TUserTicketChecker {
    public:
        /*!
         * Blackbox enviroment must be cofingured in TClientSettings
         * Can throw exception if cache is out of date or wrong config
         */
        static TCheckedUserTicket Check(TStringBuf ticket, TUserContextPtr c) {
            Y_ENSURE_EX(c, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableUserTicketChecking()");
            return c->Check(ticket);
        }
    };
}
