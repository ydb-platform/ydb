#include "service_impl.h"

#include <library/cpp/tvmauth/checked_service_ticket.h>

namespace NTvmAuth {
    static const char* EX_MSG = "Ticket already moved out";

    TCheckedServiceTicket::TCheckedServiceTicket(THolder<TImpl> impl)
        : Impl_(std::move(impl))
    {
    }

    TCheckedServiceTicket::TCheckedServiceTicket(TCheckedServiceTicket&& o) = default;
    TCheckedServiceTicket& TCheckedServiceTicket::operator=(TCheckedServiceTicket&& o) = default;
    TCheckedServiceTicket::~TCheckedServiceTicket() = default;

    TCheckedServiceTicket::operator bool() const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->operator bool();
    }

    TTvmId TCheckedServiceTicket::GetDst() const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->GetDst();
    }

    TTvmId TCheckedServiceTicket::GetSrc() const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->GetSrc();
    }

    ETicketStatus TCheckedServiceTicket::GetStatus() const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->GetStatus();
    }

    TString TCheckedServiceTicket::DebugInfo() const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->DebugInfo();
    }

    TMaybe<TUid> TCheckedServiceTicket::GetIssuerUid() const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->GetIssuerUid();
    }
}
