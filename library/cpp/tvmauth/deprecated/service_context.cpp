#include <library/cpp/tvmauth/checked_service_ticket.h>
#include <library/cpp/tvmauth/src/service_impl.h>

namespace NTvmAuth {
    static const char* EX_MSG = "ServiceContext already moved out";

    TServiceContext::TServiceContext(TStringBuf secretBase64, TTvmId selfTvmId, TStringBuf tvmKeysResponse)
        : Impl_(MakeHolder<TImpl>(secretBase64, selfTvmId, tvmKeysResponse))
    {
    }

    TServiceContext::TServiceContext(TServiceContext&& o) = default;
    TServiceContext& TServiceContext::operator=(TServiceContext&& o) = default;
    TServiceContext::~TServiceContext() = default;

    TServiceContext TServiceContext::CheckingFactory(TTvmId selfTvmId, TStringBuf tvmKeysResponse) {
        TServiceContext c;
        c.Impl_ = MakeHolder<TImpl>(selfTvmId, tvmKeysResponse);
        return c;
    }

    TServiceContext TServiceContext::SigningFactory(TStringBuf secretBase64) {
        TServiceContext c;
        c.Impl_ = MakeHolder<TImpl>(secretBase64);
        return c;
    }

    TCheckedServiceTicket TServiceContext::Check(TStringBuf ticketBody, const TCheckFlags& flags) const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->Check(ticketBody, flags);
    }

    TString TServiceContext::SignCgiParamsForTvm(TStringBuf ts, TStringBuf dst, TStringBuf scopes) const {
        Y_ENSURE(Impl_, EX_MSG);
        return Impl_->SignCgiParamsForTvm(ts, dst, scopes);
    }
}
