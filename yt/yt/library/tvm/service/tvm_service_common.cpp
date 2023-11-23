#include "tvm_service.h"

#include <library/cpp/yt/memory/new.h>

#include <yt/yt/library/tvm/tvm_base.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

template <typename TId>
class TServiceTicketAuth
    : public IServiceTicketAuth
{
public:
    TServiceTicketAuth(
        ITvmServicePtr tvmService,
        TId destServiceId)
        : TvmService_(std::move(tvmService))
        , DstServiceId_(std::move(destServiceId))
    { }

    TString IssueServiceTicket() override
    {
        return TvmService_->GetServiceTicket(DstServiceId_);
    }

private:
    const ITvmServicePtr TvmService_;
    const TId DstServiceId_;
};

////////////////////////////////////////////////////////////////////////////////

IServiceTicketAuthPtr CreateServiceTicketAuth(
    ITvmServicePtr tvmService,
    TTvmId dstServiceId)
{
    YT_VERIFY(tvmService);

    return New<TServiceTicketAuth<TTvmId>>(std::move(tvmService), dstServiceId);
}

IServiceTicketAuthPtr CreateServiceTicketAuth(
    ITvmServicePtr tvmService,
    TString dstServiceAlias)
{
    YT_VERIFY(tvmService);

    return New<TServiceTicketAuth<TString>>(std::move(tvmService), std::move(dstServiceAlias));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
