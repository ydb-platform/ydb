#include "fake_quota_manager.h"

namespace NFq {

void TQuotaServiceFakeActor::Handle(TEvQuotaService::TQuotaGetRequest::TPtr& ev) {
    auto response = MakeHolder<TEvQuotaService::TQuotaGetResponse>();
    response->SubjectId = ev->Get()->SubjectId;
    response->SubjectType = ev->Get()->SubjectType;
    response->Quotas = Quotas;
    Send(ev->Sender, std::move(response));
}

} // namespace NFq
