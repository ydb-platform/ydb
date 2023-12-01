#include "kqp_mock.h"

namespace NKikimr::NPersQueueTests {

void TKqpProxyServiceMock::Bootstrap()
{
    Become(&TKqpProxyServiceMock::StateWork);
}

STFUNC(TKqpProxyServiceMock::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(NKqp::TEvKqp::TEvQueryRequest, Handle);
    }
}

void TKqpProxyServiceMock::Handle(NKqp::TEvKqp::TEvQueryRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& event = *ev->Get();

    Y_ABORT_UNLESS(event.HasAction());
    Y_ABORT_UNLESS(event.GetAction() == NKikimrKqp::QUERY_ACTION_TOPIC);

    auto queryResponse = std::make_unique<NKqp::TEvKqp::TEvQueryResponse>();
    auto* response = queryResponse->Record.GetRef().MutableResponse();

    response->MutableTopicOperations()->SetWriteId(NextWriteId++);

    ctx.Send(ev->Sender, std::move(queryResponse));
}

}
