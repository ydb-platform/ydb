#include "partition_writer_cache_actor_fixture.h"
#include "kqp_mock.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/services/persqueue_v1/actors/partition_writer_cache_actor.h>
#include <ydb/core/persqueue/write_id.h>

namespace NKikimr::NPersQueueTests {

void TPartitionWriterCacheActorFixture::SetUp(NUnitTest::TTestContext&)
{
    SetupContext();
    SetupKqpMock();
    SetupPQTabletMock(PQTabletId);
    SetupEventObserver();
}

void TPartitionWriterCacheActorFixture::TearDown(NUnitTest::TTestContext&)
{
    CleanupContext();
}

void TPartitionWriterCacheActorFixture::SetupContext()
{
    Ctx.ConstructInPlace();
    Finalizer.ConstructInPlace(*Ctx);

    Ctx->Prepare();
}

void TPartitionWriterCacheActorFixture::SetupKqpMock()
{
    for (ui32 node = 0; node < Ctx->Runtime->GetNodeCount(); ++node) {
        Ctx->Runtime->RegisterService(NKqp::MakeKqpProxyID(Ctx->Runtime->GetNodeId(node)),
                                      Ctx->Runtime->Register(new TKqpProxyServiceMock(), node),
                                      node);
    }
}

void TPartitionWriterCacheActorFixture::SetupPQTabletMock(ui64 tabletId)
{
    auto createPQTabletMock = [&](const NActors::TActorId& tablet, NKikimr::TTabletStorageInfo* info) -> IActor* {
        PQTablet = new TPQTabletMock(tablet, info);
        return PQTablet;
    };

    CreateTestBootstrapper(*Ctx->Runtime,
                           CreateTestTabletInfo(tabletId, NKikimrTabletBase::TTabletTypes::Dummy, TErasureType::ErasureNone),
                           createPQTabletMock);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));

    Ctx->Runtime->DispatchEvents(options);
}

void TPartitionWriterCacheActorFixture::CleanupContext()
{
    Finalizer.Clear();
    Ctx.Clear();
}

TActorId TPartitionWriterCacheActorFixture::CreatePartitionWriterCacheActor(const TCreatePartitionWriterCacheActorParams& params)
{
    using TPartitionWriterCacheActor = NKikimr::NGRpcProxy::V1::TPartitionWriterCacheActor;

    NPQ::TPartitionWriterOpts options;
    options.WithDeduplication(params.WithDeduplication);
    options.WithDatabase(params.Database);
    options.WithExpectedGeneration(params.Generation);
    options.WithSourceId(params.SourceId);

    auto actor = std::make_unique<TPartitionWriterCacheActor>(Ctx->Edge,
                                                              params.Partition,
                                                              PQTabletId,
                                                              options);
    TActorId actorId = Ctx->Runtime->Register(actor.release());

    auto event = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvInitResult>();
    UNIT_ASSERT(event != nullptr);

    return actorId;
}

auto TPartitionWriterCacheActorFixture::MakeTxId(const TString& sessionId, const TString& txId) -> TTxId
{
    return {sessionId, txId};
}

void TPartitionWriterCacheActorFixture::SetupEventObserver()
{
    auto observer = [this](TAutoPtr<IEventHandle>& ev) {
        if (auto event = ev->CastAsLocal<NPQ::TEvPartitionWriter::TEvTxWriteRequest>(); event) {
            CookieToTxId[event->Request->GetCookie()] = MakeTxId(event->SessionId, event->TxId);
        } else if (auto event = ev->CastAsLocal<NPQ::TEvPartitionWriter::TEvWriteRequest>(); event) {
            auto p = CookieToTxId.find(event->GetCookie());
            UNIT_ASSERT(p != CookieToTxId.end());

            if (!TxIdToPartitionWriter.contains(p->second)) {
                TxIdToPartitionWriter[p->second] = ev->Recipient;
                PartitionWriterToTxId[ev->Recipient] = p->second;

                ++CreatePartitionWriterCount;
            }
        } else if (auto event = ev->CastAsLocal<TEvents::TEvPoisonPill>(); event) {
            auto p = PartitionWriterToTxId.find(ev->Recipient);
            if (p != PartitionWriterToTxId.end()) {
                TxIdToPartitionWriter.erase(p->second);
                PartitionWriterToTxId.erase(p);

                ++DeletePartitionWriterCount;
            }
        } else if (auto event = ev->CastAsLocal<NKqp::TEvKqp::TEvQueryRequest>(); event) {
            if (event->GetAction() == NKikimrKqp::QUERY_ACTION_TOPIC) {
                //
                // If a request comes from TPartitionWriter, then we emulate the response from KQP.
                // TPartitionWriter only needs a couple of fields
                //
                auto response = std::make_unique<NKqp::TEvKqp::TEvQueryResponse>();
                response->Record.GetRef().SetYdbStatus(Ydb::StatusIds::SUCCESS);
                NPQ::SetWriteId(*response->Record.GetRef().MutableResponse()->MutableTopicOperations(),
                                NPQ::TWriteId(0, NextWriteId++));
                Ctx->Runtime->Send(ev->Sender, ev->Recipient, response.release(), 0, true);
                return TTestActorRuntime::EEventAction::DROP;
            }
        }

        return TTestActorRuntime::EEventAction::PROCESS;
    };

    Ctx->Runtime->SetObserverFunc(std::move(observer));
}

void TPartitionWriterCacheActorFixture::SendTxWriteRequest(const TActorId& recipient,
                                                           const TSendTxWriteRequestParams& params)
{
    auto write =
        std::make_unique<NPQ::TEvPartitionWriter::TEvTxWriteRequest>(params.SessionId, params.TxId,
                                                                     MakeHolder<NPQ::TEvPartitionWriter::TEvWriteRequest>(params.Cookie));
    auto* w = write->Request->Record.MutablePartitionRequest()->AddCmdWrite();
    Y_UNUSED(w);

    Ctx->Runtime->Send(recipient, Ctx->Edge, write.release(), 0, true);
}

void TPartitionWriterCacheActorFixture::EnsurePartitionWriterExist(const TEnsurePartitionWriterExistParams& params)
{
    auto p = TxIdToPartitionWriter.find(MakeTxId(params.SessionId, params.TxId));
    UNIT_ASSERT(p != TxIdToPartitionWriter.end());
}

void TPartitionWriterCacheActorFixture::EnsurePartitionWriterNotExist(const TEnsurePartitionWriterExistParams& params)
{
    auto p = TxIdToPartitionWriter.find(MakeTxId(params.SessionId, params.TxId));
    UNIT_ASSERT(p == TxIdToPartitionWriter.end());
}

void TPartitionWriterCacheActorFixture::EnsureWriteSessionClosed(EErrorCode errorCode)
{
    while (true) {
        auto event = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvWriteResponse>();
        UNIT_ASSERT(event != nullptr);
        if (!event->IsSuccess()) {
            UNIT_ASSERT_VALUES_EQUAL((int)event->GetError().Code, (int)errorCode);
            break;
        }
    }
}

void TPartitionWriterCacheActorFixture::WaitForPartitionWriterOps(const TWaitForPartitionWriterOpsParams& params)
{
    CreatePartitionWriterCount = 0;
    DeletePartitionWriterCount = 0;

    TDispatchOptions options;
    options.CustomFinalCondition = [this, &params]() -> bool {
        if (params.CreateCount && params.DeleteCount) {
            return
                (DeletePartitionWriterCount == *params.DeleteCount) &&
                (CreatePartitionWriterCount == *params.CreateCount);
        } else if (params.DeleteCount) {
            return DeletePartitionWriterCount == *params.DeleteCount;
        } else if (params.CreateCount) {
            return CreatePartitionWriterCount == *params.CreateCount;
        } else {
            return false;
        }
    };

    Ctx->Runtime->DispatchEvents(options);
}

void TPartitionWriterCacheActorFixture::AdvanceCurrentTime(TDuration d)
{
    Ctx->Runtime->AdvanceCurrentTime(d);
}

}
