#include "topic_helpers.h"

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

namespace NSchemeShardUT_Private {

static constexpr ui64 SS = 72057594046644480l;

NKikimrSchemeOp::TModifyScheme CreateTransaction(const TString& parentPath, ::NKikimrSchemeOp::TPersQueueGroupDescription& scheme) {
    NKikimrSchemeOp::TModifyScheme tx;
    tx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    tx.SetWorkingDir(parentPath);
    tx.MutableAlterPersQueueGroup()->CopyFrom(scheme);
    return tx;
}

TEvTx* CreateRequest(ui64 txId, NKikimrSchemeOp::TModifyScheme&& tx) {
    auto ev = new TEvTx(txId, SS);
    *ev->Record.AddTransaction() = std::move(tx);
    return ev;
}

ui64 DoRequest(NActors::TTestActorRuntime& runtime, ui64& txId, NKikimrSchemeOp::TPersQueueGroupDescription& scheme) {
    Sleep(TDuration::Seconds(1));

    Cerr << "ALTER_SCHEME: " << scheme << Endl << Flush;

    const auto sender = runtime.AllocateEdgeActor();
    const auto request = CreateRequest(txId, CreateTransaction("/Root", scheme));
    runtime.Send(new IEventHandle(
            MakeTabletResolverID(),
            sender,
            new TEvTabletResolver::TEvForward(
                    SS,
                    new IEventHandle(TActorId(), sender, request),
                    { },
                    TEvTabletResolver::TEvForward::EActor::Tablet
            )),
            0);

    auto subscriber = CreateNotificationSubscriber(runtime, SS);
    runtime.Send(new IEventHandle(subscriber, sender, new TEvSchemeShard::TEvNotifyTxCompletion(txId)));
    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
    UNIT_ASSERT(event);
    UNIT_ASSERT_EQUAL(event->Record.GetTxId(), txId);

    auto e = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
    UNIT_ASSERT_EQUAL_C(e->Record.GetStatus(), TEvSchemeShard::EStatus::StatusAccepted,
        "Unexpected status " << NKikimrScheme::EStatus_Name(e->Record.GetStatus()) << " " << e->Record.GetReason());

    Sleep(TDuration::Seconds(1));

    return e->Record.GetTxId();
}

ui64 SplitPartition(NActors::TTestActorRuntime& runtime, ui64& txId, const TString& topic, const ui32 partition, TString boundary) {
    ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName(topic);
    auto* split = scheme.AddSplit();
    split->SetPartition(partition);
    split->SetSplitBoundary(boundary);

    return DoRequest(runtime, txId, scheme);
}

ui64 MergePartition(NActors::TTestActorRuntime& runtime, ui64& txId, const TString& topic, const ui32 partitionLeft, const ui32 partitionRight) {
    ::NKikimrSchemeOp::TPersQueueGroupDescription scheme;
    scheme.SetName(topic);
    auto* merge = scheme.AddMerge();
    merge->SetPartition(partitionLeft);
    merge->SetAdjacentPartition(partitionRight);

    return DoRequest(runtime, txId, scheme);
}


}
