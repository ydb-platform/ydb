#include "tx_helpers.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

namespace NKikimr {

NKikimrProto::EReplyStatus LocalQuery(TTestActorRuntime& runtime, ui64 tabletId,
    const TString& program, NKikimrMiniKQL::TResult& result)
{
    TActorId sender = runtime.AllocateEdgeActor();

    auto* req = new TEvTablet::TEvLocalMKQL;
    auto* tx = req->Record.MutableProgram();
    tx->MutableProgram()->SetText(program);

    ForwardToTablet(runtime, tabletId, sender, req);

    auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(sender);
    UNIT_ASSERT(ev);
    auto* msg = ev->Get();

    result = msg->Record.GetExecutionEngineEvaluatedResponse();

    // emulate enum behavior from proto3
    return static_cast<NKikimrProto::EReplyStatus>(msg->Record.GetStatus());
}

NKikimrProto::EReplyStatus LocalSchemeTx(TTestActorRuntime& runtime, ui64 tabletId,
    const NTabletFlatScheme::TSchemeChanges& schemeChanges, bool dryRun,
    NTabletFlatScheme::TSchemeChanges& scheme, TString& err)
{
    TActorId sender = runtime.AllocateEdgeActor();

    auto* req = new TEvTablet::TEvLocalSchemeTx;
    *req->Record.MutableSchemeChanges() = schemeChanges;
    req->Record.SetDryRun(dryRun);

    ForwardToTablet(runtime, tabletId, sender, req);

    auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvLocalSchemeTxResponse>(sender);
    UNIT_ASSERT(ev);
    auto* msg = ev->Get();

    err = msg->Record.GetErrorReason();
    scheme.CopyFrom(msg->Record.GetFullScheme());

    // emulate enum behavior from proto3
    return static_cast<NKikimrProto::EReplyStatus>(msg->Record.GetStatus());
}

NKikimrProto::EReplyStatus LocalSchemeTx(TTestActorRuntime& runtime, ui64 tabletId,
    const TString& schemeChangesStr, bool dryRun,
    NTabletFlatScheme::TSchemeChanges& scheme, TString& err)
{
    NTabletFlatScheme::TSchemeChanges schemeChanges;
    bool parseResult = ::google::protobuf::TextFormat::ParseFromString(schemeChangesStr, &schemeChanges);
    UNIT_ASSERT_C(parseResult, "protobuf parsing failed");

    return LocalSchemeTx(runtime, tabletId, schemeChanges, dryRun, scheme, err);
}

ui64 GetExecutorCacheSize(TTestActorRuntime& runtime, ui64 tabletId) {
    NTabletFlatScheme::TSchemeChanges scheme;
    TString err;
    NKikimrProto::EReplyStatus status = LocalSchemeTx(runtime, tabletId, "", true, scheme, err);
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
    //Cdbg << scheme << "\n";
    // looking for "Delta { DeltaType: UpdateExecutorInfo ExecutorCacheSize: 33554432 }"
    for (ui32 i = 0; i < scheme.DeltaSize(); ++i) {
        const auto& d = scheme.GetDelta(i);
        if (d.GetDeltaType() == NTabletFlatScheme::TAlterRecord::UpdateExecutorInfo) {
            return d.GetExecutorCacheSize();
        }
    }
    UNIT_ASSERT_C(false, "UpdateExecutorInfo delta record not found");
    return -1;
}

} // namespace NKikimr
