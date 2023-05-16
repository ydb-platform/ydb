#include "tx_helpers.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr {

NKikimrProto::EReplyStatus LocalSchemeTx(TTestActorRuntime& runtime, ui64 tabletId, const TString& schemeChangesStr, bool dryRun,
                    NTabletFlatScheme::TSchemeChanges& scheme, TString& err) {
    TActorId sender = runtime.AllocateEdgeActor();

    auto evTx = new TEvTablet::TEvLocalSchemeTx;
    evTx->Record.SetDryRun(dryRun);
    auto schemeChanges = evTx->Record.MutableSchemeChanges();
    bool parseResult = ::google::protobuf::TextFormat::ParseFromString(schemeChangesStr, schemeChanges);
    UNIT_ASSERT_C(parseResult, "protobuf parsing failed");

    ForwardToTablet(runtime, tabletId, sender, evTx);

    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvTablet::TEvLocalSchemeTxResponse>(handle);
    UNIT_ASSERT(event);

    err = event->Record.GetErrorReason();
    scheme.CopyFrom(event->Record.GetFullScheme());

    // emulate enum behavior from proto3
    return static_cast<NKikimrProto::EReplyStatus>(event->Record.GetStatus());
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
