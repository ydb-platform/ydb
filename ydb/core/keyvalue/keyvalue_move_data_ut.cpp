#include "defs.h"
#include "keyvalue.h"
#include "keyvalue_flat_impl.h"
#include "keyvalue_state.h"
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace {

void SetupLogging(TTestActorRuntime& runtime) {
    runtime.SetLogPriority(NKikimrServices::KEYVALUE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KEYVALUE_GC, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY_STATUS, NLog::PRI_DEBUG);
}

TIntrusivePtr<TTabletStorageInfo> CreateInitialTabletInfo(
    ui64 tabletId, TTabletTypes::EType tabletType,
    TBlobStorageGroupType::EErasureSpecies erasure, ui32 groupId)
{
    auto x = MakeIntrusive<TTabletStorageInfo>();

    x->TabletID = tabletId;
    x->TabletType = tabletType;
    x->Channels.resize(5);

    for (ui64 channel = 0; channel < x->Channels.size(); ++channel) {
        x->Channels[channel].Channel = channel;
        x->Channels[channel].Type = TBlobStorageGroupType(erasure);
        x->Channels[channel].History.resize(1);
        x->Channels[channel].History[0].FromGeneration = 0;
        x->Channels[channel].History[0].GroupID = groupId;
    }

    return x;
}

TIntrusivePtr<TTabletStorageInfo> CreateReassignedTabletInfo(
    ui64 tabletId, TTabletTypes::EType tabletType,
    TBlobStorageGroupType::EErasureSpecies erasure,
    ui32 groupId, ui32 groupId2, ui32 fromGeneration)
{
    auto x = MakeIntrusive<TTabletStorageInfo>();

    x->TabletID = tabletId;
    x->TabletType = tabletType;
    x->Channels.resize(5);

    for (ui64 channel = 0; channel < x->Channels.size(); ++channel) {
        x->Channels[channel].Channel = channel;
        x->Channels[channel].Type = TBlobStorageGroupType(erasure);
        x->Channels[channel].History.resize(2);
        x->Channels[channel].History[0].FromGeneration = 0;
        x->Channels[channel].History[0].GroupID = groupId;
        x->Channels[channel].History[1].FromGeneration = fromGeneration;
        x->Channels[channel].History[1].GroupID = groupId2;
    }

    return x;
}

}

Y_UNIT_TEST_SUITE(TKeyValueMoveDataTest) {

struct TTestContext {
    ui64 TabletId;
    THolder<TTestActorRuntime> Runtime;
    TActorId TabletActorId;
    TActorId Edge;
    TString Value;
    TVector<TIntrusivePtr<NFake::TProxyDS>> DsProxies;

    TTestContext() {
        TabletId = MakeTabletID(false, 1);
        Value = TString(1024, 'a');
    }

    void Prepare(std::function<void(TTestActorRuntime&)> setup) {
        Runtime.Reset(new TTestBasicRuntime);
        Runtime->SetScheduledLimit(10'000);
        Runtime->SetDispatchedEventsLimit(25'000'000);
        SetupLogging(*Runtime);

        DsProxies.clear();
        DsProxies.emplace_back(new NFake::TProxyDS(TGroupId::FromValue(0)));
        DsProxies.emplace_back(new NFake::TProxyDS(TGroupId::FromValue(2181038080)));
        DsProxies.emplace_back(new NFake::TProxyDS(TGroupId::FromValue(2181038081)));
        DsProxies.emplace_back(new NFake::TProxyDS(TGroupId::FromValue(4294967295)));

        SetupTabletServices(
            *Runtime,
            /*app*/ nullptr,
            /*mockDisk*/ true,
            /*storage*/ {},
            /*sharedCacheConfig*/ {},
            /*forceFollowers*/ false,
            DsProxies);

        setup(*Runtime);

        auto info = CreateInitialTabletInfo(TabletId, TTabletTypes::KeyValue, TErasureType::ErasureNone, 2181038080);
        auto setupInfo = MakeIntrusive<TTabletSetupInfo>(
            &CreateKeyValueFlat, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
        TabletActorId = Runtime->Register(CreateTablet({}, info.Get(), setupInfo.Get(), 0), 0);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);

        Edge = Runtime->AllocateEdgeActor();
    }

    void PoisonTablet() {
        Runtime->Send(new IEventHandle(TabletActorId, TabletActorId, new TKikimrEvents::TEvPoisonPill));
    }

    void StartReassignedTablet() {
        auto info = CreateReassignedTabletInfo(TabletId, TTabletTypes::KeyValue, TErasureType::ErasureNone, 2181038080, 2181038081, 3);
        auto setupInfo = MakeIntrusive<TTabletSetupInfo>(
            &CreateKeyValueFlat, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
        TabletActorId = Runtime->Register(CreateTablet({}, info.Get(), setupInfo.Get(), 0), 0);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);
    }

    void SendMoveData(const std::vector<ui32>& groups) {
        auto moveData = std::make_unique<TEvTablet::TEvMoveData>(groups);
        Runtime->SendToPipe(TabletId, Edge, moveData.release(), 0, GetPipeConfigWithRetries());
    }

    void SendMoveData() {
        SendMoveData({2181038080});
    }

    void WaitMoveData() {
        TAutoPtr<IEventHandle> handle;
        TEvTablet::TEvMoveDataResponse *result;
        result = Runtime->GrabEdgeEvent<TEvTablet::TEvMoveDataResponse>(handle);

        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NKikimrTabletBase::TEvMoveDataResponse::Success);
        UNIT_ASSERT_EQUAL(result->Record.GetErrorReason(), "");
    }

    void WaitMoveDataError(NKikimrTabletBase::TEvMoveDataResponse::EStatus status) {
        TAutoPtr<IEventHandle> handle;
        TEvTablet::TEvMoveDataResponse *result;
        result = Runtime->GrabEdgeEvent<TEvTablet::TEvMoveDataResponse>(handle);

        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), status);
        Cerr << "MoveData error: " << result->Record.GetErrorReason() << Endl;
    }

    void ExecuteMoveData() {
        SendMoveData();
        WaitMoveData();
    }

    void Finalize() {
        Runtime.Reset(nullptr);
    }
};

struct TFinalizer {
    TTestContext &TestContext;

    TFinalizer(TTestContext &testContext)
        : TestContext(testContext)
    {}

    ~TFinalizer() {
        TestContext.Finalize();
    }
};

void DoWithRetry(std::function<bool(void)> action, i32 retryCount = 2) {
    bool isEnd = false;
    for (i32 retriesLeft = retryCount; !isEnd && retriesLeft > 0; --retriesLeft) {
        try {
            isEnd = action();
        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT(retriesLeft != 1);
        }
    }
    UNIT_ASSERT(isEnd);
}

void CmdWrite(const TDeque<TString> &keys, const TDeque<TString> &values,
        const NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel,
        const NKikimrClient::TKeyValueRequest::EPriority priority,
        const TDeque<ui64>& creationUnixTimes,
        TTestContext &tc) {
    Y_ABORT_UNLESS(keys.size() == values.size());
    Y_ABORT_UNLESS(creationUnixTimes.empty() || (creationUnixTimes.size() == keys.size()));
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;
    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        for (ui64 idx = 0; idx < keys.size(); ++idx) {
            auto write = request->Record.AddCmdWrite();
            write->SetKey(keys[idx]);
            write->SetValue(values[idx]);
            write->SetStorageChannel(storageChannel);
            write->SetPriority(priority);
            if (idx < creationUnixTimes.size()) {
                write->SetCreationUnixTime(creationUnixTimes[idx]);
            }
        }
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.WriteResultSize(), values.size());
        for (ui64 idx = 0; idx < values.size(); ++idx) {
            const auto &writeResult = result->Record.GetWriteResult(idx);
            UNIT_ASSERT(writeResult.HasStatus());
            UNIT_ASSERT_EQUAL(writeResult.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT(writeResult.HasStatusFlags());
            if (values[idx].size()) {
                UNIT_ASSERT(writeResult.GetStatusFlags() & ui32(NKikimrBlobStorage::StatusIsValid));
            }
        }
        return true;
    });
}

void CmdWrite(const TDeque<TString> &keys, const TDeque<TString> &values,
        const NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel,
        const NKikimrClient::TKeyValueRequest::EPriority priority,
        TTestContext &tc) {
    CmdWrite(keys, values,
             storageChannel,
             priority,
             {},
             tc);
}

void CmdWrite(const TString &key, const TString &value,
        const NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel,
        const NKikimrClient::TKeyValueRequest::EPriority priority,
        const ui64 creationUnixTime,
        TTestContext &tc) {
    TDeque<TString> keys = {key};
    TDeque<TString> values = {value};
    TDeque<ui64> creationUnixTimes = {creationUnixTime};
    CmdWrite(keys, values, storageChannel, priority, creationUnixTimes, tc);
}

void CmdWrite(const TString &key, const TString &value,
        const NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel,
        const NKikimrClient::TKeyValueRequest::EPriority priority, TTestContext &tc) {
    TDeque<TString> keys = {key};
    TDeque<TString> values = {value};
    CmdWrite(keys, values, storageChannel, priority, {}, tc);
}

void CmdRead(const TDeque<TString> &keys,
             const NKikimrClient::TKeyValueRequest::EPriority priority,
             const TDeque<TString> &expectedValues, const TDeque<bool> &expectedNodatas, const TDeque<ui64> &expectedCreationUnixTimes,
             TTestContext &tc)
{
    Y_ABORT_UNLESS(keys.size() == expectedValues.size());
    Y_ABORT_UNLESS(expectedNodatas.size() == 0 || expectedNodatas.size() == keys.size());
    Y_ABORT_UNLESS(expectedCreationUnixTimes.empty() || (expectedCreationUnixTimes.size() == keys.size()));

    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        for (const auto &key: keys) {
            auto read = request->Record.AddCmdRead();
            read->SetKey(key);
            read->SetPriority(priority);
        }

        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);

        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.ReadResultSize(), keys.size());

        for (ui64 idx = 0; idx < expectedValues.size(); ++idx) {
            const auto &readResult = result->Record.GetReadResult(idx);
            UNIT_ASSERT(readResult.HasStatus());
            if (expectedNodatas.size() == 0 || !expectedNodatas[idx]) {
                UNIT_ASSERT_EQUAL(readResult.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT(readResult.HasValue());
                UNIT_ASSERT_VALUES_EQUAL(readResult.GetValue(), expectedValues[idx]);
            } else {
                UNIT_ASSERT_EQUAL(readResult.GetStatus(), NKikimrProto::NODATA);
            }
            if (idx < expectedCreationUnixTimes.size()) {
                UNIT_ASSERT_VALUES_EQUAL(readResult.GetCreationUnixTime(), expectedCreationUnixTimes[idx]);
            }
        }

        return true;
    });
}

void CmdRead(const TDeque<TString> &keys,
        const NKikimrClient::TKeyValueRequest::EPriority priority,
        const TDeque<TString> &expectedValues, const TDeque<bool> expectedNodatas, TTestContext &tc) {
    CmdRead(keys, priority, expectedValues, expectedNodatas, {}, tc);
}

void CmdRename(const TDeque<TString> &oldKeys, const TDeque<TString> &newKeys, const TDeque<ui64>& renameUnixTimes,
               TTestContext &tc, bool expectOk = true)
{
    Y_ABORT_UNLESS(oldKeys.size() == newKeys.size());
    Y_ABORT_UNLESS(renameUnixTimes.empty() || (oldKeys.size() == renameUnixTimes.size()));
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        for (ui64 idx = 0; idx < oldKeys.size(); ++idx) {
            auto cmd = request->Record.AddCmdRename();
            cmd->SetOldKey(oldKeys[idx]);
            cmd->SetNewKey(newKeys[idx]);
            if (idx < renameUnixTimes.size()) {
                cmd->SetCreationUnixTime(renameUnixTimes[idx]);
            }
        }
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        if (expectOk) {
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.RenameResultSize(), oldKeys.size());
            for (ui64 idx = 0; idx < oldKeys.size(); ++idx) {
                const auto &renameResult = result->Record.GetRenameResult(idx);
                UNIT_ASSERT(renameResult.HasStatus());
                UNIT_ASSERT_EQUAL(renameResult.GetStatus(), NKikimrProto::OK);
            }
        } else {
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR);
        }

        return true;
    });
}

void CmdRename(const TDeque<TString> &oldKeys, const TDeque<TString> &newKeys, TTestContext &tc,
        bool expectOk = true) {
    CmdRename(oldKeys, newKeys, {}, tc, expectOk);
}

void CmdRename(const TString &oldKey, const TString &newKey, const ui64 renameUnixTime,
               TTestContext &tc, bool expectOk = true) {
    TDeque<TString> oldKeys = {oldKey};
    TDeque<TString> newKeys = {newKey};
    TDeque<ui64> renameUnixTimes = {renameUnixTime};
    CmdRename(oldKeys, newKeys, renameUnixTimes, tc, expectOk);
}

void CmdRename(const TString &oldKey, const TString &newKey, TTestContext &tc, bool expectOk = true) {
    TDeque<TString> oldKeys = {oldKey};
    TDeque<TString> newKeys = {newKey};
    CmdRename(oldKeys, newKeys, {}, tc, expectOk);
}

void CmdConcat(const TDeque<TString> &inputKeys, const TString &outputKey, const bool keepInputs, TTestContext &tc) {
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        auto cmd = request->Record.AddCmdConcat();
        for (ui64 idx = 0; idx < inputKeys.size(); ++idx) {
            cmd->AddInputKeys(inputKeys[idx]);
        }
        cmd->SetOutputKey(outputKey);
        cmd->SetKeepInputs(keepInputs);
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.ConcatResultSize(), 1);
        UNIT_ASSERT(result->Record.GetConcatResult(0).HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetConcatResult(0).GetStatus(), NKikimrProto::OK);

        return true;
    });
}

void CmdDeleteRange(const TString &from, const bool includeFrom, const TString &to, const bool includeTo,
        TTestContext &tc, ui32 expectedStatus = (ui32)NMsgBusProxy::MSTATUS_OK) {
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        auto deleteRange = request->Record.AddCmdDeleteRange();
        deleteRange->MutableRange()->SetFrom(from);
        deleteRange->MutableRange()->SetIncludeFrom(includeFrom);
        deleteRange->MutableRange()->SetTo(to);
        deleteRange->MutableRange()->SetIncludeTo(includeTo);
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        if (expectedStatus == NMsgBusProxy::MSTATUS_OK) {
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.DeleteRangeResultSize(), 1);
            UNIT_ASSERT(result->Record.GetDeleteRangeResult(0).HasStatus());
            UNIT_ASSERT_EQUAL(result->Record.GetDeleteRangeResult(0).GetStatus(), NKikimrProto::OK);
        } else {
            UNIT_ASSERT_EQUAL_C(result->Record.GetStatus(), expectedStatus,
                    "Expected# " << (ui32)expectedStatus
                    << " Got# " << (ui32)result->Record.GetStatus()
                    << " ErrorReason# \"" << result->Record.GetErrorReason() << "\"");
        }

        return true;
    });
}

void CmdCopyRange(const TString &from, const bool includeFrom, const TString &to, const bool includeTo,
        const TString &prefixToAdd, const TString &prefixToRemove, TTestContext &tc) {
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        auto copyRange = request->Record.AddCmdCopyRange();
        copyRange->MutableRange()->SetFrom(from);
        copyRange->MutableRange()->SetIncludeFrom(includeFrom);
        copyRange->MutableRange()->SetTo(to);
        copyRange->MutableRange()->SetIncludeTo(includeTo);
        copyRange->SetPrefixToAdd(prefixToAdd);
        copyRange->SetPrefixToRemove(prefixToRemove);
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.CopyRangeResultSize(), 1);
        UNIT_ASSERT(result->Record.GetCopyRangeResult(0).HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetCopyRangeResult(0).GetStatus(), NKikimrProto::OK);

        return true;
    });
}

Y_UNIT_TEST(ReassignGroups) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    CmdWrite("key2", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
    CmdRead({"key2"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
}

Y_UNIT_TEST(MoveDataOneBlob) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.ExecuteMoveData();

    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
}

Y_UNIT_TEST(MoveDataOneOfTwoBlobs) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    CmdWrite("key2", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.ExecuteMoveData();

    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
    CmdRead({"key2"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
}

Y_UNIT_TEST(MoveDataReferencedByTwoKeys) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
    CmdCopyRange("key", true, "key", true, "_", "", tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.ExecuteMoveData();

    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
    CmdRead({"_key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
}

Y_UNIT_TEST(MoveDataRecordOfTwoBlobs) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    TString longValue = TString(16 << 20, 'a');
    CmdWrite("key", longValue, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.ExecuteMoveData();

    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {longValue}, {}, tc);
}

Y_UNIT_TEST(MoveDataRecordOfSameBlobs) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
    CmdConcat({"key", "key", "key", "key"}, "key2", true, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.ExecuteMoveData();

    TString concatValue = TString(4 * tc.Value.size(), 'a');
    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
    CmdRead({"key2"}, NKikimrClient::TKeyValueRequest::REALTIME, {concatValue}, {}, tc);
}

Y_UNIT_TEST(MoveDataBlobDeletedBeforeMove) {
    for (bool doUpdate : {true, false}) {
        TTestContext tc;
        TFinalizer finalizer(tc);

        TAutoPtr<IEventHandle> eventCopyBlob;
        bool stop = false;

        auto setup = [&] (TTestActorRuntime& runtime) {
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvKeyValue::TEvAdvanceMoveDataResult::EventType) {
                    auto* evAdvanceMoveDataResult = event->Get<TEvKeyValue::TEvAdvanceMoveDataResult>();
                    if (!stop && evAdvanceMoveDataResult->Result == TEvKeyValue::TEvAdvanceMoveDataResult::EResult::COPY_BLOB) {
                        eventCopyBlob = event;
                        stop = true;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
        };

        tc.Prepare(setup);

        CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

        tc.PoisonTablet();
        tc.StartReassignedTablet();

        tc.SendMoveData();

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return stop;
        };
        tc.Runtime->DispatchEvents(options);

        if (doUpdate) {
            CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
        } else {
            CmdDeleteRange("key", true, "key", true, tc);
        }

        CmdWrite("key2", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

        tc.DsProxies[1]->DeleteDoNotKeepBlobs();

        UNIT_ASSERT(eventCopyBlob);
        tc.Runtime->Send(eventCopyBlob.Release());

        tc.WaitMoveData();

        CmdRead({"key2"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
    }
}

Y_UNIT_TEST(MoveDataBlobDeletedAfterMove) {
    for (bool doUpdate : {true, false}) {
        TTestContext tc;
        TFinalizer finalizer(tc);

        TAutoPtr<IEventHandle> eventBlobCopied;
        bool stop = false;
        auto setup = [&] (TTestActorRuntime& runtime) {
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (!stop && event->GetTypeRewrite() == TEvKeyValue::TEvBlobCopied::EventType) {
                    eventBlobCopied = event;
                    stop = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
        };

        tc.Prepare(setup);

        CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

        tc.PoisonTablet();
        tc.StartReassignedTablet();

        tc.SendMoveData();

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return stop;
        };
        tc.Runtime->DispatchEvents(options);

        if (doUpdate) {
            CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
        } else {
            CmdDeleteRange("key", true, "key", true, tc);
        }

        CmdWrite("key2", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

        UNIT_ASSERT(eventBlobCopied);
        tc.Runtime->Send(eventBlobCopied.Release());

        tc.WaitMoveData();

        CmdRead({"key2"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
    }
}

Y_UNIT_TEST(MoveDataBlobMovedButThenDeleted) {
    for (bool doUpdate : {true, false}) {
        TTestContext tc;
        TFinalizer finalizer(tc);

        TAutoPtr<IEventHandle> eventCopyBlob;
        bool stop = false;
        ui32 count = 0;

        auto setup = [&] (TTestActorRuntime& runtime) {
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvKeyValue::TEvAdvanceMoveDataResult::EventType) {
                    auto* evAdvanceMoveDataResult = event->Get<TEvKeyValue::TEvAdvanceMoveDataResult>();
                    if (++count == 2 &&evAdvanceMoveDataResult->Result == TEvKeyValue::TEvAdvanceMoveDataResult::EResult::COPY_BLOB) {
                        eventCopyBlob = event;
                        stop = true;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
        };

        tc.Prepare(setup);

        CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdWrite("key2", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdCopyRange("key", true, "key", true, "x", "", tc);

        tc.PoisonTablet();
        tc.StartReassignedTablet();

        tc.SendMoveData();

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return stop;
        };
        tc.Runtime->DispatchEvents(options);

        if (doUpdate) {
            CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);
        } else {
            CmdDeleteRange("key", true, "key", true, tc);
        }

        UNIT_ASSERT(eventCopyBlob);
        tc.Runtime->Send(eventCopyBlob.Release());

        tc.WaitMoveData();

        CmdRead({"xkey"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
        CmdRead({"key2"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
    }
}

Y_UNIT_TEST(MoveDataGroupIdMismatch) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.SendMoveData({2181038081});
    tc.WaitMoveDataError(NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch);

    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
}

Y_UNIT_TEST(MoveDataSecondPass) {
    TTestContext tc;
    TFinalizer finalizer(tc);

    TAutoPtr<IEventHandle> eventBlobCopied;
    bool stop = false;
    bool caughtRepeat = false;
    auto setup = [&] (TTestActorRuntime& runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (!stop && event->GetTypeRewrite() == TEvKeyValue::TEvBlobCopied::EventType) {
                eventBlobCopied = event;
                stop = true;
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (event->GetTypeRewrite() == TEvKeyValue::TEvAdvanceMoveDataResult::EventType) {
                auto* evAdvanceMoveDataResult = event->Get<TEvKeyValue::TEvAdvanceMoveDataResult>();
                if (evAdvanceMoveDataResult->Result == TEvKeyValue::TEvAdvanceMoveDataResult::EResult::REPEAT) {
                    caughtRepeat = true;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    };

    tc.Prepare(setup);

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.SendMoveData();

    TDispatchOptions options;
    options.CustomFinalCondition = [&] {
        return stop;
    };
    tc.Runtime->DispatchEvents(options);

    CmdCopyRange("key", true, "key", true, "a", "", tc);

    UNIT_ASSERT(eventBlobCopied);
    tc.Runtime->Send(eventBlobCopied.Release());

    tc.WaitMoveData();

    UNIT_ASSERT(caughtRepeat);
}
} // TKeyValueMoveDataTest
} // NKikimr
