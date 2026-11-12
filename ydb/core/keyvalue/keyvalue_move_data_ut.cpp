#include "defs.h"
#include "keyvalue.h"
#include "keyvalue_flat_impl.h"
#include "keyvalue_state.h"
#include "ut/keyvalue_move_data_test_enums.h"
#include <util/generic/serialized_enum.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>

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

    void StartReassignedTablet(bool repeatCurrentGroup = false) {
        auto info = CreateReassignedTabletInfo(TabletId, TTabletTypes::KeyValue, TErasureType::ErasureNone, 2181038080, 2181038081, 3);
        if (repeatCurrentGroup) {
            for (auto& channel : info->Channels) {
                auto first = channel.History.back();
                first.FromGeneration = 0;
                channel.History.front().FromGeneration = 1;
                channel.History.insert(channel.History.begin(), first);
            }
        }
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

Y_UNIT_TEST(MoveDataNeedsAnotherPassDuringSecondPass) {
    TTestContext tc;
    TFinalizer finalizer(tc);

    TAutoPtr<IEventHandle> eventBlobCopied;
    ui32 repeatCount = 0;
    ui32 capturedCopies = 0;
    tc.Prepare([&](TTestActorRuntime& runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvKeyValue::TEvAdvanceMoveDataResult::EventType) {
                if (event->Get<TEvKeyValue::TEvAdvanceMoveDataResult>()->Result
                        == TEvKeyValue::TEvAdvanceMoveDataResult::EResult::REPEAT) {
                    ++repeatCount;
                }
            }
            if (event->GetTypeRewrite() == TEvKeyValue::TEvBlobCopied::EventType
                    && capturedCopies < 2 && capturedCopies == repeatCount) {
                UNIT_ASSERT(!eventBlobCopied);
                eventBlobCopied = event;
                ++capturedCopies;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    });

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN,
        NKikimrClient::TKeyValueRequest::REALTIME, tc);
    tc.PoisonTablet();
    tc.StartReassignedTablet();
    tc.SendMoveData();

    for (ui32 pass = 0; pass < 2; ++pass) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return bool(eventBlobCopied); };
        tc.Runtime->DispatchEvents(options);
        UNIT_ASSERT(eventBlobCopied);
        UNIT_ASSERT_VALUES_EQUAL(repeatCount, pass);
        UNIT_ASSERT_VALUES_EQUAL(capturedCopies, pass + 1);

        // The current key still references the old blob until BlobCopied is
        // processed. Put another reference before the cursor in each pass:
        // key -> akey in the first pass, akey -> aakey in the second pass.
        // The second mutation must set NeedsAnotherPass again after its reset.
        const TString key = pass == 0 ? "key" : "akey";
        CmdCopyRange(key, true, key, true, "a", "", tc);
        tc.Runtime->Send(eventBlobCopied.Release());
    }

    tc.WaitMoveData();
    UNIT_ASSERT_VALUES_EQUAL(repeatCount, 2);

    tc.DsProxies[1]->DeleteDoNotKeepBlobs();
    for (const auto& [id, blob] : tc.DsProxies[1]->AllMyBlobs()) {
        UNIT_ASSERT_C(id.Channel() < 2, "Old KV blob survived MoveData: " << id);
    }
    CmdRead({"key", "akey", "aakey"}, NKikimrClient::TKeyValueRequest::REALTIME,
        {tc.Value, tc.Value, tc.Value}, {}, tc);
}

Y_UNIT_TEST(MoveDataRecordTouchedPreservesNeedsAnotherPass) {
    TTestContext tc;
    TFinalizer finalizer(tc);

    TAutoPtr<IEventHandle> eventBlobCopied;
    ui32 copiedCount = 0;
    ui32 repeatCount = 0;
    tc.Prepare([&](TTestActorRuntime& runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvKeyValue::TEvAdvanceMoveDataResult::EventType
                    && event->Get<TEvKeyValue::TEvAdvanceMoveDataResult>()->Result
                        == TEvKeyValue::TEvAdvanceMoveDataResult::EResult::REPEAT) {
                ++repeatCount;
            }
            if (event->GetTypeRewrite() == TEvKeyValue::TEvBlobCopied::EventType && ++copiedCount == 2) {
                eventBlobCopied = event;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    });

    const TString valueA = tc.Value;
    const TString valueB(1024, 'b');
    CmdWrite("key", valueA, NKikimrClient::TKeyValueRequest::MAIN,
        NKikimrClient::TKeyValueRequest::REALTIME, tc);
    CmdWrite("zkey", valueB, NKikimrClient::TKeyValueRequest::MAIN,
        NKikimrClient::TKeyValueRequest::REALTIME, tc);
    CmdConcat({"key", "zkey"}, "key", true, tc);
    tc.PoisonTablet();
    tc.StartReassignedTablet();
    tc.SendMoveData();

    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return bool(eventBlobCopied); };
    tc.Runtime->DispatchEvents(options);
    UNIT_ASSERT(eventBlobCopied);
    UNIT_ASSERT_VALUES_EQUAL(repeatCount, 0);

    // The first item of key (A) has moved; copying the second item (B) is
    // pending. Leave a reference to old B before the cursor, requiring REPEAT.
    CmdCopyRange("key", true, "key", true, "a", "", tc);

    // Change the current chain from A+B to B+A+B. This sets RecordTouched
    // as well as NeedsAnotherPass. Resuming must discard the pending copy,
    // restart at chain index zero, and preserve the full-pass requirement.
    CmdConcat({"zkey", "key"}, "key", true, tc);
    tc.Runtime->Send(eventBlobCopied.Release());
    tc.WaitMoveData();
    UNIT_ASSERT_VALUES_EQUAL(repeatCount, 1);

    tc.DsProxies[1]->DeleteDoNotKeepBlobs();
    for (const auto& [id, blob] : tc.DsProxies[1]->AllMyBlobs()) {
        UNIT_ASSERT_C(id.Channel() < 2, "Old KV blob survived MoveData: " << id);
    }
    CmdRead({"akey", "key", "zkey"}, NKikimrClient::TKeyValueRequest::REALTIME,
        {valueA + valueB, valueB + valueA + valueB, valueB}, {}, tc);
}

Y_UNIT_TEST(MoveDataResumeAfterYield, EMoveDataYieldMutation) {
    const auto mutation = Arg<0>();
    struct TLimits : NKeyValue::TKeyValueState {
        using TKeyValueState::MaxMoveDataRecordsInOneTx;
    };

    TTestContext tc;
    TFinalizer finalizer(tc);
    TAutoPtr<IEventHandle> yield;
    ui32 yieldCount = 0;
    ui32 copyCount = 0;
    tc.Prepare([&](TTestActorRuntime& runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvKeyValue::TEvAdvanceMoveDataResult::EventType) {
                const auto result = event->Get<TEvKeyValue::TEvAdvanceMoveDataResult>()->Result;
                if (result == TEvKeyValue::TEvAdvanceMoveDataResult::EResult::YIELD) {
                    if (++yieldCount == 1) {
                        yield = event;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                } else if (result == TEvKeyValue::TEvAdvanceMoveDataResult::EResult::COPY_BLOB) {
                    ++copyCount;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    });

    // Exactly one transaction's worth of inline records, all before the old
    // blobs. No COPY_BLOB interrupts this scan before the YIELD boundary.
    for (ui64 begin = 0; begin < TLimits::MaxMoveDataRecordsInOneTx; begin += 256) {
        TDeque<TString> keys;
        TDeque<TString> values;
        for (ui64 i = begin; i < Min(begin + 256, TLimits::MaxMoveDataRecordsInOneTx); ++i) {
            keys.push_back(TStringBuilder() << "inline/" << i);
            values.push_back(TString());
        }
        CmdWrite(keys, values, NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
    }
    CmdWrite("y-cursor", tc.Value, NKikimrClient::TKeyValueRequest::MAIN,
        NKikimrClient::TKeyValueRequest::REALTIME, tc);
    const bool hasSuccessor = mutation != EMoveDataYieldMutation::DeleteLastCursor;
    const TString successorValue(1024, 'b');
    if (hasSuccessor) {
        CmdWrite("z-successor", successorValue, NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
    }
    tc.PoisonTablet();
    tc.StartReassignedTablet();
    tc.SendMoveData();

    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return bool(yield); };
    tc.Runtime->DispatchEvents(options);
    UNIT_ASSERT(yield);
    UNIT_ASSERT_VALUES_EQUAL(yieldCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(copyCount, 0);

    switch (mutation) {
        case EMoveDataYieldMutation::None:
            break;
        case EMoveDataYieldMutation::DeleteCursor:
        case EMoveDataYieldMutation::DeleteLastCursor:
            CmdDeleteRange("y-cursor", true, "y-cursor", true, tc);
            break;
        case EMoveDataYieldMutation::DeleteAll:
            CmdDeleteRange("", true, "z-successor", true, tc);
            break;
    }
    tc.Runtime->Send(yield.Release());
    tc.WaitMoveData();

    UNIT_ASSERT_VALUES_EQUAL(yieldCount, 1);
    const ui32 expectedCopies = mutation == EMoveDataYieldMutation::None ? 2
        : mutation == EMoveDataYieldMutation::DeleteCursor ? 1 : 0;
    UNIT_ASSERT_VALUES_EQUAL(copyCount, expectedCopies);
    tc.DsProxies[1]->DeleteDoNotKeepBlobs();
    for (const auto& [id, blob] : tc.DsProxies[1]->AllMyBlobs()) {
        UNIT_ASSERT_C(id.Channel() < 2, "Old KV blob survived MoveData: " << id);
    }

    const bool cursorDeleted = mutation != EMoveDataYieldMutation::None;
    const bool allDeleted = mutation == EMoveDataYieldMutation::DeleteAll;
    CmdRead({"inline/0", "y-cursor", "z-successor"}, NKikimrClient::TKeyValueRequest::REALTIME,
        {TString(), tc.Value, successorValue}, {allDeleted, cursorDeleted, !hasSuccessor || allDeleted}, tc);
}

void CheckMoveDataTrimWhileCopyIsInFlight(bool repeatCurrentGroup) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    TAutoPtr<IEventHandle> copied;
    bool captured = false;
    bool vacuumSeen = false;
    ui32 trimCopyOccurrences = 0;
    TLogoBlobID newBlobId;
    tc.Prepare([&](TTestActorRuntime& runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (!captured && event->GetTypeRewrite() == TEvKeyValue::TEvBlobCopied::EventType) {
                newBlobId = event->Get<TEvKeyValue::TEvBlobCopied>()->NewBlobId;
                copied = event;
                captured = true;
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (event->GetTypeRewrite() == TEvKeyValue::TEvVacuumRequest::EventType) {
                vacuumSeen = true;
            }
            if (captured && event->GetTypeRewrite() == TEvBlobStorage::TEvRangeResult::EventType) {
                for (const auto& response : event->Get<TEvBlobStorage::TEvRangeResult>()->Responses) {
                    trimCopyOccurrences += response.Id == newBlobId;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    });

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN,
        NKikimrClient::TKeyValueRequest::REALTIME, tc);
    tc.PoisonTablet();
    tc.StartReassignedTablet(repeatCurrentGroup);
    tc.SendMoveData();
    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return captured; };
    tc.Runtime->DispatchEvents(options);
    UNIT_ASSERT(copied);
    UNIT_ASSERT(tc.DsProxies[2]->AllMyBlobs().contains(newBlobId));

    // Create an earlier trash bin whose GC will cross the new copy's step.
    // Its collection is currently blocked by the copy's in-flight protection.
    CmdWrite("garbage", tc.Value, NKikimrClient::TKeyValueRequest::MAIN,
        NKikimrClient::TKeyValueRequest::REALTIME, tc);
    CmdDeleteRange("garbage", true, "garbage", true, tc);
    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, new TEvKeyValue::TEvVacuumRequest(10),
        0, GetPipeConfigWithRetries());
    options.CustomFinalCondition = [&] { return vacuumSeen; };
    tc.Runtime->DispatchEvents(options);
    UNIT_ASSERT(vacuumSeen);
    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);

    // N exists in storage but is not yet referenced by the index. Trim must
    // not classify it as leaked and place it in the newer, current trash bin.
    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.MutableCmdTrimLeakedBlobs()->SetMaxItemsToTrim(100);
    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.release(), 0, GetPipeConfigWithRetries());
    TAutoPtr<IEventHandle> handle;
    const auto* result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT(result->Record.HasTrimLeakedBlobsResult());
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTrimLeakedBlobsResult().GetStatus(), ui32(NKikimrProto::OK));
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTrimLeakedBlobsResult().GetNumItemsTrimmed(), 0);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTrimLeakedBlobsResult().GetNumItemsLeft(), 1);
    UNIT_ASSERT_VALUES_EQUAL(trimCopyOccurrences, repeatCurrentGroup ? 2 : 1);

    // Accept N into the index and allow GC of both bins. The earlier bin may
    // set Keep(N); the later bin must not subsequently send DoNotKeep(N).
    tc.Runtime->Send(copied.Release());
    tc.WaitMoveData();
    tc.DsProxies[2]->DeleteDoNotKeepBlobs();
    UNIT_ASSERT_C(tc.DsProxies[2]->AllMyBlobs().contains(newBlobId),
        "Trim and GC deleted the blob referenced by key after MoveData: " << newBlobId);
    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);
}

Y_UNIT_TEST(MoveDataTrimWhileCopyIsInFlight) {
    CheckMoveDataTrimWhileCopyIsInFlight(false);
}

Y_UNIT_TEST(MoveDataTrimWhileCopyIsInFlightRepeatedGroup) {
    CheckMoveDataTrimWhileCopyIsInFlight(true);
}

class TCheckTrashState : public NKeyValue::TKeyValueState {
public:
    using TResult = TEvKeyValue::TEvAdvanceMoveDataResult::EResult;
    using TKeyValueState::MaxMoveDataTrashCheckingBlobs;
    using TKeyValueState::Trash;
    using TKeyValueState::TrashForVacuum;
    using TKeyValueState::VacuumResetGeneration;

    TCheckTrashState() {
        TabletInfo = CreateReassignedTabletInfo(1, TTabletTypes::KeyValue,
            TErasureType::ErasureNone, 100, 200, 2);
        MoveDataGroups.insert(100);
    }

    static TLogoBlobID OldBlob(ui32 step, ui8 channel = 2) {
        return TLogoBlobID(1, 1, step, channel, 1, 0);
    }

    static TLogoBlobID CurrentBlob(ui32 step) {
        return TLogoBlobID(1, 2, step, 2, 1, 0);
    }

    void Check(TResult expected) {
        // CompleteGCComplete clears this flag before scheduling the next check.
        MoveDataTrashCheckingWaitingForGC = false;
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(CheckTrash()->Result), static_cast<int>(expected));
        UNIT_ASSERT_VALUES_EQUAL(MoveDataTrashCheckingWaitingForGC, expected == TResult::WAIT_FOR_GC);
    }

    void CheckWaiting(const TLogoBlobID& blob, std::optional<ui64> generation = {}) {
        Check(TResult::WAIT_FOR_GC);
        UNIT_ASSERT_VALUES_EQUAL(MoveDataTrashCheckingBlobId, blob);
        UNIT_ASSERT(MoveDataTrashCheckingVacuumGeneration == generation);
    }
};

using TResult = TCheckTrashState::TResult;

Y_UNIT_TEST(MoveDataTrashEmptyTrash) {
    TCheckTrashState state;
    state.Check(TResult::SUCCESS);
}

Y_UNIT_TEST(MoveDataTrashSkipUnrelatedGroupsInMultipleGenerations) {
    TCheckTrashState state;
    state.Trash.insert(state.CurrentBlob(1));
    state.TrashForVacuum[10].insert(state.CurrentBlob(2));
    state.TrashForVacuum[20].insert(state.CurrentBlob(3));
    state.TrashForVacuum[30].insert(state.CurrentBlob(4));
    state.Check(TResult::SUCCESS);
}

Y_UNIT_TEST(MoveDataTrashWaitForEveryLayer) {
    TCheckTrashState state;
    const auto blob = state.OldBlob(1);
    state.Trash.insert(blob);
    state.TrashForVacuum[10].insert(state.OldBlob(2));
    state.TrashForVacuum[20].insert(state.CurrentBlob(1));
    state.TrashForVacuum[30].insert(state.OldBlob(3));
    state.TrashForVacuum[30].insert(state.OldBlob(4));

    state.CheckWaiting(blob);
    state.CheckWaiting(blob); // An unrelated GC must not advance past this blob.
    state.Trash.erase(blob);
    state.CheckWaiting(state.OldBlob(2), 10);
    state.TrashForVacuum.erase(10);
    state.CheckWaiting(state.OldBlob(3), 30);
    state.TrashForVacuum[30].erase(state.OldBlob(3));
    state.CheckWaiting(state.OldBlob(4), 30);
    state.TrashForVacuum.erase(30);
    state.Check(TResult::SUCCESS);
}

Y_UNIT_TEST(MoveDataTrashCollectedLayerDisappearsBeforeResume) {
    TCheckTrashState state;
    state.TrashForVacuum[10].insert(state.OldBlob(1));
    state.TrashForVacuum[20].insert(state.OldBlob(2));
    state.TrashForVacuum[30].insert(state.OldBlob(3));
    state.CheckWaiting(state.OldBlob(1), 10);
    // Several GC completions can run before the queued check is dispatched.
    state.TrashForVacuum.erase(10);
    state.TrashForVacuum.erase(20);
    state.CheckWaiting(state.OldBlob(3), 30);
    state.TrashForVacuum.erase(30);
    state.Check(TResult::SUCCESS);
}

Y_UNIT_TEST(MoveDataTrashCurrentTrashMovesToNewVacuumLayer) {
    TCheckTrashState state;
    state.Trash.insert(state.OldBlob(3));
    state.TrashForVacuum[10].insert(state.OldBlob(1));
    state.TrashForVacuum[20].insert(state.OldBlob(2));
    state.CheckWaiting(state.OldBlob(3));
    // StartVacuum moves the whole current bin to a newer generation.
    state.TrashForVacuum[30].swap(state.Trash);
    state.CheckWaiting(state.OldBlob(1), 10);
    state.TrashForVacuum.erase(10);
    state.CheckWaiting(state.OldBlob(2), 20);
    state.TrashForVacuum.erase(20);
    state.CheckWaiting(state.OldBlob(3), 30);
    state.TrashForVacuum.erase(30);
    state.Check(TResult::SUCCESS);
}

Y_UNIT_TEST(MoveDataTrashResumeAtScanLimitAcrossLayers) {
    TCheckTrashState state;
    const ui32 half = state.MaxMoveDataTrashCheckingBlobs / 2;
    for (ui32 step = 1; step <= half; ++step) {
        state.Trash.insert(state.CurrentBlob(step));
        state.TrashForVacuum[10].insert(state.CurrentBlob(step + half));
    }
    state.TrashForVacuum[20].insert(state.OldBlob(1));
    state.Check(TResult::CHECK_TRASH);
    state.Check(TResult::WAIT_FOR_GC); // The first unchecked blob must not be skipped.
    state.TrashForVacuum.erase(20);
    state.Check(TResult::SUCCESS);
}

Y_UNIT_TEST(MoveDataTrashResumeInsideLayerAtScanLimit) {
    TCheckTrashState state;
    for (ui32 step = 1; step <= state.MaxMoveDataTrashCheckingBlobs; ++step) {
        state.TrashForVacuum[10].insert(state.CurrentBlob(step));
    }
    // Blob IDs sort by channel first: the old blob must follow the current
    // group's blobs while remaining in the same vacuum layer.
    const auto boundaryBlob = state.OldBlob(1, 3);
    UNIT_ASSERT(state.CurrentBlob(state.MaxMoveDataTrashCheckingBlobs) < boundaryBlob);
    state.TrashForVacuum[10].insert(boundaryBlob);
    state.Check(TResult::CHECK_TRASH);
    state.CheckWaiting(boundaryBlob, 10);
    state.TrashForVacuum[10].erase(boundaryBlob);
    state.Check(TResult::SUCCESS);
}

Y_UNIT_TEST(MoveDataTrashVacuumResetAfterScanLimit) {
    TCheckTrashState state;
    for (ui32 step = 1; step <= state.MaxMoveDataTrashCheckingBlobs + 1; ++step) {
        state.TrashForVacuum[10].insert(state.CurrentBlob(step));
    }
    const auto oldBlob = state.OldBlob(1);
    state.TrashForVacuum[20].insert(oldBlob);
    state.Check(TResult::CHECK_TRASH);

    // Reset vacuum while the scan-limit continuation is queued, without a GC
    // completion. The old blob moves behind the cursor into the already scanned
    // Trash bin, and its ID is smaller than the saved current-generation ID.
    for (const auto& [generation, bin] : state.TrashForVacuum) {
        state.Trash.insert(bin.begin(), bin.end());
    }
    state.TrashForVacuum.clear();
    ++state.VacuumResetGeneration;

    state.CheckWaiting(oldBlob); // Must restart from Trash with a fresh blob cursor.
    state.Trash.erase(oldBlob);
    state.Check(TResult::CHECK_TRASH);
    state.Check(TResult::SUCCESS);
}

// Keep real GC and trash checks queued so that each layer is exercised independently.
struct TMoveDataTrashContext : TTestContext {
    TAutoPtr<IEventHandle> Collect;
    TAutoPtr<IEventHandle> CheckTrash;
    bool BlockCollect = false;
    bool BlockCheck = true;
    bool MoveDataCompleted = false;
    ui32 CollectCount = 0;
    ui64 VacuumSeen = 0;

    void PrepareLayers(ui32 layers, bool leaveCurrentTrash = false, std::optional<ui32> oldLayer = {}) {
        Prepare([&](TTestActorRuntime& runtime) {
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvKeyValue::TEvCollect::EventType && BlockCollect) {
                    UNIT_ASSERT(!Collect);
                    Collect = event;
                    ++CollectCount;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() == TEvKeyValue::TEvCheckTrash::EventType && BlockCheck) {
                    UNIT_ASSERT(!CheckTrash);
                    CheckTrash = event;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() == TEvKeyValue::TEvVacuumRequest::EventType) {
                    VacuumSeen = event->Get<TEvKeyValue::TEvVacuumRequest>()->Record.generation();
                }
                if (event->GetTypeRewrite() == TEvTablet::TEvMoveDataResponse::EventType) {
                    MoveDataCompleted = true;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });
        });

        for (ui32 i = 0; i < layers; ++i) {
            if (oldLayer && i != *oldLayer) {
                continue;
            }
            CmdWrite(TStringBuilder() << "old" << i, Value,
                NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, *this);
        }
        PoisonTablet();
        StartReassignedTablet();
        if (oldLayer) {
            for (ui32 i = 0; i < layers; ++i) {
                if (i != *oldLayer) {
                    CmdWrite(TStringBuilder() << "old" << i, Value,
                        NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, *this);
                }
            }
        }
        CmdWrite("alive", Value, NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, *this);
        BlockCollect = true;
        for (ui32 i = 0; i < layers; ++i) {
            const TString key = TStringBuilder() << "old" << i;
            CmdDeleteRange(key, true, key, true, *this);
            if (!leaveCurrentTrash || i + 1 < layers) {
                StartVacuum((i + 1) * 10);
            }
        }
        WaitFor([&] { return bool(Collect); });
        UNIT_ASSERT_VALUES_EQUAL(CollectCount, 1);
        SendMoveData();
        WaitFor([&] { return bool(CheckTrash); });
    }

    void WaitFor(std::function<bool()> condition) {
        if (!condition()) {
            TDispatchOptions options;
            options.CustomFinalCondition = condition;
            Runtime->DispatchEvents(options);
        }
        UNIT_ASSERT(condition());
    }

    void ReadAlive() {
        CmdRead({"alive"}, NKikimrClient::TKeyValueRequest::REALTIME, {Value}, {}, *this);
    }

    void StartVacuum(ui64 generation, bool reset = false) {
        Runtime->SendToPipe(TabletId, Edge, new TEvKeyValue::TEvVacuumRequest(generation, reset),
            0, GetPipeConfigWithRetries());
        WaitFor([&] { return VacuumSeen == generation; });
        ReadAlive();
    }

    void CheckStillWaiting() {
        UNIT_ASSERT(CheckTrash);
        Runtime->Send(CheckTrash.Release());
        ReadAlive();
        UNIT_ASSERT(!CheckTrash); // WAIT_FOR_GC does not schedule a busy-loop continuation.
        UNIT_ASSERT(!MoveDataCompleted);
    }

    void CollectOneLayer() {
        UNIT_ASSERT(Collect);
        const ui32 previousCount = CollectCount;
        Runtime->Send(Collect.Release());
        WaitFor([&] { return CollectCount == previousCount + 1; });
        UNIT_ASSERT(!MoveDataCompleted);
    }

    void Finish(bool expectFinalCheck = false) {
        UNIT_ASSERT(Collect);
        BlockCollect = false;
        Runtime->Send(Collect.Release());
        if (expectFinalCheck) {
            WaitFor([&] { return bool(CheckTrash) || MoveDataCompleted; });
            UNIT_ASSERT_C(CheckTrash, "MoveData completed without checking the remaining trash after GC");
            UNIT_ASSERT(!MoveDataCompleted);
        }
        BlockCheck = false;
        if (CheckTrash) {
            Runtime->Send(CheckTrash.Release());
        }
        WaitMoveData();
        ReadAlive();
        DsProxies[1]->DeleteDoNotKeepBlobs();
        for (const auto& [id, blob] : DsProxies[1]->AllMyBlobs()) {
            // Channels 0 and 1 belong to the executor; KV values use channel 2+.
            UNIT_ASSERT_C(id.Channel() < 2, "Old KV blob survived MoveData: " << id);
        }
    }
};

void CheckMoveDataTrashSingleOldBlobAmongVacuumLayers(bool leaveCurrentTrash) {
    for (ui32 oldLayer = 0; oldLayer < 3; ++oldLayer) {
        TMoveDataTrashContext tc;
        TFinalizer finalizer(tc);
        // Only this layer can cause WAIT_FOR_GC; later layers cannot mask its skip.
        tc.PrepareLayers(3, leaveCurrentTrash, oldLayer);
        tc.CheckStillWaiting();
        if (leaveCurrentTrash) {
            tc.StartVacuum(30);
        }
        for (ui32 i = 0; i < oldLayer; ++i) {
            tc.CollectOneLayer();
            tc.WaitFor([&] { return bool(tc.CheckTrash); });
            tc.CheckStillWaiting();
        }
        tc.Finish(true);
    }
}

Y_UNIT_TEST(MoveDataTrashSingleOldBlobAmongVacuumLayers) {
    CheckMoveDataTrashSingleOldBlobAmongVacuumLayers(false);
}

Y_UNIT_TEST(MoveDataTrashSingleOldBlobAmongVacuumLayersWithCurrentTrash) {
    CheckMoveDataTrashSingleOldBlobAmongVacuumLayers(true);
}

Y_UNIT_TEST(MoveDataTrashMultipleVacuumLayers) {
    TMoveDataTrashContext tc;
    TFinalizer finalizer(tc);
    tc.PrepareLayers(3);
    tc.CheckStillWaiting();
    for (ui32 i = 0; i < 2; ++i) {
        tc.CollectOneLayer();
        tc.WaitFor([&] { return bool(tc.CheckTrash); });
        tc.CheckStillWaiting();
    }
    tc.Finish();
    UNIT_ASSERT_VALUES_EQUAL(tc.CollectCount, 3);
}

Y_UNIT_TEST(MoveDataTrashSeveralLayersCollectedBeforeCheck) {
    TMoveDataTrashContext tc;
    TFinalizer finalizer(tc);
    tc.PrepareLayers(3);
    tc.CheckStillWaiting();
    tc.CollectOneLayer();
    tc.WaitFor([&] { return bool(tc.CheckTrash); });
    // Leave the check queued while a second generation disappears.
    tc.CollectOneLayer();
    tc.CheckStillWaiting();
    tc.Finish();
    UNIT_ASSERT_VALUES_EQUAL(tc.CollectCount, 3);
}

Y_UNIT_TEST(MoveDataTrashCurrentBinBecomesVacuumLayer) {
    TMoveDataTrashContext tc;
    TFinalizer finalizer(tc);
    tc.PrepareLayers(4, true);
    tc.CheckStillWaiting();
    tc.StartVacuum(40);
    for (ui32 i = 0; i < 3; ++i) {
        tc.CollectOneLayer();
        tc.WaitFor([&] { return bool(tc.CheckTrash); });
        tc.CheckStillWaiting();
    }
    tc.Finish();
    UNIT_ASSERT_VALUES_EQUAL(tc.CollectCount, 4);
}

Y_UNIT_TEST(MoveDataTrashVacuumReset) {
    for (bool resetWhileWaiting : {true, false}) {
        TMoveDataTrashContext tc;
        TFinalizer finalizer(tc);
        tc.PrepareLayers(3);
        tc.CheckStillWaiting();
        if (resetWhileWaiting) {
            tc.StartVacuum(1, true);
        }
        tc.CollectOneLayer();
        tc.WaitFor([&] { return bool(tc.CheckTrash); });
        if (!resetWhileWaiting) {
            tc.StartVacuum(1, true);
        }
        tc.CheckStillWaiting();
        // The next GC must wake CheckTrash again, rather than MoveData completing
        // because the cursor still refers to the vanished generation 10.
        tc.Finish(true);
    }
}

Y_UNIT_TEST(MoveDataCounters) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.ExecuteMoveData();

    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME, {tc.Value}, {}, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    auto counters = tc.Runtime->GetAppData(0).Counters;
    auto dbGroup = GetServiceCounters(counters, "tablets")->GetSubgroup("type", "KeyValue")->GetSubgroup("category", "app");

    UNIT_ASSERT_EQUAL(dbGroup->GetCounter("KV/ReqWoOk")->Val(), 1);
    UNIT_ASSERT_EQUAL(dbGroup->GetCounter("KV/ReqRoOk")->Val(), 1);

    UNIT_ASSERT_EQUAL(dbGroup->GetCounter("KV/MoveDataBlobsMoved")->Val(), 1);
    UNIT_ASSERT_EQUAL(dbGroup->GetCounter("KV/MoveDataBytesMoved")->Val(), (i64)tc.Value.size());
    UNIT_ASSERT_EQUAL(dbGroup->GetCounter("KV/MoveDataRecordsScanned")->Val(), 1);
}

Y_UNIT_TEST(MoveDataCanceledByYellowStop) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare([](TTestActorRuntime &){});

    CmdWrite("key", tc.Value, NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::REALTIME, tc);

    tc.PoisonTablet();
    tc.StartReassignedTablet();

    tc.DsProxies[2]->SetStorageStatusFlags(TStorageStatusFlags(
        NKikimrBlobStorage::StatusDiskSpaceYellowStop | NKikimrBlobStorage::StatusIsValid));

    tc.SendMoveData();
    tc.WaitMoveDataError(NKikimrTabletBase::TEvMoveDataResponse::NotEnoughSpace);
}

} // TKeyValueMoveDataTest
} // NKikimr
