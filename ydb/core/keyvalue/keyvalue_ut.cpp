#include "defs.h"
#include "keyvalue.h"
#include "keyvalue_flat_impl.h"
#include "keyvalue_intermediate.h"
#include "keyvalue_state.h"
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>
#include <ydb/core/base/blobstorage.h>

const bool ENABLE_DETAILED_KV_LOG = false;
const bool ENABLE_TESTLOG_OUTPUT = false;

namespace NKikimr {
namespace {

template <typename... TArgs>
void TestLog(TArgs&&... args) {
    if constexpr (ENABLE_TESTLOG_OUTPUT || ENABLE_DETAILED_KV_LOG) {
        Cerr << ((TStringBuilder() << ... << args) << Endl);
    }
}

void SetupLogging(TTestActorRuntime& runtime) {
    NActors::NLog::EPriority priority = ENABLE_DETAILED_KV_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
    NActors::NLog::EPriority otherPriority = NLog::PRI_ERROR;

    runtime.SetLogPriority(NKikimrServices::KEYVALUE, priority);
    runtime.SetLogPriority(NKikimrServices::KEYVALUE_GC, priority);
    runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, priority);
    runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, priority);
    runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, priority);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY, priority);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY_STATUS, priority);

    runtime.SetLogPriority(NKikimrServices::HIVE, otherPriority);
    runtime.SetLogPriority(NKikimrServices::LOCAL, otherPriority);
    runtime.SetLogPriority(NKikimrServices::BS_NODE, otherPriority);
    runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, otherPriority);
    runtime.SetLogPriority(NKikimrServices::PIPE_CLIENT, otherPriority);
    runtime.SetLogPriority(NKikimrServices::TABLET_RESOLVER, otherPriority);
}

class TInitialEventsFilter: TNonCopyable {
    bool IsDone;

public:
    TInitialEventsFilter()
        : IsDone(false)
    {
    }

    TTestActorRuntime::TEventFilter Prepare() {
        IsDone = false;
        return [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            return (*this)(runtime, event);
        };
    }

    bool operator()(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
        Y_UNUSED(runtime);
        Y_UNUSED(event);
        return false;
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TKeyValueTest) {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SETUP
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TTestContext {
    TTabletTypes::EType TabletType;
    ui64 TabletId;
    TInitialEventsFilter InitialEventsFilter;
    TVector<ui64> TabletIds;
    THolder<TTestActorRuntime> Runtime;
    TActorId Edge;

    TTestContext() {
        TabletType = TTabletTypes::KeyValue;
        TabletId = MakeTabletID(false, 1);
        TabletIds.push_back(TabletId);
    }

    void Prepare(const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &outActiveZone) {
        Y_UNUSED(dispatchName);
        outActiveZone = false;
        Runtime.Reset(new TTestBasicRuntime);
        Runtime->SetScheduledLimit(200);
        Runtime->SetLogPriority(NKikimrServices::KEYVALUE, NLog::PRI_DEBUG);
        Runtime->SetDispatchedEventsLimit(25'000'000);
        SetupLogging(*Runtime);
        SetupTabletServices(*Runtime);
        setup(*Runtime);
        CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(TabletId, TabletType, TErasureType::ErasureNone),
            &CreateKeyValueFlat);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);

        Edge = Runtime->AllocateEdgeActor();
        outActiveZone = true;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SINGLE COMMAND TEST FUNCTIONS
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        const NKikimrClient::TKeyValueRequest::EPriority priority, TTestContext &tc) {
    Y_ABORT_UNLESS(keys.size() == values.size());
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

struct TDiff {
    ui32 Offset;
    TString Buffer;
};

void CmdPatch(const TString &originalKey, const TString &patchedKey, const TVector<TDiff> &diffs,
        const NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel, TTestContext &tc) {
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;
    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        auto *patch = request->Record.AddCmdPatch();
        patch->SetOriginalKey(originalKey);
        patch->SetPatchedKey(patchedKey);
        patch->SetStorageChannel(storageChannel);
        for (ui64 idx = 0; idx < diffs.size(); ++idx) {
            auto diff = patch->AddDiffs();
            diff->SetOffset(diffs[idx].Offset);
            diff->SetValue(diffs[idx].Buffer);
        }
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.PatchResultSize(), 1);
        
        const auto &patchResult = result->Record.GetPatchResult(0);
        UNIT_ASSERT(patchResult.HasStatus());
        UNIT_ASSERT_EQUAL(patchResult.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT(patchResult.HasStatusFlags());
        UNIT_ASSERT(patchResult.GetStatusFlags() & ui32(NKikimrBlobStorage::StatusIsValid));
        return true;
    });
}

void CmdWrite(const TString &key, const TString &value,
        const NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel,
        const NKikimrClient::TKeyValueRequest::EPriority priority, TTestContext &tc) {
    TDeque<TString> keys = {key};
    TDeque<TString> values = {value};
    CmdWrite(keys, values, storageChannel, priority, tc);
}

void CmdRead(const TDeque<TString> &keys,
        const NKikimrClient::TKeyValueRequest::EPriority priority,
        const TDeque<TString> &expectedValues, const TDeque<bool> expectedNodatas, TTestContext &tc) {
    Y_ABORT_UNLESS(keys.size() == expectedValues.size());
    Y_ABORT_UNLESS(expectedNodatas.size() == 0 || expectedNodatas.size() == keys.size());
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
        }
        return true;
    });
}

void CmdRename(const TDeque<TString> &oldKeys, const TDeque<TString> &newKeys, TTestContext &tc,
        bool expectOk = true) {
    Y_ABORT_UNLESS(oldKeys.size() == newKeys.size());
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

void CmdRename(const TString &oldKey, const TString &newKey, TTestContext &tc, bool expectOk = true) {
    TDeque<TString> oldKeys = {oldKey};
    TDeque<TString> newKeys = {newKey};
    CmdRename(oldKeys, newKeys, tc, expectOk);
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

template <typename TRequest>
struct TDesiredPair {
    typename TRequest::ProtoRecordType Request;
    typename TRequest::TResponse::ProtoRecordType Response;
};

template <>
struct TDesiredPair<TEvKeyValue::TEvRequest> {
    NKikimrClient::TKeyValueRequest Request;
    NKikimrClient::TKeyValueResponse Response;
};

void CheckResponse(NKikimrClient::TResponse &ar, NKikimrClient::TKeyValueResponse &er, ui64 line) {
    UNIT_ASSERT_VALUES_EQUAL_C(ar.ReadRangeResultSize(), er.ReadRangeResultSize(), "Line# " << line);
    for (size_t readRangeIdx = 0; readRangeIdx < ar.ReadRangeResultSize(); ++readRangeIdx) {
        const auto &aRange = ar.GetReadRangeResult(readRangeIdx);
        const auto &eRange = er.GetReadRangeResult(readRangeIdx);
        UNIT_ASSERT_C(aRange.HasStatus(), "Line# " << line);
        UNIT_ASSERT_C(eRange.HasStatus(), "Line# " << line);
        UNIT_ASSERT_EQUAL_C(aRange.GetStatus(), eRange.GetStatus(),
                NKikimrProto::EReplyStatus_Name((NKikimrProto::EReplyStatus)aRange.GetStatus()) << " Line# " << line);
        UNIT_ASSERT_VALUES_EQUAL_C(aRange.PairSize(), eRange.PairSize(), "Line# " << line);
        for (ui64 idx = 0; idx < aRange.PairSize(); ++idx) {
            const auto &aPair = aRange.GetPair(idx);
            const auto &ePair = eRange.GetPair(idx);
            UNIT_ASSERT_C(aPair.HasKey(), "Line# " << line);
            UNIT_ASSERT_C(ePair.HasKey(), "Line# " << line);
            if (ePair.HasValue()) {
                UNIT_ASSERT_C(aPair.HasValue(), "Line# " << line);
            } else {
                UNIT_ASSERT_C(!aPair.HasValue(), "Line# " << line);
            }
            UNIT_ASSERT_C(aPair.HasValueSize(), "Line# " << line);
            UNIT_ASSERT_VALUES_EQUAL_C(aPair.GetKey(), ePair.GetKey(), "Line# " << line);
            if (ePair.HasValue()) {
                UNIT_ASSERT_VALUES_EQUAL_C(aPair.GetValue(), ePair.GetValue(), "Line# " << line);
            }
            UNIT_ASSERT_C(aPair.HasCreationUnixTime(), "Line# " << line);
            //TODO: UNIT_ASSERT(aPair.GetCreationUnixTime() >= unixTime);
        }
    }
}

void RunRequest(TDesiredPair<TEvKeyValue::TEvRequest> &dp, TTestContext &tc, ui64 line) {
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        request->Record = dp.Request;

        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);

        UNIT_ASSERT_C(result, "Line# " << line);
        UNIT_ASSERT_C(result->Record.HasStatus(), "Line# " << line);
        UNIT_ASSERT_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK, "Line# " << line);

        CheckResponse(result->Record, dp.Response, line);

        return true;
    });
}

void AddCmdReadRange(const TString &from, const bool includeFrom, const TString &to, const bool includeTo,
        const bool includeData, const ui64 limitBytes,
        const NKikimrClient::TKeyValueRequest::EPriority priority,
        const TDeque<TString> &expectedKeys, const TDeque<TString> &expectedValues,
        const NKikimrProto::EReplyStatus expectedStatus, TTestContext &tc, TDesiredPair<TEvKeyValue::TEvRequest> &dp) {
    Y_UNUSED(tc);
    Y_ABORT_UNLESS(!includeData || expectedKeys.size() == expectedValues.size());

    {
        auto cmd = dp.Request.AddCmdReadRange();
        auto range = cmd->MutableRange();
        range->SetFrom(from);
        range->SetIncludeFrom(includeFrom);
        range->SetTo(to);
        range->SetIncludeTo(includeTo);
        cmd->SetIncludeData(includeData);
        cmd->SetLimitBytes(limitBytes);
        cmd->SetPriority(priority);
    }
    {
        auto res = dp.Response.AddReadRangeResult();
        res->SetStatus(expectedStatus);
        size_t itemCount = std::max(expectedKeys.size(), expectedValues.size());
        for (size_t i = 0; i < itemCount; ++i) {
            auto pair = res->AddPair();
            if (i < expectedKeys.size()) {
                pair->SetKey(expectedKeys[i]);
            }
            if (i < expectedValues.size()) {
                pair->SetValue(expectedValues[i]);
            }
        }
    }
}

void CmdGetStatus(const NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel,
        const ui32 expectedStatusFlags, TTestContext &tc) {
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        auto getStatus = request->Record.AddCmdGetStatus();
        getStatus->SetStorageChannel(storageChannel);
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatusResultSize(), 1);
        UNIT_ASSERT(result->Record.GetGetStatusResult(0).HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetGetStatusResult(0).GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT(result->Record.GetGetStatusResult(0).HasStatusFlags());
        UNIT_ASSERT_EQUAL(result->Record.GetGetStatusResult(0).GetStatusFlags(), expectedStatusFlags);

        return true;
    });
}

void CmdSetExecutorFastLogPolicy(bool isAllowed, TTestContext &tc) {
    TAutoPtr<IEventHandle> handle;
    TEvKeyValue::TEvResponse *result;
    THolder<TEvKeyValue::TEvRequest> request;

    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        request.Reset(new TEvKeyValue::TEvRequest);
        auto cmd = request->Record.MutableCmdSetExecutorFastLogPolicy();
        cmd->SetIsAllowed(isAllowed);
        tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Record.HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT(result->Record.HasSetExecutorFastLogPolicyResult());
        UNIT_ASSERT(result->Record.GetSetExecutorFastLogPolicyResult().HasStatus());
        UNIT_ASSERT_EQUAL(result->Record.GetSetExecutorFastLogPolicyResult().GetStatus(), NKikimrProto::OK);

        return true;
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NEW SINGLE COMMAND TEST FUNCTIONS
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TKeyValuePair {
    TString Key;
    TString Value;
};

template <typename TRequestEvent>
void SendRequest(const decltype(std::declval<TRequestEvent>().Record) &record, TTestContext &tc) {
    auto request = std::make_unique<TRequestEvent>();
    request->Record = record;
    TestLog("Send event# ", TypeName(*request));
    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.release(), 0, GetPipeConfigWithRetries());
}

template <typename TRequestEvent>
auto ReceiveResponse(TTestContext &tc) -> decltype(std::declval<typename TRequestEvent::TResponse>().Record) {
    TAutoPtr<IEventHandle> handle;
    typename TRequestEvent::TResponse *response = tc.Runtime->GrabEdgeEvent<typename TRequestEvent::TResponse>(handle);
    TestLog("Received event# ", TypeName(*response));
    return response->Record;
}

template <typename TRequestEvent>
void ExecuteEvent(TDesiredPair<TRequestEvent> &dp, TTestContext &tc) {
    DoWithRetry([&] {
        tc.Runtime->ResetScheduledCount();
        SendRequest<TRequestEvent>(dp.Request, tc);
        dp.Response = ReceiveResponse<TRequestEvent>(tc);
        return true;
    });
}


void SendWrite(TTestContext &tc, const TDeque<TKeyValuePair> &pairs, ui64 lockedGeneration, ui64 storageChannel,
        NKikimrKeyValue::Priorities::Priority priority)
{
    NKikimrKeyValue::ExecuteTransactionRequest et;

    for (auto &[key, value] : pairs) {
        NKikimrKeyValue::ExecuteTransactionRequest::Command *cmd = et.add_commands();
        NKikimrKeyValue::ExecuteTransactionRequest::Command::Write *write = cmd->mutable_write();

        write->set_key(key);
        write->set_value(value);
        write->set_storage_channel(storageChannel);
        write->set_priority(priority);
    }

    et.set_tablet_id(tc.TabletId);
    et.set_lock_generation(lockedGeneration);

    SendRequest<TEvKeyValue::TEvExecuteTransaction>(et, tc);
}

template <NKikimrKeyValue::Statuses::ReplyStatus ExpectedStatus = NKikimrKeyValue::Statuses::RSTATUS_OK>
void ExecuteWrite(TTestContext &tc, const TDeque<TKeyValuePair> &pairs, ui64 lockedGeneration, ui64 storageChannel,
        NKikimrKeyValue::Priorities::Priority priority)
{
    TDesiredPair<TEvKeyValue::TEvExecuteTransaction> dp;

    for (auto &[key, value] : pairs) {
        NKikimrKeyValue::ExecuteTransactionRequest::Command *cmd = dp.Request.add_commands();
        NKikimrKeyValue::ExecuteTransactionRequest::Command::Write *write = cmd->mutable_write();

        write->set_key(key);
        write->set_value(value);
        write->set_storage_channel(storageChannel);
        write->set_priority(priority);
    }

    dp.Request.set_tablet_id(tc.TabletId);
    dp.Request.set_lock_generation(lockedGeneration);

    ExecuteEvent(dp, tc);
    UNIT_ASSERT_C(dp.Response.status() == ExpectedStatus,
            "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
            << " exp# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(ExpectedStatus)
            << " msg# " << dp.Response.msg());
}

enum class EBorderKind {
    Include,
    Exclude,
    Without
};


void SendDeleteRange(TTestContext &tc,
        const TString &from, EBorderKind fromKind,
        const TString &to, EBorderKind toKind,
        ui64 lock_generation)
{
    NKikimrKeyValue::ExecuteTransactionRequest record;
    record.set_lock_generation(lock_generation);
    record.set_tablet_id(tc.TabletId);

    NKikimrKeyValue::ExecuteTransactionRequest::Command *cmd = record.add_commands();
    NKikimrKeyValue::ExecuteTransactionRequest::Command::DeleteRange *deleteRange = cmd->mutable_delete_range();

    auto *r = deleteRange->mutable_range();

    switch (fromKind) {
    case EBorderKind::Include:
        r->set_from_key_inclusive(from);
        break;
    case EBorderKind::Exclude:
        r->set_from_key_exclusive(from);
        break;
    case EBorderKind::Without:
        break;
    }

    switch (toKind) {
    case EBorderKind::Include:
        r->set_to_key_inclusive(to);
        break;
    case EBorderKind::Exclude:
        r->set_to_key_exclusive(to);
        break;
    case EBorderKind::Without:
        break;
    }

    SendRequest<TEvKeyValue::TEvExecuteTransaction>(record, tc);
}

template <bool IsSuccess = true>
void ExecuteDeleteRange(TTestContext &tc,
        const TString &from, EBorderKind fromKind,
        const TString &to, EBorderKind toKind,
        ui64 lock_generation)
{
    TDesiredPair<TEvKeyValue::TEvExecuteTransaction> dp;
    dp.Request.set_lock_generation(lock_generation);
    dp.Request.set_tablet_id(tc.TabletId);

    NKikimrKeyValue::ExecuteTransactionRequest::Command *cmd = dp.Request.add_commands();
    NKikimrKeyValue::ExecuteTransactionRequest::Command::DeleteRange *deleteRange = cmd->mutable_delete_range();

    auto *r = deleteRange->mutable_range();

    switch (fromKind) {
    case EBorderKind::Include:
        r->set_from_key_inclusive(from);
        break;
    case EBorderKind::Exclude:
        r->set_from_key_exclusive(from);
        break;
    case EBorderKind::Without:
        break;
    }

    switch (toKind) {
    case EBorderKind::Include:
        r->set_to_key_inclusive(to);
        break;
    case EBorderKind::Exclude:
        r->set_to_key_exclusive(to);
        break;
    case EBorderKind::Without:
        break;
    }

    ExecuteEvent(dp, tc);
    if constexpr (IsSuccess) {
        UNIT_ASSERT_C(dp.Response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK,
                "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
                << " msg# " << dp.Response.msg());
    } else {
        UNIT_ASSERT_C(dp.Response.status() == NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR,
                "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
                << " msg# " << dp.Response.msg());
    }
}


template <bool IsSuccess = true>
void ExecuteCopyRange(TTestContext &tc,
        const TString &from, EBorderKind fromKind,
        const TString &to, EBorderKind toKind,
        ui64 lock_generation, const TString &prefixToAdd, const TString &prefixToRemove)
{
   TDesiredPair<TEvKeyValue::TEvExecuteTransaction> dp;
    dp.Request.set_lock_generation(lock_generation);
    dp.Request.set_tablet_id(tc.TabletId);

    NKikimrKeyValue::ExecuteTransactionRequest::Command *cmd = dp.Request.add_commands();
    NKikimrKeyValue::ExecuteTransactionRequest::Command::CopyRange *copyRange = cmd->mutable_copy_range();

    auto *r = copyRange->mutable_range();
    switch (fromKind) {
    case EBorderKind::Include:
        r->set_from_key_inclusive(from);
        break;
    case EBorderKind::Exclude:
        r->set_from_key_exclusive(from);
        break;
    case EBorderKind::Without:
        break;
    }

    switch (toKind) {
    case EBorderKind::Include:
        r->set_to_key_inclusive(to);
        break;
    case EBorderKind::Exclude:
        r->set_to_key_exclusive(to);
        break;
    case EBorderKind::Without:
        break;
    }

    copyRange->set_prefix_to_remove(prefixToRemove);
    copyRange->set_prefix_to_add(prefixToAdd);

    ExecuteEvent(dp, tc);
    if constexpr (IsSuccess) {
        UNIT_ASSERT_C(dp.Response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK,
                "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
                << " msg# " << dp.Response.msg());
    } else {
        UNIT_ASSERT_C(dp.Response.status() == NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR,
                "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
                << " msg# " << dp.Response.msg());
    }
}


struct TKeyRenamePair {
    TString OldKey;
    TString NewKey;
};

template <NKikimrKeyValue::Statuses::ReplyStatus ExpectedStatus = NKikimrKeyValue::Statuses::RSTATUS_OK>
void ExecuteRename(TTestContext &tc, const TDeque<TKeyRenamePair> &pairs, ui64 lockedGeneration)
{
    TDesiredPair<TEvKeyValue::TEvExecuteTransaction> dp;

    for (auto &[oldKey, newKey] : pairs) {
        NKikimrKeyValue::ExecuteTransactionRequest::Command *cmd = dp.Request.add_commands();
        NKikimrKeyValue::ExecuteTransactionRequest::Command::Rename *rename = cmd->mutable_rename();

        rename->set_old_key(oldKey);
        rename->set_new_key(newKey);
    }

    dp.Request.set_tablet_id(tc.TabletId);
    dp.Request.set_lock_generation(lockedGeneration);

    ExecuteEvent(dp, tc);
    UNIT_ASSERT_C(dp.Response.status() == ExpectedStatus,
            "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
            << " msg# " << dp.Response.msg());
}

template <bool IsSuccess = true>
void ExecuteConcat(TTestContext &tc, const TString &newKey, const TDeque<TString> &inputKeys, ui64 lockedGeneration,
        bool keepKeys)
{
    TDesiredPair<TEvKeyValue::TEvExecuteTransaction> dp;

    NKikimrKeyValue::ExecuteTransactionRequest::Command *cmd = dp.Request.add_commands();
    NKikimrKeyValue::ExecuteTransactionRequest::Command::Concat *concat = cmd->mutable_concat();

    concat->set_output_key(newKey);
    concat->set_keep_inputs(keepKeys);
    for (auto &key : inputKeys) {
        concat->add_input_keys(key);
    }

    dp.Request.set_tablet_id(tc.TabletId);
    dp.Request.set_lock_generation(lockedGeneration);

    ExecuteEvent(dp, tc);
    if constexpr (IsSuccess) {
        UNIT_ASSERT_C(dp.Response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK,
                "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
                << " msg# " << dp.Response.msg());
    } else {
        UNIT_ASSERT_C(dp.Response.status() == NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR,
                "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
                << " msg# " << dp.Response.msg());
    }
}


template <NKikimrKeyValue::Statuses::ReplyStatus ExpectedStatus = NKikimrKeyValue::Statuses::RSTATUS_OK>
void ExecuteRead(TTestContext &tc, const TString &key, const TString &expectedValue, ui32 offset, ui32 size,
        ui64 lock_generation, ui64 limit_bytes=0)
{
    TDesiredPair<TEvKeyValue::TEvRead> dp;
    dp.Request.set_key(key);
    dp.Request.set_offset(offset);
    dp.Request.set_size(size);
    dp.Request.set_lock_generation(lock_generation);
    dp.Request.set_tablet_id(tc.TabletId);
    dp.Request.set_limit_bytes(limit_bytes);
    ExecuteEvent(dp, tc);

    UNIT_ASSERT_C(dp.Response.status() == ExpectedStatus,
            "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
            << " exp# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(ExpectedStatus)
            << " msg# " << dp.Response.msg());
    if constexpr (ExpectedStatus == NKikimrKeyValue::Statuses::RSTATUS_OK
            || ExpectedStatus == NKikimrKeyValue::Statuses::RSTATUS_OVERRUN)
    {
        UNIT_ASSERT(dp.Response.value() == expectedValue);
        if (size) {
            UNIT_ASSERT_VALUES_EQUAL(dp.Response.requested_size(), size);
        }
    } else {
        UNIT_ASSERT(dp.Response.value() == "");
    }
    UNIT_ASSERT_VALUES_EQUAL(dp.Response.requested_key(), key);
    UNIT_ASSERT_VALUES_EQUAL(dp.Response.requested_offset(), offset);
}


template <NKikimrKeyValue::Statuses::ReplyStatus ExpectedStatus = NKikimrKeyValue::Statuses::RSTATUS_OK>
void ExecuteReadRange(TTestContext &tc,
        const TString &from, EBorderKind fromKind,
        const TString &to, EBorderKind toKind,
        const TDeque<TKeyValuePair> &expectedPairs,
        ui64 lock_generation, bool includeData, ui64 limitBytes)
{
    TDesiredPair<TEvKeyValue::TEvReadRange> dp;

    dp.Request.set_lock_generation(lock_generation);
    dp.Request.set_include_data(includeData);
    dp.Request.set_limit_bytes(limitBytes);
    dp.Request.set_tablet_id(tc.TabletId);

    auto *r = dp.Request.mutable_range();
    switch (fromKind) {
    case EBorderKind::Include:
        r->set_from_key_inclusive(from);
        break;
    case EBorderKind::Exclude:
        r->set_from_key_exclusive(from);
        break;
    case EBorderKind::Without:
        break;
    }

    switch (toKind) {
    case EBorderKind::Include:
        r->set_to_key_inclusive(to);
        break;
    case EBorderKind::Exclude:
        r->set_to_key_exclusive(to);
        break;
    case EBorderKind::Without:
        break;
    }

    ExecuteEvent(dp, tc);
    UNIT_ASSERT_C(dp.Response.status() == ExpectedStatus,
            "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
            << " exp# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(ExpectedStatus)
            << " msg# " << dp.Response.msg());
    if constexpr (ExpectedStatus == NKikimrKeyValue::Statuses::RSTATUS_OK) {
        UNIT_ASSERT_VALUES_EQUAL(dp.Response.pair_size(), expectedPairs.size());
        for (ui32 idx = 0; idx < expectedPairs.size(); ++idx) {
            auto &pair = dp.Response.pair(idx);
            UNIT_ASSERT_C(pair.status() == NKikimrKeyValue::Statuses::RSTATUS_OK ||
                    pair.status() == NKikimrKeyValue::Statuses::RSTATUS_ERROR,
                    "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(pair.status())
                    << " msg# " << dp.Response.msg());
            UNIT_ASSERT_VALUES_EQUAL_C(pair.key(), expectedPairs[idx].Key, "msg# " << dp.Response.msg());
            UNIT_ASSERT_VALUES_EQUAL_C(pair.value(), expectedPairs[idx].Value, "msg# " << dp.Response.msg());
        }
    }
}


template <NKikimrKeyValue::Statuses::ReplyStatus ExpectedStatus = NKikimrKeyValue::Statuses::RSTATUS_OK>
void ExecuteGetStatus(TTestContext &tc, const TDeque<ui32> &channels, ui64 lock_generation) {
    TDesiredPair<TEvKeyValue::TEvGetStorageChannelStatus> dp;
    dp.Request.set_lock_generation(lock_generation);
    dp.Request.set_tablet_id(tc.TabletId);
    for (ui32 channel : channels) {
        dp.Request.add_storage_channel(channel);
    }

    ExecuteEvent(dp, tc);
    UNIT_ASSERT_C(dp.Response.status() == ExpectedStatus,
            "got# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(dp.Response.status())
            << " exp# " << NKikimrKeyValue::Statuses_ReplyStatus_Name(ExpectedStatus)
            << " msg# " << dp.Response.msg());
    if constexpr (ExpectedStatus == NKikimrKeyValue::Statuses::RSTATUS_OK) {
        for (auto &channel : dp.Response.storage_channel()) {
            UNIT_ASSERT(channel.status() == NKikimrKeyValue::Statuses::RSTATUS_OK);
        }
    }
}

void ExecuteObtainLock(TTestContext &tc, ui64 expectedLockGeneration) {
    TDesiredPair<TEvKeyValue::TEvAcquireLock> dp;
    dp.Request.set_tablet_id(tc.TabletId);
    ExecuteEvent(dp, tc);
    UNIT_ASSERT_VALUES_EQUAL(dp.Response.lock_generation(), expectedLockGeneration);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TEST CASES
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST(TestBasicWriteRead) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteWrite(tc, {{"key", "value"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRead(tc, "key", "value", 0, 0, 0);
    });
}

Y_UNIT_TEST(TestBasicWriteReadOverrun) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteWrite(tc, {{"key", "value"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ui64 limitBytes = 1 + 5 + 3 // Key id, length
                    + 1 + 5 + 1 // Value id, length, value
                    + 1 + 8 // Offset id, value
                    + 1 + 8 // Size id, value
                    + 1 + 1 // Status id, value
                    ;
        ExecuteRead<NKikimrKeyValue::Statuses::RSTATUS_OVERRUN>(tc, "key", "v", 0, 0, 0, limitBytes);
    });
}

Y_UNIT_TEST(TestWriteReadDeleteWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite("key", "value", NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {"value"}, {}, tc);
        CmdDeleteRange("key", true, "key", true, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {""}, {true}, tc);
    });
}

Y_UNIT_TEST(TestWriteReadPatchRead) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone = false;
    tc.Prepare(INITIAL_TEST_DISPATCH_NAME, [](TTestActorRuntime &){}, activeZone);
    CmdWrite("key", "value", NKikimrClient::TKeyValueRequest::MAIN,
        NKikimrClient::TKeyValueRequest::REALTIME, tc);
    CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
        {"value"}, {}, tc);
    TVector<TDiff> diffs = {TDiff{0, "m"}, TDiff{2, "t"}, TDiff{4, "r"}};
    CmdPatch("key", "key2", diffs, NKikimrClient::TKeyValueRequest::MAIN, tc);
    CmdRead({"key2"}, NKikimrClient::TKeyValueRequest::REALTIME,
        {"matur"}, {}, tc);
}

Y_UNIT_TEST(TestWriteReadDeleteWithRestartsAndCatchCollectGarbageEvents) {
    TTestContext tc;
    TMaybe<TActorId> tabletActor;
    bool firstCollect = true;
    auto setup = [&] (TTestActorRuntime &runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (tabletActor && *tabletActor == event->Recipient && event->GetTypeRewrite() == TEvBlobStorage::TEvCollectGarbageResult::EventType) {
                TestLog("CollectGarbageResult!!! ", event->Sender, "->", event->Recipient, " Cookie# ", event->Cookie);
            }
            if (tabletActor && *tabletActor == event->Sender && event->GetTypeRewrite() == TEvBlobStorage::TEvCollectGarbage::EventType) {
                TestLog("CollectGarbage!!! ", event->Sender, "->", event->Recipient, " Cookie# ", event->Cookie);
                if (!firstCollect) {
                    TestLog("Drop!");
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            if (tabletActor && *tabletActor == event->Recipient && event->GetTypeRewrite() == TEvKeyValue::TEvCollect::EventType) {
                TestLog("Collect!!! ", event->Sender, "->", event->Recipient, " Cookie# ", event->Cookie);
                if (firstCollect) {
                    TestLog("Drop!");
                    runtime.Send(new IEventHandle(event->Recipient, event->Recipient, new TKikimrEvents::TEvPoisonPill));
                    firstCollect = false;
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase& runtime, const TActorId& /*parentId*/, const TActorId& actorId) {
            if (TypeName(*runtime.FindActor(actorId)) == "NKikimr::NKeyValue::TKeyValueFlat") {
                tabletActor = actorId;
                TestLog("KV tablet was created ", actorId);
            }
        });
    };
    TFinalizer finalizer(tc);
    bool activeZone = false;
    tc.Prepare(INITIAL_TEST_DISPATCH_NAME, setup, activeZone);
    ExecuteWrite(tc, {{"key", "value"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    tc.Runtime->Send(new IEventHandle(*tabletActor, *tabletActor, new TKikimrEvents::TEvPoisonPill));
    TestLog("After the first death");
    ExecuteWrite(tc, {{"key1", "value1"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    ExecuteWrite(tc, {{"key2", "value2"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    ExecuteRead(tc, "key", "value", 0, 0, 0);
    TestLog("Before delete range");
    ExecuteDeleteRange(tc, "key", EBorderKind::Include, "key", EBorderKind::Include, 0);
    TestLog("After delete range");
    ExecuteRead<NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND>(tc, "key", "", 0, 0, 0);
    ExecuteRead(tc, "key1", "value1", 0, 0, 0);
}


Y_UNIT_TEST(TestBlockedEvGetRequest) {
    std::optional<TActorId> tabletActor;
    std::optional<TActorId> dsProxyActor;
    std::optional<ui64> keyValueTabletId;
    std::optional<ui32> keyValueTabletGeneration;
    auto setup = [&] (TTestActorRuntime &runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvBlobStorage::TEvGet::EventType) {
                if (tabletActor && *tabletActor == event->Sender) {
                    // key value tablet reads from dsproxy
                    dsProxyActor = event->Recipient;
                }

                auto readerTabletData = event->Get<TEvBlobStorage::TEvGet>()->ReaderTabletData;
                if (readerTabletData) {
                    keyValueTabletId = readerTabletData->Id;
                    keyValueTabletGeneration = readerTabletData->Generation;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase&, const TActorId& /*parentId*/, const TActorId& actorId) {
            if (TypeName(*runtime.FindActor(actorId)) == "NKikimr::NKeyValue::TKeyValueStorageReadRequest") {
                tabletActor = actorId;
            }
        });
    };
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone = false;
    tc.Prepare(INITIAL_TEST_DISPATCH_NAME, setup, activeZone);
    ExecuteWrite(tc, {{"key", "value"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    ExecuteRead(tc, "key", "value", 0, 0, 0);

    // block current generation of the key value tablet
    UNIT_ASSERT(tabletActor);
    UNIT_ASSERT(dsProxyActor);
    UNIT_ASSERT(keyValueTabletId);
    UNIT_ASSERT(keyValueTabletGeneration);
    auto generation = *keyValueTabletGeneration;
    auto ev = std::make_unique<TEvBlobStorage::TEvBlock>(*keyValueTabletId, generation, TInstant::Max());
    tc.Runtime->Send(new IEventHandle(*dsProxyActor, *tabletActor, ev.release()));

    // read with the blocked generation should fail and lead to a restart of the key value tablet
    ExecuteRead<NKikimrKeyValue::Statuses::RSTATUS_ERROR>(tc, "key", "", 0, 0, 0);
    // read data through the newly created key value tablet
    ExecuteRead(tc, "key", "value", 0, 0, 0);
    // check that the key value tablet has indeed restarted
    UNIT_ASSERT(generation < *keyValueTabletGeneration);
}

Y_UNIT_TEST(TestWriteReadDeleteWithRestartsAndCatchCollectGarbageEventsWithSlowInitialGC) {
    TTestContext tc;
    TMaybe<TActorId> tabletActor;
    TMaybe<TActorId> collectorActor;
    ui32 collectStep = 1;
    TQueue<TAutoPtr<IEventHandle>> savedInitialEvents;

    auto setup = [&] (TTestActorRuntime &runtime) {
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            //TestLog("Event ", (event && event->GetBase() ? TypeName(*event->GetBase()) : "unknown"), ' ', event->Sender, "->", event->Recipient);
            if (tabletActor && *tabletActor == event->Recipient && event->GetTypeRewrite() == TEvBlobStorage::TEvCollectGarbageResult::EventType) {
                if (collectStep == 2) {
                    savedInitialEvents.push(event);
                    TestLog("Event drop; saved intial GCresult");
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            if (tabletActor && *tabletActor == event->Recipient && event->GetTypeRewrite() == TEvKeyValue::TEvCollect::EventType) {
                switch (collectStep++) {
                    case 1: {
                        runtime.Send(new IEventHandle(event->Recipient, event->Recipient, new TKikimrEvents::TEvPoisonPill));
                        TestLog("Event drop; Collect; Tablet was poisoned");
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
            if (tabletActor && *tabletActor == parentId) {
                TestLog("CreateActor by KV ", actorId, ' ', TypeName(*runtime.FindActor(actorId)));
            }
            if (TypeName(*runtime.FindActor(actorId)) == "NKikimr::NKeyValue::TKeyValueFlat") {
                tabletActor = actorId;
            }
            if (tabletActor && *tabletActor == parentId && TypeName(*runtime.FindActor(actorId)) == "NKikimr::NKeyValue::TKeyValueCollector") {
                collectorActor = actorId;
            }
        });
    };
    TFinalizer finalizer(tc);
    bool activeZone = false;
    tc.Prepare(INITIAL_TEST_DISPATCH_NAME, setup, activeZone);
    ExecuteWrite(tc, {{"key", "value"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    tc.Runtime->Send(new IEventHandle(*tabletActor, *tabletActor, new TKikimrEvents::TEvPoisonPill));
    ExecuteWrite(tc, {{"key1", "value1"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    ExecuteWrite(tc, {{"key2", "value2"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    ExecuteRead(tc, "key", "value", 0, 0, 0);
    ExecuteDeleteRange(tc, "key", EBorderKind::Include, "key", EBorderKind::Include, 0);
    ExecuteWrite(tc, {{"key3", "value2"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);


    TDispatchOptions options2;
    options2.FinalEvents.push_back(TEvKeyValue::TEvCollect::EventType);

    bool onlyOneCollect = false;
    try {
        TestLog("Second dispatch");
        tc.Runtime->DispatchEvents(options2);
    } catch (NActors::TSchedulingLimitReachedException) {
        onlyOneCollect = true;
        TestLog("Exception was catch");
    }

    while (savedInitialEvents.size()) {
        tc.Runtime->Send(savedInitialEvents.front().Release());
        savedInitialEvents.pop();
    }

    TestLog("Third dispatch ", collectStep);
    UNIT_ASSERT_VALUES_EQUAL(collectStep, 2);

    ExecuteWrite(tc, {{"key4", "value2"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    ExecuteWrite(tc, {{"key5", "value2"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    ExecuteWrite(tc, {{"key6", "value2"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);

    UNIT_ASSERT_VALUES_EQUAL(collectStep, 3);
    UNIT_ASSERT(onlyOneCollect);
}


Y_UNIT_TEST(TestWriteReadDeleteWithRestartsThenResponseOkWithNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteWrite(tc, {{"key", "value"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRead(tc, "key", "value", 0, 0, 0);
        ExecuteDeleteRange(tc, "key", EBorderKind::Include, "key", EBorderKind::Include, 0);
        ExecuteRead<NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND>(tc, "key", "", 0, 0, 0);
    });
}


Y_UNIT_TEST(TestRewriteThenLastValue) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TVector<NKikimrClient::TKeyValueRequest::EStorageChannel> channels =
            {NKikimrClient::TKeyValueRequest::MAIN, NKikimrClient::TKeyValueRequest::INLINE};
        for (auto &ch1 : channels) {
            for (auto &ch2 : channels) {
                CmdWrite("key", "value", ch1,
                    NKikimrClient::TKeyValueRequest::REALTIME, tc);
                CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
                    {"value"}, {}, tc);
                CmdWrite("key", "updated", ch2,
                    NKikimrClient::TKeyValueRequest::REALTIME, tc);
                CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
                    {"updated"}, {}, tc);
            }
        }
    });
}


Y_UNIT_TEST(TestRewriteThenLastValueNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TVector<ui32> channels = {0, 1};
        for (auto &ch1 : channels) {
            for (auto &ch2 : channels) {
                ExecuteWrite(tc, {{"key", "value"}}, 0, ch1, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
                ExecuteRead(tc, "key", "value", 0, 0, 0);
                ExecuteWrite(tc, {{"key", "updated"}}, 0, ch2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
                ExecuteRead(tc, "key", "updated", 0, 0, 0);
            }
        }
    });
}


Y_UNIT_TEST(TestWriteTrimWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite("key", "value", NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
            try {
                THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
                auto trim = request->Record.MutableCmdTrimLeakedBlobs();
                trim->SetMaxItemsToTrim(100);
                tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
                TAutoPtr<IEventHandle> handle;
                TEvKeyValue::TEvResponse *result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
                UNIT_ASSERT(result);
                UNIT_ASSERT(result->Record.HasStatus());
                UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
                retriesLeft = 0;
            } catch (NActors::TSchedulingLimitReachedException) {
                UNIT_ASSERT(retriesLeft == 2);
            }
        }

    });
}

Y_UNIT_TEST(TestIncorrectRequestThenResponseError) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        activeZone = false;

        TAutoPtr<IEventHandle> handle;
        TEvKeyValue::TEvResponse *result;
        THolder<TEvKeyValue::TEvRequest> request;
        try {
            tc.Runtime->ResetScheduledCount();
            request.Reset(new TEvKeyValue::TEvRequest);
            auto write = request->Record.AddCmdWrite();
            //write->SetKey("k");
            write->SetValue("v");
            write->SetStorageChannel(NKikimrClient::TKeyValueRequest::MAIN);
            write->SetPriority(NKikimrClient::TKeyValueRequest::REALTIME);
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_INTERNALERROR);

            request.Reset(new TEvKeyValue::TEvRequest);
            auto write2 = request->Record.AddCmdWrite();
            write2->SetKey("k");
            //write->SetValue("v");
            write2->SetStorageChannel(NKikimrClient::TKeyValueRequest::MAIN);
            write2->SetPriority(NKikimrClient::TKeyValueRequest::REALTIME);
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            result = tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_INTERNALERROR);

            CmdRename("2", "3", tc, false);

        } catch (NActors::TSchedulingLimitReachedException) {
            UNIT_ASSERT(false);
        }

    });
}

Y_UNIT_TEST(TestInlineWriteReadDeleteWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite("key", "value", NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {"value"}, {}, tc);
        CmdDeleteRange("key", true, "key", true, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {""}, {true}, tc);
    });
}


Y_UNIT_TEST(TestInlineWriteReadDeleteWithRestartsThenResponseOkNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteWrite(tc, {{"key", "value"}}, 0, 1, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRead(tc, "key", "value", 0, 0, 0);
        ExecuteDeleteRange(tc, "key", EBorderKind::Include, "key", EBorderKind::Include, 0);
        ExecuteRead<NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND>(tc, "key", "", 0, 0, 0);
    });
}


Y_UNIT_TEST(TestWrite200KDeleteThenResponseError) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        tc.Runtime->GetAppData(0).AllowHugeKeyValueDeletes = false;
        tc.Runtime->SetDispatchedEventsLimit(10'000'0000);
        activeZone = false;
        const ui32 BatchCount = 60;
        const ui32 ItemsPerBatch = 2048;
        for (ui32 i = 0; i < BatchCount; ++i) {
            TDeque<TString> keys;
            TDeque<TString> values;
            for (ui32 j = 0; j < ItemsPerBatch; ++j) {
                TString key = Sprintf("k%08" PRIu32, i * ItemsPerBatch + j);
                keys.push_back(key);
                values.push_back(TString("v"));
            }
            CmdWrite(keys, values, //NKikimrClient::TKeyValueRequest::INLINE,
                    NKikimrClient::TKeyValueRequest::MAIN,
                    NKikimrClient::TKeyValueRequest::BACKGROUND, tc); // Background is selected to increase coverage
        }
        CmdDeleteRange("a", true, "z", true, tc, NMsgBusProxy::MSTATUS_INTERNALERROR);
    });
}


Y_UNIT_TEST(TestWrite200KDeleteThenResponseErrorNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        tc.Runtime->GetAppData(0).AllowHugeKeyValueDeletes = false;
        tc.Runtime->SetDispatchedEventsLimit(10'000'0000);
        activeZone = false;
        const ui32 BatchCount = 60;
        const ui32 ItemsPerBatch = 2048;
        for (ui32 i = 0; i < BatchCount; ++i) {
            TDeque<TKeyValuePair> pairs;
            for (ui32 j = 0; j < ItemsPerBatch; ++j) {
                TString key = Sprintf("k%08" PRIu32, i * ItemsPerBatch + j);
                pairs.push_back({key, TString("v")});
            }
            ExecuteWrite(tc, pairs, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_BACKGROUND);
        }
        ExecuteDeleteRange<false>(tc, "a", EBorderKind::Include, "z", EBorderKind::Include, 0);
    });
}


/*Y_UNIT_TEST(TestWrite16MillionReadDeleteWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TInstant startTime = TInstant::Now();
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        activeZone = false;
        const ui32 BatchCount = 4;
        //const ui32 ItemsPerBatch = 2048000;
        const ui32 ItemsPerBatch = 2048;
        for (ui32 i = 0; i < BatchCount; ++i) {
            Cerr << "i=" << i << " Duration=" << (TInstant::Now() - startTime).Seconds() << Endl;
            TDeque<TString> keys;
            TDeque<TString> values;
            for (ui32 j = 0; j < ItemsPerBatch; ++j) {
                TString key = Sprintf("k%08" PRIu32, i * ItemsPerBatch + j);
                keys.push_back(key);
                values.push_back(TString("v"));
            }
            CmdWrite(keys, values, //NKikimrClient::TKeyValueRequest::INLINE,
                    NKikimrClient::TKeyValueRequest::MAIN,
                    NKikimrClient::TKeyValueRequest::REALTIME, tc);
        }
        //activeZone = true;
//        CmdRead({"k00100500"}, NKikimrClient::TKeyValueRequest::REALTIME,
//            {"v"}, {}, tc);
//        CmdDeleteRange("a", true, "z", true, tc, NMsgBusProxy::MSTATUS_INTERNALERROR);
        ui32 deleteBatchSize = 200;
        ui32 deleteBatchCount = (BatchCount * ItemsPerBatch + deleteBatchSize - 1) / deleteBatchSize;
        for (ui32 batch = 0; batch < deleteBatchCount; ++batch) {
            Cerr << "batch=" << batch << " Duration=" << (TInstant::Now() - startTime).Seconds() << Endl;
            TString k1 = Sprintf("k%08" PRIu32, batch * deleteBatchSize);
            TString k2 = Sprintf("k%08" PRIu32, (batch + 1) * deleteBatchSize);
            CmdDeleteRange(k1, true, k2, false, tc);
        }

        CmdRead({"k00100500"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {""}, {true}, tc);
    });
}
*/

Y_UNIT_TEST(TestWriteReadWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TDeque<TString> keys;
        TDeque<TString> values;
        {
            TStringStream value;
            for (ui32 itemIdx = 0; itemIdx <= 4; ++itemIdx) {
                value << "x";
                keys.push_back(Sprintf("key%" PRIu32, itemIdx));
                values.push_back(value.Str());
            }
        }
        CmdWrite(keys, values, NKikimrClient::TKeyValueRequest::EXTRA9,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        for (ui32 itemIdx = 1; itemIdx < 4; ++itemIdx) {
            expectedKeys.push_back(keys[itemIdx]);
            expectedValues.push_back(values[itemIdx]);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("key1", true, "key4", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("key0", false, "key4", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("key0", false, "key3", true, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


Y_UNIT_TEST(TestWriteReadWithRestartsThenResponseOkNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TDeque<TKeyValuePair> pairs;
        {
            TStringBuilder value;
            for (ui32 itemIdx = 0; itemIdx <= 4; ++itemIdx) {
                value << "x";
                pairs.push_back({Sprintf("key%" PRIu32, itemIdx), TString(value)});
            }
        }

        ExecuteWrite(tc, pairs, 0, 11, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);

        TDeque<TKeyValuePair> expectedPairs;
        for (ui32 itemIdx = 1; itemIdx < 4; ++itemIdx) {
            expectedPairs.push_back(pairs[itemIdx]);
        }

        ExecuteReadRange(tc, "key1", EBorderKind::Include, "key4", EBorderKind::Exclude, expectedPairs, 0, true, 0);
        ExecuteReadRange(tc, "key0", EBorderKind::Exclude, "key4", EBorderKind::Exclude, expectedPairs, 0, true, 0);
        ExecuteReadRange(tc, "key0", EBorderKind::Exclude, "key3", EBorderKind::Include, expectedPairs, 0, true, 0);
        ExecuteReadRange(tc, "key1", EBorderKind::Include, "key3", EBorderKind::Include, expectedPairs, 0, true, 0);
    });
}


Y_UNIT_TEST(TestInlineWriteReadWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        TDeque<TString> keys;
        TDeque<TString> values;
        tc.Prepare(dispatchName, setup, activeZone);
        {
            TStringStream value;
            for (ui32 itemIdx = 0; itemIdx <= 4; ++itemIdx) {
                TDeque<TString> keys1;
                TDeque<TString> values1;
                value << "x";
                keys1.push_back(Sprintf("key%" PRIu32, itemIdx));
                keys.push_back(Sprintf("key%" PRIu32, itemIdx));
                values1.push_back(value.Str());
                values.push_back(value.Str());
                CmdWrite(keys1, values1,
                    itemIdx % 2 == 0 ?
                        NKikimrClient::TKeyValueRequest::INLINE :
                        NKikimrClient::TKeyValueRequest::MAIN,
                    NKikimrClient::TKeyValueRequest::REALTIME, tc);
            }
        }

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        for (ui32 itemIdx = 1; itemIdx < 4; ++itemIdx) {
            expectedKeys.push_back(keys[itemIdx]);
            expectedValues.push_back(values[itemIdx]);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("key1", true, "key4", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("key0", false, "key4", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("key0", false, "key3", true, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


Y_UNIT_TEST(TestInlineWriteReadWithRestartsThenResponseOkNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TDeque<TKeyValuePair> expectedPairs;
        {
            TStringBuilder value;
            for (ui32 itemIdx = 0; itemIdx <= 4; ++itemIdx) {
                value << "x";
                TKeyValuePair pair{Sprintf("key%" PRIu32, itemIdx), TString(value)};
                if (itemIdx && itemIdx < 4) {
                    expectedPairs.push_back(pair);
                }
                ExecuteWrite(tc, {pair}, 0, itemIdx % 2 == 0, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
            }
        }

        ExecuteReadRange(tc, "key1", EBorderKind::Include, "key4", EBorderKind::Exclude, expectedPairs, 0, true, 0);
        ExecuteReadRange(tc, "key0", EBorderKind::Exclude, "key4", EBorderKind::Exclude, expectedPairs, 0, true, 0);
        ExecuteReadRange(tc, "key0", EBorderKind::Exclude, "key3", EBorderKind::Include, expectedPairs, 0, true, 0);
        ExecuteReadRange(tc, "key1", EBorderKind::Include, "key3", EBorderKind::Include, expectedPairs, 0, true, 0);
    });
}

Y_UNIT_TEST(TestInlineWriteReadWithRestartsWithNotCorrectUTF8NewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);

        TDeque<TKeyValuePair> expectedPairs;
        TKeyValuePair pair{TString("key1\0"), TString("value")};
        expectedPairs.push_back({TString("key1\\0"), TString()});
        ExecuteWrite(tc, {pair}, 0, 1, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        expectedPairs.clear();
        ExecuteReadRange<NKikimrKeyValue::Statuses::RSTATUS_OK>(tc, "key0", EBorderKind::Include,
                "key1", EBorderKind::Exclude, expectedPairs, 0, true, 0);
    });
}


Y_UNIT_TEST(TestEmptyWriteReadDeleteWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite("key", "", NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {""}, {}, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        expectedKeys.push_back("key");
        expectedValues.push_back("");
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("a", true, "z", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }

        CmdDeleteRange("key", true, "key", true, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {""}, {true}, tc);
    });
}


Y_UNIT_TEST(TestEmptyWriteReadDeleteWithRestartsThenResponseOkNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TDeque<TKeyValuePair> pairs;
        pairs.push_back({"key", ""});
        ExecuteWrite(tc, pairs, 0, 0, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRead(tc, "key", "", 0, 0, 0);

        ExecuteReadRange(tc, "", EBorderKind::Without, "", EBorderKind::Without, pairs, 0, true, 0);
        ExecuteDeleteRange(tc, "key", EBorderKind::Include, "key", EBorderKind::Include, 0);
        ExecuteRead<NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND>(tc, "key", "", 0, 0, 0);
    });
}


Y_UNIT_TEST(TestInlineEmptyWriteReadDeleteWithRestartsThenResponseOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite("key", "", NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {""}, {}, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        expectedKeys.push_back("key");
        expectedValues.push_back("");
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("a", true, "z", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }

        CmdDeleteRange("key", true, "key", true, tc);
        CmdRead({"key"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {""}, {true}, tc);
    });
}


Y_UNIT_TEST(TestInlineEmptyWriteReadDeleteWithRestartsThenResponseOkNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TDeque<TKeyValuePair> pairs;
        pairs.push_back({"key", ""});
        ExecuteWrite(tc, pairs, 0, 1, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRead(tc, "key", "", 0, 0, 0);

        ExecuteReadRange(tc, "", EBorderKind::Without, "", EBorderKind::Without, pairs, 0, true, 0);
        ExecuteDeleteRange(tc, "key", EBorderKind::Include, "key", EBorderKind::Include, 0);
        ExecuteRead<NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND>(tc, "key", "", 0, 0, 0);
    });
}


TString PrepareData(ui32 size, ui32 flavor) {
    TString data = TString::Uninitialized(size);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = '0' + (i + size + flavor) % 8;
    }
    return data;
}


Y_UNIT_TEST(TestWriteReadRangeLimitThenLimitWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 1000;
        TDeque<TString> keys;
        TDeque<TString> values;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            keys.push_back(Sprintf("k%07" PRIu32, itemIdx));
            values.push_back(PrepareData(10000, itemIdx));
        }
        CmdWrite(keys, values, NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        ui64 totalSize = 0;
        ui64 sizeLimit = 200;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            totalSize += keys[itemIdx].size() + 32;
            if (totalSize > sizeLimit) {
                break;
            }
            expectedKeys.push_back(keys[itemIdx]);
        }
        Y_ABORT_UNLESS(expectedKeys.size());

        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("k0000000", true, "k9999999", true, false, sizeLimit,
                    NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OVERRUN, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("k0000000", true, "k9999999", true, false, sizeLimit,
                    NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OVERRUN, tc, dp);
            AddCmdReadRange("k0000000", true, "k9999999", true, false, sizeLimit,
                    NKikimrClient::TKeyValueRequest::REALTIME, {}, {}, NKikimrProto::OVERRUN, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("k0000000", true, expectedKeys[expectedKeys.size() - 1], true, false, sizeLimit,
                    NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
            AddCmdReadRange("k0000000", true, "k9999999", true, false, sizeLimit,
                    NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OVERRUN, tc, dp);
            AddCmdReadRange("k0000000", true, "k9999999", true, false, sizeLimit,
                    NKikimrClient::TKeyValueRequest::REALTIME, {}, {}, NKikimrProto::OVERRUN, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


Y_UNIT_TEST(TestWriteReadRangeLimitThenLimitWorksNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 1000;
        TDeque<TKeyValuePair> pairs;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            pairs.push_back({Sprintf("k%07" PRIu32, itemIdx), PrepareData(10000, itemIdx)});
        }
        ExecuteWrite(tc, pairs, 0, 0, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);

        TDeque<TKeyValuePair> expectedPairs;
        ui64 totalSize = 0;
        ui64 sizeLimit = 200;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            totalSize += pairs[itemIdx].Key.size() + 32;
            if (totalSize > sizeLimit) {
                break;
            }
            expectedPairs.push_back({pairs[itemIdx].Key, ""});
        }
        Y_ABORT_UNLESS(expectedPairs.size());

        ExecuteReadRange(tc, "", EBorderKind::Without,
                expectedPairs[expectedPairs.size() - 1].Key, EBorderKind::Include,
                expectedPairs, 0, false, sizeLimit);
        ExecuteReadRange<NKikimrKeyValue::Statuses::RSTATUS_OVERRUN>(tc,
                "", EBorderKind::Without, "", EBorderKind::Without,
                expectedPairs, 0, false, sizeLimit);
    });
}


Y_UNIT_TEST(TestWriteReadRangeDataLimitThenLimitWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 10000;
        TDeque<TString> keys;
        TDeque<TString> values;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            keys.push_back(Sprintf("k%07" PRIu32, itemIdx));
            values.push_back(PrepareData(8, itemIdx));
        }
        CmdWrite(keys, values, NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        ui64 totalSize = 0;
        ui64 sizeLimit = 200;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            totalSize += values[itemIdx].size() + keys[itemIdx].size() + 32;
            if (totalSize > sizeLimit) {
                break;
            }
            expectedKeys.push_back(keys[itemIdx]);
            expectedValues.push_back(values[itemIdx]);
        }
        Y_ABORT_UNLESS(expectedKeys.size());

        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("k0000000", true, "k9999999", true, true, sizeLimit,
                NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OVERRUN, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("k0000000", true, "k9999999", true, true, sizeLimit,
                NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OVERRUN, tc, dp);
            AddCmdReadRange("k0000000", true, "k9999999", true, true, sizeLimit,
                NKikimrClient::TKeyValueRequest::REALTIME, {}, {}, NKikimrProto::OVERRUN, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


void MakeTestWriteReadRangeDataLimitThenLimitWorksNewApi(ui32 storageChannel) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 1000;
        TDeque<TKeyValuePair> pairs;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            pairs.push_back({Sprintf("k%07" PRIu32, itemIdx), PrepareData(8, itemIdx)});
        }
        ExecuteWrite(tc, pairs, 0, storageChannel, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);

        TDeque<TKeyValuePair> expectedPairs;
        ui64 totalSize = 0;
        ui64 sizeLimit = 200;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            totalSize += pairs[itemIdx].Key.size() + pairs[itemIdx].Value.size() + 32;
            if (totalSize > sizeLimit) {
                break;
            }
            expectedPairs.push_back(pairs[itemIdx]);
        }

        ExecuteReadRange(tc, "", EBorderKind::Without,
                expectedPairs[expectedPairs.size() - 1].Key, EBorderKind::Include,
                expectedPairs, 0, true, sizeLimit);
        ExecuteReadRange<NKikimrKeyValue::Statuses::RSTATUS_OVERRUN>(tc,
                "", EBorderKind::Without, "", EBorderKind::Without,
                expectedPairs, 0, true, sizeLimit);
    });
}

Y_UNIT_TEST(TestWriteReadRangeDataLimitThenLimitWorksNewApi) {
    MakeTestWriteReadRangeDataLimitThenLimitWorksNewApi(0);
}


Y_UNIT_TEST(TestInlineWriteReadRangeLimitThenLimitWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 1000;
        TDeque<TString> keys;
        TDeque<TString> values;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            keys.push_back(Sprintf("k%07" PRIu32, itemIdx));
            values.push_back(PrepareData(8, itemIdx));
        }
        CmdWrite(keys, values, NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        ui64 totalSize = 0;
        ui64 sizeLimit = 200;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            totalSize += values[itemIdx].size() + keys[itemIdx].size() + 32;
            if (totalSize > sizeLimit) {
                break;
            }
            expectedKeys.push_back(keys[itemIdx]);
            expectedValues.push_back(values[itemIdx]);
        }
        Y_ABORT_UNLESS(expectedKeys.size());

        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("k0000000", true, "k9999999", true, true, sizeLimit,
                NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OVERRUN, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("k0000000", true, "k9999999", true, true, sizeLimit,
                NKikimrClient::TKeyValueRequest::REALTIME, expectedKeys, expectedValues, NKikimrProto::OVERRUN, tc, dp);
            AddCmdReadRange("k0000000", true, "k9999999", true, true, sizeLimit,
                NKikimrClient::TKeyValueRequest::REALTIME, {}, {}, NKikimrProto::OVERRUN, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


Y_UNIT_TEST(TestInlineWriteReadRangeLimitThenLimitWorksNewApi) {
    MakeTestWriteReadRangeDataLimitThenLimitWorksNewApi(1);
}


Y_UNIT_TEST(TestCopyRangeWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 100;
        TDeque<TString> keys;
        TDeque<TString> values;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            keys.push_back(Sprintf("k%07" PRIu32, itemIdx));
            values.push_back(Sprintf("v%07" PRIu32, (ui32)itemIdx));
        }
        CmdWrite(keys, values, NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        CmdCopyRange("", false, "~", false, "p", "", tc);
        CmdWrite("0", "0", NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            expectedKeys.push_back(Sprintf("pk%07" PRIu32, itemIdx));
            expectedValues.push_back(Sprintf("v%07" PRIu32, (ui32)itemIdx));
        }
        TDesiredPair<TEvKeyValue::TEvRequest> dp;
        AddCmdReadRange("p", false, "q", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
            expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
        RunRequest(dp, tc, __LINE__);
    });
}


void MakeTestCopyRangeWorksNewApi(ui64 storageChannel) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 100;
        TDeque<TKeyValuePair> pairs;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            pairs.push_back({Sprintf("k%07" PRIu32, itemIdx), Sprintf("v%07" PRIu32, (ui32)itemIdx)});
        }
        ExecuteWrite(tc, pairs, 0, storageChannel, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);

        ExecuteCopyRange(tc, "", EBorderKind::Without, "", EBorderKind::Without, 0, "p", "");
        ExecuteWrite(tc, {{"0", "0"}}, 0, 0, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;

        TDeque<TKeyValuePair> expectedPairs;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            expectedPairs.push_back({Sprintf("pk%07" PRIu32, itemIdx), Sprintf("v%07" PRIu32, (ui32)itemIdx)});
        }
        ExecuteReadRange(tc, "p", EBorderKind::Exclude, "q", EBorderKind::Exclude,
                expectedPairs, 0, true, 0);
    });
}


Y_UNIT_TEST(TestCopyRangeWorksNewApi) {
    MakeTestCopyRangeWorksNewApi(0);
}


Y_UNIT_TEST(TestInlineCopyRangeWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        const ui32 itemCount = 100;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ) {
            TDeque<TString> keys;
            TDeque<TString> values;
            ui32 endItemIdx = Min(itemCount, itemIdx + 20);
            for (; itemIdx < endItemIdx; ++itemIdx) {
                keys.push_back(Sprintf("k%07" PRIu32, itemIdx));
                values.push_back(Sprintf("v%07" PRIu32, (ui32)itemIdx));
            }
            CmdWrite(keys, values,
                    ((itemIdx / 20) % 2 == 0) ?
                        NKikimrClient::TKeyValueRequest::INLINE :
                        NKikimrClient::TKeyValueRequest::INLINE,
                    NKikimrClient::TKeyValueRequest::REALTIME, tc);
        }

        CmdCopyRange("", false, "~", false, "p", "", tc);
        CmdWrite("0", "0", NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);

        TDeque<TString> expectedKeys;
        TDeque<TString> expectedValues;
        for (ui32 itemIdx = 0; itemIdx < itemCount; ++itemIdx) {
            expectedKeys.push_back(Sprintf("pk%07" PRIu32, itemIdx));
            expectedValues.push_back(Sprintf("v%07" PRIu32, (ui32)itemIdx));
        }
        TDesiredPair<TEvKeyValue::TEvRequest> dp;
        AddCmdReadRange("p", false, "q", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
            expectedKeys, expectedValues, NKikimrProto::OK, tc, dp);
        RunRequest(dp, tc, __LINE__);
    });
}


Y_UNIT_TEST(TestInlineCopyRangeWorksNewApi) {
    MakeTestCopyRangeWorksNewApi(1);
}


Y_UNIT_TEST(TestConcatWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite({"1", "2", "3", "4"}, {"hello", ", ", "world", "!"}, NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdConcat({"1", "2", "3", "4"}, "5", false, tc);
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("1", true, "5", true, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                    {"5"}, {"hello, world!"}, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
        CmdConcat({"5", "5"}, "5", true, tc);
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("1", true, "5", true, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                {"5"}, {"hello, world!hello, world!"}, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


Y_UNIT_TEST(TestConcatWorksNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        TDeque<TKeyValuePair> pairs = {
            {"1", "hello"},
            {"2", ", "},
            {"3", "world"},
            {"4", "!"},
        };
        ExecuteWrite(tc, pairs, 0, 0, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteConcat(tc, "5", {"1", "2", "3", "4"}, 0, false);
        ExecuteReadRange(tc, "1", EBorderKind::Include, "5", EBorderKind::Include, {{"5", "hello, world!"}}, 0, true, 0);
        ExecuteConcat(tc, "5", {"5", "5"}, 0, true);
        ExecuteReadRange(tc, "1", EBorderKind::Include, "5", EBorderKind::Include,
                {{"5", "hello, world!hello, world!"}}, 0, true, 0);
    });
}


Y_UNIT_TEST(TestRenameWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite({"1", "2", "3", "4"}, {"123", "456", "789", "012"}, NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdRename("2", "3", tc);
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("1", true, "3", true, true, 1000, NKikimrClient::TKeyValueRequest::REALTIME,
                {"1", "3"}, {"123", "456"}, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
   });
}


Y_UNIT_TEST(TestRenameWorksNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);

        TDeque<TKeyValuePair> pairs = {
            {"1", "123"},
            {"2", "456"},
            {"3", "789"},
            {"4", "012"},
        };
        ExecuteWrite(tc, pairs, 0, 0, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRename(tc, {{"2", "3"}}, 0);
        ExecuteReadRange(tc, "1", EBorderKind::Include, "3", EBorderKind::Include,
                {{"1", "123"}, {"3", "456"}}, 0, true, 0);
   });
}


Y_UNIT_TEST(TestWriteToExtraChannelThenReadMixedChannelsReturnsOk) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite("key1", "value1", NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdWrite("key2", "value2", NKikimrClient::TKeyValueRequest::EXTRA,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdWrite("key3", "value3", NKikimrClient::TKeyValueRequest::MAIN,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdRead({"key1", "key2", "key3"}, NKikimrClient::TKeyValueRequest::REALTIME,
            {"value1", "value2", "value3"}, {}, tc);
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("key1", true, "key3", true, true, 1000, NKikimrClient::TKeyValueRequest::REALTIME,
                {"key1", "key2", "key3"}, {"value1", "value2", "value3"}, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


Y_UNIT_TEST(TestWriteToExtraChannelThenReadMixedChannelsReturnsOkNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);

        ExecuteWrite(tc, {{"key1", "value1"}}, 0, 1, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteWrite(tc, {{"key2", "value2"}}, 0, 3, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteWrite(tc, {{"key3", "value3"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteReadRange(tc, "key1", EBorderKind::Include, "key3", EBorderKind::Include,
                {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}}, 0, true, 0);
    });
}


Y_UNIT_TEST(TestIncrementalKeySet) {
    // generate initial key set
    TSet<TString> keys;
    for (ui32 i = 0; i < 100; ++i) {
        keys.insert(Sprintf("%04d", i));
    }

    // fill index to match those keys
    TMap<TString, NKeyValue::TIndexRecord> index;
    for (const TString& key : keys) {
        index[key];
    }

    TReallyFastRng32 rng(1);

    NKeyValue::TKeyValueState::TIncrementalKeySet incr(index);
    for (ui32 iteration = 0; iteration < 1500; ++iteration) {
        // compare
        auto iter = keys.begin();
        auto iiter = incr.begin();
        while (iter != keys.end() && iiter != incr.end()) {
            UNIT_ASSERT_VALUES_EQUAL(*iter, *iiter);
            ++iter;
            ++iiter;
        }
        UNIT_ASSERT_EQUAL(iter, keys.end());
        UNIT_ASSERT_EQUAL(iiter, incr.end());

        // find
        for (const TString& key : keys) {
            auto iiter = incr.find(key);
            UNIT_ASSERT_UNEQUAL(iiter, incr.end());
            UNIT_ASSERT_VALUES_EQUAL(key, *iiter);
        }

        // lower_bound
        for (ui32 i = 0; i < 150; ++i) {
            TString key = Sprintf("%04d", i);
            auto iter = keys.lower_bound(key);
            auto iiter = incr.lower_bound(key);
            if (iter == keys.end()) {
                UNIT_ASSERT_EQUAL(iiter, incr.end());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(*iiter, *iter);
            }
        }

        // same for upper_bound
        for (ui32 i = 0; i < 150; ++i) {
            TString key = Sprintf("%04d", i);
            auto iter = keys.upper_bound(key);
            auto iiter = incr.upper_bound(key);
            if (iter == keys.end()) {
                UNIT_ASSERT_EQUAL(iiter, incr.end());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(*iiter, *iter);
            }
        }

        if (!keys.empty() && rng() % 2 == 1) {
            // generate delete command
            ui32 index = rng() % keys.size();
            TSet<TString>::iterator iter = keys.begin();
            std::advance(iter, index);
            TString key = *iter;
            auto iiter = incr.find(key);
            UNIT_ASSERT_UNEQUAL(iiter, incr.end());
            UNIT_ASSERT_VALUES_EQUAL(*iiter, key);
            keys.erase(iter);
            incr.erase(iiter);
        } else {
            // generate insert command
            TString key = Sprintf("%04d", rng() % 150);
            keys.insert(key);
            incr.insert(key);
        }
    }
}

Y_UNIT_TEST(TestGetStatusWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdGetStatus(NKikimrClient::TKeyValueRequest::MAIN,
            ui32(NKikimrBlobStorage::StatusIsValid), tc);
        CmdGetStatus(NKikimrClient::TKeyValueRequest::INLINE,
            ui32(NKikimrBlobStorage::StatusIsValid), tc);
   });
}

Y_UNIT_TEST(TestGetStatusWorksNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteGetStatus(tc, {1, 2}, 0);
   });
}

Y_UNIT_TEST(TestWriteReadWhileWriteWorks) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdWrite("key1", "value1", NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdWrite("key2", "value2", NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        CmdWrite("key3", "value3", NKikimrClient::TKeyValueRequest::INLINE,
            NKikimrClient::TKeyValueRequest::REALTIME, tc);
        activeZone = false;
        // Send huge Write + Read
        for (ui32 n = 1; n <= 20; ++n) {
            tc.Runtime->ResetScheduledCount();
            THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
            auto write = request->Record.AddCmdWrite();
            TStringStream str;
            str << "value4.";
            for (ui32 i = 0; i < (8ul << n); ++i) {
            str << "x";
            }
            TString value = str.Str();
            write->SetKey("key4");
            write->SetValue(value);
            write->SetStorageChannel(n == 0 ?
                    NKikimrClient::TKeyValueRequest::INLINE :
                    NKikimrClient::TKeyValueRequest::MAIN);
            write->SetPriority(NKikimrClient::TKeyValueRequest::REALTIME);
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
        }

        ExecuteRead<>(tc, "key2", "value2", 0, 0, 0);
    });
}

Y_UNIT_TEST(TestSetExecutorFastLogPolicy) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        CmdSetExecutorFastLogPolicy(true, tc);
        CmdSetExecutorFastLogPolicy(false, tc);
        CmdSetExecutorFastLogPolicy(true, tc);
   });
}

Y_UNIT_TEST(TestWriteDeleteThenReadRemaining) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        activeZone = false;
        const ui32 BatchCount = 5;
        const ui32 ItemsPerBatch = 50;
        TDeque<TString> allKeys;
        TDeque<TString> allValues;
        for (ui32 i = 0; i < BatchCount; ++i) {
            TDeque<TString> batchKeys;
            TDeque<TString> batchValues;
            for (ui32 j = 0; j < ItemsPerBatch; ++j) {
                TString key = Sprintf("k%08" PRIu32, i * ItemsPerBatch + j);
                TString val = Sprintf("v%08" PRIu32, i * ItemsPerBatch + j);
                batchKeys.push_back(key);
                batchValues.push_back(val);
                allKeys.push_back(key);
                allValues.push_back(val);
            }
            CmdWrite(batchKeys, batchValues,
                    NKikimrClient::TKeyValueRequest::INLINE,
                    NKikimrClient::TKeyValueRequest::REALTIME, tc);
        }
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("a", true, "z", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                allKeys, allValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }

        ui32 deleteBound = 50; // just for speedup

        TReallyFastRng32 rng(1);
        TDeque<TString> aliveKeys;
        TDeque<TString> aliveValues;
        while (!allKeys.empty()) {
            TString key = allKeys.front();
            TString val = allValues.front();
            allKeys.pop_front();
            allValues.pop_front();
            if (deleteBound && rng() % 2) {
                CmdDeleteRange(key, true, key, true, tc);  // one key per delete
                CmdRead({key}, NKikimrClient::TKeyValueRequest::REALTIME, {""}, {true}, tc);
                deleteBound--;
            } else {
                aliveKeys.push_back(key);
                aliveValues.push_back(val);
            }
        }
        // Cerr << "total keys: " << BatchCount * ItemsPerBatch << ", after deletes: " << aliveKeys.size() << Endl;
        activeZone = true;
        {
            TDesiredPair<TEvKeyValue::TEvRequest> dp;
            AddCmdReadRange("a", true, "z", false, true, Max<ui64>(), NKikimrClient::TKeyValueRequest::REALTIME,
                aliveKeys, aliveValues, NKikimrProto::OK, tc, dp);
            RunRequest(dp, tc, __LINE__);
        }
    });
}


Y_UNIT_TEST(TestObtainLockNewApi) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        constexpr NKikimrKeyValue::Statuses::ReplyStatus status = NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION;
        ExecuteObtainLock(tc, 1);

        ExecuteGetStatus<status>(tc, {1, 2}, 0);
        ExecuteWrite<status>(tc, {{"key", "value"}}, 0, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRead<status>(tc, "key", "value", 0, 0, 0);
        ExecuteReadRange<status>(tc, "key", EBorderKind::Include, "key", EBorderKind::Include,
                {{"key", "value"}}, 0, true, 0);

        ExecuteGetStatus(tc, {1, 2}, 1);
        ExecuteWrite(tc, {{"key", "value"}}, 1, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
   });
}


Y_UNIT_TEST(TestLargeWriteAndDelete) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteObtainLock(tc, 1);
        ui32 iteration = 0;
        TDeque<TKeyValuePair> keys;
        for (ui32 idx = 0; idx < 15'000; ++idx) {
            TString key = TStringBuilder() << iteration << ':' << idx;
            keys.push_back({key, ""});
        }
        ExecuteWrite(tc, keys, 1, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteDeleteRange(tc, "", EBorderKind::Without, "", EBorderKind::Without, 1);
   });
}

Y_UNIT_TEST(TestWriteLongKey) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteObtainLock(tc, 1);

        TDeque<TKeyValuePair> keys;
        keys.push_back({TString{10_KB, '_'}, ""});

        ExecuteWrite<NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR>(tc, keys, 1, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    });
}

Y_UNIT_TEST(TestRenameToLongKey) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteObtainLock(tc, 1);

        TDeque<TKeyValuePair> keys;
        keys.push_back({"oldKey", ""});

        ExecuteWrite(tc, keys, 1, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteRename<NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR>(tc, { {"oldKey", TString{10_KB, '_'}} }, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
    });
}

Y_UNIT_TEST(TestCopyRangeToLongKey) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteObtainLock(tc, 1);

        TDeque<TKeyValuePair> keys;
        keys.push_back({"oldKey", ""});

        ExecuteWrite(tc, keys, 1, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteCopyRange<false>(tc, "", EBorderKind::Without, "", EBorderKind::Without, 1, TString{10_KB, '_'}, "");
    });
}

Y_UNIT_TEST(TestConcatToLongKey) {
    TTestContext tc;
    RunTestWithReboots(tc.TabletIds, [&]() {
        return tc.InitialEventsFilter.Prepare();
    }, [&](const TString &dispatchName, std::function<void(TTestActorRuntime&)> setup, bool &activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        ExecuteObtainLock(tc, 1);

        TDeque<TKeyValuePair> keys;
        keys.push_back({"oldKey1", "1"});
        keys.push_back({"oldKey2", "2"});

        ExecuteWrite(tc, keys, 1, 2, NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        ExecuteConcat<false>(tc, TString{10_KB, '_'}, {"oldKey1", "oldKey2"}, 1, 1);
    });
}

} // TKeyValueTest
} // NKikimr
