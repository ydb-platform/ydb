#include <ydb/core/test_tablet/state_server_interface.h>
#include <ydb/core/test_tablet/test_shard_impl.h>
#include <ydb/core/test_tablet/test_tablet.h>
#include <ydb/core/test_tablet/processor.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTestShard {
namespace {

using TStateServer = ::NTestShard::TStateServer;

enum class EInterruption {
    NONE,
    REQUEST,
    RESPONSE,
};

void RecoverStateServer(EInterruption interruption) {
    TTestBasicRuntime runtime;
    SetupTabletServices(runtime, nullptr, true);
    const auto edge = runtime.AllocateEdgeActor();
    const ui64 tabletId = MakeTabletID(false, 1);
    auto stateServer = TTestShardContext::Create();
    runtime.RegisterService(MakeStateServerInterfaceActorId(),
                            runtime.Register(CreateStateServerInterfaceActor(stateServer)));
    CreateTestBootstrapper(runtime,
                           CreateTestTabletInfo(tabletId, TTabletTypes::TestShard, TErasureType::ErasureNone),
                           &CreateTestShard);

    auto write = std::make_unique<TEvKeyValue::TEvRequest>();
    for (ui64 seed : {1, 2}) {
        auto* cmd = write->Record.AddCmdWrite();
        cmd->SetKey(TStringBuilder() << "16," << seed << ',' << seed);
        cmd->SetValue(FastGenDataForLZ4(16, seed));
        cmd->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
    }
    runtime.SendToPipe(tabletId, edge, write.release(), 0, GetPipeConfigWithRetries());
    auto written = runtime.GrabEdgeEventRethrow<TEvKeyValue::TEvResponse>(edge);
    UNIT_ASSERT_VALUES_EQUAL(written->Get()->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(written->Get()->Record.WriteResultSize(), 2);
    for (const auto& result : written->Get()->Record.GetWriteResult()) {
        UNIT_ASSERT_VALUES_EQUAL(static_cast<NKikimrProto::EReplyStatus>(result.GetStatus()), NKikimrProto::OK);
    }

    ui32 generation = 0;
    ui32 initializations = 0;
    ui32 responses = 0;
    ui32 completedValidations = 0;
    TActorId validationActor;
    auto observer = runtime.AddObserver<TEvStateServerRequest>([&](TEvStateServerRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.HasRead() && ev->Sender != edge) {
            generation = record.GetRead().GetGeneration();
        } else if (record.HasInitialize()) {
            const auto& cmd = record.GetInitialize();
            UNIT_ASSERT_VALUES_EQUAL(cmd.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(cmd.KeysSize(), 2);
            if (++initializations == 1) {
                validationActor = ev->Sender;
                if (interruption == EInterruption::REQUEST) {
                    ev.Reset();
                }
            }
        } else {
            UNIT_ASSERT_C(!record.HasWrite(), "Recovery must publish all keys in a single request");
        }
    });
    auto writeObserver = runtime.AddObserver<TEvStateServerWriteResult>([&](TEvStateServerWriteResult::TPtr& ev) {
        UNIT_ASSERT_EQUAL(ev->Get()->Record.GetStatus(), TStateServer::OK);
        if (ev->Recipient == validationActor) {
            ++responses;
            if (interruption == EInterruption::RESPONSE) {
                ev.Reset();
            }
        }
    });
    auto modeObserver = runtime.AddObserver<TTestShard::TEvSwitchMode>([&](TTestShard::TEvSwitchMode::TPtr& ev) {
        // Other actors may use the same private event number.
        const auto* mode = dynamic_cast<TTestShard::TEvSwitchMode*>(ev->GetBase());
        if (mode && mode->Mode == TTestShard::EMode::WRITE) {
            ++completedValidations;
        }
    });

    auto initialize = std::make_unique<TEvControlRequest>();
    auto* settings = initialize->Record.MutableInitialize();
    settings->SetStorageServerHost("in-process");
    settings->SetStorageServerPort(1);
    settings->SetMaxDataBytes(1024);
    settings->SetMaxInFlight(0);
    settings->SetMaxReadsInFlight(0);
    runtime.SendToPipe(tabletId, edge, initialize.release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<TEvControlResponse>(edge);

    runtime.WaitFor("StateServer initialization", [&] {
        return initializations == 1 && (interruption == EInterruption::REQUEST || responses == 1);
    }, TDuration::Seconds(10));

    auto checkState = [&](ui32 expectedKeys) {
        auto readState = std::make_unique<TEvStateServerRequest>();
        readState->Record.MutableRead()->SetTabletId(tabletId);
        readState->Record.MutableRead()->SetGeneration(generation);
        runtime.Send(new IEventHandle(MakeStateServerInterfaceActorId(), edge, readState.release()));
        auto state = runtime.GrabEdgeEventRethrow<TEvStateServerReadResult>(edge);
        const auto& record = state->Get()->Record;
        UNIT_ASSERT_EQUAL(record.GetStatus(), TStateServer::OK);
        UNIT_ASSERT_VALUES_EQUAL(record.ItemsSize(), expectedKeys);
        for (ui32 i = 0; i < expectedKeys; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetItems(i).GetKey(), TStringBuilder() << "16," << i + 1 << ',' << i + 1);
            UNIT_ASSERT_EQUAL(record.GetItems(i).GetState(), TStateServer::CONFIRMED);
        }
    };
    checkState(interruption == EInterruption::REQUEST ? 0 : 2);

    if (interruption != EInterruption::NONE) {
        UNIT_ASSERT_VALUES_EQUAL(completedValidations, 0);
    } else {
        runtime.WaitFor("completed StateServer recovery", [&] {
            return completedValidations == 1;
        }, TDuration::Seconds(10));
    }

    // Keep the KV data and StateServer intact, but restart the tablet and its validation actor.
    RebootTablet(runtime, tabletId, edge);
    runtime.WaitFor("validation after tablet restart", [&] {
        return completedValidations == (interruption == EInterruption::NONE ? 2 : 1);
    }, TDuration::Seconds(10));
    UNIT_ASSERT_VALUES_EQUAL(initializations, interruption == EInterruption::REQUEST ? 2 : 1);
    checkState(2);
}

Y_UNIT_TEST_SUITE(TTestShardStateServerRecovery) {
    Y_UNIT_TEST(CompletedRecoverySurvivesRestart) {
        RecoverStateServer(EInterruption::NONE);
    }

    Y_UNIT_TEST(LostInitializationRequestSurvivesRestart) {
        RecoverStateServer(EInterruption::REQUEST);
    }

    Y_UNIT_TEST(LostInitializationResponseSurvivesRestart) {
        RecoverStateServer(EInterruption::RESPONSE);
    }

    Y_UNIT_TEST(InitializationRejectsStaleGeneration) {
        TProcessor processor;
        TStateServer::TRead read;
        read.SetTabletId(1);
        read.SetGeneration(2);
        UNIT_ASSERT_EQUAL(processor.Execute(read).GetStatus(), TStateServer::OK);

        TStateServer::TInitialize initialize;
        initialize.SetTabletId(1);
        initialize.SetGeneration(1);
        initialize.AddKeys("stale");
        UNIT_ASSERT_EQUAL(processor.Execute(initialize).GetStatus(), TStateServer::RACE);
        UNIT_ASSERT_VALUES_EQUAL(processor.Execute(read).ItemsSize(), 0);

        initialize.SetGeneration(2);
        UNIT_ASSERT_EQUAL(processor.Execute(initialize).GetStatus(), TStateServer::OK);
        const auto state = processor.Execute(read);
        UNIT_ASSERT_VALUES_EQUAL(state.ItemsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state.GetItems(0).GetKey(), "stale");
        UNIT_ASSERT_EQUAL(state.GetItems(0).GetState(), TStateServer::CONFIRMED);
    }

    Y_UNIT_TEST(InitializationPreservesExistingState) {
        TProcessor processor;
        TStateServer::TWrite write;
        write.SetTabletId(1);
        write.SetGeneration(1);
        write.SetKey("existing");
        write.SetOriginState(TStateServer::ABSENT);
        write.SetTargetState(TStateServer::WRITE_PENDING);
        UNIT_ASSERT_EQUAL(processor.Execute(write).GetStatus(), TStateServer::OK);

        TStateServer::TInitialize initialize;
        initialize.SetTabletId(1);
        initialize.SetGeneration(2);
        initialize.AddKeys("replacement");
        UNIT_ASSERT_EQUAL(processor.Execute(initialize).GetStatus(), TStateServer::ERROR);

        TStateServer::TRead read;
        read.SetTabletId(1);
        read.SetGeneration(2);
        const auto state = processor.Execute(read);
        UNIT_ASSERT_EQUAL(state.GetStatus(), TStateServer::OK);
        UNIT_ASSERT_VALUES_EQUAL(state.ItemsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state.GetItems(0).GetKey(), "existing");
        UNIT_ASSERT_EQUAL(state.GetItems(0).GetState(), TStateServer::WRITE_PENDING);
    }
}

} // namespace
} // namespace NKikimr::NTestShard
