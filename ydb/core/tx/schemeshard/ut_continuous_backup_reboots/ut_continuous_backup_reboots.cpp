#include <ydb/core/base/hive.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/escape.h>
#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TContinuousBackupWithRebootsTests) {
    Y_UNIT_TEST(Basic) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableProtoSourceIdInfo(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 txId = 100;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, txId);
            }

            TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                ContinuousBackupDescription {
                    StreamName: "0_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                    NLs::StreamVirtualTimestamps(false),
                });
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathExist});
            }

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                Stop {}
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateDisabled),
                });
            }

            TestDropContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(TakeIncrementalBackup) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableProtoSourceIdInfo(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 txId = 100;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, txId);
            }

            TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                ContinuousBackupDescription {
                    StreamName: "0_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                    NLs::StreamVirtualTimestamps(false),
                });
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {
                    NLs::PathExist,
                    NLs::HasNotOffloadConfig,
                });
            }

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl"
                    DstStreamPath: "1_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                auto pathInfo = DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl");
                auto ownerId = pathInfo.GetPathOwnerId();
                auto localId = pathInfo.GetPathId();

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {
                    NLs::PathExist,
                    NLs::HasOffloadConfig(Sprintf(R"(
                            IncrementalBackup: {
                                DstPath: "/MyRoot/IncrBackupImpl"
                                DstPathId: {
                                    OwnerId: %)" PRIu64 R"(
                                    LocalId: %)" PRIu64 R"(
                                }
                                TxId: %)" PRIu64 R"(
                            }
                        )", ownerId, localId, txId)),
                });

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                    NLs::StreamVirtualTimestamps(false),
                });
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl/streamImpl"), {
                    NLs::PathExist,
                    NLs::HasNotOffloadConfig,
                });
            }

            t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(5));
            {
                TInactiveZone inactive(activeZone);
                // Check that stream is deleted after offloading
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl", {"key", "value", "__ydb_incrBackupImpl_changeMetadata"}, {}, {"key"}),
                });
            }
        });
    }

    Y_UNIT_TEST(TakeSeveralIncrementalBackups) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableProtoSourceIdInfo(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 txId = 100;

            {
                TInactiveZone inactive(activeZone);
                TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint64" }
                    Columns { Name: "value" Type: "Uint64" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, txId);
            }

            TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                ContinuousBackupDescription {
                    StreamName: "0_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                    NLs::StreamVirtualTimestamps(false),
                });
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {
                    NLs::PathExist,
                    NLs::HasNotOffloadConfig,
                });
            }

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl1"
                    DstStreamPath: "1_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl2"
                    DstStreamPath: "2_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            TestAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
                TableName: "Table"
                TakeIncrementalBackup {
                    DstPath: "IncrBackupImpl3"
                    DstStreamPath: "3_continuousBackupImpl"
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, txId);

            t.TestEnv->SimulateSleep(runtime, TDuration::Seconds(5));

            {
                TInactiveZone inactive(activeZone);
                // Check that streams are deleted after offloading
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/2_continuousBackupImpl"), {NLs::PathNotExist});
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/2_continuousBackupImpl/streamImpl"), {NLs::PathNotExist});

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/3_continuousBackupImpl"), {
                    NLs::PathExist,
                    NLs::StreamMode(NKikimrSchemeOp::ECdcStreamModeUpdate),
                    NLs::StreamFormat(NKikimrSchemeOp::ECdcStreamFormatProto),
                    NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
                    NLs::StreamVirtualTimestamps(false),
                });
                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/3_continuousBackupImpl/streamImpl"), {
                    NLs::PathExist,
                    NLs::HasNotOffloadConfig,
                });

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl1", {"key", "value", "__ydb_incrBackupImpl_changeMetadata"}, {}, {"key"}),
                });

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl2"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl2", {"key", "value", "__ydb_incrBackupImpl_changeMetadata"}, {}, {"key"}),
                });

                TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/IncrBackupImpl3"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckColumns("IncrBackupImpl3", {"key", "value", "__ydb_incrBackupImpl_changeMetadata"}, {}, {"key"}),
                });
            }
        });
    }

    // Regression test for issue #33764: "Parent path not found" on SchemeShard init.
    //
    // TSchemeShard::CreateTx increments DbRefCount of both TargetPathId and
    // SourcePathId, and RemoveTx unconditionally decrements both. But TTxInit
    // restores the SourcePathId reference only for a whitelist of tx types
    // (CopyTable/CopySequence/Move*), which does not include TxRotateCdcStream
    // and TxRotateCdcStreamAtTable — both of which hold SourcePathId = the old
    // continuous backup stream.
    //
    // A reboot while the rotation is in flight therefore makes the subsequent
    // RemoveTx steal two references from the old stream path — including the +1
    // held by its streamImpl child row. When the old stream is later dropped
    // (as the continuous backup cleaner does after offload), its DbRefCount
    // reaches zero while the child's Paths row still exists, TTxCleanDroppedPaths
    // erases the parent's row, and the next TTxInit fails:
    //   MakePathElement(): requirement Self->PathsById.contains(parentPathId) failed
    Y_UNIT_TEST(RebootMidRotationOrphansStreamImplPathRow) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableProtoSourceIdInfo(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            ContinuousBackupDescription {
                StreamName: "0_continuousBackupImpl"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Freeze the rotation before it can be planned: the multi-part
        // AlterContinuousBackup operation cannot propose to the coordinator
        // until every part finishes ConfigureParts, so suppressing the first
        // datashard propose keeps TxRotateCdcStream/TxRotateCdcStreamAtTable
        // in flight (persisted in TxInFlightV2).
        TVector<THolder<IEventHandle>> suppressedProposes;
        auto prevObserver = SetSuppressObserver(runtime, suppressedProposes,
            TEvDataShard::TEvProposeTransaction::EventType);

        AsyncAlterContinuousBackup(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            TakeIncrementalBackup {
                DstPath: "IncrBackupImpl"
                DstStreamPath: "1_continuousBackupImpl"
            }
        )");
        const ui64 rotateTxId = txId;
        TestModificationResult(runtime, rotateTxId, NKikimrScheme::StatusAccepted);

        WaitForSuppressed(runtime, suppressedProposes, 1, prevObserver);

        // Reboot while the rotation is in flight: TTxInit restores both rotate
        // tx states without re-incrementing the old stream's DbRefCount.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // The restarted SchemeShard re-sends the suppressed propose and the
        // rotation completes; RemoveTx now over-decrements the old stream's
        // DbRefCount by two.
        env.TestWaitNotification(runtime, rotateTxId);

        // Keep PQ shards alive so the streamImpl child rows cannot be cleaned
        // (the child's Paths row must outlive the parent's for the corruption
        // to be observable at init), and delay TTxCleanDroppedPaths so the
        // premature cleanup does not race the drop operation itself — without
        // the delay it fires mid-operation and reproduces the precursor crash
        //   DoDoneTransactions(): requirement ss->PathsById.contains(pathId)
        // from the same issue instead of the init crash.
        TVector<THolder<IEventHandle>> suppressedCleanups;
        runtime.SetObserverFunc([&suppressedCleanups](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvHive::TEvDeleteTablet::EventType:
                    return TTestActorRuntime::EEventAction::DROP;
                // The still-alive PQ tablet reports the (empty) offload after
                // its Topics entry is erased by the drop; keep it away from
                // OnOffloadStatus, it is not part of this scenario.
                case TEvPersQueue::TEvOffloadStatus::EventType:
                    return TTestActorRuntime::EEventAction::DROP;
                case TEvPrivate::TEvCleanDroppedPaths::EventType:
                    suppressedCleanups.push_back(std::move(ev));
                    return TTestActorRuntime::EEventAction::DROP;
                default:
                    return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        // Drop the old stream exactly like the continuous backup cleaner does
        // after the offload completes: an internal ESchemeOpDropCdcStream for
        // the rotated-out stream only.
        AsyncSend(runtime, TTestTxConfig::SchemeShard,
            InternalTransaction(DropCdcStreamRequest(++txId, "/MyRoot", R"(
                TableName: "Table"
                StreamName: "0_continuousBackupImpl"
            )")));
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, txId);

        // Now let TTxCleanDroppedPaths run: with the stolen references the old
        // stream's Paths row is erased while its streamImpl child row remains.
        for (auto& ev : suppressedCleanups) {
            runtime.Send(ev.Release());
        }
        env.SimulateSleep(runtime, TDuration::Seconds(1));

        // With correct refcounting this reboot succeeds; with the bug TTxInit
        // crashes with "Parent path not found" (schemeshard__init.cpp,
        // MakePathElement), the exact fingerprint of issue #33764.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table"), {
            NLs::PathExist,
            NLs::IsTable,
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/0_continuousBackupImpl"), {
            NLs::PathNotExist,
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl"), {
            NLs::PathExist,
            NLs::StreamState(NKikimrSchemeOp::ECdcStreamStateReady),
        });
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/1_continuousBackupImpl/streamImpl"), {
            NLs::PathExist,
        });
    }
} // TContinuousBackupWithRebootsTests
