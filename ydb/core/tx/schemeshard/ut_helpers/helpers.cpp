#include "helpers.h"

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/sequenceproxy/sequenceproxy.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/string/split.h>
#include <util/system/env.h>

namespace NSchemeShardUT_Private {
    using namespace NKikimr;

    void SetConfig(
    TTestActorRuntime &runtime,
    ui64 schemeShard,
    THolder<NConsole::TEvConsole::TEvConfigNotificationRequest> request)
    {
        auto sender = runtime.AllocateEdgeActor();

        runtime.SendToPipe(schemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvConfigNotificationResponse>(handle);
    }

    ////////// Hive

    // Stop tablet.
    // Also see ydb/core/mind/hive/hive_ut.cpp, SendStopTablet
    void HiveStopTablet(TTestActorRuntime &runtime, ui64 hiveTablet, ui64 tabletId, ui32 nodeIndex) {
        TActorId senderB = runtime.AllocateEdgeActor(nodeIndex);
        runtime.SendToPipe(hiveTablet, senderB, new TEvHive::TEvStopTablet(tabletId), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEventRethrow<TEvHive::TEvStopTabletResult>(handle);
        UNIT_ASSERT(event);
        const auto& stopResult = event->Record;
        UNIT_ASSERT_EQUAL_C(stopResult.GetTabletID(), tabletId, stopResult.GetTabletID() << " != " << tabletId);
        UNIT_ASSERT_EQUAL_C(stopResult.GetStatus(), NKikimrProto::OK, (ui32)stopResult.GetStatus() << " != " << (ui32)NKikimrProto::OK);
    }

    // Retrieve tablets that belong to the given subdomain
    std::vector<NKikimrHive::TTabletInfo> HiveGetSubdomainTablets(TTestActorRuntime &runtime, const ui64 hiveTablet, const TPathId& subdomainPathId) {
        TActorId senderA = runtime.AllocateEdgeActor();
        runtime.SendToPipe(hiveTablet, senderA, new TEvHive::TEvRequestHiveInfo(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(handle);
        UNIT_ASSERT(event);
        const auto& response = event->Record;

        // Cerr << "TEST: HiveGetSubdomainTablets: " << response.ShortDebugString() << Endl;

        std::vector<NKikimrHive::TTabletInfo> result;
        std::copy_if(response.GetTablets().begin(), response.GetTablets().end(),
            std::back_inserter(result),
            [&subdomainPathId] (auto& tablet) {
                return (tablet.GetObjectDomain().GetSchemeShard() == subdomainPathId.OwnerId
                    && tablet.GetObjectDomain().GetPathId() == subdomainPathId.LocalPathId
                );
            }
        );
        return result;
    }

    template <typename TEvResponse, typename TEvRequest, typename TStatus>
    static ui32 ReliableProposeImpl(
        NActors::TTestActorRuntime& runtime, const TActorId& proposer,
        TEvRequest* evRequest, const TVector<TStatus>& expectedStatuses)
    {
        TActorId sender = runtime.AllocateEdgeActor();
        ui64 txId = evRequest->Record.GetTxId();
        runtime.Send(new IEventHandle(proposer, sender, evRequest));

        auto evResponse = runtime.GrabEdgeEvent<TEvResponse>(sender);
        UNIT_ASSERT(evResponse);

        const auto& record = evResponse->Get()->Record;
        UNIT_ASSERT(record.GetTxId() == txId);

        ui32 result = 0;

        if constexpr (std::is_same_v<TEvSchemeShard::TEvModifySchemeTransactionResult, TEvResponse>) {
            result = record.GetStatus();
            CheckExpectedStatus(expectedStatuses, record.GetStatus(), record.GetReason());
        } else if constexpr (std::is_same_v<TEvSchemeShard::TEvCancelTxResult, TEvResponse>) {
            result = record.GetStatus();
            CheckExpectedStatus(expectedStatuses, record.GetStatus(), record.GetResult());
        } else {
            result = record.GetResponse().GetStatus();
            CheckExpectedStatusCode(expectedStatuses, record.GetResponse().GetStatus(), "unexpected");
        }

        return result;
    }

    ui32 NSchemeShardUT_Private::TTestEnv::ReliablePropose(
        NActors::TTestActorRuntime& runtime, TEvSchemeShard::TEvModifySchemeTransaction* evTx,
        const TVector<TEvSchemeShard::EStatus>& expectedResults)
    {
        return ReliableProposeImpl<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            runtime, TxReliablePropose, evTx, expectedResults);
    }

    ui32 NSchemeShardUT_Private::TTestEnv::ReliablePropose(
        NActors::TTestActorRuntime& runtime, TEvSchemeShard::TEvCancelTx* evTx,
        const TVector<TEvSchemeShard::EStatus>& expectedResults)
    {
        return ReliableProposeImpl<TEvSchemeShard::TEvCancelTxResult>(
            runtime, TxReliablePropose, evTx, expectedResults);
    }

    ui32 NSchemeShardUT_Private::TTestEnv::ReliablePropose(
        NActors::TTestActorRuntime& runtime, TEvExport::TEvCancelExportRequest* ev,
        const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses)
    {
        return ReliableProposeImpl<TEvExport::TEvCancelExportResponse>(
            runtime, TxReliablePropose, ev, expectedStatuses);
    }

    ui32 NSchemeShardUT_Private::TTestEnv::ReliablePropose(
        NActors::TTestActorRuntime& runtime, TEvExport::TEvForgetExportRequest* ev,
        const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses)
    {
        return ReliableProposeImpl<TEvExport::TEvForgetExportResponse>(
            runtime, TxReliablePropose, ev, expectedStatuses);
    }

    ui32 NSchemeShardUT_Private::TTestEnv::ReliablePropose(
        NActors::TTestActorRuntime& runtime, TEvImport::TEvCancelImportRequest* ev,
        const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses)
    {
        return ReliableProposeImpl<TEvImport::TEvCancelImportResponse>(
            runtime, TxReliablePropose, ev, expectedStatuses);
    }

    NKikimrSchemeOp::TAlterUserAttributes AlterUserAttrs(const TVector<std::pair<TString, TString>>& add, const TVector<TString>& drop) {
        NKikimrSchemeOp::TAlterUserAttributes result;
        for (const auto& item: add) {
            auto attr = result.AddUserAttributes();
            attr->SetKey(item.first);
            attr->SetValue(item.second);
        }
        for (const auto& item: drop) {
            auto attr = result.AddUserAttributes();
            attr->SetKey(item);
        }
        return result;
    }

    //
    // CheckExpectedResult checks actual result (status-reason pair) against a list of equally acceptable results.
    // That is: result should match one of the `expected` items to be accepted as good.
    //
    // Reasons are matched by finding if expected fragment is contained in full actual reason.
    // Empty expected fragment disables reason check.
    //
    void CheckExpectedResult(const TVector<TExpectedResult>& expected, TEvSchemeShard::EStatus actualStatus, const TString& actualReason)
    {
        for (auto i : expected) {
            if (actualStatus == i.Status) {
                if (i.ReasonFragment.empty() || actualReason.Contains(i.ReasonFragment)) {
                    return;
                }
            }
        }
        Cdbg << "Unexpected result: " << NKikimrScheme::EStatus_Name(actualStatus) << ": " << actualReason << Endl;
        UNIT_FAIL("Unexpected result: " << NKikimrScheme::EStatus_Name(actualStatus) << ": " << actualReason);
    }

    // CheckExpectedStatus is a deprecated version of CheckExpectedResult that can't check reasons.
    // Used by non generic test helpers. Should be replaced by CheckExpectedResult.
    void CheckExpectedStatus(const TVector<NKikimrScheme::EStatus>& expected, TEvSchemeShard::EStatus actualStatus, const TString& actualReason)
    {
        for (auto expectedStatus : expected) {
            if (actualStatus == expectedStatus) {
                return;
            }
        }
        Cdbg << "Unexpected result: " << NKikimrScheme::EStatus_Name(actualStatus) << ": " << actualReason << Endl;
        UNIT_FAIL("Unexpected result: " << NKikimrScheme::EStatus_Name(actualStatus) << ": " << actualReason);
    }

    void SkipModificationReply(TTestActorRuntime& runtime, ui32 num) {
        TAutoPtr<IEventHandle> handle;
        for (ui32 i = 0; i < num; ++i)
            runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
    }

    void TestModificationResult(TTestActorRuntime& runtime, ui64 txId, TEvSchemeShard::EStatus expectedStatus) {
        TestModificationResults(runtime, txId, {{expectedStatus, ""}});
    }

    ui64 TestModificationResults(TTestActorRuntime& runtime, ui64 txId, const TVector<TExpectedResult>& expectedResults) {
        TAutoPtr<IEventHandle> handle;
        TEvSchemeShard::TEvModifySchemeTransactionResult* event;
        do {
            Cerr << "TestModificationResults wait txId: " <<  txId << "\n";
            event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
            UNIT_ASSERT(event);
            Cerr << "TestModificationResult got TxId: " << event->Record.GetTxId() << ", wait until txId: " << txId << "\n";
        } while(event->Record.GetTxId() < txId);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);

        CheckExpectedResult(expectedResults, event->Record.GetStatus(), event->Record.GetReason());
        return event->Record.GetStatus();
    }

    void SetApplyIf(NKikimrSchemeOp::TModifyScheme& transaction, const TApplyIf& applyIf) {
        for (const auto& applyIfUnit: applyIf) {
            auto condition = transaction.AddApplyIf();

            if (applyIfUnit.PathId != TPathId()) {
                condition->SetPathId(applyIfUnit.PathId.LocalPathId);
                condition->SetPathVersion(applyIfUnit.Version);
            }

            for (auto pathType : applyIfUnit.PathTypes) {
                condition->AddPathTypes(pathType);
            }
        }
    }

    TEvSchemeShard::TEvModifySchemeTransaction* CreateModifyACLRequest(
        ui64 txId, ui64 schemeshard,
        TString parentPath, TString name,
        const TString& diffAcl, const TString& newOwner, const TApplyIf& applyIf
    )
    {
        auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(txId, schemeshard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetWorkingDir(parentPath);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL);

        SetApplyIf(*transaction, applyIf);

        auto op = transaction->MutableModifyACL();
        op->SetName(name);
        if (diffAcl) {
            op->SetDiffACL(diffAcl);
        }
        if (newOwner) {
            op->SetNewOwner(newOwner);
        }

        return evTx;
    }

    void AsyncModifyACL(
        TTestActorRuntime& runtime,
        ui64 schemeShardId, ui64 txId,
        TString parentPath, TString name,
        const TString& diffAcl, const TString& newOwner, const TApplyIf& applyIf
    )
    {
        AsyncSend(runtime, schemeShardId, CreateModifyACLRequest(txId, schemeShardId, parentPath, name, diffAcl, newOwner, applyIf));
    }

    void AsyncModifyACL(
        TTestActorRuntime& runtime,
        ui64 txId, TString parentPath, TString name,
        const TString& diffAcl, const TString& newOwner, const TApplyIf& applyIf
    )
    {
        return AsyncModifyACL(runtime, TTestTxConfig::SchemeShard, txId, parentPath, name, diffAcl, newOwner, applyIf);
    }

    void TestModifyACL(
        TTestActorRuntime& runtime,
        ui64 schemeShardId, ui64 txId,
        TString parentPath, TString name,
        const TString& diffAcl, const TString& newOwner,
        TEvSchemeShard::EStatus expectedResult,
        const TApplyIf& applyIf
    )
    {
        AsyncModifyACL(runtime, schemeShardId, txId, parentPath, name, diffAcl, newOwner, applyIf);
        TestModificationResult(runtime, txId, expectedResult);
    }

    void TestModifyACL(
        TTestActorRuntime& runtime,
        ui64 txId, TString parentPath, TString name,
        const TString& diffAcl, const TString& newOwner,
        TEvSchemeShard::EStatus expectedResult,
        const TApplyIf& applyIf
    )
    {
        TestModifyACL(runtime, TTestTxConfig::SchemeShard, txId, parentPath, name, diffAcl, newOwner, expectedResult, applyIf);
    }


    //

    NKikimrScheme::TEvDescribeSchemeResult DescribePath(TTestActorRuntime& runtime, ui64 schemeShard, const TString& path, const NKikimrSchemeOp::TDescribeOptions& opts) {
        TActorId sender = runtime.AllocateEdgeActor();
        auto evLs = new TEvSchemeShard::TEvDescribeScheme(path);
        evLs->Record.MutableOptions()->CopyFrom(opts);
        ForwardToTablet(runtime, schemeShard, sender, evLs);
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvDescribeSchemeResult>(handle);
        UNIT_ASSERT(event);

        return event->GetRecord();
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePathId(TTestActorRuntime& runtime, ui64 schemeShard, ui64 pathId, const NKikimrSchemeOp::TDescribeOptions& opts = { }) {
        TActorId sender = runtime.AllocateEdgeActor();
        auto evLs = new TEvSchemeShard::TEvDescribeScheme(schemeShard, pathId);
        evLs->Record.MutableOptions()->CopyFrom(opts);
        ForwardToTablet(runtime, schemeShard, sender, evLs);
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvDescribeSchemeResult>(handle);
        UNIT_ASSERT(event);

        return event->GetRecord();
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePath(TTestActorRuntime& runtime, const TString& path, const NKikimrSchemeOp::TDescribeOptions& opts) {
        return DescribePath(runtime, TTestTxConfig::SchemeShard, path, opts);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePathId(TTestActorRuntime& runtime, ui64 pathId, const NKikimrSchemeOp::TDescribeOptions& opts = { }) {
        return DescribePathId(runtime, TTestTxConfig::SchemeShard, pathId, opts);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePrivatePath(TTestActorRuntime& runtime, ui64 schemeShard, const TString& path, bool returnPartitioning, bool returnBoundaries) {
        return DescribePath(runtime, schemeShard, path, returnPartitioning, returnBoundaries, true);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePath(TTestActorRuntime& runtime, ui64 schemeShard, const TString& path, bool returnPartitioning, bool returnBoundaries, bool showPrivate, bool returnBackups) {
        NKikimrSchemeOp::TDescribeOptions opts;
        opts.SetReturnPartitioningInfo(returnPartitioning);
        opts.SetReturnPartitionConfig(returnPartitioning);
        opts.SetBackupInfo(returnBackups);
        opts.SetReturnBoundaries(returnBoundaries);
        opts.SetShowPrivateTable(showPrivate);

        return DescribePath(runtime, schemeShard, path, opts);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePrivatePath(TTestActorRuntime& runtime, const TString& path, bool returnPartitioning, bool returnBoundaries) {
        return DescribePath(runtime, TTestTxConfig::SchemeShard, path, returnPartitioning, returnBoundaries, true);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePath(TTestActorRuntime& runtime, const TString& path, bool returnPartitioning, bool returnBoundaries, bool showPrivate, bool returnBackups) {
        return DescribePath(runtime, TTestTxConfig::SchemeShard, path, returnPartitioning, returnBoundaries, showPrivate, returnBackups);
    }

    TPathVersion ExtractPathVersion(const NKikimrScheme::TEvDescribeSchemeResult& describe) {
        TPathVersion result;
        result.PathId = TPathId(describe.GetPathDescription().GetSelf().GetSchemeshardId(), describe.GetPathDescription().GetSelf().GetPathId());
        result.Version = describe.GetPathDescription().GetSelf().GetPathVersion();
        return result;
    }

    TPathVersion TestDescribeResult(const NKikimrScheme::TEvDescribeSchemeResult& describe, TVector<NLs::TCheckFunc> checks) {
        for (const auto& check: checks) {
            if (check) {
                check(describe);
            }
        }
        return ExtractPathVersion(describe);
    }

    TString TestLs(TTestActorRuntime& runtime, const TString& path, bool returnPartitioningInfo,
                NLs::TCheckFunc check) {
        auto record = DescribePath(runtime, path, returnPartitioningInfo);

        if (check) {
            check(record);
        }
        return record.DebugString();
    }

    TString TestLs(TTestActorRuntime& runtime, const TString& path, const NKikimrSchemeOp::TDescribeOptions& opts,
                NLs::TCheckFunc check) {
        auto record = DescribePath(runtime, path, opts);

        if (check) {
            check(record);
        }
        return record.DebugString();
    }

    TString TestLsPathId(TTestActorRuntime& runtime, ui64 pathId, NLs::TCheckFunc check) {
        auto record = DescribePathId(runtime, pathId);

        if (check) {
            check(record);
        }
        return record.DebugString();
    }

    THolder<NSchemeCache::TSchemeCacheNavigate> Navigate(TTestActorRuntime& runtime, const TString& path,
            NSchemeCache::TSchemeCacheNavigate::EOp op)
    {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
        using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

        const auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(path);
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        entry.Operation = op;
        entry.ShowPrivatePath = true;
        runtime.Send(new IEventHandle(MakeSchemeCacheID(), sender, new TEvRequest(request.Release())));

        auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get());

        auto* response = ev->Get()->Request.Release();
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.size(), 1);

        return THolder(response);
    }

    TEvSchemeShard::TEvModifySchemeTransaction* CopyTableRequest(ui64 txId, const TString& dstPath, const TString& dstName, const TString& srcFullName, TApplyIf applyIf) {
        auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(txId, TTestTxConfig::SchemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        transaction->SetWorkingDir(dstPath);

        auto op = transaction->MutableCreateTable();
        op->SetName(dstName);
        op->SetCopyFromTable(srcFullName);

        SetApplyIf(*transaction, applyIf);
        return evTx;
    }

    void AsyncCopyTable(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId,
                        const TString& dstPath, const TString& dstName, const TString& srcFullName) {
        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, schemeShardId, sender, CopyTableRequest(txId, dstPath, dstName, srcFullName));
    }

    void AsyncCopyTable(TTestActorRuntime& runtime, ui64 txId,
                        const TString& dstPath, const TString& dstName, const TString& srcFullName) {
        AsyncCopyTable(runtime, TTestTxConfig::SchemeShard, txId, dstPath, dstName, srcFullName);
    }

    void TestCopyTable(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId,
                       const TString& dstPath, const TString& dstName, const TString& srcFullName,
                       TEvSchemeShard::EStatus expectedResult) {
        AsyncCopyTable(runtime, schemeShardId, txId, dstPath, dstName, srcFullName);
        TestModificationResult(runtime, txId, expectedResult);
    }

    void TestCopyTable(TTestActorRuntime& runtime, ui64 txId,
                       const TString& dstPath, const TString& dstName, const TString& srcFullName,
                       TEvSchemeShard::EStatus expectedResult) {
        TestCopyTable(runtime, TTestTxConfig::SchemeShard, txId, dstPath, dstName, srcFullName, expectedResult);
    }

    TString TestDescribe(TTestActorRuntime& runtime, const TString& path) {
        return TestLs(runtime, path, true);
    }

    TEvSchemeShard::TEvModifySchemeTransaction* MoveTableRequest(ui64 txId, const TString& srcPath, const TString& dstPath, ui64 schemeShard, const TApplyIf& applyIf) {
        THolder<TEvSchemeShard::TEvModifySchemeTransaction> evTx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, schemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable);
        SetApplyIf(*transaction, applyIf);

        auto descr = transaction->MutableMoveTable();
        descr->SetSrcPath(srcPath);
        descr->SetDstPath(dstPath);


        return evTx.Release();
    }

    void AsyncMoveTable(TTestActorRuntime& runtime, ui64 txId, const TString& srcPath, const TString& dstPath, ui64 schemeShard) {
        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, schemeShard, sender, MoveTableRequest(txId, srcPath, dstPath, schemeShard));
    }

    void TestMoveTable(TTestActorRuntime& runtime, ui64 txId, const TString& src, const TString& dst, const TVector<TExpectedResult>& expectedResults) {
        TestMoveTable(runtime, TTestTxConfig::SchemeShard, txId, src, dst, expectedResults);
    }

    void TestMoveTable(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, const TString& src, const TString& dst, const TVector<TExpectedResult>& expectedResults) {
        AsyncMoveTable(runtime, txId, src, dst, schemeShard);
        TestModificationResults(runtime, txId, expectedResults);
    }

    TEvSchemeShard::TEvModifySchemeTransaction* MoveIndexRequest(ui64 txId, const TString& tablePath, const TString& srcPath, const TString& dstPath, bool allowOverwrite, ui64 schemeShard, const TApplyIf& applyIf) {
        THolder<TEvSchemeShard::TEvModifySchemeTransaction> evTx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, schemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex);
        SetApplyIf(*transaction, applyIf);

        auto descr = transaction->MutableMoveIndex();
        descr->SetTablePath(tablePath);
        descr->SetSrcPath(srcPath);
        descr->SetDstPath(dstPath);
        descr->SetAllowOverwrite(allowOverwrite);

        return evTx.Release();
    }

    void AsyncMoveIndex(TTestActorRuntime& runtime, ui64 txId, const TString& tablePath, const TString& srcPath, const TString& dstPath, bool allowOverwrite, ui64 schemeShard) {
        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, schemeShard, sender, MoveIndexRequest(txId, tablePath, srcPath, dstPath, allowOverwrite, schemeShard));
    }

    void TestMoveIndex(TTestActorRuntime& runtime, ui64 txId, const TString& tablePath, const TString& src, const TString& dst, bool allowOverwrite, const TVector<TExpectedResult>& expectedResults) {
        TestMoveIndex(runtime, TTestTxConfig::SchemeShard, txId, tablePath, src, dst, allowOverwrite, expectedResults);
    }

    void TestMoveIndex(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, const TString& tablePath, const TString& src, const TString& dst, bool allowOverwrite, const TVector<TExpectedResult>& expectedResults) {
        AsyncMoveIndex(runtime, txId, tablePath, src, dst, allowOverwrite, schemeShard);
        TestModificationResults(runtime, txId, expectedResults);
    }

    TEvSchemeShard::TEvModifySchemeTransaction* TruncateTableRequest(ui64 txId, const TString& workingDir, const TString& tableName, ui64 schemeShard, const TApplyIf& applyIf) {
        THolder<TEvSchemeShard::TEvModifySchemeTransaction> evTx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, schemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpTruncateTable);
        transaction->SetWorkingDir(workingDir);
        SetApplyIf(*transaction, applyIf);

        auto descr = transaction->MutableTruncateTable();
        descr->SetTableName(tableName);

        return evTx.Release();
    }

    void AsyncTruncateTable(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir, const TString& tableName, ui64 schemeShard) {
        AsyncSend(runtime, schemeShard, TruncateTableRequest(txId, workingDir, tableName, schemeShard, {}));
    }

    void TestTruncateTable(TTestActorRuntime& runtime, ui64 txId, const TString& workingDir, const TString& tableName, const TVector<TExpectedResult>& expectedResults) {
        TestTruncateTable(runtime, TTestTxConfig::SchemeShard, txId, workingDir, tableName, expectedResults);
    }

    void TestTruncateTable(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, const TString& workingDir, const TString& tableName, const TVector<TExpectedResult>& expectedResults) {
        AsyncTruncateTable(runtime, txId, workingDir, tableName, schemeShard);
        TestModificationResults(runtime, txId, expectedResults);
    }

    // copy and rename *MoveTable* family
    //TODO: generalize all Move* stuff
    TEvSchemeShard::TEvModifySchemeTransaction* MoveSequenceRequest(ui64 txId, const TString& src, const TString& dst, ui64 schemeShard, const TApplyIf& applyIf) {
        auto tx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, schemeShard);
        auto transaction = tx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveSequence);
        SetApplyIf(*transaction, applyIf);

        auto descr = transaction->MutableMoveSequence();
        descr->SetSrcPath(src);
        descr->SetDstPath(dst);

        return tx.Release();
    }

    void AsyncMoveSequence(TTestActorRuntime& runtime, ui64 txId, const TString& src, const TString& dst, ui64 schemeShard) {
        AsyncSend(runtime, schemeShard, MoveSequenceRequest(txId, src, dst, schemeShard));
    }

    void TestMoveSequence(TTestActorRuntime& runtime, ui64 txId, const TString& src, const TString& dst, const TVector<TExpectedResult>& expectedResults) {
        TestMoveSequence(runtime, TTestTxConfig::SchemeShard, txId, src, dst, expectedResults);
    }

    void TestMoveSequence(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, const TString& src, const TString& dst, const TVector<TExpectedResult>& expectedResults) {
        AsyncMoveSequence(runtime, txId, src, dst, schemeShard);
        TestModificationResults(runtime, txId, expectedResults);
    }

    TEvSchemeShard::TEvModifySchemeTransaction* LockRequest(ui64 txId, const TString &parentPath, const TString& name) {
        THolder<TEvSchemeShard::TEvModifySchemeTransaction> evTx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock);
        transaction->SetWorkingDir(parentPath);
        auto op = transaction->MutableLockConfig();
        op->SetName(name);
        return evTx.Release();
    }

    void AsyncLock(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, const TString& parentPath, const TString& name) {
        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, schemeShard, sender, LockRequest(txId, parentPath, name));
    }

    void AsyncLock(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name) {
        AsyncLock(runtime, TTestTxConfig::SchemeShard, txId, parentPath, name);
    }

    void TestLock(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, const TString& parentPath, const TString& name,
                        const TVector<TExpectedResult> expectedResults) {
        AsyncLock(runtime, schemeShard, txId, parentPath, name);
        TestModificationResults(runtime, txId, expectedResults);
    }

    void TestLock(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name,
                   const TVector<TExpectedResult> expectedResults) {
        TestLock(runtime, TTestTxConfig::SchemeShard, txId, parentPath, name, expectedResults);
    }

    TEvSchemeShard::TEvModifySchemeTransaction* UnlockRequest(ui64 txId, ui64 lockId, const TString &parentPath, const TString& name) {
        THolder<TEvSchemeShard::TEvModifySchemeTransaction> evTx = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropLock);
        transaction->SetWorkingDir(parentPath);
        auto op = transaction->MutableLockConfig();
        op->SetName(name);
        auto guard = transaction->MutableLockGuard();
        guard->SetOwnerTxId(lockId);
        return evTx.Release();
    }

    void AsyncUnlock(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, ui64 lockId, const TString& parentPath, const TString& name) {
        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, schemeShard, sender, UnlockRequest(txId, lockId, parentPath, name));
    }

    void AsyncUnlock(TTestActorRuntime& runtime, ui64 txId, ui64 lockId,const TString& parentPath, const TString& name) {
        AsyncUnlock(runtime, TTestTxConfig::SchemeShard, txId, lockId, parentPath, name);
    }

    void TestUnlock(TTestActorRuntime& runtime, ui64 schemeShard, ui64 txId, ui64 lockId, const TString& parentPath, const TString& name,
                  const TVector<TExpectedResult> expectedResults) {
        AsyncUnlock(runtime, schemeShard, txId, lockId, parentPath, name);
        TestModificationResults(runtime, txId, expectedResults);
    }

    void TestUnlock(TTestActorRuntime& runtime, ui64 txId, ui64 lockId, const TString& parentPath, const TString& name,
                  const TVector<TExpectedResult> expectedResults) {
        TestUnlock(runtime, TTestTxConfig::SchemeShard, txId, lockId, parentPath, name, expectedResults);
    }

    template <typename T>
    using TModifySchemeFunc = T*(NKikimrSchemeOp::TModifyScheme::*)();

    // Generic
    template <typename T>
    auto CreateTransaction(const TString& parentPath, const TString& scheme, const TApplyIf& applyIf,
            NKikimrSchemeOp::EOperationType type, TModifySchemeFunc<T> func)
    {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        tx.SetWorkingDir(parentPath);
        SetApplyIf(tx, applyIf);

        const bool ok = google::protobuf::TextFormat::ParseFromString(scheme, std::apply(func, std::tie(tx)));
        UNIT_ASSERT_C(ok, "protobuf parsing failed");

        return tx;
    }

    // SplitMerge
    template <>
    auto CreateTransaction(const TString& tablePath, const TString& scheme, const TApplyIf& applyIf,
            NKikimrSchemeOp::EOperationType type, TModifySchemeFunc<NKikimrSchemeOp::TSplitMergeTablePartitions> func)
    {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        SetApplyIf(tx, applyIf);

        const bool ok = google::protobuf::TextFormat::ParseFromString(scheme, std::apply(func, std::tie(tx)));
        UNIT_ASSERT_C(ok, "protobuf parsing failed");
        tx.MutableSplitMergeTablePartitions()->SetTablePath(tablePath);

        return tx;
    }

    // Drop
    template <>
    auto CreateTransaction(const TString& parentPath, const TString& name, const TApplyIf& applyIf,
            NKikimrSchemeOp::EOperationType type, TModifySchemeFunc<NKikimrSchemeOp::TDrop> func)
    {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        tx.SetWorkingDir(parentPath);
        SetApplyIf(tx, applyIf);

        std::apply(func, std::tie(tx))->SetName(name);

        return tx;
    }

    // Backup
    template <>
    auto CreateTransaction(const TString& parentPath, const TString& scheme, const TApplyIf& applyIf,
            NKikimrSchemeOp::EOperationType type, TModifySchemeFunc<NKikimrSchemeOp::TBackupTask> func)
    {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        tx.SetWorkingDir(parentPath);
        SetApplyIf(tx, applyIf);

        auto task = std::apply(func, std::tie(tx));
        const bool ok = google::protobuf::TextFormat::ParseFromString(scheme, task);

        if (!ok || task->HasYTSettings()) {
            const auto ytProxy = GetEnv("YT_PROXY");
            UNIT_ASSERT(ytProxy);

            if (!task->HasTableName()) {
                task->SetTableName(scheme);
            }

            TString ytHost;
            TMaybe<ui16> ytPort;
            Split(ytProxy, ':', ytHost, ytPort);

            auto& settings = *task->MutableYTSettings();
            settings.SetHost(ytHost);
            settings.SetPort(ytPort.GetOrElse(80));

            if (!settings.HasTablePattern()) {
                settings.SetTablePattern("<append=true>//tmp/table");
            }
        }

        return tx;
    }

    // Generic with attrs
    template <typename T>
    auto CreateTransaction(const TString& parentPath, const TString& scheme,
            const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf,
            NKikimrSchemeOp::EOperationType type, TModifySchemeFunc<T> func)
    {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        tx.SetWorkingDir(parentPath);
        if (userAttrs.UserAttributesSize()) {
            tx.MutableAlterUserAttributes()->CopyFrom(userAttrs);
        }
        SetApplyIf(tx, applyIf);

        const bool ok = google::protobuf::TextFormat::ParseFromString(scheme, std::apply(func, std::tie(tx)));
        UNIT_ASSERT_C(ok, "protobuf parsing failed");

        return tx;
    }

    // MkDir
    template <>
    auto CreateTransaction(const TString& parentPath, const TString& name,
            const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf,
            NKikimrSchemeOp::EOperationType type, TModifySchemeFunc<NKikimrSchemeOp::TMkDir> func)
    {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        tx.SetWorkingDir(parentPath);
        if (userAttrs.UserAttributesSize()) {
            tx.MutableAlterUserAttributes()->CopyFrom(userAttrs);
        }
        SetApplyIf(tx, applyIf);

        std::apply(func, std::tie(tx))->SetName(name);

        return tx;
    }

    // AlterUserAttrs
    template <>
    auto CreateTransaction(const TString& parentPath, const TString& name,
            const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf,
            NKikimrSchemeOp::EOperationType type, TModifySchemeFunc<NKikimrSchemeOp::TAlterUserAttributes> func)
    {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        tx.SetWorkingDir(parentPath);
        tx.MutableAlterUserAttributes()->CopyFrom(userAttrs);
        SetApplyIf(tx, applyIf);

        std::apply(func, std::tie(tx))->SetPathName(name);

        return tx;
    }

    // Drop by pathId
    auto CreateTransaction(ui64 pathId, const TApplyIf& applyIf, NKikimrSchemeOp::EOperationType type) {
        NKikimrSchemeOp::TModifyScheme tx;

        tx.SetOperationType(type);
        tx.MutableDrop()->SetId(pathId);
        SetApplyIf(tx, applyIf);

        return tx;
    }

    TEvTx* CreateRequest(ui64 schemeShardId, ui64 txId, NKikimrSchemeOp::TModifyScheme&& tx) {
        auto ev = new TEvTx(txId, schemeShardId);
        *ev->Record.AddTransaction() = std::move(tx);

        return ev;
    }

    #define GENERIC_HELPERS(name, op, func) \
        TEvTx* name##Request(ui64 schemeShardId, ui64 txId, const TString& parentPath, const TString& scheme, const TApplyIf& applyIf) { \
            return CreateRequest(schemeShardId, txId, CreateTransaction(parentPath, scheme, applyIf, op, func)); \
        } \
        \
        TEvTx* name##Request(ui64 txId, const TString& parentPath, const TString& scheme, const TApplyIf& applyIf) { \
            return name##Request(TTestTxConfig::SchemeShard, txId, parentPath, scheme, applyIf); \
        } \
        \
        void Async##name(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& parentPath, const TString& scheme, const TApplyIf& applyIf) { \
            AsyncSend(runtime, schemeShardId, \
                name##Request(schemeShardId, txId, parentPath, scheme, applyIf)); \
        } \
        \
        void Async##name(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& scheme, const TApplyIf& applyIf) { \
            Async##name(runtime, TTestTxConfig::SchemeShard, txId, parentPath, scheme, applyIf); \
        } \
        \
        ui64 Test##name(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& parentPath, const TString& scheme, \
                const TVector<TExpectedResult>& expectedResults, const TApplyIf& applyIf) \
        { \
            Async##name(runtime, schemeShardId, txId, parentPath, scheme, applyIf); \
            return TestModificationResults(runtime, txId, expectedResults); \
        } \
        \
        ui64 Test##name(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& scheme, \
                const TVector<TExpectedResult>& expectedResults, const TApplyIf& applyIf) \
        { \
            return Test##name(runtime, TTestTxConfig::SchemeShard, txId, parentPath, scheme, expectedResults, applyIf); \
        }

    #define GENERIC_WITH_ATTRS_HELPERS(name, op, func) \
        TEvTx* name##Request(ui64 schemeShardId, ui64 txId, const TString& parentPath, const TString& scheme, \
                const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf) \
        { \
            return CreateRequest(schemeShardId, txId, CreateTransaction(parentPath, scheme, userAttrs, applyIf, op, func)); \
        } \
        \
        TEvTx* name##Request(ui64 txId, const TString& parentPath, const TString& scheme, \
                const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf) \
        { \
            return name##Request(TTestTxConfig::SchemeShard, txId, parentPath, scheme, userAttrs, applyIf); \
        } \
        \
        void Async##name(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& parentPath, const TString& scheme, \
                const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf) \
        { \
            TEvTx* req = name##Request(schemeShardId, txId, parentPath, scheme, userAttrs, applyIf); \
            if (req->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain) { \
                for (const auto& [kind, pool] : runtime.GetAppData().DomainsInfo->GetDomain(0).StoragePoolTypes) { \
                    auto* pbPool = req->Record.MutableTransaction(0)->MutableSubDomain()->AddStoragePools(); \
                    pbPool->SetKind(kind); \
                    pbPool->SetName(pool.GetName()); \
                } \
            } \
            AsyncSend(runtime, schemeShardId, req); \
        } \
        \
        void Async##name(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& scheme, \
                const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf) \
        { \
            Async##name(runtime, TTestTxConfig::SchemeShard, txId, parentPath, scheme, userAttrs, applyIf); \
        } \
        \
        ui64 Test##name(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString& parentPath, const TString& scheme, \
                const TVector<TExpectedResult>& expectedResults, const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf) \
        { \
            Async##name(runtime, schemeShardId, txId, parentPath, scheme, userAttrs, applyIf); \
            return TestModificationResults(runtime, txId, expectedResults); \
        } \
        \
        ui64 Test##name(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& scheme, \
                const TVector<TExpectedResult>& expectedResults, const NKikimrSchemeOp::TAlterUserAttributes& userAttrs, const TApplyIf& applyIf) \
        { \
            return Test##name(runtime, TTestTxConfig::SchemeShard, txId, parentPath, scheme, expectedResults, userAttrs, applyIf); \
        }

    #define DROP_BY_PATH_ID_HELPERS(name, op) \
        TEvTx* name##Request(ui64 schemeShardId, ui64 txId, ui64 pathId, const TApplyIf& applyIf) { \
            return CreateRequest(schemeShardId, txId, CreateTransaction(pathId, applyIf, op)); \
        } \
        \
        TEvTx* name##Request(ui64 txId, ui64 pathId, const TApplyIf& applyIf) { \
            return name##Request(TTestTxConfig::SchemeShard, txId, pathId, applyIf); \
        } \
        \
        void Async##name(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, ui64 pathId, const TApplyIf& applyIf) { \
            AsyncSend(runtime, schemeShardId, \
                name##Request(schemeShardId, txId, pathId, applyIf)); \
        } \
        \
        void Async##name(TTestActorRuntime& runtime, ui64 txId, ui64 pathId, const TApplyIf& applyIf) { \
            Async##name(runtime, TTestTxConfig::SchemeShard, txId, pathId, applyIf); \
        } \
        \
        ui64 Test##name(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, ui64 pathId, \
                const TVector<TExpectedResult>& expectedResults, const TApplyIf& applyIf) \
        { \
            Async##name(runtime, schemeShardId, txId, pathId, applyIf); \
            return TestModificationResults(runtime, txId, expectedResults); \
        } \
        \
        ui64 Test##name(TTestActorRuntime& runtime, ui64 txId, ui64 pathId, \
                const TVector<TExpectedResult>& expectedResults, const TApplyIf& applyIf) \
        { \
            return Test##name(runtime, TTestTxConfig::SchemeShard, txId, pathId, expectedResults, applyIf); \
        }

    // subdomain
    GENERIC_WITH_ATTRS_HELPERS(CreateSubDomain, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain, &NKikimrSchemeOp::TModifyScheme::MutableSubDomain)
    GENERIC_HELPERS(AlterSubDomain, NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain, &NKikimrSchemeOp::TModifyScheme::MutableSubDomain)
    GENERIC_HELPERS(DropSubDomain, NKikimrSchemeOp::EOperationType::ESchemeOpDropSubDomain, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    GENERIC_HELPERS(ForceDropSubDomain, NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain, &NKikimrSchemeOp::TModifyScheme::MutableDrop)

    // ext subdomain
    GENERIC_WITH_ATTRS_HELPERS(CreateExtSubDomain, NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain, &NKikimrSchemeOp::TModifyScheme::MutableSubDomain)
    GENERIC_WITH_ATTRS_HELPERS(AlterExtSubDomain, NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomain, &NKikimrSchemeOp::TModifyScheme::MutableSubDomain)
    GENERIC_HELPERS(ForceDropExtSubDomain, NKikimrSchemeOp::EOperationType::ESchemeOpForceDropExtSubDomain, &NKikimrSchemeOp::TModifyScheme::MutableDrop)

    // dir
    GENERIC_WITH_ATTRS_HELPERS(MkDir, NKikimrSchemeOp::EOperationType::ESchemeOpMkDir, &NKikimrSchemeOp::TModifyScheme::MutableMkDir)
    GENERIC_HELPERS(RmDir, NKikimrSchemeOp::EOperationType::ESchemeOpRmDir, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(ForceDropUnsafe, NKikimrSchemeOp::EOperationType::ESchemeOpForceDropUnsafe)

    // user attrs
    GENERIC_WITH_ATTRS_HELPERS(UserAttrs, NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes, &NKikimrSchemeOp::TModifyScheme::MutableAlterUserAttributes)

    // table
    GENERIC_WITH_ATTRS_HELPERS(CreateTable, NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable, &NKikimrSchemeOp::TModifyScheme::MutableCreateTable)
    GENERIC_HELPERS(SimpleCreateTable, NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable, &NKikimrSchemeOp::TModifyScheme::MutableCreateTable)
    GENERIC_HELPERS(CreateIndexedTable, NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable, &NKikimrSchemeOp::TModifyScheme::MutableCreateIndexedTable)
    GENERIC_HELPERS(ConsistentCopyTables, NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables, &NKikimrSchemeOp::TModifyScheme::MutableCreateConsistentCopyTables)
    GENERIC_HELPERS(AlterTable, NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable, &NKikimrSchemeOp::TModifyScheme::MutableAlterTable)
    GENERIC_HELPERS(SplitTable, NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions, &NKikimrSchemeOp::TModifyScheme::MutableSplitMergeTablePartitions)
    GENERIC_HELPERS(DropTable, NKikimrSchemeOp::EOperationType::ESchemeOpDropTable, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropTable, NKikimrSchemeOp::EOperationType::ESchemeOpDropTable)
    GENERIC_HELPERS(DropTableIndex, NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex, &NKikimrSchemeOp::TModifyScheme::MutableDropIndex)

    // backup & restore
    GENERIC_HELPERS(Backup, NKikimrSchemeOp::EOperationType::ESchemeOpBackup, &NKikimrSchemeOp::TModifyScheme::MutableBackup)
    GENERIC_HELPERS(BackupToYt, NKikimrSchemeOp::EOperationType::ESchemeOpBackup, &NKikimrSchemeOp::TModifyScheme::MutableBackup)
    GENERIC_HELPERS(Restore, NKikimrSchemeOp::EOperationType::ESchemeOpRestore, &NKikimrSchemeOp::TModifyScheme::MutableRestore)

    // cdc stream
    GENERIC_HELPERS(CreateCdcStream, NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream, &NKikimrSchemeOp::TModifyScheme::MutableCreateCdcStream)
    GENERIC_HELPERS(AlterCdcStream, NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream, &NKikimrSchemeOp::TModifyScheme::MutableAlterCdcStream)
    GENERIC_HELPERS(DropCdcStream, NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream, &NKikimrSchemeOp::TModifyScheme::MutableDropCdcStream)

    // continuous backup
    GENERIC_HELPERS(CreateContinuousBackup, NKikimrSchemeOp::EOperationType::ESchemeOpCreateContinuousBackup, &NKikimrSchemeOp::TModifyScheme::MutableCreateContinuousBackup)
    GENERIC_HELPERS(AlterContinuousBackup, NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup, &NKikimrSchemeOp::TModifyScheme::MutableAlterContinuousBackup)
    GENERIC_HELPERS(DropContinuousBackup, NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup, &NKikimrSchemeOp::TModifyScheme::MutableDropContinuousBackup)

    // olap store
    GENERIC_HELPERS(CreateOlapStore, NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore, &NKikimrSchemeOp::TModifyScheme::MutableCreateColumnStore)
    GENERIC_HELPERS(AlterOlapStore, NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore, &NKikimrSchemeOp::TModifyScheme::MutableAlterColumnStore)
    GENERIC_HELPERS(DropOlapStore, NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropOlapStore, NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore)

    // olap table
    GENERIC_HELPERS(CreateColumnTable, NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable, &NKikimrSchemeOp::TModifyScheme::MutableCreateColumnTable)
    GENERIC_HELPERS(AlterColumnTable, NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable, &NKikimrSchemeOp::TModifyScheme::MutableAlterColumnTable)
    GENERIC_HELPERS(DropColumnTable, NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropColumnTable, NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable)

    // sequence
    GENERIC_HELPERS(CreateSequence, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence, &NKikimrSchemeOp::TModifyScheme::MutableSequence)
    GENERIC_HELPERS(DropSequence, NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    GENERIC_HELPERS(AlterSequence, NKikimrSchemeOp::EOperationType::ESchemeOpAlterSequence, &NKikimrSchemeOp::TModifyScheme::MutableSequence)
    DROP_BY_PATH_ID_HELPERS(DropSequence, NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence)

    // replication
    GENERIC_HELPERS(CreateReplication, NKikimrSchemeOp::EOperationType::ESchemeOpCreateReplication, &NKikimrSchemeOp::TModifyScheme::MutableReplication)
    GENERIC_HELPERS(AlterReplication, NKikimrSchemeOp::EOperationType::ESchemeOpAlterReplication, &NKikimrSchemeOp::TModifyScheme::MutableAlterReplication)
    GENERIC_HELPERS(DropReplication, NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropReplication, NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication)
    GENERIC_HELPERS(DropReplicationCascade, NKikimrSchemeOp::EOperationType::ESchemeOpDropReplicationCascade, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropReplicationCascade, NKikimrSchemeOp::EOperationType::ESchemeOpDropReplicationCascade)

    // transfer
    GENERIC_HELPERS(CreateTransfer, NKikimrSchemeOp::EOperationType::ESchemeOpCreateTransfer, &NKikimrSchemeOp::TModifyScheme::MutableReplication)
    GENERIC_HELPERS(AlterTransfer, NKikimrSchemeOp::EOperationType::ESchemeOpAlterTransfer, &NKikimrSchemeOp::TModifyScheme::MutableAlterReplication)
    GENERIC_HELPERS(DropTransfer, NKikimrSchemeOp::EOperationType::ESchemeOpDropTransfer, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropTransfer, NKikimrSchemeOp::EOperationType::ESchemeOpDropTransfer)
    GENERIC_HELPERS(DropTransferCascade, NKikimrSchemeOp::EOperationType::ESchemeOpDropTransferCascade, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropTransferCascade, NKikimrSchemeOp::EOperationType::ESchemeOpDropTransferCascade)

    // pq
    GENERIC_HELPERS(CreatePQGroup, NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup, &NKikimrSchemeOp::TModifyScheme::MutableCreatePersQueueGroup)
    GENERIC_HELPERS(AlterPQGroup, NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup, &NKikimrSchemeOp::TModifyScheme::MutableAlterPersQueueGroup)
    GENERIC_HELPERS(DropPQGroup, NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropPQGroup, NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup)

    // rtmr
    GENERIC_HELPERS(CreateRtmrVolume, NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume, &NKikimrSchemeOp::TModifyScheme::MutableCreateRtmrVolume)

    // solomon
    GENERIC_HELPERS(CreateSolomon, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume, &NKikimrSchemeOp::TModifyScheme::MutableCreateSolomonVolume)
    GENERIC_HELPERS(AlterSolomon, NKikimrSchemeOp::EOperationType::ESchemeOpAlterSolomonVolume, &NKikimrSchemeOp::TModifyScheme::MutableAlterSolomonVolume)
    GENERIC_HELPERS(DropSolomon, NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropSolomon, NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume)

    // kesus
    GENERIC_HELPERS(CreateKesus, NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus, &NKikimrSchemeOp::TModifyScheme::MutableKesus)
    GENERIC_HELPERS(AlterKesus, NKikimrSchemeOp::EOperationType::ESchemeOpAlterKesus, &NKikimrSchemeOp::TModifyScheme::MutableKesus)
    GENERIC_HELPERS(DropKesus, NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropKesus, NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus)

    // filestore
    GENERIC_HELPERS(CreateFileStore, NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore, &NKikimrSchemeOp::TModifyScheme::MutableCreateFileStore)
    GENERIC_HELPERS(AlterFileStore, NKikimrSchemeOp::EOperationType::ESchemeOpAlterFileStore, &NKikimrSchemeOp::TModifyScheme::MutableAlterFileStore)
    GENERIC_HELPERS(DropFileStore, NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropFileStore, NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore)

    // nbs
    GENERIC_HELPERS(CreateBlockStoreVolume, NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume, &NKikimrSchemeOp::TModifyScheme::MutableCreateBlockStoreVolume)
    GENERIC_HELPERS(AlterBlockStoreVolume, NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlockStoreVolume, &NKikimrSchemeOp::TModifyScheme::MutableAlterBlockStoreVolume)

    // external table
    GENERIC_HELPERS(CreateExternalTable, NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable, &NKikimrSchemeOp::TModifyScheme::MutableCreateExternalTable)
    GENERIC_HELPERS(DropExternalTable, NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalTable, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropExternalTable, NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalTable)

    // external data source
    GENERIC_HELPERS(CreateExternalDataSource, NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource, &NKikimrSchemeOp::TModifyScheme::MutableCreateExternalDataSource)
    GENERIC_HELPERS(DropExternalDataSource, NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalDataSource, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropExternalDataSource, NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalDataSource)

    // view
    GENERIC_HELPERS(CreateView, NKikimrSchemeOp::EOperationType::ESchemeOpCreateView, &NKikimrSchemeOp::TModifyScheme::MutableCreateView)
    GENERIC_HELPERS(DropView, NKikimrSchemeOp::EOperationType::ESchemeOpDropView, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropView, NKikimrSchemeOp::EOperationType::ESchemeOpDropView)

    // resource pool
    GENERIC_HELPERS(CreateResourcePool, NKikimrSchemeOp::EOperationType::ESchemeOpCreateResourcePool, &NKikimrSchemeOp::TModifyScheme::MutableCreateResourcePool)
    GENERIC_HELPERS(AlterResourcePool, NKikimrSchemeOp::EOperationType::ESchemeOpAlterResourcePool, &NKikimrSchemeOp::TModifyScheme::MutableCreateResourcePool)
    GENERIC_HELPERS(DropResourcePool, NKikimrSchemeOp::EOperationType::ESchemeOpDropResourcePool, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropResourcePool, NKikimrSchemeOp::EOperationType::ESchemeOpDropResourcePool)

    // backup collection
    GENERIC_HELPERS(CreateBackupCollection, NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection, &NKikimrSchemeOp::TModifyScheme::MutableCreateBackupCollection)
    GENERIC_HELPERS(DropBackupCollection, NKikimrSchemeOp::EOperationType::ESchemeOpDropBackupCollection, &NKikimrSchemeOp::TModifyScheme::MutableDropBackupCollection)
    DROP_BY_PATH_ID_HELPERS(DropBackupCollection, NKikimrSchemeOp::EOperationType::ESchemeOpDropBackupCollection)
    GENERIC_HELPERS(BackupBackupCollection, NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection, &NKikimrSchemeOp::TModifyScheme::MutableBackupBackupCollection)
    GENERIC_HELPERS(BackupIncrementalBackupCollection, NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection, &NKikimrSchemeOp::TModifyScheme::MutableBackupIncrementalBackupCollection)
    GENERIC_HELPERS(RestoreBackupCollection, NKikimrSchemeOp::EOperationType::ESchemeOpRestoreBackupCollection, &NKikimrSchemeOp::TModifyScheme::MutableRestoreBackupCollection)

    // sysview
    GENERIC_HELPERS(CreateSysView, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSysView, &NKikimrSchemeOp::TModifyScheme::MutableCreateSysView)
    GENERIC_HELPERS(DropSysView, NKikimrSchemeOp::EOperationType::ESchemeOpDropSysView, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropSysView, NKikimrSchemeOp::EOperationType::ESchemeOpDropSysView)

    // secret
    GENERIC_HELPERS(CreateSecret, NKikimrSchemeOp::EOperationType::ESchemeOpCreateSecret, &NKikimrSchemeOp::TModifyScheme::MutableCreateSecret)
    GENERIC_HELPERS(AlterSecret, NKikimrSchemeOp::EOperationType::ESchemeOpAlterSecret, &NKikimrSchemeOp::TModifyScheme::MutableAlterSecret)
    GENERIC_HELPERS(DropSecret, NKikimrSchemeOp::EOperationType::ESchemeOpDropSecret, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropSecret, NKikimrSchemeOp::EOperationType::ESchemeOpDropSecret)

    // streaming query
    GENERIC_HELPERS(CreateStreamingQuery, NKikimrSchemeOp::EOperationType::ESchemeOpCreateStreamingQuery, &NKikimrSchemeOp::TModifyScheme::MutableCreateStreamingQuery)
    GENERIC_HELPERS(AlterStreamingQuery, NKikimrSchemeOp::EOperationType::ESchemeOpAlterStreamingQuery, &NKikimrSchemeOp::TModifyScheme::MutableCreateStreamingQuery)
    GENERIC_HELPERS(DropStreamingQuery, NKikimrSchemeOp::EOperationType::ESchemeOpDropStreamingQuery, &NKikimrSchemeOp::TModifyScheme::MutableDrop)
    DROP_BY_PATH_ID_HELPERS(DropStreamingQuery, NKikimrSchemeOp::EOperationType::ESchemeOpDropStreamingQuery)

    #undef DROP_BY_PATH_ID_HELPERS
    #undef GENERIC_WITH_ATTRS_HELPERS
    #undef GENERIC_HELPERS

    void TestCreateStreamingQueryOrReplace(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& scheme, const TVector<TExpectedResult>& expectedResults) {
        auto* ev = CreateStreamingQueryRequest(TTestTxConfig::SchemeShard, txId, parentPath, scheme);
        ev->Record.MutableTransaction()->Mutable(0)->SetReplaceIfExists(true);
        AsyncSend(runtime, TTestTxConfig::SchemeShard, ev);
        TestModificationResults(runtime, txId, expectedResults);
    }

    ui64 TestCreateSubDomain(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& scheme,
            const NKikimrSchemeOp::TAlterUserAttributes& userAttrs)
    {
        return TestCreateSubDomain(runtime, txId, parentPath, scheme, {NKikimrScheme::StatusAccepted}, userAttrs);
    }

    ui64 TestCreateExtSubDomain(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& scheme,
            const NKikimrSchemeOp::TAlterUserAttributes& userAttrs)
    {
        return TestCreateExtSubDomain(runtime, txId, parentPath, scheme, {NKikimrScheme::StatusAccepted}, userAttrs);
    }

    ui64 TestUserAttrs(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name,
            const NKikimrSchemeOp::TAlterUserAttributes& userAttrs)
    {
        return TestUserAttrs(runtime, txId, parentPath, name, {NKikimrScheme::StatusAccepted}, userAttrs);
    }

    void AsyncDropBlockStoreVolume(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name,
            ui64 fillGeneration)
    {
        auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(txId, TTestTxConfig::SchemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetWorkingDir(parentPath);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume);

        transaction->MutableDrop()->SetName(name);

        transaction->MutableDropBlockStoreVolume()->SetFillGeneration(fillGeneration);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, evTx);
    }

    void TestDropBlockStoreVolume(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name,
            ui64 fillGeneration, const TVector<TExpectedResult>& expectedResults)
    {
        AsyncDropBlockStoreVolume(runtime, txId, parentPath, name, fillGeneration);
        TestModificationResults(runtime, txId, expectedResults);
    }

    void AsyncAssignBlockStoreVolume(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name,
            const TString& mountToken, ui64 tokenVersion)
    {
        auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(txId, TTestTxConfig::SchemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetWorkingDir(parentPath);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAssignBlockStoreVolume);

        transaction->MutableAssignBlockStoreVolume()->SetName(name);
        transaction->MutableAssignBlockStoreVolume()->SetNewMountToken(mountToken);
        transaction->MutableAssignBlockStoreVolume()->SetTokenVersion(tokenVersion);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, evTx);
    }

    void TestAssignBlockStoreVolume(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name,
            const TString& mountToken, ui64 tokenVersion, const TVector<TExpectedResult>& expectedResults)
    {
        AsyncAssignBlockStoreVolume(runtime, txId, parentPath, name, mountToken, tokenVersion);
        TestModificationResults(runtime, txId, expectedResults);
    }

    TEvSchemeShard::TEvCancelTx *CancelTxRequest(ui64 txId, ui64 targetTxId) {
        auto evTx = new TEvSchemeShard::TEvCancelTx();
        evTx->Record.SetTxId(txId);
        evTx->Record.SetTargetTxId(targetTxId);
        return evTx;
    }

    void AsyncCancelTxTable(TTestActorRuntime& runtime, ui64 txId, ui64 targetTxId) {
        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, CancelTxRequest(txId, targetTxId));
    }

    void TestCancelTxTable(TTestActorRuntime& runtime, ui64 txId, ui64 targetTxId,
                               const TVector<TExpectedResult>& expectedResults) {
        AsyncCancelTxTable(runtime, txId, targetTxId);

        TAutoPtr<IEventHandle> handle;
        TEvSchemeShard::TEvCancelTxResult* event;
        do {
            event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvCancelTxResult>(handle);
            UNIT_ASSERT(event);
            Cerr << "TEvCancelTxResult for TargetTxId: " << event->Record.GetTargetTxId() << ", wait until TargetTxId: " << targetTxId << "\n";
        } while(event->Record.GetTargetTxId() < targetTxId);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTargetTxId(), targetTxId);

        CheckExpectedResult(expectedResults, event->Record.GetStatus(), event->Record.GetResult());
    }

    TVector<TString> GetExportTargetPaths(const TString& requestStr) {
        NKikimrExport::TCreateExportRequest request;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(requestStr, &request));

        TVector<TString> result;
        for (const auto& item : request.GetExportToS3Settings().items()) {
            TStringBuilder dest;
            if (request.GetExportToS3Settings().destination_prefix()) {
                dest << request.GetExportToS3Settings().destination_prefix() << '/';
            }
            if (item.destination_prefix()) {
                dest << item.destination_prefix();
            } else if (request.GetExportToS3Settings().has_encryption_settings()) {
                continue; // validated separately
            } else if (item.destination_prefix() || request.GetExportToS3Settings().destination_prefix()) {
                TString src = item.source_path();
                if (size_t pos = src.find_last_of('/'); pos != TString::npos) {
                    dest << src.substr(pos + 1);
                } else {
                    if (src[0] == '/') {
                        dest << src.substr(1);
                    } else {
                        dest << src;
                    }
                }
            }
            result.push_back(dest);
        }

        return result;
    }

    void AsyncExport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName) {
        NKikimrExport::TCreateExportRequest request;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(requestStr, &request));

        if (request.HasExportToYtSettings()) {
            TString host;
            TMaybe<ui16> port;
            Split(GetEnv("YT_PROXY"), ':', host, port);

            auto& settings = *request.MutableExportToYtSettings();
            settings.set_host(host);
            settings.set_port(port.GetOrElse(80));
        }

        auto ev = MakeHolder<TEvExport::TEvCreateExportRequest>(id, dbName, request);
        if (userSID) {
            ev->Record.SetUserSID(userSID);
        }
        if (peerName) {
            ev->Record.SetPeerName(peerName);
        }

        AsyncSend(runtime, schemeshardId, ev.Release());
    }

    void AsyncExport(TTestActorRuntime& runtime, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName) {
        AsyncExport(runtime, TTestTxConfig::SchemeShard, id, dbName, requestStr, userSID, peerName);
    }

    void TestExport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        AsyncExport(runtime, schemeshardId, id, dbName, requestStr, userSID, peerName);

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvExport::TEvCreateExportResponse>(handle);
        const auto& entry = ev->Record.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL_C(entry.GetStatus(), expectedStatus, entry.GetIssues());
    }

    void TestExport(TTestActorRuntime& runtime, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        TestExport(runtime, TTestTxConfig::SchemeShard, id, dbName, requestStr, userSID, peerName, expectedStatus);
    }

    NKikimrExport::TEvGetExportResponse TestGetExport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName,
            const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses) {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), new TEvExport::TEvGetExportRequest(dbName, id));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvExport::TEvGetExportResponse>(handle);
        const auto& entry = ev->Record.GetResponse().GetEntry();
        const auto status = entry.GetStatus();

        if (!IsIn(expectedStatuses, status)) {
            UNIT_FAIL("Unexpected status: " << Ydb::StatusIds::StatusCode_Name(status) << ", issues: " << entry.GetIssues());
        }

        return ev->Record;
    }

    NKikimrExport::TEvGetExportResponse TestGetExport(TTestActorRuntime& runtime, ui64 id, const TString& dbName,
            const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses) {
        return TestGetExport(runtime, TTestTxConfig::SchemeShard, id, dbName, expectedStatuses);
    }

    NKikimrExport::TEvGetExportResponse TestGetExport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        return TestGetExport(runtime, schemeshardId, id, dbName, TVector<Ydb::StatusIds::StatusCode>(1, expectedStatus));
    }

    NKikimrExport::TEvGetExportResponse TestGetExport(TTestActorRuntime& runtime, ui64 id, const TString& dbName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        return TestGetExport(runtime, TTestTxConfig::SchemeShard, id, dbName, expectedStatus);
    }

    TEvExport::TEvCancelExportRequest* CancelExportRequest(ui64 txId, const TString& dbName, ui64 exportId) {
        return new TEvExport::TEvCancelExportRequest(txId, dbName, exportId);
    }

    NKikimrExport::TEvCancelExportResponse TestCancelExport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 txId, const TString& dbName, ui64 exportId,
            Ydb::StatusIds::StatusCode expectedStatus) {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), CancelExportRequest(txId, dbName, exportId));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvExport::TEvCancelExportResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetResponse().GetStatus(), expectedStatus);

        return ev->Record;
    }

    NKikimrExport::TEvCancelExportResponse TestCancelExport(TTestActorRuntime& runtime, ui64 txId, const TString& dbName, ui64 exportId,
            Ydb::StatusIds::StatusCode expectedStatus) {
        return TestCancelExport(runtime, TTestTxConfig::SchemeShard, txId, dbName, exportId, expectedStatus);
    }

    TEvExport::TEvForgetExportRequest* ForgetExportRequest(ui64 txId, const TString& dbName, ui64 exportId) {
        return new TEvExport::TEvForgetExportRequest(txId, dbName, exportId);
    }

    void AsyncForgetExport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 txId, const TString& dbName, ui64 exportId) {
        AsyncSend(runtime, schemeshardId, ForgetExportRequest(txId, dbName, exportId));
    }

    void AsyncForgetExport(TTestActorRuntime& runtime, ui64 txId, const TString& dbName, ui64 exportId) {
        AsyncForgetExport(runtime, TTestTxConfig::SchemeShard, txId, dbName, exportId);
    }

    NKikimrExport::TEvForgetExportResponse TestForgetExport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 txId, const TString& dbName, ui64 exportId,
            Ydb::StatusIds::StatusCode expectedStatus) {
        AsyncForgetExport(runtime, schemeshardId, txId, dbName, exportId);

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvExport::TEvForgetExportResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetResponse().GetStatus(), expectedStatus);

        return ev->Record;
    }

    NKikimrExport::TEvForgetExportResponse TestForgetExport(TTestActorRuntime& runtime, ui64 txId, const TString& dbName, ui64 exportId,
            Ydb::StatusIds::StatusCode expectedStatus) {
        return TestForgetExport(runtime, TTestTxConfig::SchemeShard, txId, dbName, exportId, expectedStatus);
    }

    void AsyncImport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName) {
        NKikimrImport::TCreateImportRequest request;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(requestStr, &request));

        auto ev = MakeHolder<TEvImport::TEvCreateImportRequest>(id, dbName, request);
        if (userSID) {
            ev->Record.SetUserSID(userSID);
        }
        if (peerName) {
            ev->Record.SetPeerName(peerName);
        }

        AsyncSend(runtime, schemeshardId, ev.Release());
    }

    void AsyncImport(TTestActorRuntime& runtime, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName) {
        AsyncImport(runtime, TTestTxConfig::SchemeShard, id, dbName, requestStr, userSID, peerName);
    }

    void TestImport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        AsyncImport(runtime, schemeshardId, id, dbName, requestStr, userSID, peerName);

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvImport::TEvCreateImportResponse>(handle);
        const auto& entry = ev->Record.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL_C(entry.GetStatus(), expectedStatus, entry.GetIssues());
    }

    void TestImport(TTestActorRuntime& runtime, ui64 id, const TString& dbName, const TString& requestStr, const TString& userSID, const TString& peerName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        TestImport(runtime, TTestTxConfig::SchemeShard, id, dbName, requestStr, userSID, peerName, expectedStatus);
    }

    NKikimrImport::TEvGetImportResponse TestGetImport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName,
            const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses) {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), new TEvImport::TEvGetImportRequest(dbName, id));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvImport::TEvGetImportResponse>(handle);
        const auto& entry = ev->Record.GetResponse().GetEntry();
        const auto status = entry.GetStatus();

        if (!IsIn(expectedStatuses, status)) {
            UNIT_FAIL("Unexpected status: " << Ydb::StatusIds::StatusCode_Name(status) << ", issues: " << entry.GetIssues());
        }

        return ev->Record;
    }

    NKikimrImport::TEvGetImportResponse TestGetImport(TTestActorRuntime& runtime, ui64 id, const TString& dbName,
            const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses) {
        return TestGetImport(runtime, TTestTxConfig::SchemeShard, id, dbName, expectedStatuses);
    }

    NKikimrImport::TEvGetImportResponse TestGetImport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        return TestGetImport(runtime, schemeshardId, id, dbName, TVector<Ydb::StatusIds::StatusCode>(1, expectedStatus));
    }

    NKikimrImport::TEvGetImportResponse TestGetImport(TTestActorRuntime& runtime, ui64 id, const TString& dbName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        return TestGetImport(runtime, TTestTxConfig::SchemeShard, id, dbName, expectedStatus);
    }

    TEvImport::TEvCancelImportRequest* CancelImportRequest(ui64 txId, const TString& dbName, ui64 importId) {
        return new TEvImport::TEvCancelImportRequest(txId, dbName, importId);
    }

    NKikimrImport::TEvCancelImportResponse TestCancelImport(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 txId, const TString& dbName, ui64 importId,
            Ydb::StatusIds::StatusCode expectedStatus) {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), CancelImportRequest(txId, dbName, importId));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvImport::TEvCancelImportResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetResponse().GetStatus(), expectedStatus);

        return ev->Record;
    }

    NKikimrImport::TEvCancelImportResponse TestCancelImport(TTestActorRuntime& runtime, ui64 txId, const TString& dbName, ui64 importId,
            Ydb::StatusIds::StatusCode expectedStatus) {
        return TestCancelImport(runtime, TTestTxConfig::SchemeShard, txId, dbName, importId, expectedStatus);
    }

    NKikimrBackup::TEvGetIncrementalBackupResponse TestGetIncrementalBackup(TTestActorRuntime& runtime, ui64 id, const TString& dbName,
            Ydb::StatusIds::StatusCode expectedStatus) {
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), new TEvBackup::TEvGetIncrementalBackupRequest(dbName, id));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvBackup::TEvGetIncrementalBackupResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetIncrementalBackup().GetStatus(), expectedStatus);

        return ev->Record;
    }

    NKikimrBackup::TEvForgetIncrementalBackupResponse TestForgetIncrementalBackup(TTestActorRuntime& runtime, ui64 txId, const TString& dbName, ui64 backupId,
            Ydb::StatusIds::StatusCode expectedStatus) {
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), new TEvBackup::TEvForgetIncrementalBackupRequest(txId, dbName, backupId));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvBackup::TEvForgetIncrementalBackupResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetStatus(), expectedStatus);

        return ev->Record;
    }

    NKikimrSchemeOp::TCreateSolomonVolume TakeTabletsFromAnotherSolomonVol(TString name, TString ls, ui32 count) {
        NKikimrSchemeOp::TCreateSolomonVolume volume;

        NKikimrScheme::TEvDescribeSchemeResult describe;
        bool parseResult = ::google::protobuf::TextFormat::ParseFromString(ls, &describe);
        Y_ASSERT(parseResult);

        auto& partitions = describe.GetPathDescription().GetSolomonDescription().GetPartitions();
        if (count == 0) {
            count = partitions.size();
        }


        for (auto it = partitions.begin(); it != partitions.end() && count != 0; --count, ++it) {
            NKikimrSchemeOp::TCreateSolomonVolume::TAdoptedPartition* part = volume.AddAdoptedPartitions();
            part->SetOwnerId(describe.GetPathDescription().GetSelf().GetSchemeshardId());
            part->SetShardIdx(it->GetShardIdx());
            part->SetTabletId(it->GetTabletId());
        }

        volume.SetName(name);
        return volume;
    }

    NKikimrProto::EReplyStatus LocalMiniKQL(TTestActorRuntime& runtime, ui64 tabletId, const TString& query, NKikimrMiniKQL::TResult& result, TString& err) {
        TActorId sender = runtime.AllocateEdgeActor();

        auto evTx = new TEvTablet::TEvLocalMKQL;
        auto *mkql = evTx->Record.MutableProgram();
        mkql->MutableProgram()->SetText(query);

        ForwardToTablet(runtime, tabletId, sender, evTx);

        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(handle);
        UNIT_ASSERT(event);

        NYql::TIssues programErrors;
        NYql::TIssues paramsErrors;
        NYql::IssuesFromMessage(event->Record.GetCompileResults().GetProgramCompileErrors(), programErrors);
        NYql::IssuesFromMessage(event->Record.GetCompileResults().GetParamsCompileErrors(), paramsErrors);
        err = programErrors.ToString() + paramsErrors.ToString() + event->Record.GetMiniKQLErrors();

        result.CopyFrom(event->Record.GetExecutionEngineEvaluatedResponse());

        // emulate enum behavior from proto3
        return static_cast<NKikimrProto::EReplyStatus>(event->Record.GetStatus());
    }

    NKikimrMiniKQL::TResult LocalMiniKQL(TTestActorRuntime& runtime, ui64 tabletId, const TString& query) {
        NKikimrMiniKQL::TResult result;
        TString error;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, query, result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        return result;
    }

    bool CheckLocalRowExists(TTestActorRuntime& runtime, ui64 tabletId, const TString& tableName, const TString& keyColumn, ui64 keyValue) {
        auto query = Sprintf(
            R"(
                (
                    (let key '('('%s (Uint64 '%s))))
                    (let select '('%s))
                    (return (AsList
                        (SetResult 'Result (SelectRow '%s key select))
                    ))
                )
            )",
            keyColumn.c_str(), (TStringBuilder() << keyValue).c_str(),
            keyColumn.c_str(),
            tableName.c_str());
        auto result = LocalMiniKQL(runtime, tabletId, query);
        // Row exists:  Value { Struct { Optional { Optional { Struct { Optional { Uint64: 2 } } } } } } }
        // Row missing: Value { Struct { Optional { } } } }
        return result.GetValue().GetStruct(0).GetOptional().HasOptional();
    }

    // Read records from a local table by a single component key
    NKikimrMiniKQL::TResult ReadLocalTableRecords(TTestActorRuntime& runtime, ui64 tabletId, const TString& tableName, const TString& keyColumn) {
        const auto query = Sprintf(
            R"(
                (
                    (let range '('('%s (Null) (Void))))
                    (let fields '('%s))
                    (return (AsList
                        (SetResult 'Result (SelectRange '%s range fields '()))
                    ))
                )
            )",
            keyColumn.c_str(),
            keyColumn.c_str(),
            tableName.c_str()
        );
        auto result = LocalMiniKQL(runtime, tabletId, query);
        // Result: Value { Struct { Optional { Struct { List {
        //     Struct { Optional { Uint64: 2 } } }
        //     ...
        // } } } } }
        return result;
    };

    ui64 GetDatashardState(TTestActorRuntime& runtime, ui64 tabletId) {
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, R"(
                                   (
                                        (let row '('('Id (Uint64 '2)))) # Sys_State
                                        (let select '('Uint64))
                                        (let ret(AsList(SetResult 'State (SelectRow 'Sys row select))))
                                        (return ret)
                                   )
                                   )", result, err);
        // Cdbg << result << "\n";
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        // Value { Struct { Optional { Optional { Struct { Optional { Uint64: 100 } } } } } } }
        return result.GetValue().GetStruct(0).GetOptional().GetOptional().GetStruct(0).GetOptional().GetUint64();
    }

    TVector<ui64> GetTableShards(TTestActorRuntime& runtime, ui64 schemeShard,  const TString& path) {
        TVector<ui64> shards;
        const auto tableDescription = DescribePath(runtime, schemeShard, path, true);
        for (const auto& part : tableDescription.GetPathDescription().GetTablePartitions()) {
            shards.emplace_back(part.GetDatashardId());
        }
        return shards;
    }

    NLs::TCheckFunc ShardsIsReady(TTestActorRuntime& runtime) {
        return [&] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
            TVector<ui64> datashards;
            for (const auto& partition: record.GetPathDescription().GetTablePartitions()) {
                ui64 dataShardId = partition.GetDatashardId();
                UNIT_ASSERT_VALUES_EQUAL(GetDatashardState(runtime, dataShardId), (ui64)NKikimrTxDataShard::Ready);
            }
        };
    }

    TLocalPathId GetNextLocalPathId(TTestActorRuntime& runtime, ui64& txId) {
        TestMkDir(runtime, ++txId, "/MyRoot", "test42");
        TLocalPathId res = DescribePath(runtime, "/MyRoot/test42").GetPathId() + 1;
        TestRmDir(runtime, ++txId, "/MyRoot", "test42");
        return res;
    }

    TString SetAllowLogBatching(TTestActorRuntime& runtime, ui64 tabletId, bool v) {
        NTabletFlatScheme::TSchemeChanges scheme;
        TString errStr;
        LocalSchemeTx(runtime, tabletId,
                 Sprintf("Delta { DeltaType: UpdateExecutorInfo ExecutorAllowLogBatching: %s }", v ? "true" : "false"),
                 false, scheme, errStr);
        return errStr;
    }

    ui64 GetDatashardSysTableValue(TTestActorRuntime& runtime, ui64 tabletId, ui64 sysKey) {
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"((
            (let Sys_ValueKey '%ld)
            (let row '('('Id (Uint64 Sys_ValueKey))))
            (let select '('Uint64))
            (let ret(AsList(SetResult 'Value (SelectRow 'Sys row select))))
            (return ret)
        ))", sysKey), result, err);
        // Cdbg << result << "\n";
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        // Value { Struct { Optional { Optional { Struct { Optional { Uint64: 100 } } } } } } }
        return result.GetValue().GetStruct(0).GetOptional().GetOptional().GetStruct(0).GetOptional().GetUint64();
    }

    ui64 GetTxReadSizeLimit(TTestActorRuntime& runtime, ui64 tabletId) {
        return GetDatashardSysTableValue(runtime, tabletId, 18);
    }

    ui64 GetStatDisabled(TTestActorRuntime& runtime, ui64 tabletId) {
        return GetDatashardSysTableValue(runtime, tabletId, 20);
    }

    bool GetFastLogPolicy(TTestActorRuntime& runtime, ui64 tabletId) {
        NTabletFlatScheme::TSchemeChanges scheme;
        TString err;
        NKikimrProto::EReplyStatus status = LocalSchemeTx(runtime, tabletId, "", true, scheme, err);
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        //Cdbg << scheme << "\n";
        for (ui32 i = 0; i < scheme.DeltaSize(); ++i) {
            const auto& d = scheme.GetDelta(i);
            if (d.GetDeltaType() == NTabletFlatScheme::TAlterRecord::UpdateExecutorInfo &&
                d.HasExecutorLogFastCommitTactic())
            {
                return d.GetExecutorLogFastCommitTactic();
            }
        }
        UNIT_ASSERT_C(false, "ExecutorLogFastCommitTactic delta record not found");
        return false;
    }

    bool GetByKeyFilterEnabled(TTestActorRuntime& runtime, ui64 tabletId, ui32 table) {
        NTabletFlatScheme::TSchemeChanges scheme;
        TString err;
        NKikimrProto::EReplyStatus status = LocalSchemeTx(runtime, tabletId, "", true, scheme, err);
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        //Cdbg << scheme << "\n";
        for (ui32 i = 0; i < scheme.DeltaSize(); ++i) {
            const auto& d = scheme.GetDelta(i);
            if (d.GetDeltaType() == NTabletFlatScheme::TAlterRecord::SetTable &&
                d.GetTableId() == table &&
                d.HasByKeyFilter())
            {
                return d.GetByKeyFilter();
            }
        }
        UNIT_ASSERT_C(false, "ByKeyFilter delta record not found");
        return false;
    }

    bool GetEraseCacheEnabled(TTestActorRuntime& runtime, ui64 tabletId, ui32 table) {
        NTabletFlatScheme::TSchemeChanges scheme;
        TString err;
        NKikimrProto::EReplyStatus status = LocalSchemeTx(runtime, tabletId, "", true, scheme, err);
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        //Cdbg << scheme << "\n";
        bool found = false;
        bool enabled = false;
        for (ui32 i = 0; i < scheme.DeltaSize(); ++i) {
            const auto& d = scheme.GetDelta(i);
            if (d.GetDeltaType() == NTabletFlatScheme::TAlterRecord::SetTable &&
                d.GetTableId() == table &&
                d.HasEraseCacheEnabled())
            {
                found = true;
                enabled = d.GetEraseCacheEnabled();
            }
        }
        UNIT_ASSERT_C(found, "EraseCacheEnabled delta record not found");
        return enabled;
    }

    NKikimr::NLocalDb::TCompactionPolicyPtr GetCompactionPolicy(TTestActorRuntime& runtime, ui64 tabletId, ui32 localTableId) {
        NTabletFlatScheme::TSchemeChanges scheme;
        TString err;
        NKikimrProto::EReplyStatus status = LocalSchemeTx(runtime, tabletId, "", true, scheme, err);
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        Cdbg << scheme << "\n";
        // looking for "Delta { DeltaType: SetCompactionPolicy TableId: 1001 CompactionPolicy { ... } }"
        for (ui32 i = 0; i < scheme.DeltaSize(); ++i) {
            const auto& d = scheme.GetDelta(i);
            if (d.GetDeltaType() == NTabletFlatScheme::TAlterRecord::SetCompactionPolicy && d.GetTableId() == localTableId) {
                return new NKikimr::NLocalDb::TCompactionPolicy(d.GetCompactionPolicy());
            }
        }
        UNIT_ASSERT_C(false, "SetCompactionPolicy delta record not found");
        return nullptr;
    }

    void SetSchemeshardReadOnlyMode(TTestActorRuntime& runtime, bool isReadOnly) {
        ui64 schemeshardTabletId = TTestTxConfig::SchemeShard;
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeshardTabletId,
                                   Sprintf(R"(
                                   (
                                        (let key '('('Id (Uint64 '3)))) # SysParam_IsReadOnlyMode
                                        (let value '('('Value (Utf8 '"%s"))))
                                        (let ret (AsList (UpdateRow 'SysParams key value)))
                                        (return ret)
                                   ))", (isReadOnly ? "1" : "0")), result, err);
        Cdbg << result << "\n";
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
    }

    void SetSchemeshardSchemaLimits(TTestActorRuntime& runtime, NSchemeShard::TSchemeLimits limits) {
        SetSchemeshardSchemaLimits(runtime, limits, TTestTxConfig::SchemeShard);
    }


    TString EscapedDoubleQuote(const TString src) {
        auto result = src;

        auto pos = src.find('"');
        if (pos == src.npos) {
            return result;
        }

        result.replace(pos, pos + 1, "\\\"");
        return result;
    }

    void SetSchemeshardSchemaLimits(TTestActorRuntime &runtime, TSchemeLimits limits, ui64 schemeShard) {
        const ui64 domainId = 1;
        NKikimrMiniKQL::TResult result;
        TString err;
        auto escapedStr = EscapedDoubleQuote(limits.ExtraPathSymbolsAllowed);
        TString prog = Sprintf(R"(
                                   (
                                        (let key '('('PathId (Uint64 '%lu)))) # RootPathId
                                        (let depth '('DepthLimit (Uint64 '%lu)))
                                        (let paths '('PathsLimit (Uint64 '%lu)))
                                        (let child '('ChildrenLimit (Uint64 '%lu)))
                                        (let acl '('AclByteSizeLimit (Uint64 '%lu)))
                                        (let columns '('TableColumnsLimit (Uint64 '%lu)))
                                        (let columnColumns '('ColumnTableColumnsLimit (Uint64 '%lu)))
                                        (let colName '('TableColumnNameLengthLimit (Uint64 '%lu)))
                                        (let keyCols '('TableKeyColumnsLimit (Uint64 '%lu)))
                                        (let indices '('TableIndicesLimit (Uint64 '%lu)))
                                        (let streams '('TableCdcStreamsLimit (Uint64 '%lu)))
                                        (let shards '('ShardsLimit (Uint64 '%lu)))
                                        (let pathShards '('PathShardsLimit (Uint64 '%lu)))
                                        (let consCopy '('ConsistentCopyingTargetsLimit (Uint64 '%lu)))
                                        (let maxPathLength '('PathElementLength (Uint64 '%lu)))
                                        (let extraSymbols '('ExtraPathSymbolsAllowed (Utf8 '"%s")))
                                        (let pqPartitions '('PQPartitionsLimit (Uint64 '%lu)))
                                        (let exports '('ExportsLimit (Uint64 '%lu)))
                                        (let imports '('ImportsLimit (Uint64 '%lu)))
                                        (let ret (AsList (UpdateRow 'SubDomains key '(depth paths child acl columns columnColumns colName keyCols indices streams shards pathShards consCopy maxPathLength extraSymbols pqPartitions exports imports))))
                                        (return ret)
                                    )
                                 )", domainId, limits.MaxDepth, limits.MaxPaths, limits.MaxChildrenInDir, limits.MaxAclBytesSize,
                               limits.MaxTableColumns, limits.MaxColumnTableColumns, limits.MaxTableColumnNameLength, limits.MaxTableKeyColumns,
                               limits.MaxTableIndices, limits.MaxTableCdcStreams,
                               limits.MaxShards, limits.MaxShardsInPath, limits.MaxConsistentCopyTargets,
                               limits.MaxPathElementLength, escapedStr.c_str(), limits.MaxPQPartitions,
                               limits.MaxExports, limits.MaxImports);
        Cdbg << prog << "\n";
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeShard, prog, result, err);
        Cdbg << result << "\n";
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, schemeShard, sender);
    }

    void SetSchemeshardDatabaseQuotas(TTestActorRuntime& runtime, Ydb::Cms::DatabaseQuotas databaseQuotas, ui64 domainId) {

        SetSchemeshardDatabaseQuotas(runtime, databaseQuotas, domainId, TTestTxConfig::SchemeShard);
    }

    void SetSchemeshardDatabaseQuotas(TTestActorRuntime& runtime, Ydb::Cms::DatabaseQuotas databaseQuotas, ui64 domainId, ui64 schemeShard) {
        NKikimrMiniKQL::TResult result;
        TString err;

        TString serialized;
        Y_ABORT_UNLESS(databaseQuotas.SerializeToString(&serialized));
        TString prog = Sprintf(R"(
                                   (
                                        (let key '('('PathId (Uint64 '%lu)))) # RootPathId
                                        (let quotas '('DatabaseQuotas (String '%s)))
                                        (let ret (AsList (UpdateRow 'SubDomains key '(quotas))))
                                        (return ret)
                                    )
                                 )", domainId, serialized.c_str());
        Cdbg << prog << "\n";
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeShard, prog, result, err);

        Cdbg << result << "\n";
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, schemeShard, sender);

    }


    NKikimrSchemeOp::TTableDescription GetDatashardSchema(TTestActorRuntime& runtime, ui64 tabletId, ui64 tid) {
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
            (
                (let sel_ (SelectRow 'UserTables '('('Tid (Uint64 '%lu))) '('Schema)))
                (let schema_ (Coalesce (FlatMap sel_ (lambda '(x) (Member x 'Schema))) (String '"")))
                (return (AsList (SetResult 'Result schema_)))
            ))", tid), result, err);

        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        //Cerr << result.GetType() << ' ' << result.GetValue() << Endl;
        TString schema = result.GetValue().GetStruct(0).GetOptional().GetBytes();
        NKikimrSchemeOp::TTableDescription tableDescription;
        bool parseOk = ParseFromStringNoSizeLimit(tableDescription, schema);
        UNIT_ASSERT(parseOk);
        return tableDescription;
    }

    TEvSchemeShard::TEvModifySchemeTransaction *UpgradeSubDomainRequest(ui64 txId, const TString &parentPath, const TString &name) {
        auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(txId, TTestTxConfig::SchemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain);
        transaction->SetWorkingDir(parentPath);
        transaction->MutableUpgradeSubDomain()->SetName(name);
        return evTx;
    }

    void AsyncUpgradeSubDomain(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &name) {
        auto evTx = UpgradeSubDomainRequest(txId, parentPath, name);
        AsyncSend(runtime, TTestTxConfig::SchemeShard, evTx);
    }

    void TestUpgradeSubDomain(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &name, const TVector<TExpectedResult> &expectedResults) {
        AsyncUpgradeSubDomain(runtime, txId, parentPath, name);
        TestModificationResults(runtime, txId, expectedResults);
    }

    void TestUpgradeSubDomain(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &name) {
        AsyncUpgradeSubDomain(runtime, txId, parentPath, name);
        TestModificationResults(runtime, txId, {{TEvSchemeShard::EStatus::StatusAccepted, ""}});
    }

    TEvSchemeShard::TEvModifySchemeTransaction *UpgradeSubDomainDecisionRequest(ui64 txId, const TString &parentPath, const TString &name, NKikimrSchemeOp::TUpgradeSubDomain::EDecision decision) {
        auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(txId, TTestTxConfig::SchemeShard);
        auto transaction = evTx->Record.AddTransaction();
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision);
        transaction->SetWorkingDir(parentPath);
        transaction->MutableUpgradeSubDomain()->SetName(name);
        transaction->MutableUpgradeSubDomain()->SetDecision(decision);
        return evTx;
    }

    void AsyncUpgradeSubDomainDecision(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &name, NKikimrSchemeOp::TUpgradeSubDomain::EDecision decision) {
        auto evTx = UpgradeSubDomainDecisionRequest(txId, parentPath, name, decision);
        AsyncSend(runtime, TTestTxConfig::SchemeShard, evTx);
    }

    void TestUpgradeSubDomainDecision(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &name, const TVector<TExpectedResult> &expectedResults, NKikimrSchemeOp::TUpgradeSubDomain::EDecision decision) {
        AsyncUpgradeSubDomainDecision(runtime, txId, parentPath, name, decision);
        TestModificationResults(runtime, txId, expectedResults);
    }

    void TestUpgradeSubDomainDecision(TTestActorRuntime &runtime, ui64 txId, const TString &parentPath, const TString &name, NKikimrSchemeOp::TUpgradeSubDomain::EDecision decision) {
        AsyncUpgradeSubDomainDecision(runtime, txId, parentPath, name, decision);
        TestModificationResults(runtime, txId, {{TEvSchemeShard::EStatus::StatusAccepted, ""}});
    }

    TRowVersion CreateVolatileSnapshot(
        TTestActorRuntime& runtime,
        const TVector<TString>& tables,
        TDuration timeout)
    {
        TActorId sender = runtime.AllocateEdgeActor();

        {
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            auto* tx = request->Record.MutableTransaction()->MutableCreateVolatileSnapshot();
            for (const auto& path : tables) {
                tx->AddTables()->SetTablePath(path);
            }
            tx->SetTimeoutMs(timeout.MilliSeconds());
            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
        }

        auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
        const auto& record = ev->Get()->Record;
        auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(record.GetStatus());
        Y_VERIFY_S(status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete,
            "Unexpected status " << status);

        auto step = record.GetStep();
        auto txId = record.GetTxId();
        Y_VERIFY_S(step != 0 && txId != 0,
            "Unexpected step " << step << " and txId " << txId);

        return { step, txId };
    }

    TEvIndexBuilder::TEvCreateRequest* CreateBuildIndexRequest(ui64 id, const TString& dbName, const TString& src, const Ydb::Table::TableIndex& index) {
        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path(src);
        settings.MutableScanSettings()->SetMaxBatchRows(1);
        settings.set_max_shards_in_flight(2);
        *settings.mutable_index() = index;
        return new TEvIndexBuilder::TEvCreateRequest(id, dbName, std::move(settings));
    }

    TEvIndexBuilder::TEvCreateRequest* CreateBuildIndexRequest(ui64 id, const TString& dbName, const TString& src, const TBuildIndexConfig& cfg) {
        Ydb::Table::TableIndex index;
        index.set_name(cfg.IndexName);
        *index.mutable_index_columns() = {cfg.IndexColumns.begin(), cfg.IndexColumns.end()};
        *index.mutable_data_columns() = {cfg.DataColumns.begin(), cfg.DataColumns.end()};

        switch (cfg.IndexType) {
        case NKikimrSchemeOp::EIndexTypeGlobal: {
            auto& settings = *index.mutable_global_index()->mutable_settings();
            if (cfg.GlobalIndexSettings.size() == 1) {
                cfg.GlobalIndexSettings[0].SerializeTo(settings);
            }
        } break;
        case NKikimrSchemeOp::EIndexTypeGlobalUnique: {
            auto& settings = *index.mutable_global_unique_index()->mutable_settings();
            if (cfg.GlobalIndexSettings.size() == 1) {
                cfg.GlobalIndexSettings[0].SerializeTo(settings);
            }
        } break;
        case NKikimrSchemeOp::EIndexTypeGlobalAsync: {
            auto& settings = *index.mutable_global_async_index()->mutable_settings();
            if (cfg.GlobalIndexSettings.size() == 1) {
                cfg.GlobalIndexSettings[0].SerializeTo(settings);
            }
        } break;
        case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
            auto& settings = *index.mutable_global_vector_kmeans_tree_index();

            auto& kmeansTreeSettings = *settings.mutable_vector_settings();
            if (cfg.KMeansTreeSettings) {
                cfg.KMeansTreeSettings->SerializeTo(kmeansTreeSettings);
            } else {
                // valid settings for tests - uint8 vectors of size 4
                kmeansTreeSettings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
                kmeansTreeSettings.mutable_settings()->set_vector_dimension(4);
                kmeansTreeSettings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
                kmeansTreeSettings.set_clusters(4);
                // More than 2 is too long for reboot tests
                kmeansTreeSettings.set_levels(2);
            }

            const bool isPrefixed = cfg.IndexColumns.size() > 1;
            if (cfg.GlobalIndexSettings.size() == (isPrefixed ? 3 : 2)) {
                cfg.GlobalIndexSettings[NTableIndex::NKMeans::LevelTablePosition].SerializeTo(*settings.mutable_level_table_settings());
                cfg.GlobalIndexSettings[NTableIndex::NKMeans::PostingTablePosition].SerializeTo(*settings.mutable_posting_table_settings());
                if (isPrefixed) {
                    cfg.GlobalIndexSettings[NTableIndex::NKMeans::PrefixTablePosition].SerializeTo(*settings.mutable_prefix_table_settings());
                }
            }
        } break;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain: {
            if (cfg.GlobalIndexSettings.size() == 1) {
                cfg.GlobalIndexSettings[0].SerializeTo(*index.mutable_global_fulltext_plain_index()->mutable_settings());
            }
        } break;
        case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance: {
            auto& settings = *index.mutable_global_fulltext_relevance_index();
            if (cfg.GlobalIndexSettings.size() == 4) {
                cfg.GlobalIndexSettings[NTableIndex::NFulltext::DictTablePosition].SerializeTo(*settings.mutable_dict_table_settings());
                cfg.GlobalIndexSettings[NTableIndex::NFulltext::DocsTablePosition].SerializeTo(*settings.mutable_docs_table_settings());
                cfg.GlobalIndexSettings[NTableIndex::NFulltext::StatsTablePosition].SerializeTo(*settings.mutable_stats_table_settings());
                cfg.GlobalIndexSettings[NTableIndex::NFulltext::PostingTablePosition].SerializeTo(*settings.mutable_posting_table_settings());
            }
        } break;
        default:
            UNIT_ASSERT_C(false, "Unknown index type: " << static_cast<ui32>(cfg.IndexType));
        }

        return CreateBuildIndexRequest(id, dbName, src, index);
    }

    std::unique_ptr<TEvIndexBuilder::TEvCreateRequest> CreateBuildColumnRequest(ui64 id, const TString& dbName, const TString& src, const TString& columnName, const Ydb::TypedValue& literal) {
        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path(src);
        settings.MutableScanSettings()->SetMaxBatchRows(1);
        settings.set_max_shards_in_flight(2);

        auto* col = settings.mutable_column_build_operation()->add_column();
        col->SetColumnName(columnName);
        col->mutable_default_from_literal()->CopyFrom(literal);

        return std::make_unique<TEvIndexBuilder::TEvCreateRequest>(id, dbName, std::move(settings));
    }

    void AsyncBuildIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName, const TString &src, const Ydb::Table::TableIndex& index) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = CreateBuildIndexRequest(id, dbName, src, index);

        ForwardToTablet(runtime, schemeShard, sender, request);
    }

    void AsyncBuildIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName, const TString &src, const TBuildIndexConfig &cfg) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = CreateBuildIndexRequest(id, dbName, src, cfg);

        ForwardToTablet(runtime, schemeShard, sender, request);
    }

    void AsyncBuildColumn(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName, const TString &src, const TString& columnName, const Ydb::TypedValue& literal) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = CreateBuildColumnRequest(id, dbName, src, columnName, literal);

        ForwardToTablet(runtime, schemeShard, sender, request.release());
    }

    void AsyncBuildIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                       const TString &src, const TString &name, TVector<TString> columns, TVector<TString> dataColumns)
    {
        AsyncBuildIndex(runtime, id, schemeShard, dbName, src, TBuildIndexConfig{
            name, NKikimrSchemeOp::EIndexTypeGlobal, columns, dataColumns, {}
        });
    }

    void AsyncBuildUniqIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                       const TString &src, const TString &name, TVector<TString> columns, TVector<TString> dataColumns)
    {
        AsyncBuildIndex(runtime, id, schemeShard, dbName, src, TBuildIndexConfig{
            name, NKikimrSchemeOp::EIndexTypeGlobalUnique, columns, dataColumns, {}
        });
    }

    void AsyncBuildVectorIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                              const TString &src, const TString &name, TVector<TString> columns, TVector<TString> dataColumns)
    {
        AsyncBuildIndex(runtime, id, schemeShard, dbName, src, TBuildIndexConfig{
            name, NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, columns, std::move(dataColumns), {}
        });
    }

    void TestBuildColumn(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
        const TString &src, const TString& columnName, const Ydb::TypedValue& literal, Ydb::StatusIds::StatusCode expectedStatus)
    {
        AsyncBuildColumn(runtime, id, schemeShard, dbName, src, columnName, literal);

        TAutoPtr<IEventHandle> handle;
        TEvIndexBuilder::TEvCreateResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
        UNIT_ASSERT(event);

        Cerr << "BUILDCOLUMN RESPONSE CREATE: " << event->ToString() << Endl;
        UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), expectedStatus,
                            "status mismatch"
                                << " got " << Ydb::StatusIds::StatusCode_Name(event->Record.GetStatus())
                                << " expected "  << Ydb::StatusIds::StatusCode_Name(expectedStatus)
                                << " issues was " << event->Record.GetIssues());
    }

    void TestBuildIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                       const TString &src, const Ydb::Table::TableIndex& index, Ydb::StatusIds::StatusCode expectedStatus)
    {
        AsyncBuildIndex(runtime, id, schemeShard, dbName, src, index);

        TAutoPtr<IEventHandle> handle;
        TEvIndexBuilder::TEvCreateResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
        UNIT_ASSERT(event);

        Cerr << "BUILDINDEX RESPONSE CREATE: " << event->ToString() << Endl;
        UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), expectedStatus,
                            "status mismatch"
                                << " got " << Ydb::StatusIds::StatusCode_Name(event->Record.GetStatus())
                                << " expected "  << Ydb::StatusIds::StatusCode_Name(expectedStatus)
                                << " issues was " << event->Record.GetIssues());
    }

    void TestBuildIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                       const TString &src, const TBuildIndexConfig& cfg, Ydb::StatusIds::StatusCode expectedStatus)
    {
        AsyncBuildIndex(runtime, id, schemeShard, dbName, src, cfg);

        TAutoPtr<IEventHandle> handle;
        TEvIndexBuilder::TEvCreateResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCreateResponse>(handle);
        UNIT_ASSERT(event);

        Cerr << "BUILDINDEX RESPONSE CREATE: " << event->ToString() << Endl;
        UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), expectedStatus,
                            "status mismatch"
                                << " got " << Ydb::StatusIds::StatusCode_Name(event->Record.GetStatus())
                                << " expected "  << Ydb::StatusIds::StatusCode_Name(expectedStatus)
                                << " issues was " << event->Record.GetIssues());
    }

    void TestBuildIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                       const TString &src, const TString &name, TVector<TString> columns,
                       Ydb::StatusIds::StatusCode expectedStatus)
    {
        TestBuildIndex(runtime, id, schemeShard, dbName, src, TBuildIndexConfig{
            name, NKikimrSchemeOp::EIndexTypeGlobal, columns, {}, {}
        }, expectedStatus);
    }

    void TestBuildUniqIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                           const TString &src, const TString &name, TVector<TString> columns,
                           Ydb::StatusIds::StatusCode expectedStatus)
    {
        TestBuildIndex(runtime, id, schemeShard, dbName, src, TBuildIndexConfig{
            name, NKikimrSchemeOp::EIndexTypeGlobalUnique, columns, {}, {}
        }, expectedStatus);
    }

    void TestBuildVectorIndex(TTestActorRuntime& runtime, ui64 id, ui64 schemeShard, const TString &dbName,
                              const TString &src, const TString &name, TVector<TString> columns,
                              Ydb::StatusIds::StatusCode expectedStatus)
    {
        TestBuildIndex(runtime, id, schemeShard, dbName, src, TBuildIndexConfig{
            name, NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, columns, {}, {}
        }, expectedStatus);
    }

    TEvIndexBuilder::TEvCancelRequest* CreateCancelBuildIndexRequest(
        const ui64 id, const TString& dbName, const ui64 buildIndexId)
    {
        return new TEvIndexBuilder::TEvCancelRequest(id, dbName, buildIndexId);
    }

    void CheckExpectedStatusCode(const TVector<Ydb::StatusIds::StatusCode>& expected, Ydb::StatusIds_StatusCode result, const TString& reason)
    {
        bool isExpectedStatus = false;
        for (Ydb::StatusIds::StatusCode exp : expected) {
            if (result == exp) {
                isExpectedStatus = true;
                break;
            }
        }
        if (!isExpectedStatus)
            Cdbg << "Unexpected status code: " << Ydb::StatusIds::StatusCode_Name(result) << ": " << reason << Endl;
        UNIT_ASSERT_C(isExpectedStatus, "Unexpected status code: " << Ydb::StatusIds::StatusCode_Name(result) << ": " << reason);
    }

    NKikimrIndexBuilder::TEvCancelResponse TestCancelBuildIndex(TTestActorRuntime& runtime, const ui64 id, const ui64 schemeShard, const TString &dbName,
                              const ui64 buildIndexId,
                              const TVector<Ydb::StatusIds::StatusCode>& expectedStatuses)
    {
        auto sender = runtime.AllocateEdgeActor();
        auto request = CreateCancelBuildIndexRequest(id, dbName, buildIndexId);

        ForwardToTablet(runtime, schemeShard, sender, request);

        TAutoPtr<IEventHandle> handle;
        TEvIndexBuilder::TEvCancelResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvCancelResponse>(handle);
        UNIT_ASSERT(event);

        Cerr << "BUILDINDEX RESPONSE CANCEL: " << event->ToString() << Endl;
        CheckExpectedStatusCode(expectedStatuses, event->Record.GetStatus(), TStringBuilder{} << event->Record.GetIssues());

        return event->Record;
    }


    TEvIndexBuilder::TEvListRequest* ListBuildIndexRequest(const TString& dbName) {
        return new TEvIndexBuilder::TEvListRequest(dbName, 100, "");
    }

    NKikimrIndexBuilder::TEvListResponse TestListBuildIndex(TTestActorRuntime& runtime, ui64 schemeShard, const TString &dbName) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = ListBuildIndexRequest(dbName);

        ForwardToTablet(runtime, schemeShard, sender, request);

        TAutoPtr<IEventHandle> handle;
        TEvIndexBuilder::TEvListResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvListResponse>(handle);
        UNIT_ASSERT(event);

        Cerr << "BUILDINDEX RESPONSE LIST: " << event->ToString() << Endl;
        UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), 400000, event->Record.GetIssues());
        return event->Record;
    }

    TEvIndexBuilder::TEvGetRequest* GetBuildIndexRequest(const TString& dbName, ui64 id) {
        return new TEvIndexBuilder::TEvGetRequest(dbName, id);
    }

    NKikimrIndexBuilder::TEvGetResponse TestGetBuildIndex(TTestActorRuntime& runtime, ui64 schemeShard, const TString &dbName, ui64 id) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = GetBuildIndexRequest(dbName, id);

        ForwardToTablet(runtime, schemeShard, sender, request);

        TAutoPtr<IEventHandle> handle;
        TEvIndexBuilder::TEvGetResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvGetResponse>(handle);
        UNIT_ASSERT(event);

        Cerr << "BUILDINDEX RESPONSE Get: " << event->ToString() << Endl;
        UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), 400000, event->Record.GetIssues());
        return event->Record;
    }

    TString TestGetBuildIndexHtml(TTestActorRuntime& runtime, ui64 schemeShard, ui64 id) {
        auto sender = runtime.AllocateEdgeActor();
        auto httpRequest = std::make_unique<NActors::NMon::TEvRemoteHttpInfo>(TStringBuilder() << "/app?Page=BuildIndexInfo&BuildIndexId=" << id);
        runtime.SendToPipe(schemeShard, sender, httpRequest.release(), 0, {});
        auto httpResponse = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteHttpInfoRes>(sender);
        return httpResponse->Get()->Html;
    }

    TEvIndexBuilder::TEvForgetRequest* ForgetBuildIndexRequest(const ui64 id, const TString &dbName, const ui64 buildIndexId) {
        return new TEvIndexBuilder::TEvForgetRequest(id, dbName, buildIndexId);
    }

    NKikimrIndexBuilder::TEvForgetResponse TestForgetBuildIndex(
        TTestActorRuntime& runtime,
        const ui64 id,
        const ui64 schemeShard,
        const TString &dbName,
        const ui64 buildIndexId,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        auto sender = runtime.AllocateEdgeActor();
        auto request = ForgetBuildIndexRequest(id, dbName, buildIndexId);

        ForwardToTablet(runtime, schemeShard, sender, request);

        TAutoPtr<IEventHandle> handle;
        TEvIndexBuilder::TEvForgetResponse* event = runtime.GrabEdgeEvent<TEvIndexBuilder::TEvForgetResponse>(handle);
        UNIT_ASSERT(event);

        Cerr << "BUILDINDEX RESPONSE Forget: " << event->ToString() << Endl;
        UNIT_ASSERT_EQUAL_C(event->Record.GetStatus(), expectedStatus, event->Record.GetIssues());

        return event->Record;
    }

    void AsyncCompact(TTestActorRuntime& runtime, ui64 schemeshardId, ui64 id, const TString& dbName, const TString& tablePath, ui32 maxShardsInFlight) {
        NKikimrForcedCompaction::TForcedCompactionSettings settings;
        settings.set_source_path(tablePath);
        settings.set_max_shards_in_flight(maxShardsInFlight);
        auto ev = MakeHolder<TEvForcedCompaction::TEvCreateRequest>(id, dbName, settings);
        AsyncSend(runtime, schemeshardId, ev.Release());
    }

    void AsyncCompact(TTestActorRuntime& runtime, ui64 id, const TString& dbName, const TString& tablePath, ui32 maxShardsInFlight) {
        AsyncCompact(runtime, TTestTxConfig::SchemeShard, id, dbName, tablePath, maxShardsInFlight);
    }

    void TestCompact(
        TTestActorRuntime& runtime,
        ui64 schemeshardId,
        ui64 id,
        const TString& dbName,
        const TString& tablePath,
        ui32 maxShardsInFlight,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        AsyncCompact(runtime, schemeshardId, id, dbName, tablePath, maxShardsInFlight);

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvForcedCompaction::TEvCreateResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL_C(ev->Record.GetStatus(), expectedStatus, ev->Record.GetIssues());
    }

    void TestCompact(TTestActorRuntime& runtime, ui64 id, const TString& dbName, const TString& tablePath, ui32 maxShardsInFlight, Ydb::StatusIds::StatusCode expectedStatus) {
        TestCompact(runtime, TTestTxConfig::SchemeShard, id, dbName, tablePath, maxShardsInFlight, expectedStatus);
    }

    NKikimrForcedCompaction::TEvGetResponse TestGetCompaction(
        TTestActorRuntime& runtime,
        ui64 schemeshardId,
        ui64 id,
        const TString& dbName,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), new TEvForcedCompaction::TEvGetRequest(dbName, id));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvForcedCompaction::TEvGetResponse>(handle);

        UNIT_ASSERT_VALUES_EQUAL_C(ev->Record.GetStatus(), expectedStatus, ev->Record.GetIssues());

        return ev->Record;
    }

    NKikimrForcedCompaction::TEvGetResponse TestGetCompaction(
        TTestActorRuntime& runtime,
        ui64 id,
        const TString& dbName,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        return TestGetCompaction(runtime, TTestTxConfig::SchemeShard, id, dbName, expectedStatus);
    }

    NKikimrForcedCompaction::TEvCancelResponse TestCancelCompaction(
        TTestActorRuntime& runtime,
        ui64 schemeshardId,
        ui64 txId,
        const TString& dbName,
        ui64 id,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), new TEvForcedCompaction::TEvCancelRequest(txId, dbName, id));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvForcedCompaction::TEvCancelResponse>(handle);

        UNIT_ASSERT_VALUES_EQUAL_C(ev->Record.GetStatus(), expectedStatus, ev->Record.GetIssues());

        return ev->Record;
    }

    NKikimrForcedCompaction::TEvCancelResponse TestCancelCompaction(
        TTestActorRuntime& runtime,
        ui64 txId,
        const TString& dbName,
        ui64 id,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        return TestCancelCompaction(runtime, TTestTxConfig::SchemeShard, txId, dbName, id, expectedStatus);
    }

    NKikimrForcedCompaction::TEvForgetResponse TestForgetCompaction(
        TTestActorRuntime& runtime,
        ui64 schemeshardId,
        ui64 txId,
        const TString& dbName,
        ui64 id,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), new TEvForcedCompaction::TEvForgetRequest(txId, dbName, id));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvForcedCompaction::TEvForgetResponse>(handle);

        UNIT_ASSERT_VALUES_EQUAL_C(ev->Record.GetStatus(), expectedStatus, ev->Record.GetIssues());

        return ev->Record;
    }

    NKikimrForcedCompaction::TEvForgetResponse TestForgetCompaction(
        TTestActorRuntime& runtime,
        ui64 txId,
        const TString& dbName,
        ui64 id,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        return TestForgetCompaction(runtime, TTestTxConfig::SchemeShard, txId, dbName, id, expectedStatus);
    }

    NKikimrForcedCompaction::TEvListResponse TestListCompactions(
        TTestActorRuntime& runtime,
        ui64 schemeshardId,
        const TString& dbName,
        ui64 pageSize,
        const TString& pageToken,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        ForwardToTablet(runtime, schemeshardId, runtime.AllocateEdgeActor(), new TEvForcedCompaction::TEvListRequest(dbName, pageSize, pageToken));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvForcedCompaction::TEvListResponse>(handle);

        UNIT_ASSERT_VALUES_EQUAL_C(ev->Record.GetStatus(), expectedStatus, ev->Record.GetIssues());

        return ev->Record;
    }

    NKikimrForcedCompaction::TEvListResponse TestListCompactions(
        TTestActorRuntime& runtime,
        const TString& dbName,
        ui64 pageSize,
        const TString& pageToken,
        Ydb::StatusIds::StatusCode expectedStatus)
    {
        return TestListCompactions(runtime, TTestTxConfig::SchemeShard, dbName, pageSize, pageToken, expectedStatus);
    }

    TPathId TestFindTabletSubDomainPathId(
            TTestActorRuntime& runtime, ui64 tabletId,
            NKikimrScheme::TEvFindTabletSubDomainPathIdResult::EStatus expected)
    {
        return TestFindTabletSubDomainPathId(runtime, TTestTxConfig::SchemeShard, tabletId, expected);
    }

    TPathId TestFindTabletSubDomainPathId(
            TTestActorRuntime& runtime, ui64 schemeShard, ui64 tabletId,
            NKikimrScheme::TEvFindTabletSubDomainPathIdResult::EStatus expected)
    {
        auto sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, schemeShard, sender, new TEvSchemeShard::TEvFindTabletSubDomainPathId(tabletId));

        auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvFindTabletSubDomainPathIdResult>(sender);
        UNIT_ASSERT(ev);

        const auto& record = ev->Get()->Record;
        UNIT_ASSERT_EQUAL_C(record.GetStatus(), expected,
            "Unexpected status "
            << NKikimrScheme::TEvFindTabletSubDomainPathIdResult::EStatus_Name(record.GetStatus())
            << " (expected status "
            << NKikimrScheme::TEvFindTabletSubDomainPathIdResult::EStatus_Name(record.GetStatus())
            << ")");

        return TPathId(record.GetSchemeShardId(), record.GetSubDomainPathId());
    }

    void CreateAlterLoginCreateUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, const TString& password, const TVector<TExpectedResult>& expectedResults) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);

        auto createUser = transaction->MutableAlterLogin()->MutableCreateUser();
        createUser->SetUser(user);
        createUser->SetPassword(password);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TestModificationResults(runtime, txId, expectedResults);
    }

    void CreateAlterLoginCreateUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, const TString& hashedPassword, const TString& hashedPasswordOldFormat, const TVector<TExpectedResult>& expectedResults) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);

        auto createUser = transaction->MutableAlterLogin()->MutableCreateUser();
        createUser->SetUser(user);
        createUser->SetIsHashedPassword(true);
        createUser->SetPassword(hashedPasswordOldFormat);
        createUser->SetHashedPassword(hashedPassword);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TestModificationResults(runtime, txId, expectedResults);
    }

    void CreateAlterLoginRemoveUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, const TVector<TExpectedResult>& expectedResults) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);
        auto removeUser = transaction->MutableAlterLogin()->MutableRemoveUser();
        removeUser->SetUser(user);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TestModificationResults(runtime, txId, expectedResults);
    }

    void CreateAlterLoginCreateGroup(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& group, const TVector<TExpectedResult>& expectedResults) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);
        auto createGroup = transaction->MutableAlterLogin()->MutableCreateGroup();
        createGroup->SetGroup(group);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TestModificationResults(runtime, txId, expectedResults);
    }

    void CreateAlterLoginRemoveGroup(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& group, const TVector<TExpectedResult>& expectedResults) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);
        auto createGroup = transaction->MutableAlterLogin()->MutableRemoveGroup();
        createGroup->SetGroup(group);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TestModificationResults(runtime, txId, expectedResults);
    }

    void AlterLoginAddGroupMembership(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& member, const TString& group, const TVector<TExpectedResult>& expectedResults) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);

        auto addGroupMembership = transaction->MutableAlterLogin()->MutableAddGroupMembership();
        addGroupMembership->SetMember(member);
        addGroupMembership->SetGroup(group);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TestModificationResults(runtime, txId, expectedResults);
    }

    void AlterLoginRemoveGroupMembership(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& member, const TString& group, const TVector<TExpectedResult>& expectedResults) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);

        auto removeGroupMembership = transaction->MutableAlterLogin()->MutableRemoveGroupMembership();
        removeGroupMembership->SetMember(member);
        removeGroupMembership->SetGroup(group);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TestModificationResults(runtime, txId, expectedResults);
    }

    NKikimrScheme::TEvLoginResult Login(TTestActorRuntime& runtime, const TString& user, const TString& password) {
        TActorId sender = runtime.AllocateEdgeActor();
        auto evLogin = new TEvSchemeShard::TEvLogin();
        evLogin->Record.SetUser(user);
        evLogin->Record.SetPassword(password);

        if (auto ldapDomain = runtime.GetAppData().AuthConfig.GetLdapAuthenticationDomain(); user.EndsWith("@" + ldapDomain)) {
            evLogin->Record.SetExternalAuth(ldapDomain);
        }
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, evLogin);
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvLoginResult>(handle);
        UNIT_ASSERT(event);
        return event->Record;
    }

    NKikimrScheme::TEvLoginResult Login(TTestActorRuntime& runtime, const TString& user, NLoginProto::ESaslAuthMech::SaslAuthMech authMech, NLoginProto::EHashType::HashType hashType, const TString& hash, const TString& authMessage) {
        TActorId sender = runtime.AllocateEdgeActor();
        auto evLogin = new TEvSchemeShard::TEvLogin();
        evLogin->Record.SetUser(user);
        auto& hashesToValidate = *evLogin->Record.MutableHashToValidate();
        hashesToValidate.SetAuthMech(authMech);
        hashesToValidate.SetHashType(hashType);
        hashesToValidate.SetHash(hash);
        hashesToValidate.SetAuthMessage(authMessage);

        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, evLogin);
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvLoginResult>(handle);
        UNIT_ASSERT(event);
        return event->Record;
    }

    NKikimrScheme::TEvLoginResult LoginFinalize(
        TTestActorRuntime& runtime,
        const NLogin::TLoginProvider::TLoginUserRequest& request,
        const NLogin::TLoginProvider::TPasswordCheckResult& checkResult,
        const TString& passwordHash,
        const bool needUpdateCache
    ) {
        const auto evLoginFinalize = new NSchemeShard::TEvPrivate::TEvLoginFinalize(
            request, checkResult, runtime.AllocateEdgeActor(), passwordHash, needUpdateCache
        );
        AsyncSend(runtime, TTestTxConfig::SchemeShard, evLoginFinalize);
        TAutoPtr<IEventHandle> handle;
        const auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvLoginResult>(handle);
        UNIT_ASSERT(event);
        return event->Record;
    }

    void ModifyUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, std::function<void(::NKikimrSchemeOp::TLoginModifyUser*)>&& initiator) {
        auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
        auto transaction = modifyTx->Record.AddTransaction();
        transaction->SetWorkingDir(database);
        transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);

        auto alterUser = transaction->MutableAlterLogin()->MutableModifyUser();

        initiator(alterUser);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
        TAutoPtr<IEventHandle> handle;
        [[maybe_unused]]auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle); // wait()
    }

    void ChangeIsEnabledUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, bool isEnabled) {
        ModifyUser(runtime, txId, database, [user, isEnabled](auto* alterUser) {
            alterUser->SetUser(std::move(user));
            alterUser->SetCanLogin(isEnabled);
        });
    }

    void ChangePasswordUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, const TString& password) {
        ModifyUser(runtime, txId, database, [user, password](auto* alterUser) {
            alterUser->SetUser(std::move(user));
            alterUser->SetPassword(std::move(password));
        });
    }

    void ChangePasswordHashUser(TTestActorRuntime& runtime, ui64 txId, const TString& database, const TString& user, const TString& hash) {
        ModifyUser(runtime, txId, database, [user, hash](auto* alterUser) {
            alterUser->SetUser(std::move(user));
            alterUser->SetPassword(std::move(hash));
            alterUser->SetIsHashedPassword(true);
        });
    }

    // class TFakeDataReq {
    TFakeDataReq::TFakeDataReq(NActors::TTestActorRuntime &runtime, ui64 txId, const TString &table, const TString &query)
        : Runtime(runtime)
        , Env(Alloc)
        , TxId(txId)
        , Table(table)
        , Query(query)
    {
        Alloc.Release();
    }

    TFakeDataReq::~TFakeDataReq() {
        Engine.Destroy();
        Alloc.Acquire();
    }

    NMiniKQL::IEngineFlat::EStatus TFakeDataReq::Propose(bool immediate, bool &activeZone, ui32 txFlags) {
        NMiniKQL::TRuntimeNode pgm = ProgramText2Bin(Query);

        NMiniKQL::TEngineFlatSettings settings(NMiniKQL::IEngineFlat::EProtocol::V1,
                                               Runtime.GetAppData().FunctionRegistry,
                                               *TAppData::RandomProvider, *TAppData::TimeProvider);
        settings.BacktraceWriter = [](const char* operation, ui32 line, const TBackTrace* backtrace) {
            Cerr << "\nEngine backtrace, operation: " << operation << " (" << line << ")\n";
            if (backtrace) {
                backtrace->PrintTo(Cerr);
            }
        };

        NKikimrTxDataShard::ETransactionKind kind = NKikimrTxDataShard::TX_KIND_DATA;
        Engine = CreateEngineFlat(settings);
        auto result = Engine->SetProgram(SerializeRuntimeNode(pgm, Env));
        UNIT_ASSERT_EQUAL_C(result, NMiniKQL::IEngineFlat::EResult::Ok, Engine->GetErrors());
        auto& dbKeys = Engine->GetDbKeys();
        TSet<ui64> resolvedShards;
        for (auto& dbKey : dbKeys) {
            ResolveKey(*dbKey);
            UNIT_ASSERT(dbKey->Status == TKeyDesc::EStatus::Ok);
            for (auto& partition : dbKey->GetPartitions()) {
                resolvedShards.insert(partition.ShardId);
            }
        }

        result = Engine->PrepareShardPrograms();
        if (result != NMiniKQL::IEngineFlat::EResult::Ok) {
            Cerr << Engine->GetErrors() << Endl;
            return NMiniKQL::IEngineFlat::EStatus::Error;
        }

        const ui32 shardsCount = Engine->GetAffectedShardCount();
        UNIT_ASSERT_VALUES_EQUAL(shardsCount, resolvedShards.size());
        bool hasErrors = false;
        for (ui32 i = 0; i < shardsCount; ++i) {
            NMiniKQL::IEngineFlat::TShardData shardData;
            result = Engine->GetAffectedShard(i, shardData);
            UNIT_ASSERT_EQUAL_C(result, NMiniKQL::IEngineFlat::EResult::Ok, Engine->GetErrors());
            NKikimrTxDataShard::TDataTransaction tx;
            tx.SetMiniKQL(shardData.Program);
            tx.SetImmediate(immediate && shardData.Immediate);
            auto txBody = tx.SerializeAsString();

            TActorId sender = Runtime.AllocateEdgeActor();
            for (;;) {
                auto proposal = new TEvDataShard::TEvProposeTransaction(kind, sender, TxId, txBody, txFlags);

                activeZone = false;
                Runtime.SendToPipe(shardData.ShardId, sender, proposal);
                TAutoPtr<IEventHandle> handle;
                auto event = Runtime.GrabEdgeEventIf<TEvDataShard::TEvProposeTransactionResult>(handle,
                                                                                                [this, shardData](const TEvDataShard::TEvProposeTransactionResult& event) {
                    return event.GetTxId() == TxId && event.GetOrigin() == shardData.ShardId;
                });
                activeZone = true;

                UNIT_ASSERT(event);
                UNIT_ASSERT_EQUAL(event->GetTxKind(), kind);
                if (event->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::TRY_LATER)
                    continue;

                if (event->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::ERROR ||
                        event->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::OVERLOADED)
                {
                    hasErrors = true;
                    for (auto err : event->Record.GetError()) {
                        Cerr << "DataShard error: " << shardData.ShardId << ", kind: " <<
                                NKikimrTxDataShard::TError::EKind_Name(err.GetKind()) << ", reason: " << err.GetReason() << Endl;
                        Errors[shardData.ShardId].push_back(err);
                    }

                    break;
                }

                if (event->GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE) {
                    Engine->AddShardReply(event->GetOrigin(), event->Record.GetTxResult());
                    Engine->FinalizeOriginReplies(shardData.ShardId);
                    break;
                }

                UNIT_ASSERT_VALUES_EQUAL_C(event->GetStatus(), NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED, "Unexpected Propose result");
                AffectedShards.push_back(shardData.ShardId);
                break;
            }
        }

        if (hasErrors) {
            return NMiniKQL::IEngineFlat::EStatus::Error;
        }

        Engine->AfterShardProgramsExtracted();
        if (!AffectedShards.empty())
            return NMiniKQL::IEngineFlat::EStatus::Unknown;

        Engine->BuildResult();
        if (Engine->GetStatus() == NMiniKQL::IEngineFlat::EStatus::Error) {
            Cerr << Engine->GetErrors() << Endl;
        }

        return Engine->GetStatus();
    }

    void TFakeDataReq::Plan(ui64 coordinatorId) {
        TActorId sender = Runtime.AllocateEdgeActor();

        ui64 minStep = 0;
        ui64 maxStep = Max<ui64>(); // unlimited
        ui8 execLevel = 0;

        THolder<TEvTxProxy::TEvProposeTransaction> ex(
                    new TEvTxProxy::TEvProposeTransaction(coordinatorId, TxId, execLevel, minStep, maxStep));

        auto *reqAffectedSet = ex->Record.MutableTransaction()->MutableAffectedSet();
        reqAffectedSet->Reserve(AffectedShards.size());
        for (auto affectedTablet : AffectedShards) {
            auto *x = reqAffectedSet->Add();
            x->SetTabletId(affectedTablet);
            x->SetFlags(2 /*todo: use generic enum*/);
        }

        Runtime.SendToPipe(coordinatorId, sender, ex.Release());
    }

    NMiniKQL::TRuntimeNode TFakeDataReq::ProgramText2Bin(const TString &query) {
        auto expr = NYql::ParseText(query);

        TMockDbSchemeResolver dbSchemeResolver;
        FillTableInfo(dbSchemeResolver);

        auto resFuture = NYql::ConvertToMiniKQL(
                    expr, Runtime.GetAppData().FunctionRegistry,
                    &Env, &dbSchemeResolver
                    );

        const TDuration TIME_LIMIT = TDuration::Seconds(60);
        NYql::TConvertResult res = resFuture.GetValue(TIME_LIMIT);
        res.Errors.PrintTo(Cerr);
        UNIT_ASSERT(res.Node.GetNode());
        return res.Node;
    }

    void TFakeDataReq::FillTableInfo(TMockDbSchemeResolver &dbSchemeResolver) const {
        // Synchronously get table description from SS
        auto fnFIllInfo = [this, &dbSchemeResolver] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);

            using namespace NYql;
            using TColumn = IDbSchemeResolver::TTableResult::TColumn;

            const auto& pathDesc = record.GetPathDescription();
            const auto& tdesc = pathDesc.GetTable();

            IDbSchemeResolver::TTableResult table(IDbSchemeResolver::TTableResult::Ok);
            table.Table.TableName = Table;
            table.TableId.Reset(new TTableId(pathDesc.GetSelf().GetSchemeshardId(), pathDesc.GetSelf().GetPathId()));
            table.KeyColumnCount = tdesc.KeyColumnIdsSize();
            for (size_t i = 0; i < tdesc.ColumnsSize(); i++) {
                auto& c = tdesc.GetColumns(i);
                table.Table.ColumnNames.insert(c.GetName());
                i32 keyIdx = -1;
                for (size_t ki = 0; ki < tdesc.KeyColumnIdsSize(); ki++) {
                    if (tdesc.GetKeyColumnIds(ki) == c.GetId()) {
                        keyIdx = ki;
                    }
                }
                table.Columns.insert(std::make_pair(c.GetName(), TColumn{c.GetId(), keyIdx, NScheme::TTypeInfo(c.GetTypeId()), 0,
                    EColumnTypeConstraint::Nullable}));
            }
            dbSchemeResolver.AddTable(table);
        };

        TestLs(Runtime, Table, true, fnFIllInfo);
    }

    void TFakeDataReq::FillTablePartitioningInfo() {
        auto fnFillInfo = [this] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);

            const auto& pathDesc = record.GetPathDescription();
            const auto& tdesc = pathDesc.GetTable();

            // Cout << pathDesc << Endl;
            std::unordered_map<ui32, NScheme::TTypeInfo> columnId2Type;
            for (size_t i = 0; i < tdesc.ColumnsSize(); ++i) {
                ui32 id = tdesc.GetColumns(i).GetId();
                auto typeInfo = NScheme::TTypeInfo(tdesc.GetColumns(i).GetTypeId());
                columnId2Type[id] = typeInfo;
            }

            for (size_t i = 0; i < tdesc.KeyColumnIdsSize(); ++i) {
                ui32 id = tdesc.GetKeyColumnIds(i);
                TablePartitioningInfo.KeyColumnTypes.push_back(columnId2Type[id]);
            }

            for (size_t i = 0; i < pathDesc.TablePartitionsSize(); ++i) {
                const auto& pi = pathDesc.GetTablePartitions(i);
                TablePartitioningInfo.Partitioning.push_back(TTablePartitioningInfo::TBorder());
                TablePartitioningInfo.Partitioning.back().KeyTuple = TSerializedCellVec(pi.GetEndOfRangeKeyPrefix());
                TablePartitioningInfo.Partitioning.back().Inclusive = pi.GetIsInclusive();
                TablePartitioningInfo.Partitioning.back().Point = pi.GetIsPoint();
                TablePartitioningInfo.Partitioning.back().Defined = true;
                TablePartitioningInfo.Partitioning.back().Datashard = pi.GetDatashardId();
            }
        };

        TestLs(Runtime, Table, true, fnFillInfo);
    }

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> TFakeDataReq::TTablePartitioningInfo::ResolveKey(
        const TTableRange& range) const
    {
        Y_ABORT_UNLESS(!Partitioning.empty());

        auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();

        // Temporary fix: for an empty range we need to return some datashard so that it can handle readset logic (
        // send empty result to other tx participants etc.)
        if (range.IsEmptyRange(KeyColumnTypes)) {
            partitions->push_back(TKeyDesc::TPartitionInfo(Partitioning.begin()->Datashard));
            return partitions;
        }

        TVector<TBorder>::const_iterator low = LowerBound(Partitioning.begin(), Partitioning.end(), true,
                                                          [&](const TBorder &left, bool) {
            const int compares = CompareBorders<true, false>(left.KeyTuple.GetCells(), range.From, left.Inclusive || left.Point, range.InclusiveFrom || range.Point, KeyColumnTypes);
            return (compares < 0);
        });

        Y_ABORT_UNLESS(low != Partitioning.end(), "last key must be (inf)");
        do {
            partitions->push_back(TKeyDesc::TPartitionInfo(low->Datashard));

            if (range.Point)
                return partitions;

            int prevComp = CompareBorders<true, true>(low->KeyTuple.GetCells(), range.To, low->Point || low->Inclusive, range.InclusiveTo, KeyColumnTypes);
            if (prevComp >= 0)
                return partitions;
        } while (++low != Partitioning.end());

        return partitions;
    }

    TEvSchemeShard::TEvModifySchemeTransaction* CombineSchemeTransactions(const TVector<TEvSchemeShard::TEvModifySchemeTransaction*>& transactions) {
        ui64 txId = 0;
        ui64 tabletId = 0;
        if (transactions) {
            txId = transactions.front()->Record.GetTxId();
            tabletId = transactions.front()->Record.GetTabletId();
        }
        TEvSchemeShard::TEvModifySchemeTransaction* combination = new TEvSchemeShard::TEvModifySchemeTransaction(txId, tabletId);
        for ( auto& modifyTx: transactions) {
            for (const auto& tx: modifyTx->Record.GetTransaction()) {
                *combination->Record.AddTransaction() = tx;
            }
            delete modifyTx;
        }
        return combination;
    }

    void AsyncSend(TTestActorRuntime &runtime, ui64 targetTabletId, IEventBase *ev,
            ui32 nodeIndex, TActorId sender) {
        if (sender == TActorId()) {
            ForwardToTablet(runtime, targetTabletId, runtime.AllocateEdgeActor(nodeIndex), ev);
        } else {
            ForwardToTablet(runtime, targetTabletId, sender, ev, nodeIndex);
        }
    }

    TEvTx* InternalTransaction(TEvTx* tx) {
        for (auto& x : *tx->Record.MutableTransaction()) {
            x.SetInternal(true);
        }

        return tx;
    }

    TTestActorRuntimeBase::TEventObserver SetSuppressObserver(TTestActorRuntime &runtime, TVector<THolder<IEventHandle> > &suppressed, ui32 type) {
        return runtime.SetObserverFunc([&suppressed, type](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == type) {
                suppressed.push_back(std::move(ev));
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    void WaitForSuppressed(TTestActorRuntime &runtime, TVector<THolder<IEventHandle> > &suppressed, ui32 count, TTestActorRuntimeBase::TEventObserver prevObserver) {
        Y_VERIFY_S(suppressed.size() <= count, "suppressed.size(): " << suppressed.size() << " expected " << count);

        if (suppressed.size() < count) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition([&](IEventHandle&) -> bool {
                                              return suppressed.size() >= count;
                                          }));
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
    }

    NKikimrTxDataShard::TEvCompactTableResult CompactTable(
        TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId, bool compactBorrowed, bool compactSinglePartedShards)
    {
        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvDataShard::TEvCompactTable>(tableId.PathId);
        request->Record.SetCompactBorrowed(compactBorrowed);
        request->Record.SetCompactSinglePartedShards(compactSinglePartedShards);
        runtime.SendToPipe(shardId, sender, request.Release(), 0, GetPipeConfigWithRetries());

        auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
        return ev->Get()->Record;
    }

    void SendTEvPeriodicTopicStats(TTestActorRuntime& runtime, ui64 topicId, ui64 generation, ui64 round, ui64 dataSize, ui64 usedReserveSize) {
        TActorId sender = runtime.AllocateEdgeActor();

        TEvPersQueue::TEvPeriodicTopicStats* ev = new TEvPersQueue::TEvPeriodicTopicStats();
        auto& rec = ev->Record;
        rec.SetPathId(topicId);
        rec.SetGeneration(generation);
        rec.SetRound(round);
        rec.SetDataSize(dataSize);
        rec.SetUsedReserveSize(usedReserveSize);

        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, ev);
    }

    void WriteToTopic(TTestActorRuntime& runtime, const TString& path, ui32& msgSeqNo, const TString& message) {
        auto topicDescr = DescribePath(runtime, path).GetPathDescription().GetPersQueueGroup();
        auto partitionId = topicDescr.GetPartitions()[0].GetPartitionId();
        auto tabletId = topicDescr.GetPartitions()[0].GetTabletId();

        const auto edge = runtime.AllocateEdgeActor();
        TString cookie = NKikimr::NPQ::CmdSetOwner(&runtime, tabletId, edge, partitionId, "default", true).first;

        TVector<std::pair<ui64, TString>> data;
        data.push_back({1, message});
        NKikimr::NPQ::CmdWrite(&runtime, tabletId, edge, partitionId, "sourceid0", msgSeqNo, data, false, {}, true, cookie, 0);
    }

    void UpdateRow(TTestActorRuntime& runtime, const TString& table, const ui32 key, const TString& value, ui64 tabletId) {
        NKikimrMiniKQL::TResult result;
        TString error;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
            (
                (let key '( '('key (Uint32 '%d) ) ) )
                (let row '( '('value (Utf8 '%s) ) ) )
                (return (AsList (UpdateRow '__user__%s key row) ))
            )
        )", key, value.c_str(), table.c_str()), result, error);

        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }

    void UpdateRowPg(TTestActorRuntime& runtime, const TString& table, const ui32 key, ui32 value, ui64 tabletId) {
        NKikimrMiniKQL::TResult result;
        TString error;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
            (
                (let key '( '('key (Utf8 '%d) ) ) )
                (let row '( '('value (PgConst '%u (PgType 'int4)) ) ) )
                (return (AsList (UpdateRow '__user__%s key row) ))
            )
        )", key, value, table.c_str()), result, error);

        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }

    void UploadRow(TTestActorRuntime& runtime, const TString& tablePath, int partitionIdx, const TVector<ui32>& keyTags, const TVector<ui32>& valueTags, const TVector<TCell>& keys, const TVector<TCell>& values)
    {
        auto tableDesc = DescribePath(runtime, tablePath, true, true);
        const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT(partitionIdx < tablePartitions.size());
        const ui64 datashardTabletId = tablePartitions[partitionIdx].GetDatashardId();

        auto ev = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        ev->Record.SetTableId(tableDesc.GetPathId());

        auto& scheme = *ev->Record.MutableRowScheme();
        for (ui32 tag : keyTags) {
            scheme.AddKeyColumnIds(tag);
        }
        for (ui32 tag : valueTags) {
            scheme.AddValueColumnIds(tag);
        }

        auto& row = *ev->Record.AddRows();
        row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
        row.SetValueColumns(TSerializedCellVec::Serialize(values));

        const auto& sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, datashardTabletId, sender, ev.Release());
        auto evResponse = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(sender);
        UNIT_ASSERT_C(evResponse->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK, "Status: " << evResponse->Get()->Record.GetStatus() << " Issues: " << evResponse->Get()->Record.GetErrorDescription());
    }

    void WriteOp(
        TTestActorRuntime& runtime,
        ui64 schemeshardId,
        const ui64 txId,
        const TString& tablePath,
        int partitionIdx,
        NKikimrDataEvents::TEvWrite_TOperation::EOperationType operationType,
        const std::vector<ui32>& columnIds,
        TSerializedCellMatrix&& data,
        bool successIsExpected)
    {
        auto tableDesc = DescribePath(runtime, schemeshardId, tablePath, true, true);
        const auto& pathDesc = tableDesc.GetPathDescription();
        TTableId tableId(pathDesc.GetSelf().GetSchemeshardId(), pathDesc.GetSelf().GetPathId(), pathDesc.GetTable().GetTableSchemaVersion());

        const auto& tablePartitions = pathDesc.GetTablePartitions();
        UNIT_ASSERT(partitionIdx < tablePartitions.size());
        const ui64 datashardTabletId = tablePartitions[partitionIdx].GetDatashardId();

        const auto& sender = runtime.AllocateEdgeActor();

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(data.ReleaseBuffer()));
        evWrite->AddOperation(operationType, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

        ForwardToTablet(runtime, datashardTabletId, sender, evWrite.release());

        auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender);
        auto status = ev->Get()->Record.GetStatus();

        UNIT_ASSERT_C(successIsExpected == (status == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED), "Status: " << ev->Get()->Record.GetStatus() << " Issues: " << ev->Get()->Record.GetIssues());
    }

    void WriteRow(TTestActorRuntime& runtime, ui64 schemeshardId, const ui64 txId, const TString& tablePath, int partitionIdx, const ui32 key, const TString& value, bool successIsExpected) {
        std::vector<ui32> columnIds{1, 2};

        TVector<TCell> cells{TCell((const char*)&key, sizeof(ui32)), TCell(value.c_str(), value.size())};
        TSerializedCellMatrix matrix(cells, 1, 2);

        WriteOp(runtime, schemeshardId, txId, tablePath, partitionIdx, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, columnIds, std::move(matrix), successIsExpected);
    }

    void WriteRow(TTestActorRuntime& runtime, const ui64 txId, const TString& tablePath, int partitionIdx, const ui32 key, const TString& value, bool successIsExpected) {
        WriteRow(runtime, TTestTxConfig::SchemeShard, txId, tablePath, partitionIdx, key, value, successIsExpected);
    }

    void DeleteRow(TTestActorRuntime& runtime, ui64 schemeshardId, const ui64 txId, const TString& tablePath, int partitionIdx, const ui32 key, bool successIsExpected) {
        std::vector<ui32> columnIds{1};

        TVector<TCell> cells{TCell((const char*)&key, sizeof(ui32))};
        TSerializedCellMatrix matrix(cells, 1, 1);

        WriteOp(runtime, schemeshardId, txId, tablePath, partitionIdx, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE, columnIds, std::move(matrix), successIsExpected);
    }

    void DeleteRow(TTestActorRuntime& runtime, const ui64 txId, const TString& tablePath, int partitionIdx, const ui32 key, bool successIsExpected) {
        DeleteRow(runtime, TTestTxConfig::SchemeShard, txId, tablePath, partitionIdx, key, successIsExpected);
    }

    void SendNextValRequest(TTestActorRuntime& runtime, const TActorId& sender, const TString& path) {
        auto request = MakeHolder<NSequenceProxy::TEvSequenceProxy::TEvNextVal>(path);
        runtime.Send(new IEventHandle(NSequenceProxy::MakeSequenceProxyServiceID(), sender, request.Release()));
    }

    i64 WaitNextValResult(
            TTestActorRuntime& runtime, const TActorId& sender, Ydb::StatusIds::StatusCode expectedStatus) {
        auto ev = runtime.GrabEdgeEventRethrow<NSequenceProxy::TEvSequenceProxy::TEvNextValResult>(sender);
        auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->Status, expectedStatus);
        return msg->Value;
    }

    i64 DoNextVal(TTestActorRuntime& runtime, const TString& path, Ydb::StatusIds::StatusCode expectedStatus) {
        auto sender = runtime.AllocateEdgeActor(0);
        SendNextValRequest(runtime, sender, path);
        return WaitNextValResult(runtime, sender, expectedStatus);
    }

    NKikimrMiniKQL::TResult ReadSystemTable(TTestActorRuntime& runtime, ui64 tabletId,
            const TString& table, const TVector<TString>& pk, const TVector<TString>& columns,
            const TString& rangeFlags)
    {
        TStringBuilder keyFmt;
        for (const auto& k : pk) {
            keyFmt << "'('" << k << " (Null) (Void)) ";
        }
        const auto columnsFmt = "'" + JoinSeq(" '", columns);

        NKikimrMiniKQL::TResult result;
        TString error;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"((
            (let range '(%s%s))
            (let columns '(%s))
            (let result (SelectRange '%s range columns '()))
            (return (AsList (SetResult 'Result result) ))
        ))", rangeFlags.data(), keyFmt.data(), columnsFmt.data(), table.data()), result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        return result;
    }

    NKikimrMiniKQL::TResult ReadTable(TTestActorRuntime& runtime, ui64 tabletId,
            const TString& table, const TVector<TString>& pk, const TVector<TString>& columns,
            const TString& rangeFlags)
    {
        return ReadSystemTable(runtime, tabletId, "__user__"+table, pk, columns, rangeFlags);
    }

    TVector<TString> ReadShards(TTestActorRuntime& runtime, ui64 schemeshardId, const TString& table) {
        auto pathDesc = DescribePath(runtime, schemeshardId, table, true, false, true);
        auto tableDesc = pathDesc.GetPathDescription().GetTable();
        TVector<TString> columns;
        for (const auto& c : tableDesc.GetColumns()) {
            columns.push_back(c.GetName());
        }
        TVector<TString> shardRows;
        for (const auto& partition : pathDesc.GetPathDescription().GetTablePartitions()) {
            auto result = ReadTable(runtime, partition.GetDatashardId(), tableDesc.GetName(),
                {tableDesc.GetKeyColumnNames().begin(), tableDesc.GetKeyColumnNames().end()}, {columns.begin(), columns.end()});

            TStringStream ysonStream;
            NYson::TYsonWriter writer(&ysonStream, NYson::EYsonFormat::Text);
            NYql::IDataProvider::TFillSettings fillSettings;
            bool truncated;
            KikimrResultToYson(ysonStream, writer, result, {}, fillSettings, truncated);
            UNIT_ASSERT(!truncated);
            shardRows.push_back(ysonStream.Str());
        }
        return shardRows;
    }

    ui32 CountRows(TTestActorRuntime& runtime, ui64 schemeshardId, const TString& table) {
        auto tableDesc = DescribePath(runtime, schemeshardId, table, true, false, true);
        const auto& pathDesc = tableDesc.GetPathDescription();
        const auto& key = pathDesc.GetTable().GetKeyColumnNames();
        ui32 rows = 0;
        for (const auto& x : pathDesc.GetTablePartitions()) {
            auto result = ReadTable(runtime, x.GetDatashardId(), pathDesc.GetSelf().GetName(),
                {key.begin(), key.end()}, {pathDesc.GetTable().GetKeyColumnNames()[0]});
            auto value = NClient::TValue::Create(result);
            rows += value["Result"]["List"].Size();
        }
        return rows;
    }

    ui32 CountRows(TTestActorRuntime& runtime, const TString& table) {
        return CountRows(runtime, TTestTxConfig::SchemeShard, table);
    }

    void WriteVectorTableRows(TTestActorRuntime& runtime, ui64 schemeShardId, ui64 txId, const TString & tablePath,
        ui32 shard, ui32 min, ui32 max, std::vector<ui32> columnIds, ui32 vectorDimension) {
        TVector<TCell> cells;
        TVector<ui8> vec(vectorDimension + 1);
        vec[vectorDimension] = (ui8)Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8;
        for (ui32 key = min; key < max; ++key) {
            for (ui32 index : xrange(vectorDimension)) {
                vec[index] = ((key+106)* (10*index+7)) % 256;
                if (index == 2) {
                    vec[2] = ((key+106)*37) % 256;
                }
                if (index == 3) {
                    vec[3] = ((key+106)*47) % 256;
                }
            }
            cells.emplace_back(TCell::Make(key));
            cells.emplace_back(TCell((const char*)vec.data(), vec.size()));
            // optional prefix ui32 column
            cells.emplace_back(TCell::Make(key % 17));
            // optionally use the same value for an additional covered string column
            cells.emplace_back(TCell((const char*)vec.data(), vec.size()));
        }
        if (!columnIds.size()) {
            columnIds = {1, 2, 3, 4};
        }
        TSerializedCellMatrix matrix(cells, max-min, columnIds.size());
        WriteOp(runtime, schemeShardId, txId, tablePath,
            shard, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            columnIds, std::move(matrix), true);
    };

    void TestCreateServerLessDb(TTestActorRuntime& runtime, TTestEnv& env, ui64& txId, ui64& tenantSchemeShard) {
        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", "Name: \"ResourceDB\"");
        env.TestWaitNotification(runtime, txId);

        const auto describeResult = DescribePath(runtime, "/MyRoot/ResourceDB");
        const auto subDomainPathId = describeResult.GetPathId();

        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", R"(
            StoragePools {
              Name: "pool-1"
              Kind: "pool-kind-1"
            }
            StoragePools {
              Name: "pool-2"
              Kind: "pool-kind-2"
            }
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            Name: "ResourceDB"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto attrs = AlterUserAttrs({
            {"cloud_id", "CLOUD_ID_VAL"},
            {"folder_id", "FOLDER_ID_VAL"},
            {"database_id", "DATABASE_ID_VAL"},
        });
        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "ServerLessDB"
            ResourcesDomainKey {
                SchemeShard: %lu
                PathId: %lu
            }
        )", TTestTxConfig::SchemeShard, subDomainPathId), attrs);
        env.TestWaitNotification(runtime, txId);

        TString alterData = R"(
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: false
            Name: "ServerLessDB"
            StoragePools {
                Name: "pool-1"
                Kind: "pool-kind-1"
            }
        )";
        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", alterData);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLessDB"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("ServerLessDB"),
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
    }

    void MeteringDataEqual(const TString& leftMsg, const TString& rightMsg) {
        const auto leftMeteringData = NJson::ReadJsonFastTree(leftMsg);
        const auto rightMeteringData = NJson::ReadJsonFastTree(rightMsg);

        const auto leftIdParts = SplitString(leftMeteringData["id"].GetString(), "-");
        const auto rightIdParts = SplitString(rightMeteringData["id"].GetString(), "-");
        UNIT_ASSERT_VALUES_EQUAL(leftIdParts.size(), rightIdParts.size());

        size_t localPathIdIndex = 2;
        if (leftMeteringData["source_id"] == "sless-docapi-ydb-storage") {
            localPathIdIndex = 1;
        }

        for (size_t i = 0; i < leftIdParts.size(); ++i) {
            if (i != localPathIdIndex) { // no need to compare localPathIds
                UNIT_ASSERT_VALUES_EQUAL(leftIdParts[i], rightIdParts[i]);
            }
        }

        const auto& leftUsage = leftMeteringData["usage"];
        const auto& rightUsage = rightMeteringData["usage"];
        for (const auto& field : {"quantity", "unit", "type"}) {
            Cerr << field << ": " << leftUsage[field] << ", " << rightUsage[field] << Endl;
            UNIT_ASSERT_VALUES_EQUAL(leftUsage[field], rightUsage[field]);
        }

        for (const auto& field : {"version", "schema", "cloud_id", "folder_id", "resource_id", "source_id"}) {
            UNIT_ASSERT_VALUES_EQUAL(leftMeteringData[field], rightMeteringData[field]);
        }

        const auto& leftTags = leftMeteringData["tags"].GetMap();
        const auto& rightTags = rightMeteringData["tags"].GetMap();
        UNIT_ASSERT_VALUES_EQUAL(leftTags.size(), rightTags.size());

        for (const auto& [tag, value] : leftTags) {
            auto it = rightTags.find(tag);
            UNIT_ASSERT(it != rightTags.end());
            UNIT_ASSERT_VALUES_EQUAL(value, it->second);
        }

        const auto& leftLabels = leftMeteringData["labels"].GetMap();
        const auto& rightLabels = rightMeteringData["labels"].GetMap();
        UNIT_ASSERT_VALUES_EQUAL(leftLabels.size(), rightLabels.size());

        for (const auto& [label, value] : leftLabels) {
            auto it = rightLabels.find(label);
            UNIT_ASSERT(it != rightLabels.end());
            UNIT_ASSERT_VALUES_EQUAL(value, it->second);
        }
    }

    NKikimrBackup::TEvGetBackupCollectionRestoreResponse TestGetBackupCollectionRestore(TTestActorRuntime& runtime, ui64 id, const TString& dbName, Ydb::StatusIds::StatusCode expectedStatus) {
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), new TEvBackup::TEvGetBackupCollectionRestoreRequest(dbName, id));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvBackup::TEvGetBackupCollectionRestoreResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetBackupCollectionRestore().GetStatus(), expectedStatus);

        return ev->Record;
    }

    NKikimrBackup::TEvForgetBackupCollectionRestoreResponse TestForgetBackupCollectionRestore(TTestActorRuntime& runtime, ui64 txId, const TString& dbName, ui64 restoreId, Ydb::StatusIds::StatusCode expectedStatus) {
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), new TEvBackup::TEvForgetBackupCollectionRestoreRequest(txId, dbName, restoreId));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvBackup::TEvForgetBackupCollectionRestoreResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetStatus(), expectedStatus);

        return ev->Record;
    }

    NKikimrBackup::TEvListBackupCollectionRestoresResponse TestListBackupCollectionRestores(TTestActorRuntime& runtime, const TString& dbName, ui64 pageSize, const TString& pageToken, Ydb::StatusIds::StatusCode expectedStatus) {
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), new TEvBackup::TEvListBackupCollectionRestoresRequest(dbName, pageSize, pageToken));

        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvBackup::TEvListBackupCollectionRestoresResponse>(handle);
        UNIT_ASSERT_EQUAL(ev->Record.GetStatus(), expectedStatus);

        return ev->Record;
    }
}
