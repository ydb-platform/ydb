#include "datashard_ut_common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_bio_events.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/balance_coverage/balance_coverage_builder.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <ydb/library/yql/minikql/mkql_node_serialization.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

#include <util/system/valgrind.h>

namespace NKikimr {

using namespace NMiniKQL;
using namespace NSchemeShard;
using namespace Tests;

const bool ENABLE_DATASHARD_LOG = true;
const bool DUMP_RESULT = false;

void TTester::Setup(TTestActorRuntime& runtime, const TOptions& opts) {
    Y_UNUSED(opts);
    if (ENABLE_DATASHARD_LOG) {
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    }
    runtime.SetLogPriority(NKikimrServices::MINIKQL_ENGINE, NActors::NLog::PRI_DEBUG);

    ui32 domainId = 0;
    ui32 planResolution = 500;

    TAppPrepare app;


    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
                      "dc-1", domainId, FAKE_SCHEMESHARD_TABLET_ID,
                      domainId, domainId, TVector<ui32>{domainId},
                      domainId, TVector<ui32>{domainId},
                      planResolution,
                      TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(domainId, 1)},
                      TVector<ui64>{},
                      TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(domainId, 1)});

    TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
    ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
    runtime.SetTxAllocatorTabletIds(ids);

    app.AddDomain(domain.Release());
    SetupTabletServices(runtime, &app);
}

TTester::TTester(ESchema schema, const TOptions& opts)
    : Schema(schema)
    , LastTxId(0)
    , LastStep(opts.FirstStep)
{
    Setup(Runtime, opts);
    Sender = Runtime.AllocateEdgeActor();

    // Schemeshard is only used to receive notifications
    CreateTestBootstrapper(Runtime, CreateTestTabletInfo(FAKE_SCHEMESHARD_TABLET_ID, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
    CreateTestBootstrapper(Runtime, CreateTestTabletInfo(FAKE_TX_ALLOCATOR_TABLET_ID, TTabletTypes::TxAllocator), &CreateTxAllocator);
    CreateSchema(schema, opts);
}

TTester::TTester(ESchema schema, const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup,
                 bool& activeZone, const TOptions& opts)
    : Schema(schema)
    , LastTxId(0)
    , LastStep(1)
{
    Setup(Runtime, opts);
    setup(Runtime);
    Sender = Runtime.AllocateEdgeActor();
    AllowIncompleteResult = (dispatchName != INITIAL_TEST_DISPATCH_NAME);
    ActiveZone = &activeZone;
    DispatchName = dispatchName;

    // Schemeshard is only used to receive notifications
    CreateTestBootstrapper(Runtime, CreateTestTabletInfo(FAKE_SCHEMESHARD_TABLET_ID, TTabletTypes::SchemeShard), &CreateFlatTxSchemeShard);
    CreateTestBootstrapper(Runtime, CreateTestTabletInfo(FAKE_TX_ALLOCATOR_TABLET_ID, TTabletTypes::TxAllocator), &CreateTxAllocator);
    CreateSchema(schema, opts);
}

void TTester::EmptyShardKeyResolver(TKeyDesc& key) {
    Y_UNUSED(key);
    Y_FAIL();
}

void TTester::SingleShardKeyResolver(TKeyDesc& key) {
    key.Status = TKeyDesc::EStatus::Ok;

    auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
    partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet0));
    key.Partitioning = partitions;
}

void TTester::ThreeShardPointKeyResolver(TKeyDesc& key) {
    const ui32 ShardBorder1 = 1000;
    const ui32 ShardBorder2 = 2000;

    auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
    key.Status = TKeyDesc::EStatus::Ok;
    if (key.Range.Point) {
        ui32 key0 = *(ui32*)key.Range.From[0].Data();
        if (key0 < ShardBorder1) {
            partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet0));
        } else if (key0 < ShardBorder2) {
            partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet1));
        } else {
            partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet2));
        }
    } else {
        UNIT_ASSERT(key.Range.From.size() > 0);
        UNIT_ASSERT(key.Range.To.size() > 0);
        UNIT_ASSERT(key.Range.InclusiveFrom);
        UNIT_ASSERT(key.Range.InclusiveTo);

        ui32 from = *(ui32*)key.Range.From[0].Data();
        ui32 to = *(ui32*)key.Range.To[0].Data();
        UNIT_ASSERT(from <= to);

        if (from < ShardBorder1)
            partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet0));
        if (from < ShardBorder2 && to >= ShardBorder1)
            partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet1));
        if (to >= ShardBorder2)
            partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet2));
    }

    key.Partitioning = partitions;
}

TTester::TKeyResolver TTester::GetKeyResolver() const {
    switch (Schema) {
        case ESchema_KV:
        case ESchema_SpecialKV:
        case ESchema_DoubleKV:
        case ESchema_DoubleKVExternal:
            return SingleShardKeyResolver;
        case ESchema_MultiShardKV:
            return ThreeShardPointKeyResolver;
    }
    return EmptyShardKeyResolver;
}

void TTester::CreateDataShard(TFakeMiniKQLProxy& proxy, ui64 tabletId, const TString& schemeText, bool withRegister) {
    TActorId actorId = CreateTestBootstrapper(Runtime, CreateTestTabletInfo(tabletId, TTabletTypes::DataShard),
        &::NKikimr::CreateDataShard);
    Y_UNUSED(actorId);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    Runtime.DispatchEvents(options);

    UNIT_ASSERT_EQUAL(proxy.ExecSchemeCreateTable(schemeText, {tabletId}), IEngineFlat::EStatus::Complete);

    RebootTablet(Runtime, tabletId, Sender);

    //Runtime.EnableScheduleForActor(actorId, true);

    if (withRegister) {
        RegisterTableInResolver(schemeText);
    }
}

// needed for MiniKQL compiling
void TTester::RegisterTableInResolver(const TString& schemeText)
{
    NKikimrSchemeOp::TTableDescription tdesc;
    bool parsed = NProtoBuf::TextFormat::ParseFromString(schemeText, &tdesc);
    UNIT_ASSERT(parsed);

    using namespace NYql;
    using TColumn = IDbSchemeResolver::TTableResult::TColumn;
    IDbSchemeResolver::TTableResult table(IDbSchemeResolver::TTableResult::Ok);
    table.Table.TableName = tdesc.GetName();
    table.TableId.Reset(new TTableId(FAKE_SCHEMESHARD_TABLET_ID, tdesc.GetId_Deprecated()));
    if (tdesc.HasPathId()) {
        table.TableId.Reset(new TTableId(PathIdFromPathId(tdesc.GetPathId())));
    }
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
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(c.GetTypeId(),
            c.HasTypeInfo() ? &c.GetTypeInfo() : nullptr);
        table.Columns.insert(std::make_pair(c.GetName(), TColumn{c.GetId(), keyIdx, typeInfoMod.TypeInfo, 0, EColumnTypeConstraint::Nullable}));
    }
    DbSchemeResolver.AddTable(table);
}


void TTester::CreateSchema(ESchema schema, const TOptions& opts) {
    TString keyValueSchemeText =
        "Name: \"table1\"\n"
        "Id_Deprecated: 13\n"
        "Path: \"/Root/table1\"\n"
        "Columns { Id: 34 Name: \"key\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint32) + " }\n"
        "Columns { Id: 56 Name: \"value\" TypeId: " + ToString<int>(NScheme::NTypeIds::Utf8) + " }\n"
        "Columns { Id: 57 Name: \"uint\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint32) + " }\n"
        "KeyColumnIds: [ 34 ]\n"
        ;

    TString doubleKeyValueSchemeText =
        "Name: \"table2\"\n"
        "PathId { OwnerId: " + ToString(FAKE_SCHEMESHARD_TABLET_ID) + " LocalId: 14 }\n"

        "Path: \"/Root/table2\"\n"
        "Columns { Id: 34 Name: \"key1\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint32) + " }\n"
        "Columns { Id: 35 Name: \"key2\" TypeId: " + ToString<int>(NScheme::NTypeIds::Utf8) + " }\n"
        "Columns { Id: 56 Name: \"value\" TypeId: " + ToString<int>(NScheme::NTypeIds::Utf8) + " }\n"
        "KeyColumnIds: [ 34, 35 ]\n"
        ;

    TString doubleKeyValueExternalSchemeText =
        "Name: \"table2\"\n"
        "PathId { OwnerId: " + ToString(FAKE_SCHEMESHARD_TABLET_ID) + " LocalId: 14 }\n"

        "Path: \"/Root/table2\"\n"
        "Columns { Id: 34 Name: \"key1\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint32) + " }\n"
        "Columns { Id: 35 Name: \"key2\" TypeId: " + ToString<int>(NScheme::NTypeIds::Utf8) + " }\n"
        "Columns { Id: 56 Name: \"value\" TypeId: " + ToString<int>(NScheme::NTypeIds::Utf8) + " }\n"
        "KeyColumnIds: [ 34, 35 ]\n"
        "PartitionConfig { ColumnFamilies { Id: 0 Storage: ColumnStorageTest_1_2_1k } }\n"
        ;

    TString specialKeyValueSchemeText =
        "Name: \"table1\"\n"
        "Id_Deprecated: 13\n"
        "PathId { OwnerId: " + ToString(FAKE_SCHEMESHARD_TABLET_ID) + " LocalId: 13 }\n"
        "Path: \"/Root/table1\"\n"
        "Columns { Id: 34 Name: \"key\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint32) + " }\n"
        "Columns { Id: 56 Name: \"value\" TypeId: " + ToString<int>(NScheme::NTypeIds::Utf8) + " }\n"
        "Columns { Id: 57 Name: \"__tablet\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint64) + " }\n"
        "Columns { Id: 58 Name: \"__updateEpoch\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint64) + " }\n"
        "Columns { Id: 59 Name: \"__updateNo\" TypeId: " + ToString<int>(NScheme::NTypeIds::Uint64) + " }\n"
        "KeyColumnIds: [ 34 ]\n"
        ;

    TString partConfig = opts.PartConfig();
    if (partConfig) {
        keyValueSchemeText += partConfig;
        doubleKeyValueSchemeText += partConfig;
    }

    TFakeMiniKQLProxy proxy(*this);
    switch (schema) {
    case ESchema_KV:
        CreateDataShard(proxy, TTestTxConfig::TxTablet0, keyValueSchemeText, true);
        break;
    case ESchema_DoubleKV:
        CreateDataShard(proxy, TTestTxConfig::TxTablet0, doubleKeyValueSchemeText, true);
        break;
    case ESchema_DoubleKVExternal:
        CreateDataShard(proxy, TTestTxConfig::TxTablet0, doubleKeyValueExternalSchemeText, true);
        break;
    case ESchema_SpecialKV:
        CreateDataShard(proxy, TTestTxConfig::TxTablet0, specialKeyValueSchemeText, true);
        break;
    case ESchema_MultiShardKV:
        CreateDataShard(proxy, TTestTxConfig::TxTablet0, keyValueSchemeText, true);
        CreateDataShard(proxy, TTestTxConfig::TxTablet1, keyValueSchemeText);
        CreateDataShard(proxy, TTestTxConfig::TxTablet2, keyValueSchemeText);
        break;
    }
}

//

TRuntimeNode TEngineHolder::ProgramText2Bin(TTester& tester, const TString& programText) {
    auto expr = NYql::ParseText(programText);

    auto resFuture = NYql::ConvertToMiniKQL(
        expr, tester.Runtime.GetAppData().FunctionRegistry,
        &Env, &tester.DbSchemeResolver
    );

    const TDuration TIME_LIMIT = TDuration::Seconds(NValgrind::PlainOrUnderValgrind(60, 300));
    Y_VERIFY(resFuture.Wait(TIME_LIMIT), "ProgramText2Bin is taking too long to compile");
    NYql::TConvertResult res = resFuture.GetValue();
    res.Errors.PrintTo(Cerr);
    UNIT_ASSERT(res.Node.GetNode());
    return res.Node;
}

TBalanceCoverageBuilder * TFakeProxyTx::GetCoverageBuilder(ui64 shard) {
    auto it = CoverageBuilders.find(shard);
    if (it == CoverageBuilders.end()) {
        it = CoverageBuilders.insert(std::make_pair(shard, std::make_shared<TBalanceCoverageBuilder>())).first;
    }
    return it->second.get();
}

//

ui32 TFakeProxyTx::SetProgram(TTester& tester) {
    return SetProgram(tester, TxBody());
}

ui32 TFakeProxyTx::SetProgram(TTester& tester, const TString& programText) {
    TEngineFlatSettings settings(IEngineFlat::EProtocol::V1,
                                 tester.Runtime.GetAppData().FunctionRegistry,
                                 *TAppData::RandomProvider, *TAppData::TimeProvider);

    NMiniKQL::TRuntimeNode pgm = ProgramText2Bin(tester, programText);

    settings.BacktraceWriter = [](const char* operation, ui32 line, const TBackTrace* backtrace) {
        Cerr << "\nEngine backtrace, operation: " << operation << " (" << line << ")\n";
        if (backtrace) {
            backtrace->PrintTo(Cerr);
        }
    };
    settings.ForceOnline = (TxFlags_ & NDataShard::TTxFlags::ForceOnline);

    Engine = CreateEngineFlat(settings);

    auto result = Engine->SetProgram(SerializeRuntimeNode(pgm, Env));
    UNIT_ASSERT_EQUAL_C(result, IEngineFlat::EResult::Ok, Engine->GetErrors());
    auto& dbKeys = Engine->GetDbKeys();

    TSet<ui64> resolvedShards;
    TTester::TKeyResolver keyResolver = tester.GetKeyResolver();
    for (auto& dbKey : dbKeys) {
        keyResolver(*dbKey);
        UNIT_ASSERT(dbKey->Status == TKeyDesc::EStatus::Ok);

        for (auto& partition : dbKey->GetPartitions()) {
            resolvedShards.insert(partition.ShardId);
        }
    }

    result = Engine->PrepareShardPrograms();
    UNIT_ASSERT_EQUAL_C(result, IEngineFlat::EResult::Ok, Engine->GetErrors());

    ShardsCount_ = Engine->GetAffectedShardCount();
    UNIT_ASSERT_VALUES_EQUAL(ShardsCount_, resolvedShards.size());
    return ShardsCount_;
}

ui32 TFakeProxyTx::GetShardProgram(ui32 idx, TString& outTxBody) {
    IEngineFlat::TShardData shardData;
    auto result = Engine->GetAffectedShard(idx, shardData);
    UNIT_ASSERT_EQUAL_C(result, IEngineFlat::EResult::Ok, Engine->GetErrors());

    NKikimrTxDataShard::TDataTransaction tx;
    tx.SetMiniKQL(shardData.Program);
    tx.SetImmediate(shardData.Immediate);
    tx.SetReadOnly(Engine->IsReadOnlyProgram());
    outTxBody = tx.SerializeAsString();
    return shardData.ShardId;
}

void TFakeProxyTx::AddProposeShardResult(ui32 shardId, const TEvDataShard::TEvProposeTransactionResult * event) {
    if (event->IsExecError() || event->IsError()) {
        for (auto err : event->Record.GetError()) {
            Cerr << "DataShard error: " << shardId << ", kind: " <<
                NKikimrTxDataShard::TError::EKind_Name(err.GetKind()) << ", reason: " << err.GetReason() << Endl;
            Errors[shardId].push_back(err);
        }
    }

    if (event->IsComplete()) {
        Engine->AddShardReply(event->GetOrigin(), event->Record.GetTxResult());
        Engine->FinalizeOriginReplies(shardId);
    } else if (event->IsPrepared()) {
        Shards.push_back(shardId);
        MinStep = Max(MinStep, event->Record.GetMinStep());
        MaxStep = Min(MaxStep, event->Record.GetMaxStep());
    } else {
        UNIT_ASSERT(event->IsExecError() || event->IsError() || event->IsBadRequest());
    }
}

void TFakeProxyTx::AddPlanStepShardResult(ui32 shardId, const TEvDataShard::TEvProposeTransactionResult * event,
                                                           bool complete) {
    Engine->AddShardReply(event->GetOrigin(), event->Record.GetTxResult());
    if (complete)
        Engine->FinalizeOriginReplies(shardId);
}

IEngineFlat::EStatus TFakeProxyTx::GetStatus(bool atPropose) {
    if (atPropose) {
        Engine->AfterShardProgramsExtracted();

        if (HasErrors())
            return IEngineFlat::EStatus::Error;
        if (!Shards.empty())
            return IEngineFlat::EStatus::Unknown;
    }

    Engine->BuildResult();
    if (Engine->GetStatus() == IEngineFlat::EStatus::Error) {
        Cerr << Engine->GetErrors() << Endl;
    }

    return Engine->GetStatus();
}

NKikimrMiniKQL::TResult TFakeProxyTx::GetResult() const {
    NKikimrMiniKQL::TResult result;
    UNIT_ASSERT(Engine);
    auto fillResult = Engine->FillResultValue(result);
    UNIT_ASSERT_EQUAL(fillResult, NMiniKQL::IEngineFlat::EResult::Ok);
    if (DUMP_RESULT) {
        TString strRes;
        ::google::protobuf::TextFormat::PrintToString(result, &strRes);
        Cout << strRes << Endl;
    }
    return result;
}

//

ui32 TFakeScanTx::SetProgram(TTester& tester) {
    NKikimrTxDataShard::TDataTransaction dataTransaction;
    Y_PROTOBUF_SUPPRESS_NODISCARD dataTransaction.ParseFromArray(TxBody_.data(), TxBody_.size());
    ActorIdToProto(tester.Sender, dataTransaction.MutableSink());
    TxBody_ = dataTransaction.SerializeAsString();

    const char * rangePattern = R"(
            (let $%u '('IncFrom 'IncTo '('key (Uint32 '%u) (Uint32 '%u))))
            (let points_ (Extend points_ (Member (SelectRange 'table1 $%u '('key 'uint) '()) 'List))))";

    TString body;
    body += Sprintf(rangePattern, 10, 0, Max<ui32>(), 10);

    auto pgm = Sprintf(R"((
            (let points_ (List (ListType (TypeOf (Unwrap (SelectRow 'table1 '('('key (Uint32 '0))) '('key 'uint)))))))
            %s
            (return (AsList (SetResult 'Result points_)))
        ))", body.data());
    return TFakeProxyTx::SetProgram(tester, pgm);
}

ui32 TFakeScanTx::GetShardProgram(ui32 idx, TString& outTxBody) {
    IEngineFlat::TShardData shardData;
    auto result = Engine->GetAffectedShard(idx, shardData);
    UNIT_ASSERT_EQUAL_C(result, IEngineFlat::EResult::Ok, Engine->GetErrors());

    outTxBody = TxBody();
    return shardData.ShardId;
}

void TFakeScanTx::AddPlanStepShardResult(ui32 /*shardId*/,
                                         const TEvDataShard::TEvProposeTransactionResult * event,
                                         bool /*complete*/) {
    if (event->Record.GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA) {
        auto &res = event->Record.GetTxResult();
        YdbOld::ResultSet part;
        UNIT_ASSERT(part.ParseFromArray(res.data(), res.size()));

        if (Result.column_metaSize())
            part.Clearcolumn_meta();
        Result.MergeFrom(part);
        UNIT_ASSERT(Result.column_metaSize());
    } else if (event->Record.GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::ERROR) {
        Status = IEngineFlat::EStatus::Error;
    } else {
        UNIT_ASSERT(event->Record.GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        Status = IEngineFlat::EStatus::Complete;
    }
}

YdbOld::ResultSet TFakeScanTx::GetScanResult() const {
    return Result;
}

IEngineFlat::EStatus TFakeScanTx::GetStatus(bool /*atPropose*/) {
    return Status;
}

//

void TFakeMiniKQLProxy::Enqueue(const TString& programText, std::function<bool(TFakeProxyTx&)> check, ui32 flags) {
    TxQueue.push_back(std::make_shared<TFakeProxyTx>(++LastTxId_, programText, flags));
    TxQueue.back()->SetCheck(check);
}

void TFakeMiniKQLProxy::EnqueueScan(const TString& programText, std::function<bool(TFakeProxyTx&)> check, ui32 flags) {
    TxQueue.push_back(std::make_shared<TFakeScanTx>(++LastTxId_, programText, flags));
    TxQueue.back()->SetCheck(check);
}

void TFakeMiniKQLProxy::ExecQueue() {
    TMap<ui64, TFakeProxyTx::TPtr> needPlan;
    for (auto& tx : TxQueue) {
        Propose(*tx, true);
        UNIT_ASSERT(tx->Immediate() || tx->GetStatus(true) == IEngineFlat::EStatus::Unknown);
        needPlan[tx->TxId()] = tx;
    }

    if (needPlan) {
        ui64 stepId = ++LastStep_;
        LastStep_ = Plan(stepId, needPlan);
        for (auto& pair : needPlan) {
            TFakeProxyTx::TPtr tx = pair.second;
            auto status = tx->GetStatus(tx->Immediate());
            UNIT_ASSERT_EQUAL(status, IEngineFlat::EStatus::Complete);
            UNIT_ASSERT(tx->CheckResult());
        }
    }

    TxQueue.clear();
}

IEngineFlat::EStatus TFakeMiniKQLProxy::Execute(const TString& programText,
                                                NKikimrMiniKQL::TResult& out,
                                                bool waitForResult) {
    ui32 txId = ++LastTxId_;
    TMap<ui64, TFakeProxyTx::TPtr> txs;
    txs[txId] = std::make_shared<TFakeProxyTx>(txId, programText);
    TFakeProxyTx& tx = *(txs.begin()->second);

    Propose(tx);
    auto status = tx.GetStatus(true);
    if (status == IEngineFlat::EStatus::Unknown) {
        ui64 stepId = ++LastStep_;
        Plan(stepId, txs, waitForResult);
        status = tx.GetStatus(false);
    }
    if (waitForResult)
        out = tx.GetResult();
    return status;
}

IEngineFlat::EStatus TFakeMiniKQLProxy::ExecSchemeCreateTable(const TString& tableDesc, const TVector<ui64>& shards) {
    ui64 txId = ++LastTxId_;
    ui64 stepId = ++LastStep_;
    TMap<ui64, TFakeProxyTx::TPtr> txs;
    txs[txId] = std::make_shared<TFakeProxyTx>(txId, tableDesc);
    TFakeProxyTx& tx = *(txs.begin()->second);
    tx.SetKindSchema();

    ProposeSchemeCreateTable(tx, shards);
    if (stepId < tx.MinStep) {
        stepId = LastStep_ = tx.MinStep;
    }
    UNIT_ASSERT(tx.MaxStep == Max<ui64>());
    LastStep_ = Plan(stepId, txs);
    return IEngineFlat::EStatus::Complete;
}

#if 0
void TFakeMiniKQLProxy::Cancel(ui64 txId) {
    for (auto shard : proxyTx.Shards) {
        auto cancel = new TEvDataShard::TEvCancelTransactionProposal(txId);
        Tester.Runtime.SendToPipe(shard, Tester.Sender, cancel);
    }

    Tester.Runtime.DispatchEvents();
}
#endif

void TFakeMiniKQLProxy::ProposeSchemeCreateTable(TFakeProxyTx& tx, const TVector<ui64>& shards) {
    const TString& schemaText = tx.TxBody();
    NKikimrSchemeOp::TTableDescription tableDesc;
    bool parsed = NProtoBuf::TextFormat::ParseFromString(schemaText, &tableDesc);
    UNIT_ASSERT(parsed);

    auto txBodyForShard = [&](ui64 shard) {
        Y_UNUSED(shard);

        NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
        schemeTx.MutableCreateTable()->CopyFrom(tableDesc);
        return schemeTx;
    };

    return ProposeScheme(tx, shards, txBodyForShard);
}

void TFakeMiniKQLProxy::ProposeScheme(TFakeProxyTx& tx, const TVector<ui64>& shards,
        const std::function<NKikimrTxDataShard::TFlatSchemeTransaction(ui64)>& txBodyForShard)
{
    NKikimrTxDataShard::ETransactionKind kind = NKikimrTxDataShard::TX_KIND_SCHEME;

    ui64 txId = tx.TxId();
    bool hasErrors = false;
    for (ui32 i = 0; i < shards.size(); ++i) {
        ui64 shardId = shards[i];
        auto txBody = txBodyForShard(shards[i]).SerializeAsString();
        ui32 txFlags = NDataShard::TTxFlags::Default;

        for (;;) {
            auto proposal = new TEvDataShard::TEvProposeTransaction(kind, FAKE_SCHEMESHARD_TABLET_ID,
                Tester.Sender, txId, txBody, NKikimrSubDomains::TProcessingParams(), txFlags);
            Tester.Runtime.SendToPipe(shardId, Tester.Sender, proposal);
            TAutoPtr<IEventHandle> handle;
            auto event = Tester.Runtime.GrabEdgeEventIf<TEvDataShard::TEvProposeTransactionResult>(handle,
                [=](const TEvDataShard::TEvProposeTransactionResult& event) {
                return event.GetTxId() == txId && event.GetOrigin() == shardId;
            });

            UNIT_ASSERT(event);
            UNIT_ASSERT_EQUAL(event->GetTxKind(), kind);
            if (event->IsTryLater())
                continue;

            if (event->IsError()) {
                hasErrors = true;
                for (auto err : event->Record.GetError()) {
                    Cerr << "DataShard error: " << shardId << ", kind: " <<
                        NKikimrTxDataShard::TError::EKind_Name(err.GetKind()) << ", reason: " << err.GetReason() << Endl;
                }

                break;
            }

            if (event->IsComplete())
                break;

            UNIT_ASSERT(event->IsPrepared());
            tx.Shards.push_back(shardId);
            tx.MinStep = Max(tx.MinStep, event->Record.GetMinStep());
            tx.MaxStep = Min(tx.MaxStep, event->Record.GetMaxStep());
            break;
        }
    }

    UNIT_ASSERT(!hasErrors);
    UNIT_ASSERT(!tx.Shards.empty());
}

void TFakeMiniKQLProxy::Propose(TFakeProxyTx& tx, bool holdImmediate) {
    ui64 txId = tx.TxId();

    ui32 shardsCount = tx.SetProgram(Tester);
    if (holdImmediate && tx.Immediate())
        return;

    TSet<ui64> shards;
    for (ui32 i = 0; i < shardsCount; ++i) {
        TString txBody;
        ui32 shard = tx.GetShardProgram(i, txBody);
        shards.insert(shard);

        auto proposal = new TEvDataShard::TEvProposeTransaction(
            tx.TxKind(), Tester.Sender, txId, txBody, tx.TxFlags());
        Tester.Runtime.SendToPipe(shard, Tester.Sender, proposal);
    }

    ResolveShards(shards);

    while (shards) {
        TAutoPtr<IEventHandle> handle;
        auto event = Tester.Runtime.GrabEdgeEvent<TEvDataShard::TEvProposeTransactionResult>(handle);
        UNIT_ASSERT(event);
        if (event->GetTxId() != txId)
            continue;

        UNIT_ASSERT(!event->IsTryLater()); // need resend otherwise
        UNIT_ASSERT_EQUAL(event->GetTxKind(), tx.TxKind());

        ui64 shard = event->GetOrigin();

        tx.AddProposeShardResult(shard, event);
        shards.erase(shard);
    }
}

void TFakeMiniKQLProxy::ResolveShards(const TSet<ui64>& shards) {
    for (ui64 shard : shards) {
        auto event = new TEvDataShard::TEvGetShardState(Tester.Sender);
        ForwardToTablet(Tester.Runtime, shard, Tester.Sender, event);
    }

    for (ui32 results = 0; results < shards.size(); ++results) {
        TAutoPtr<IEventHandle> handle;
        auto resolve = Tester.Runtime.GrabEdgeEvent<TEvTabletResolver::TEvForwardResult>(handle);
        UNIT_ASSERT(resolve && resolve->Tablet);
        ShardActors[resolve->TabletID] = resolve->TabletActor;
    }
}

ui64 TFakeMiniKQLProxy::Plan(ui64 stepId, const TMap<ui64, TFakeProxyTx::TPtr>& txs,
                             bool waitForResult) {
    using TEvPlanStepAck = TEvTxProcessing::TEvPlanStepAck;
    using TEvPlanStepAccepted = TEvTxProcessing::TEvPlanStepAccepted;
    using TEvReadSet = TEvTxProcessing::TEvReadSet;
    //using TEvReadSetAck = TEvTxProcessing::TEvReadSetAck;
    using TEvProposeTransactionResult = TEvDataShard::TEvProposeTransactionResult;
    using TEvStreamClearanceRequest = TEvTxProcessing::TEvStreamClearanceRequest;
    using TEvStreamClearancePending = TEvTxProcessing::TEvStreamClearancePending;
    using TEvStreamClearanceResponse = TEvTxProcessing::TEvStreamClearanceResponse;
    using TEvStreamQuotaRequest = TEvTxProcessing::TEvStreamQuotaRequest;
    using TEvStreamQuotaResponse = TEvTxProcessing::TEvStreamQuotaResponse;
    using TEvStreamDataAck = TEvTxProcessing::TEvStreamDataAck;

    TSet<ui64> immediateTxs;
    THashMap<ui64, TVector<ui64>> plans;
    THashMap<ui64, TVector<ui64>> imm2onlineTxs;
    TSet<std::pair<ui64, ui64>> acks;
    TSet<std::pair<ui64, ui64>> results;
    TSet<std::pair<ui64, ui64>> streams;
    TSet<std::pair<ui64, ui64>> scans;
    for (auto& pair : txs) {
        TFakeProxyTx::TPtr tx = pair.second;
        ui64 txId = tx->TxId();
        if (tx->Immediate()) {
            immediateTxs.insert(txId);
        } else {
            for (ui64 shard : tx->Shards) {
                plans[shard].push_back(txId);
                acks.insert(std::make_pair(shard, txId));
                if (tx->IsReadTable()) {
                    streams.insert(std::make_pair(shard, txId));
                    scans.insert(std::make_pair(shard, txId));
                } else {
                    results.insert(std::make_pair(shard, txId));
                }
            }
        }
    }

    // send plan step
    for (const auto& pair : plans) {
        ui64 shard = pair.first;
        auto planStep = new TEvTxProcessing::TEvPlanStep(stepId, 0, shard);
        for (ui64 txId : pair.second) {
            TFakeProxyTx::TPtr tx = txs.find(txId)->second;
            if (!tx->Immediate()) {
                auto plannedTx = planStep->Record.MutableTransactions()->Add();
                plannedTx->SetTxId(txId);
                ActorIdToProto(Tester.Sender, plannedTx->MutableAckTo());
            }
        }
        Tester.Runtime.SendToPipe(shard, Tester.Sender, planStep);
    }

    // prepare immediate
    TDeque<std::pair<ui64, THolder<TEvDataShard::TEvProposeTransaction>>> immEvents;
    for (ui64 txId : immediateTxs) {
        TFakeProxyTx::TPtr tx = txs.find(txId)->second;
        UNIT_ASSERT_VALUES_EQUAL(tx->ShardsCount(), 1);
        TString txBody;
        ui32 shard = tx->GetShardProgram(0, txBody);
        THolder<TEvDataShard::TEvProposeTransaction> event = MakeHolder<TEvDataShard::TEvProposeTransaction>(
            NKikimrTxDataShard::TX_KIND_DATA, Tester.Sender, txId, txBody, tx->TxFlags());
        immEvents.emplace_back(std::make_pair(shard, std::move(event)));
        results.insert(std::make_pair(shard, txId));
    }
    immediateTxs.clear();

    THolder<IEventHandle> delayedEvent;
    UNIT_ASSERT(DelayedReadSets.size() <= 1);
    UNIT_ASSERT(DelayedData.size() <= 1);

    auto observer = [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
        // send immediate in the right moment
        if (ev->Type == TEvProposeTransactionResult::EventType) {
            auto event = ev->Get<TEvProposeTransactionResult>();
            if (event->GetTxKind() != NKikimrTxDataShard::TX_KIND_DATA ||
                event->GetStepOrderId().first == 0) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            ui64 prevTxId = event->GetTxId();
            while (immEvents) {
                ui64 shard = immEvents.front().first;
                auto& immEvent = immEvents.front().second;
                if ((prevTxId+1) != immEvent->GetTxId())
                    break;
                //Cerr << ">>> imm to " << shard << Endl;
                UNIT_ASSERT(ShardActors.contains(shard));
                THolder<IEventHandle> handle(new IEventHandle(ShardActors[shard], Tester.Sender, immEvent.Release()));
                runtime.Send(handle.Release());
                immEvents.pop_front();
                ++prevTxId;
            }
        }

        // delayed ReadSet
        bool needDelay = false;
        if (ev->Type == TEvReadSet::EventType && DelayedReadSets) {
            TExpectedReadSet& rs = DelayedReadSets.front();
            auto event = ev->Get<TEvReadSet>();

            needDelay = (rs.SrcTablet == event->Record.GetTabletSource()) &&
                (rs.DstTablet == event->Record.GetTabletDest()) &&
                (rs.TxId == event->Record.GetTxId());
            if (needDelay)
                Cerr << event->ToString() << Endl;
        }

        // delayed data
        if (ev->Type == NTabletFlatExecutor::NBlockIO::TEvData::EventType) {
            // WARNING: NShared::TEvResult and NBlockIO::TEvData currently share an event id
            auto event = ev->Get<NTabletFlatExecutor::NBlockIO::TEvData>();
            needDelay = DelayedData && !delayedEvent;

            event->Describe(Cerr);
            Cerr << (needDelay ? " delayed" : "") << Endl;
        }

        if (needDelay) {
            delayedEvent.Reset(ev.Release());
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };

    if (immEvents || DelayedReadSets || DelayedData) {
        Tester.Runtime.SetObserverFunc(observer);
    }

    while (acks || plans || (results && waitForResult) || streams) {
#if 0
        Cerr << "acks: " << acks.size() << " plans: " << plans.size() << " results: " << results.size() << Endl;
#endif
        TAutoPtr<IEventHandle> handle;
        auto eventTuple = Tester.Runtime.GrabEdgeEvents<TEvPlanStepAck, TEvPlanStepAccepted, TEvProposeTransactionResult,
                                                        TEvStreamClearanceRequest>(handle);

        switch (handle->Type) {
            case TEvPlanStepAck::EventType: {
                auto event = std::get<TEvPlanStepAck*>(eventTuple);
                for (ui32 i = 0; i < event->Record.TxIdSize(); ++i) {
                    ui64 txId = event->Record.GetTxId().Get(i);
                    ui64 shardId = event->Record.GetTabletId();
                    acks.erase(std::make_pair(shardId, txId));
                }
                break;
            }
            case TEvPlanStepAccepted::EventType: {
                auto event = std::get<TEvPlanStepAccepted*>(eventTuple);
                UNIT_ASSERT_EQUAL(stepId, event->Record.GetStep());
                ui64 shard = event->Record.GetTabletId();
                plans.erase(shard);
                break;
            }
            case TEvProposeTransactionResult::EventType: {
                auto event = std::get<TEvProposeTransactionResult*>(eventTuple);
                ui64 txId = event->GetTxId();
                ui64 shard = event->GetOrigin();

                if (event->IsPrepared()) {
                    Cerr << "immediate -> online tx " << txId << Endl;
                    results.erase(std::make_pair(shard, txId));
                    imm2onlineTxs[shard].push_back(txId);
                    break;
                }

                UNIT_ASSERT(event->IsComplete());
                if (event->GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                    results.erase(std::make_pair(shard, txId));
                    break;
                }

                auto it = txs.find(txId);
                if (it == txs.end())
                    continue;
                TFakeProxyTx::TPtr tx = it->second;
                UNIT_ASSERT_EQUAL(event->GetTxKind(), tx->TxKind());

                if (tx->Immediate()) {
                    UNIT_ASSERT_VALUES_EQUAL(event->GetStepOrderId().first, 0);
                    tx->AddProposeShardResult(shard, event);
                    results.erase(std::make_pair(shard, txId));
                    break;
                }

                if (DelayedReadSets) {
                    TExpectedReadSet::TWaitFor waitFor = DelayedReadSets.front().Freedom;
                    if (shard == waitFor.Shard && tx->TxId() == waitFor.TxId) {
                        if (RebootOnDelay) {
                            RebootTablet(Tester.Runtime, TTestTxConfig::TxTablet0, Tester.Sender);
                        }

                        Y_VERIFY(delayedEvent);
                        DelayedReadSets.clear();
                        Tester.Runtime.Send(delayedEvent.Release());
                        Cerr << "resending delayed RS" << Endl;
                    }
                } else if (DelayedData) {
                    TExpectedReadSet::TWaitFor waitFor = DelayedData.front();
                    if (shard == waitFor.Shard && tx->TxId() == waitFor.TxId) {
                        DelayedData.clear();
                        if (delayedEvent) {
                            Tester.Runtime.Send(delayedEvent.Release());
                            Cerr << "resending delayed data" << Endl;
                        }
                    }
                }

                UNIT_ASSERT_EQUAL(event->GetStepOrderId().first, stepId);
                TBalanceCoverageBuilder * builder = tx->GetCoverageBuilder(shard);
                if (builder->AddResult(event->Record.GetBalanceTrackList())) {
                    tx->AddPlanStepShardResult(shard, event, builder->IsComplete());
                    if (builder->IsComplete()) {
                        results.erase(std::make_pair(shard, txId));
                    }
                }
                break;
            }
            case TEvStreamClearanceRequest::EventType: {
                auto event = std::get<TEvStreamClearanceRequest*>(eventTuple);
                ui64 txId = event->Record.GetTxId();
                ui64 shard = event->Record.GetShardId();
                streams.erase(std::make_pair(shard, txId));
                Tester.Runtime.SendToPipe(shard, Tester.Sender, new TEvStreamClearancePending(txId));
            }
        }
    }

    // exec imm2online

    if (imm2onlineTxs)
        ++stepId;

    for (const auto& pair : imm2onlineTxs) {
        ui64 shard = pair.first;
        auto planStep = new TEvTxProcessing::TEvPlanStep(stepId, 0, shard);
        for (ui64 txId : pair.second) {
            TFakeProxyTx::TPtr tx = txs.find(txId)->second;
            UNIT_ASSERT(tx->Immediate());

            results.insert(std::make_pair(shard, txId));
            auto plannedTx = planStep->Record.MutableTransactions()->Add();
            plannedTx->SetTxId(txId);
            ActorIdToProto(Tester.Sender, plannedTx->MutableAckTo());
        }
        Tester.Runtime.SendToPipe(shard, Tester.Sender, planStep);
    }

    // finish scans

    results.clear();
    for (const auto& pair : scans) {
        ui64 shard = pair.first;
        ui64 txId = pair.second;
        results.insert(std::make_pair(shard, txId));
        TAutoPtr<TEvStreamClearanceResponse> ev = new TEvStreamClearanceResponse;
        ev->Record.SetTxId(txId);
        ev->Record.SetCleared(true);
        Tester.Runtime.SendToPipe(shard, Tester.Sender, ev.Release());
    }

    while (results) {
        TAutoPtr<IEventHandle> handle;
        auto eventTuple = Tester.Runtime.GrabEdgeEvents<TEvProposeTransactionResult, TEvStreamQuotaRequest, TEvStreamDataAck>(handle);

        switch (handle->Type) {
            case TEvProposeTransactionResult::EventType: {
                auto event = std::get<TEvProposeTransactionResult*>(eventTuple);
                ui64 txId = event->GetTxId();
                ui64 shard = event->GetOrigin();

                auto it = txs.find(txId);
                UNIT_ASSERT(it != txs.end());
                TFakeProxyTx::TPtr tx = it->second;
                UNIT_ASSERT_EQUAL(event->GetTxKind(), tx->TxKind());

                //tx->AddProposeShardResult(shard, event);
                if (event->Record.GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA) {
                    Tester.Runtime.Send(new IEventHandle(handle->Sender, Tester.Sender, new TEvStreamDataAck));
                    tx->AddPlanStepShardResult(shard, event, false);
                } else {
                    UNIT_ASSERT(event->IsComplete());
                    tx->AddPlanStepShardResult(shard, event, true);
                    results.erase(std::make_pair(shard, txId));
                }
                break;
            }
            case TEvStreamQuotaRequest::EventType: {
                auto event = std::get<TEvStreamQuotaRequest*>(eventTuple);
                ui64 txId = event->Record.GetTxId();
                TAutoPtr<TEvStreamQuotaResponse> ev = new TEvStreamQuotaResponse;
                ev->Record.SetTxId(txId);
                ev->Record.SetMessageSizeLimit(1 << 30);
                ev->Record.SetReservedMessages(10);
                ev->Record.SetRowLimit(1 << 20);
                Tester.Runtime.Send(new IEventHandle(handle->Sender, Tester.Sender, ev.Release()));
                break;
            }
        }
    }

    return stepId;
}

//

TKeyExtractor::TKeyExtractor(TTester& tester, TString programText) {
    Engine = CreateEngineFlat(TEngineFlatSettings(IEngineFlat::EProtocol::V1, tester.Runtime.GetAppData().FunctionRegistry,
                                *TAppData::RandomProvider, *TAppData::TimeProvider));

    NMiniKQL::TRuntimeNode pgm = ProgramText2Bin(tester, programText);
    auto result = Engine->SetProgram(SerializeRuntimeNode(pgm, Env));
    UNIT_ASSERT_EQUAL_C(result, IEngineFlat::EResult::Ok, Engine->GetErrors());

    for (auto& key : Engine->GetDbKeys()) {
        key->Status = TKeyDesc::EStatus::Ok;

        auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
        partitions->push_back(TKeyDesc::TPartitionInfo((ui64)TTestTxConfig::TxTablet0));
        key->Partitioning = partitions;
    }
}

//

TDatashardInitialEventsFilter::TDatashardInitialEventsFilter(const TVector<ui64>& tabletIds)
    : TabletIds(tabletIds)
{
}

TTestActorRuntime::TEventFilter TDatashardInitialEventsFilter::Prepare() {
    RemainTablets = TabletIds;
    return [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
        return (*this)(runtime, event);
    };
}

bool TDatashardInitialEventsFilter::operator()(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
    Y_UNUSED(runtime);
    if (event->GetTypeRewrite() == TEvTxProcessing::EvPlanStepAck) {
        ui64 tabletId = reinterpret_cast<TEvTxProcessing::TEvPlanStepAck::TPtr&>(event)->Get()->Record.GetTabletId();
        auto it = Find(RemainTablets.begin(), RemainTablets.end(), tabletId);
        if (it != RemainTablets.end())
            RemainTablets.erase(it);
        return true;
    }

    return !RemainTablets.empty();
}

THolder<NKqp::TEvKqp::TEvQueryRequest> MakeSQLRequest(const TString &sql,
                                                      bool dml)
{
    auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    if (dml) {
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    }
    request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request->Record.MutableRequest()->SetType(dml
                                              ? NKikimrKqp::QUERY_TYPE_SQL_DML
                                              : NKikimrKqp::QUERY_TYPE_SQL_DDL);
    request->Record.MutableRequest()->SetQuery(sql);
    return request;
}

void InitRoot(Tests::TServer::TPtr server,
    TActorId sender) {
    server->SetupRootStoragePools(sender);
}

static THolder<TEvTxUserProxy::TEvProposeTransaction> SchemeTxTemplate(
        NKikimrSchemeOp::EOperationType type,
        const TString& workingDir = {})
{
    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetExecTimeoutPeriod(Max<ui64>());

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetOperationType(type);

    if (workingDir) {
        tx.SetWorkingDir(workingDir);
    }

    return request;
}

static ui64 RunSchemeTx(
        TTestActorRuntimeBase& runtime,
        THolder<TEvTxUserProxy::TEvProposeTransaction>&& request,
        TActorId sender = {},
        bool viaActorSystem = false,
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus expectedStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress)
{
    if (!sender) {
        sender = runtime.AllocateEdgeActor();
    }

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, viaActorSystem);
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), expectedStatus);

    return ev->Get()->Record.GetTxId();
}

void CreateShardedTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString &root,
        const TString &name,
        const TShardedTableOptions &opts)
{
    // Create table with four shards.
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateTable, root);

    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    NKikimrSchemeOp::TTableDescription* desc = nullptr;
    if (opts.Indexes_) {
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
        desc = tx.MutableCreateIndexedTable()->MutableTableDescription();
    } else {
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
        desc = tx.MutableCreateTable();
    }

    UNIT_ASSERT(desc);
    desc->SetName(name);

    for (const auto& column : opts.Columns_) {
        auto col = desc->AddColumns();
        col->SetName(column.Name);
        col->SetType(column.Type);
        col->SetNotNull(column.NotNull);
        if (column.IsKey) {
            desc->AddKeyColumnNames(column.Name);
        }
    }

    for (const auto& index : opts.Indexes_) {
        auto* indexDesc = tx.MutableCreateIndexedTable()->MutableIndexDescription()->Add();

        indexDesc->SetName(index.Name);
        indexDesc->SetType(index.Type);

        for (const auto& col : index.IndexColumns) {
            indexDesc->AddKeyColumnNames(col);
        }
        for (const auto& col : index.DataColumns) {
            indexDesc->AddDataColumnNames(col);
        }
    }

    desc->SetUniformPartitionsCount(opts.Shards_);

    if (!opts.EnableOutOfOrder_)
        desc->MutablePartitionConfig()->MutablePipelineConfig()->SetEnableOutOfOrder(false);

    if (opts.Policy_) {
        opts.Policy_->Serialize(*desc->MutablePartitionConfig()->MutableCompactionPolicy());
    }

    switch (opts.ShadowData_) {
        case EShadowDataMode::Default:
            break;
        case EShadowDataMode::Enabled:
            desc->MutablePartitionConfig()->SetShadowData(true);
            break;
    }

    if (opts.Followers_ > 0) {
        auto& followerGroup = *desc->MutablePartitionConfig()->AddFollowerGroups();
        followerGroup.SetFollowerCount(opts.Followers_);
        followerGroup.SetAllowLeaderPromotion(opts.FollowerPromotion_);
    }

    if (opts.ExternalStorage_) {
        auto& family = *desc->MutablePartitionConfig()->AddColumnFamilies();
        family.SetStorage(NKikimrSchemeOp::ColumnStorageTest_1_2_1k);
    }

    if (opts.ExecutorCacheSize_) {
        desc->MutablePartitionConfig()->SetExecutorCacheSize(*opts.ExecutorCacheSize_);
    }

    if (opts.Replicated_) {
        desc->MutableReplicationConfig()->SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
    }

    if (opts.ReplicationConsistency_) {
        desc->MutableReplicationConfig()->SetConsistency(
            static_cast<NKikimrSchemeOp::TTableReplicationConfig::EConsistency>(*opts.ReplicationConsistency_));
    }

    WaitTxNotification(server, sender, RunSchemeTx(*server->GetRuntime(), std::move(request), sender));
}

void CreateShardedTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString &root,
        const TString &name,
        ui64 shards,
        bool enableOutOfOrder,
        const NLocalDb::TCompactionPolicy* policy,
        EShadowDataMode shadowData)
{
    auto opts = TShardedTableOptions()
        .Shards(shards)
        .EnableOutOfOrder(enableOutOfOrder)
        .Policy(policy)
        .ShadowData(shadowData);
    CreateShardedTable(server, sender, root, name, opts);
}

ui64 AsyncCreateCopyTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString &root,
        const TString &name,
        const TString &from)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateTable, root);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableCreateTable();
    desc.SetName(name);
    desc.SetCopyFromTable(from);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender);
}

NKikimrScheme::TEvDescribeSchemeResult DescribeTable(Tests::TServer::TPtr server,
                                                     TActorId sender,
                                                     const TString &path)
{
    auto &runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    TVector<ui64> shards;

    auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(path);
    request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    auto reply = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDescribeSchemeResult>(handle);

    return *reply->MutableRecord();
}

TVector<ui64> GetTableShards(Tests::TServer::TPtr server,
                             TActorId sender,
                             const TString &path)
{
    TVector<ui64> shards;
    auto lsResult = DescribeTable(server, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    return shards;
}

NKikimrTxDataShard::TEvCompactTableResult CompactTable(
    TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId, bool compactBorrowed)
{
    auto sender = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvDataShard::TEvCompactTable>(tableId.PathId);
    request->Record.SetCompactBorrowed(compactBorrowed);
    runtime.SendToPipe(shardId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
    return ev->Get()->Record;
}

std::pair<TTableInfoMap, ui64> GetTables(
    Tests::TServer::TPtr server,
    ui64 tabletId)
{
    auto &runtime = *server->GetRuntime();

    auto sender = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvDataShard::TEvGetInfoRequest>();
    runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TTableInfoMap result;

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
    for (auto& table: response->Record.GetUserTables()) {
        result[table.GetName()] = table;
    }

    auto ownerId = response->Record.GetTabletInfo().GetSchemeShard();

    return std::make_pair(result, ownerId);
}

TTableId ResolveTableId(Tests::TServer::TPtr server, TActorId sender, const TString& path) {
    auto response = Navigate(*server->GetRuntime(), sender, path);
    return response->ResultSet.at(0).TableId;
}

NTable::TRowVersionRanges GetRemovedRowVersions(
        Tests::TServer::TPtr server,
        ui64 shardId)
{
    auto& runtime = *server->GetRuntime();
    TActorId sender = runtime.AllocateEdgeActor();

    {
        auto request = MakeHolder<TEvDataShard::TEvGetRemovedRowVersions>(TPathId{});
        ForwardToTablet(runtime, shardId, sender, request.Release());
    }

    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetRemovedRowVersionsResult>(sender);
    return ev->Get()->RemovedRowVersions;
}

void SendCreateVolatileSnapshot(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        const TVector<TString>& tables,
        TDuration timeout)
{
    auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto* tx = request->Record.MutableTransaction()->MutableCreateVolatileSnapshot();
    for (const auto& path : tables) {
        tx->AddTables()->SetTablePath(path);
    }
    tx->SetTimeoutMs(timeout.MilliSeconds());
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
}

TRowVersion GrabCreateVolatileSnapshotResult(
        TTestActorRuntime& runtime,
        const TActorId& sender)
{
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

TRowVersion CreateVolatileSnapshot(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        TDuration timeout)
{
    auto& runtime = *server->GetRuntime();

    TActorId sender = runtime.AllocateEdgeActor();

    SendCreateVolatileSnapshot(runtime, sender, tables, timeout);

    return GrabCreateVolatileSnapshotResult(runtime, sender);
}

bool RefreshVolatileSnapshot(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        TRowVersion snapshot)
{
    auto& runtime = *server->GetRuntime();

    TActorId sender = runtime.AllocateEdgeActor();

    {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* tx = request->Record.MutableTransaction()->MutableRefreshVolatileSnapshot();
        for (const auto& path : tables) {
            tx->AddTables()->SetTablePath(path);
        }
        tx->SetSnapshotStep(snapshot.Step);
        tx->SetSnapshotTxId(snapshot.TxId);
        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    }

    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    const auto& record = ev->Get()->Record;
    auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(record.GetStatus());
    return status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete;
}

bool DiscardVolatileSnapshot(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        TRowVersion snapshot)
{
    auto& runtime = *server->GetRuntime();

    TActorId sender = runtime.AllocateEdgeActor();

    {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* tx = request->Record.MutableTransaction()->MutableDiscardVolatileSnapshot();
        for (const auto& path : tables) {
            tx->AddTables()->SetTablePath(path);
        }
        tx->SetSnapshotStep(snapshot.Step);
        tx->SetSnapshotTxId(snapshot.TxId);
        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    }

    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    const auto& record = ev->Get()->Record;
    auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(record.GetStatus());
    return status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete;
}

void ApplyChanges(
        const Tests::TServer::TPtr& server,
        ui64 shardId,
        const TTableId& tableId,
        const TString& sourceId,
        const TVector<TChange>& changes,
        NKikimrTxDataShard::TEvApplyReplicationChangesResult::EStatus expected)
{
    auto &runtime = *server->GetRuntime();

    auto evReq = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>(tableId.PathId, tableId.SchemaVersion);
    evReq->Record.SetSource(sourceId);
    for (const auto& change : changes) {
        auto* p = evReq->Record.AddChanges();
        p->SetSourceOffset(change.Offset);
        p->SetWriteTxId(change.WriteTxId);
        TCell keyCell = TCell::Make(change.Key);
        p->SetKey(TSerializedCellVec::Serialize({ &keyCell, 1 }));
        auto* u = p->MutableUpsert();
        u->AddTags(2);
        TCell valueCell = TCell::Make(change.Value);
        u->SetData(TSerializedCellVec::Serialize({ &valueCell, 1 }));
    }

    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(shardId, sender, evReq.Release(), 0, GetPipeConfigWithRetries());

    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(sender);
    auto status = ev->Get()->Record.GetStatus();
    UNIT_ASSERT_C(status == expected,
        "Unexpected status " << NKikimrTxDataShard::TEvApplyReplicationChangesResult::EStatus_Name(status)
        << ", expected " << NKikimrTxDataShard::TEvApplyReplicationChangesResult::EStatus_Name(expected));
}

TRowVersion CommitWrites(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        ui64 writeTxId)
{
    auto& runtime = *server->GetRuntime();

    TActorId sender = runtime.AllocateEdgeActor();

    {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* tx = request->Record.MutableTransaction()->MutableCommitWrites();
        for (const auto& path : tables) {
            tx->AddTables()->SetTablePath(path);
        }
        tx->SetWriteTxId(writeTxId);
        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    }

    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
    const auto& record = ev->Get()->Record;
    auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(record.GetStatus());
    Y_VERIFY_S(status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete,
        "Unexpected status " << status);

    auto step = record.GetStep();
    auto txId = record.GetTxId();
    Y_VERIFY_S(txId != 0,
        "Unexpected step " << step << " and txId " << txId);

    return { step, txId };
}

ui64 AsyncDropTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& workingDir,
        const TString& name)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpDropTable, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableDrop();
    desc.SetName(name);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender, true);
}

ui64 AsyncSplitTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& path,
        ui64 sourceTablet,
        ui32 splitKey)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableSplitMergeTablePartitions();
    desc.SetTablePath(path);
    desc.AddSourceTabletId(sourceTablet);
    desc.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint32(splitKey);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender, true);
}

ui64 AsyncMergeTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& path,
        const TVector<ui64>& sourceTabletIds)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableSplitMergeTablePartitions();
    desc.SetTablePath(path);
    for (ui64 tabletId : sourceTabletIds) {
        desc.AddSourceTabletId(tabletId);
    }

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender);
}

ui64 AsyncMoveTable(Tests::TServer::TPtr server,
        const TString& srcTable,
        const TString& dstTable)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpMoveTable);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableMoveTable();
    desc.SetSrcPath(srcTable);
    desc.SetDstPath(dstTable);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterAddExtraColumn(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterTable, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterTable();
    desc.SetName(name);
    auto& col = *desc.AddColumns();
    col.SetName("extra");
    col.SetType("Uint32");

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterDropColumn(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name,
        const TString& colName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterTable, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterTable();
    desc.SetName(name);
    auto& col = *desc.AddDropColumns();
    col.SetName(colName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterAndDisableShadow(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name,
        const NLocalDb::TCompactionPolicy* policy)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterTable, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterTable();
    desc.SetName(name);
    desc.MutablePartitionConfig()->SetShadowData(false);

    if (policy) {
        policy->Serialize(*desc.MutablePartitionConfig()->MutableCompactionPolicy());
    }

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterAddIndex(
        Tests::TServer::TPtr server,
        const TString& dbName,
        const TString& tablePath,
        const TShardedTableOptions::TIndex& indexDesc)
{
    auto &runtime = *server->GetRuntime();
    auto &settings = server->GetSettings();
    auto sender = runtime.AllocateEdgeActor();

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, new TEvTxUserProxy::TEvAllocateTxId()));
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvAllocateTxIdResult>(sender);
    const auto txId = ev->Get()->TxId;

    NKikimrIndexBuilder::TIndexBuildSettings buildSettings;
    buildSettings.set_source_path(tablePath);

    Ydb::Table::TableIndex& index = *buildSettings.mutable_index();
    index.set_name(indexDesc.Name);
    *index.mutable_index_columns() = {indexDesc.IndexColumns.begin(), indexDesc.IndexColumns.end()};
    *index.mutable_data_columns() = {indexDesc.DataColumns.begin(), indexDesc.DataColumns.end()};

    switch (indexDesc.Type) {
    case NKikimrSchemeOp::EIndexTypeGlobal:
        *index.mutable_global_index() = Ydb::Table::GlobalIndex();
        break;
    case NKikimrSchemeOp::EIndexTypeGlobalAsync:
        *index.mutable_global_async_index() = Ydb::Table::GlobalAsyncIndex();
        break;
    default:
        UNIT_ASSERT_C(false, "Unknown index type: " << static_cast<ui32>(indexDesc.Type));
    }

    auto req = MakeHolder<TEvIndexBuilder::TEvCreateRequest>(txId, dbName, std::move(buildSettings));
    auto tabletId = ChangeStateStorage(SchemeRoot, settings.Domain);
    runtime.SendToPipe(tabletId, sender, req.Release(), 0, GetPipeConfigWithRetries());

    auto resp = runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
    UNIT_ASSERT_EQUAL(resp->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
    return txId;
}

void CancelAddIndex(Tests::TServer::TPtr server, const TString& dbName, ui64 buildIndexId) {
    auto &runtime = *server->GetRuntime();
    auto &settings = server->GetSettings();
    auto sender = runtime.AllocateEdgeActor();

    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, new TEvTxUserProxy::TEvAllocateTxId()));
    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvAllocateTxIdResult>(sender);
    const auto txId = ev->Get()->TxId;

    auto req = MakeHolder<TEvIndexBuilder::TEvCancelRequest>(txId, dbName, buildIndexId);
    auto tabletId = ChangeStateStorage(SchemeRoot, settings.Domain);
    runtime.SendToPipe(tabletId, sender, req.Release(), 0, GetPipeConfigWithRetries());

    auto resp = runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCancelResponse>(sender);
    UNIT_ASSERT_EQUAL(resp->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
}

ui64 AsyncAlterDropIndex(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TString& indexName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpDropIndex, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableDropIndex();
    desc.SetTableName(tableName);
    desc.SetIndexName(indexName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterAddStream(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TShardedTableOptions::TCdcStream& streamDesc)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateCdcStream, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableCreateCdcStream();
    desc.SetTableName(tableName);
    desc.MutableStreamDescription()->SetName(streamDesc.Name);
    desc.MutableStreamDescription()->SetMode(streamDesc.Mode);
    desc.MutableStreamDescription()->SetFormat(streamDesc.Format);
    desc.MutableStreamDescription()->SetVirtualTimestamps(streamDesc.VirtualTimestamps);
    if (streamDesc.InitialState) {
        desc.MutableStreamDescription()->SetState(*streamDesc.InitialState);
    }

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterDisableStream(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TString& streamName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterCdcStream, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterCdcStream();
    desc.SetTableName(tableName);
    desc.SetStreamName(streamName);
    desc.MutableDisable();

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterDropStream(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TString& streamName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpDropCdcStream, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableDropCdcStream();
    desc.SetTableName(tableName);
    desc.SetStreamName(streamName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

void WaitTxNotification(Tests::TServer::TPtr server, TActorId sender, ui64 txId) {
    auto &runtime = *server->GetRuntime();
    auto &settings = server->GetSettings();

    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);
    auto tid = ChangeStateStorage(SchemeRoot, settings.Domain);
    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void WaitTxNotification(Tests::TServer::TPtr server, ui64 txId) {
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    WaitTxNotification(server, sender, txId);
}

void SimulateSleep(Tests::TServer::TPtr server, TDuration duration) {
    auto &runtime = *server->GetRuntime();
    SimulateSleep(runtime, duration);
}

void SimulateSleep(TTestActorRuntime& runtime, TDuration duration) {
    auto sender = runtime.AllocateEdgeActor();
    runtime.Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), duration);
    runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender);
}

THolder<NSchemeCache::TSchemeCacheNavigate> Navigate(TTestActorRuntime& runtime, const TActorId& sender,
        const TString& path, NSchemeCache::TSchemeCacheNavigate::EOp op)
{
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto request = MakeHolder<TNavigate>();
    auto& entry = request->ResultSet.emplace_back();
    entry.Path = SplitPath(path);
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.Operation = op;
    entry.ShowPrivatePath = true;
    runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.Release()));

    auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT(ev);
    UNIT_ASSERT(ev->Get());

    auto* response = ev->Get()->Request.Release();
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->ErrorCount == 0);
    UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.size(), 1);

    return THolder(response);
}

THolder<NSchemeCache::TSchemeCacheNavigate> Ls(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        const TString& path)
{
    return Navigate(runtime, sender, path, NSchemeCache::TSchemeCacheNavigate::EOp::OpList);
}

void SendSQL(Tests::TServer::TPtr server,
             TActorId sender,
             const TString &sql,
             bool dml)
{
    auto &runtime = *server->GetRuntime();
    auto request = MakeSQLRequest(sql, dml);
    runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
}

void ExecSQL(Tests::TServer::TPtr server,
             TActorId sender,
             const TString &sql,
             bool dml,
             Ydb::StatusIds::StatusCode code)
{
    auto &runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;

    auto request = MakeSQLRequest(sql, dml);
    runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
    auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetRef().GetYdbStatus(), code);
}

void WaitTabletBecomesOffline(TServer::TPtr server, ui64 tabletId)
{
    struct IsShardStateChange
    {
        IsShardStateChange(ui64 tabletId)
            : TabletId(tabletId)
        {
        }

        bool operator()(IEventHandle& ev)
        {
            if (ev.GetTypeRewrite() == TEvDataShard::EvStateChanged) {
                auto &rec = ev.Get<TEvDataShard::TEvStateChanged>()->Record;
                if (rec.GetTabletId() == TabletId
                        && rec.GetState() == NDataShard::TShardState::Offline)
                    return true;
            }
            return false;
        }

        ui64 TabletId;
    };

    TDispatchOptions options;
    options.FinalEvents.emplace_back(IsShardStateChange(tabletId));
    server->GetRuntime()->DispatchEvents(options);
}

namespace {

    class TReadTableImpl : public TActorBootstrapped<TReadTableImpl> {
    public:
        enum EEv {
            EvResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvResume,
        };

        enum class EState {
            PauseWait,
            PauseSent,
            Normal,
        };

        struct TEvResult : public TEventLocal<TEvResult, EvResult> {
            TString Result;

            TEvResult(TString result)
                : Result(std::move(result))
            { }
        };

        struct TEvResume : public TEventLocal<TEvResume, EvResume> {
            // nothing
        };

        TReadTableImpl(TActorId owner, const TString& path, TRowVersion snapshot, bool pause, bool ordered)
            : Owner(owner)
            , Path(path)
            , Snapshot(snapshot)
            , Ordered(ordered)
            , State(pause ? EState::PauseWait : EState::Normal)
        { }

        void Bootstrap(const TActorContext& ctx) {
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            request->Record.SetStreamResponse(true);
            auto &tx = *request->Record.MutableTransaction()->MutableReadTableTransaction();
            tx.SetPath(Path);
            tx.SetOrdered(Ordered);
            tx.SetApiVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);

            if (!Snapshot.IsMax()) {
                tx.SetSnapshotStep(Snapshot.Step);
                tx.SetSnapshotTxId(Snapshot.TxId);
            }

            ctx.Send(MakeTxProxyID(), request.Release());

            Become(&TThis::StateWork);
        }

        STRICT_STFUNC(StateWork,
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle)
            HFunc(TEvTxProcessing::TEvStreamQuotaRequest, Handle)
            HFunc(TEvTxProcessing::TEvStreamQuotaRelease, Handle)
            IgnoreFunc(TEvents::TEvSubscribe)
            HFunc(TEvResume, Handle)
        )

        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
            const auto* msg = ev->Get();

            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
            switch (status) {
                case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResponseData: {
                    const auto rsData = msg->Record.GetSerializedReadTableResponse();
                    Ydb::ResultSet rsParsed;
                    Y_VERIFY(rsParsed.ParseFromString(rsData));
                    NYdb::TResultSet rs(rsParsed);
                    auto& columns = rs.GetColumnsMeta();
                    NYdb::TResultSetParser parser(rs);
                    while (parser.TryNextRow()) {
                        for (size_t idx = 0; idx < columns.size(); ++idx) {
                            if (idx > 0) {
                                Result << ", ";
                            }
                            Result << columns[idx].Name << " = ";
                            PrintValue(Result, parser.ColumnParser(idx));
                        }
                        Result << Endl;
                    }
                    break;
                }
                case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete: {
                    ctx.Send(Owner, new TEvResult(Result));
                    return Die(ctx);
                }
                default: {
                    Result << "ERROR: " << status << Endl;
                    ctx.Send(Owner, new TEvResult(Result));
                    return Die(ctx);
                }
            }
        }

        void Handle(TEvTxProcessing::TEvStreamQuotaRequest::TPtr& ev, const TActorContext& ctx) {
            const auto* msg = ev->Get();

            auto& req = QuotaRequests.emplace_back();
            req.Sender = ev->Sender;
            req.Cookie = ev->Cookie;
            req.TxId = msg->Record.GetTxId();

            switch (State) {
                case EState::PauseWait:
                    ctx.Send(Owner, new TEvResult("PAUSED"));
                    State = EState::PauseSent;
                    break;
                case EState::PauseSent:
                    break;
                case EState::Normal:
                    SendQuotas(ctx);
                    break;
            }
        }

        void Handle(TEvTxProcessing::TEvStreamQuotaRelease::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ev);
            Y_UNUSED(ctx);
        }

        void Handle(TEvResume::TPtr&, const TActorContext& ctx) {
            Y_VERIFY(State == EState::PauseSent);
            State = EState::Normal;
            SendQuotas(ctx);
        }

        void SendQuotas(const TActorContext& ctx) {
            while (QuotaRequests) {
                auto& req = QuotaRequests.front();

                auto response = MakeHolder<TEvTxProcessing::TEvStreamQuotaResponse>();
                response->Record.SetTxId(req.TxId);
                response->Record.SetMessageSizeLimit(16 * 1024 * 1024);
                response->Record.SetReservedMessages(1);

                ctx.Send(req.Sender, response.Release(), 0, req.Cookie);

                QuotaRequests.pop_front();
            }
        }

        static void PrintValue(TStringBuilder& out, NYdb::TValueParser& parser) {
            switch (parser.GetKind()) {
            case NYdb::TTypeParser::ETypeKind::Optional:
                parser.OpenOptional();
                if (parser.IsNull()) {
                    out << "(empty maybe)";
                } else {
                    PrintValue(out, parser);
                }
                parser.CloseOptional();
                break;

            case NYdb::TTypeParser::ETypeKind::Primitive:
                PrintPrimitive(out, parser);
                break;

            default:
                Y_FAIL("Unhandled");
            }
        }

        static void PrintPrimitive(TStringBuilder& out, const NYdb::TValueParser& parser) {
            #define PRINT_PRIMITIVE(type) \
                case NYdb::EPrimitiveType::type: \
                    out << parser.Get##type(); \
                    break

            switch (parser.GetPrimitiveType()) {
            PRINT_PRIMITIVE(Uint32);
            PRINT_PRIMITIVE(Uint64);
            PRINT_PRIMITIVE(Date);
            PRINT_PRIMITIVE(Datetime);
            PRINT_PRIMITIVE(Timestamp);
            PRINT_PRIMITIVE(String);
            PRINT_PRIMITIVE(DyNumber);

            default:
                Y_FAIL("Unhandled");
            }

            #undef PRINT_PRIMITIVE
        }

    private:
        struct TPendingRequest {
            TActorId Sender;
            ui64 Cookie;
            ui64 TxId;
        };

    private:
        const TActorId Owner;
        const TString Path;
        const TRowVersion Snapshot;
        const bool Ordered;
        TStringBuilder Result;

        EState State;
        TDeque<TPendingRequest> QuotaRequests;
    };

} // namespace

TReadShardedTableState StartReadShardedTable(
        Tests::TServer::TPtr server,
        const TString& path,
        TRowVersion snapshot,
        bool pause,
        bool ordered)
{
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    auto worker = runtime.Register(new TReadTableImpl(sender, path, snapshot, pause, ordered));
    auto ev = runtime.GrabEdgeEventRethrow<TReadTableImpl::TEvResult>(sender);
    TString result = ev->Get()->Result;
    if (pause) {
        UNIT_ASSERT_VALUES_EQUAL(result, "PAUSED");
    }
    return { sender, worker, result };
}

void ResumeReadShardedTable(
        Tests::TServer::TPtr server,
        TReadShardedTableState& state)
{
    auto& runtime = *server->GetRuntime();
    runtime.Send(new IEventHandle(state.Worker, TActorId(), new TReadTableImpl::TEvResume()), 0, true);
    auto ev = runtime.GrabEdgeEventRethrow<TReadTableImpl::TEvResult>(state.Sender);
    state.Result = ev->Get()->Result;
}

TString ReadShardedTable(
        Tests::TServer::TPtr server,
        const TString& path,
        TRowVersion snapshot)
{
    return StartReadShardedTable(server, path, snapshot, /* pause = */ false).Result;
}

}
