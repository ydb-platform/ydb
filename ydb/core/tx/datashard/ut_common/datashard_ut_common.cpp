#include "datashard_ut_common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_bio_events.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/balance_coverage/balance_coverage_builder.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

#include <util/system/valgrind.h>

namespace NKikimr {

using namespace NMiniKQL;
using namespace NSchemeShard;
using namespace Tests;

const bool ENABLE_DATASHARD_LOG = true;

namespace {

    TEvDataShard::TEvGetInfoResponse* GetEvGetInfo(
        Tests::TServer::TPtr server,
        ui64 tabletId,
        TAutoPtr<IEventHandle>& handle)
    {
        auto &runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvDataShard::TEvGetInfoRequest>();
        runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TTableInfoMap result;

        return runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
    }

    void SendReadTablePart(
        Tests::TServer::TPtr server,
        ui64 tabletId,
        const TTableId& tableId,
        const NKikimrSchemeOp::TTableDescription& description,
        ui64 readId,
        NKikimrDataEvents::EDataFormat format)
    {
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        auto request = GetBaseReadRequest(tableId, description, readId, format);

        AddFullRangeQuery(*request);

        SendReadAsync(server, tabletId, request.release(), sender);
    }

    void PrintTableFromResult(TStringBuilder& out, const TEvDataShard::TEvReadResult& event, const NKikimrSchemeOp::TTableDescription& description) {
        auto nrows = event.GetRowsCount();
        for (size_t i = 0; i < nrows; ++i) {
            const auto& cellArray = event.GetCells(i);
            UNIT_ASSERT_VALUES_EQUAL(cellArray.size(), description.ColumnsSize());
            bool first = true;
            for (size_t j = 0; j < cellArray.size(); ++j) {
                if (first) {
                    first = false;
                } else {
                    out << ", ";
                }
                const auto& columnDescription = description.GetColumns(j);
                TString cellValue;
                DbgPrintValue(cellValue, cellArray[j], NScheme::TTypeInfo(columnDescription.GetTypeId()));
                out << columnDescription.GetName() << " = " << cellValue;
            }
            out << '\n';
        }
    }

} // namespace

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
                      planResolution,
                      TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
                      TVector<ui64>{},
                      TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)});

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
    Y_ENSURE(false);
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

void TTester::CreateDataShard(ui64 tabletId, const TString& schemeText, bool withRegister) {
    CreateTestBootstrapper(Runtime, CreateTestTabletInfo(tabletId, TTabletTypes::DataShard),
        &::NKikimr::CreateDataShard);

    {
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime.DispatchEvents(options);
    }

    NKikimrSchemeOp::TTableDescription tableDesc;
    UNIT_ASSERT(NProtoBuf::TextFormat::ParseFromString(schemeText, &tableDesc));

    NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
    schemeTx.MutableCreateTable()->CopyFrom(tableDesc);

    ui64 txId = ++LastTxId;
    auto proposal = new TEvDataShard::TEvProposeTransaction(
        NKikimrTxDataShard::TX_KIND_SCHEME, FAKE_SCHEMESHARD_TABLET_ID,
        Sender, txId, schemeTx.SerializeAsString(),
        NKikimrSubDomains::TProcessingParams(), NDataShard::TTxFlags::Default);
    Runtime.SendToPipe(tabletId, Sender, proposal);

    ui64 minStep = 0;
    {
        TAutoPtr<IEventHandle> handle;
        auto event = Runtime.GrabEdgeEventIf<TEvDataShard::TEvProposeTransactionResult>(handle,
            [=](const TEvDataShard::TEvProposeTransactionResult& ev) {
                return ev.GetTxId() == txId && ev.GetOrigin() == tabletId;
            });
        UNIT_ASSERT(event);
        UNIT_ASSERT_C(!event->IsError(), event->GetError());
        UNIT_ASSERT(event->IsPrepared());
        minStep = event->Record.GetMinStep();
    }

    ui64 stepId = Max(++LastStep, minStep);
    LastStep = stepId;

    auto planStep = new TEvTxProcessing::TEvPlanStep(stepId, 0, tabletId);
    auto plannedTx = planStep->Record.MutableTransactions()->Add();
    plannedTx->SetTxId(txId);
    ActorIdToProto(Sender, plannedTx->MutableAckTo());
    Runtime.SendToPipe(tabletId, Sender, planStep);

    {
        TAutoPtr<IEventHandle> handle;
        auto event = Runtime.GrabEdgeEventIf<TEvDataShard::TEvProposeTransactionResult>(handle,
            [=](const TEvDataShard::TEvProposeTransactionResult& ev) {
                return ev.GetTxId() == txId && ev.GetOrigin() == tabletId;
            });
        UNIT_ASSERT(event);
        UNIT_ASSERT_C(event->IsComplete(), event->GetError());
    }

    RebootTablet(Runtime, tabletId, Sender);

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
        table.TableId.Reset(new TTableId(TPathId::FromProto(tdesc.GetPathId())));
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

    switch (schema) {
    case ESchema_KV:
        CreateDataShard(TTestTxConfig::TxTablet0, keyValueSchemeText, true);
        break;
    case ESchema_DoubleKV:
        CreateDataShard(TTestTxConfig::TxTablet0, doubleKeyValueSchemeText, true);
        break;
    case ESchema_DoubleKVExternal:
        CreateDataShard(TTestTxConfig::TxTablet0, doubleKeyValueExternalSchemeText, true);
        break;
    case ESchema_SpecialKV:
        CreateDataShard(TTestTxConfig::TxTablet0, specialKeyValueSchemeText, true);
        break;
    case ESchema_MultiShardKV:
        CreateDataShard(TTestTxConfig::TxTablet0, keyValueSchemeText, true);
        CreateDataShard(TTestTxConfig::TxTablet1, keyValueSchemeText);
        CreateDataShard(TTestTxConfig::TxTablet2, keyValueSchemeText);
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
    Y_ENSURE(resFuture.Wait(TIME_LIMIT), "ProgramText2Bin is taking too long to compile");
    NYql::TConvertResult res = resFuture.GetValue();
    res.Errors.PrintTo(Cerr);
    UNIT_ASSERT(res.Node.GetNode());
    return res.Node;
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
                                                      bool dml,
                                                      const TString& userSID /*= TString()*/)
{
    auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>(userSID);
    if (dml) {
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    }
    request->Record.SetRequestType("_document_api_request");
    request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request->Record.MutableRequest()->SetType(dml
                                              ? NKikimrKqp::QUERY_TYPE_SQL_DML
                                              : NKikimrKqp::QUERY_TYPE_SQL_DDL);
    request->Record.MutableRequest()->SetQuery(sql);
    request->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);
    return request;
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

    UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Record.GetStatus(), expectedStatus, "Status: " << ev->Get()->Record.GetStatus() << " Issues: " << ev->Get()->Record.GetIssues());

    return ev->Get()->Record.GetTxId();
}

std::tuple<TVector<ui64>, TTableId> CreateShardedTable(
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
    if (opts.Indexes_ || opts.Sequences_) {
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
        desc = tx.MutableCreateIndexedTable()->MutableTableDescription();
    } else {
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
        desc = tx.MutableCreateTable();
    }

    UNIT_ASSERT(desc);
    desc->SetName(name);

    if (opts.AllowSystemColumnNames_) {
        desc->SetSystemColumnNamesAllowed(true);
    }

    std::vector<TString> defaultFromSequences;

    for (const auto& column : opts.Columns_) {
        auto col = desc->AddColumns();
        col->SetName(column.Name);
        col->SetType(column.Type);
        col->SetNotNull(column.NotNull);
        if (column.IsKey) {
            desc->AddKeyColumnNames(column.Name);
        }
        col->SetFamilyName(column.Family);
        if (column.DefaultFromSequence) {
            col->SetDefaultFromSequence(column.DefaultFromSequence);
            defaultFromSequences.emplace_back(column.DefaultFromSequence);
        }
    }

    for (const auto& family : opts.Families_) {
        auto fam = desc->MutablePartitionConfig()->AddColumnFamilies();
        if (family.Name) fam->SetName(family.Name);
        if (family.LogPoolKind) fam->MutableStorageConfig()->MutableLog()->SetPreferredPoolKind(family.LogPoolKind);
        if (family.SysLogPoolKind) fam->MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind(family.SysLogPoolKind);
        if (family.DataPoolKind) fam->MutableStorageConfig()->MutableData()->SetPreferredPoolKind(family.DataPoolKind);
        if (family.ExternalPoolKind) fam->MutableStorageConfig()->MutableExternal()->SetPreferredPoolKind(family.ExternalPoolKind);
        if (family.DataThreshold) fam->MutableStorageConfig()->SetDataThreshold(family.DataThreshold);
        if (family.ExternalThreshold) fam->MutableStorageConfig()->SetExternalThreshold(family.ExternalThreshold);
        if (family.ExternalChannelsCount) fam->MutableStorageConfig()->SetExternalChannelsCount(family.ExternalChannelsCount);
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

    for (const auto& [k, v] : opts.Attributes_) {
        auto* attr = tx.MutableAlterUserAttributes()->AddUserAttributes();
        attr->SetKey(k);
        attr->SetValue(v);
    }

    for(const TString& name: defaultFromSequences) {
        auto seq = tx.MutableCreateIndexedTable()->MutableSequenceDescription()->Add();
        seq->SetName(name);
    }

    desc->SetUniformPartitionsCount(opts.Shards_);

    if (!opts.EnableOutOfOrder_)
        desc->MutablePartitionConfig()->MutablePipelineConfig()->SetEnableOutOfOrder(false);

    if (opts.DataTxCacheSize_)
        desc->MutablePartitionConfig()->MutablePipelineConfig()->SetDataTxCacheSize(*opts.DataTxCacheSize_);

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

    if (opts.ReplicationConsistencyLevel_) {
        desc->MutableReplicationConfig()->SetConsistencyLevel(
            static_cast<NKikimrSchemeOp::TTableReplicationConfig::EConsistencyLevel>(*opts.ReplicationConsistencyLevel_));
    }

    WaitTxNotification(server, sender, RunSchemeTx(*server->GetRuntime(), std::move(request), sender));

    TString path = TStringBuilder() << root << "/" << name;
    const auto& shards = GetTableShards(server, sender, path);
    TTableId tableId = ResolveTableId(server, sender, path);

    return {shards, tableId};
}

std::tuple<TVector<ui64>, TTableId> CreateShardedTable(
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
    return CreateShardedTable(server, sender, root, name, opts);
}

ui64 AsyncCreateCopyTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString &root,
        const TString &name,
        const TString &from,
        bool isBackup)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateTable, root);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableCreateTable();
    desc.SetName(name);
    desc.SetCopyFromTable(from);
    desc.SetIsBackup(isBackup);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender);
}

NKikimrTxDataShard::TEvCompactTableResult CompactTable(
    TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId, bool compactBorrowed, ui64 cookie)
{
    auto sender = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvDataShard::TEvCompactTable>(tableId.PathId);
    request->Record.SetCompactBorrowed(compactBorrowed);
    runtime.SendToPipe(shardId, sender, request.Release(), 0, GetPipeConfigWithRetries(), TActorId(), cookie);

    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
    UNIT_ASSERT_C(ev->Cookie == cookie,
        "Unexpected cookie for EvCompactTableResult " << ev->Cookie << ", expected " << cookie);
    return ev->Get()->Record;
}

NKikimrTxDataShard::TEvCompactBorrowedResult CompactBorrowed(TTestActorRuntime& runtime, ui64 shardId, const TTableId& tableId) {
    auto request = MakeHolder<TEvDataShard::TEvCompactBorrowed>(tableId.PathId);
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(shardId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactBorrowedResult>(sender);
    return ev->Get()->Record;
}

std::pair<TTableInfoMap, ui64> GetTables(
    Tests::TServer::TPtr server,
    ui64 tabletId)
{
    TTableInfoMap result;

    TAutoPtr<IEventHandle> handle;
    auto response = GetEvGetInfo(server, tabletId, handle);
    for (auto& table: response->Record.GetUserTables()) {
        result[table.GetName()] = table;
    }

    auto ownerId = response->Record.GetTabletInfo().GetSchemeShard();

    return std::make_pair(result, ownerId);
}

std::pair<TTableInfoByPathIdMap, ui64> GetTablesByPathId(
    Tests::TServer::TPtr server,
    ui64 tabletId)
{
    TTableInfoByPathIdMap result;

    TAutoPtr<IEventHandle> handle;
    auto response = GetEvGetInfo(server, tabletId, handle);
    for (auto& table: response->Record.GetUserTables()) {
        result[TPathId::FromProto(table.GetDescription().GetPathId())] = table;
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
    Y_ENSURE(status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete,
        "Unexpected status " << status);

    auto step = record.GetStep();
    auto txId = record.GetTxId();
    Y_ENSURE(step != 0 && txId != 0,
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
        TTestActorRuntime& runtime,
        const TVector<TString>& tables,
        ui64 writeTxId)
{
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
    Y_ENSURE(status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete,
        "Unexpected status " << status);

    auto step = record.GetStep();
    auto txId = record.GetTxId();
    Y_ENSURE(txId != 0,
        "Unexpected step " << step << " and txId " << txId);

    return { step, txId };
}

TRowVersion CommitWrites(
        Tests::TServer::TPtr server,
        const TVector<TString>& tables,
        ui64 writeTxId)
{
    return CommitWrites(*server->GetRuntime(), tables, writeTxId);
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
        NKikimrMiniKQL::TValue&& splitKey)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableSplitMergeTablePartitions();
    desc.SetTablePath(path);
    desc.AddSourceTabletId(sourceTablet);
    *desc.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional() = std::move(splitKey);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender, true);
}

ui64 AsyncSplitTable(
        Tests::TServer::TPtr server,
        TActorId sender,
        const TString& path,
        ui64 sourceTablet,
        ui32 splitKey)
{
    NKikimrMiniKQL::TValue protoKey;
    protoKey.SetUint32(splitKey);
    return AsyncSplitTable(server, sender, path, sourceTablet, std::move(protoKey));
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

ui64 AsyncSetEnableFilterByKey(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name,
        bool value)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterTable, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterTable();
    desc.SetName(name);
    desc.MutablePartitionConfig()->SetEnableFilterByKey(value);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncSetColumnFamily(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& name,
        const TString& colName,
        TShardedTableOptions::TFamily family)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterTable, workingDir);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterTable();
    desc.SetName(name);

    auto col = desc.AddColumns();
    col->SetName(colName);
    col->SetFamilyName(family.Name);

    auto fam = desc.MutablePartitionConfig()->AddColumnFamilies();
    if (family.Name) fam->SetName(family.Name);
    if (family.LogPoolKind) fam->MutableStorageConfig()->MutableLog()->SetPreferredPoolKind(family.LogPoolKind);
    if (family.SysLogPoolKind) fam->MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind(family.SysLogPoolKind);
    if (family.DataPoolKind) fam->MutableStorageConfig()->MutableData()->SetPreferredPoolKind(family.DataPoolKind);
    if (family.ExternalPoolKind) fam->MutableStorageConfig()->MutableExternal()->SetPreferredPoolKind(family.ExternalPoolKind);
    if (family.DataThreshold) fam->MutableStorageConfig()->SetDataThreshold(family.DataThreshold);
    if (family.ExternalThreshold) fam->MutableStorageConfig()->SetExternalThreshold(family.ExternalThreshold);

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

ui64 AsyncMoveIndex(
        Tests::TServer::TPtr server,
        const TString& tablePath,
        const TString& srcIndexName,
        const TString& dstIndexName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpMoveIndex);
    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableMoveIndex();
    desc.SetTablePath(tablePath);
    desc.SetSrcPath(srcIndexName);
    desc.SetDstPath(dstIndexName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
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
    request->Record.SetRequestType("_document_api_request");

    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableCreateCdcStream();
    desc.SetTableName(tableName);
    desc.MutableStreamDescription()->SetName(streamDesc.Name);
    desc.MutableStreamDescription()->SetMode(streamDesc.Mode);
    desc.MutableStreamDescription()->SetFormat(streamDesc.Format);
    desc.MutableStreamDescription()->SetVirtualTimestamps(streamDesc.VirtualTimestamps);
    desc.MutableStreamDescription()->SetSchemaChanges(streamDesc.SchemaChanges);
    desc.MutableStreamDescription()->SetUserSIDs(streamDesc.UserSIDs);
    if (streamDesc.ResolvedTimestamps) {
        desc.MutableStreamDescription()->SetResolvedTimestampsIntervalMs(streamDesc.ResolvedTimestamps->MilliSeconds());
    }
    if (streamDesc.InitialState) {
        desc.MutableStreamDescription()->SetState(*streamDesc.InitialState);
    }
    if (streamDesc.AwsRegion) {
        desc.MutableStreamDescription()->SetAwsRegion(*streamDesc.AwsRegion);
    }
    if (streamDesc.TopicAutoPartitioning) {
        desc.SetTopicAutoPartitioning(true);
        desc.SetMaxPartitionCount(1000);
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
    desc.AddStreamName(streamName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterDropReplicationConfig(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterTable, workingDir);
    auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
    tx.SetInternal(true);

    auto& desc = *tx.MutableAlterTable();
    desc.SetName(tableName);
    desc.MutableReplicationConfig()->SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncCreateContinuousBackup(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& tableName,
        const TString& streamName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateContinuousBackup, workingDir);

    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableCreateContinuousBackup();
    desc.SetTableName(tableName);
    desc.MutableContinuousBackupDescription()->SetStreamName(streamName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterTakeIncrementalBackup(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& srcTableName,
        const TString& dstTableName,
        const TString& dstStreamName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterContinuousBackup, workingDir);

    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableAlterContinuousBackup();
    desc.SetTableName(srcTableName);
    auto& incBackup = *desc.MutableTakeIncrementalBackup();
    incBackup.SetDstPath(dstTableName);
    incBackup.SetDstStreamPath(dstStreamName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request));
}

ui64 AsyncAlterRestoreIncrementalBackup(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TString& srcTablePath,
        const TString& dstTablePath)
{
    return AsyncAlterRestoreMultipleIncrementalBackups(server, workingDir, {srcTablePath}, dstTablePath);
}

ui64 AsyncAlterRestoreMultipleIncrementalBackups(
        Tests::TServer::TPtr server,
        const TString& workingDir,
        const TVector<TString>& srcTablePaths,
        const TString& dstTablePath)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups, workingDir);

    auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableRestoreMultipleIncrementalBackups();
    for (const auto& srcTablePath: srcTablePaths) {
        desc.AddSrcTablePaths(srcTablePath);
    }
    desc.SetDstTablePath(dstTablePath);

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

void WaitTableStatsImpl(TTestActorRuntime& runtime,
    std::function<void(typename TEvDataShard::TEvPeriodicTableStats::TPtr&)> observerFunc,
    bool& captured) {

    auto observer = runtime.AddObserver<TEvDataShard::TEvPeriodicTableStats>(observerFunc);

    for (int i = 0; i < 5 && !captured; ++i) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() { return captured; };
        runtime.DispatchEvents(options, TDuration::Seconds(5));
    }

    observer.Remove();

    UNIT_ASSERT(captured);
}

NKikimrTxDataShard::TEvPeriodicTableStats WaitTableFollowerStats(TTestActorRuntime& runtime, ui64 datashardId,
    std::function<bool(const NKikimrTableStats::TTableStats& stats)> condition)
{
    NKikimrTxDataShard::TEvPeriodicTableStats stats;
    bool captured = false;

    auto observerFunc = [&](auto& ev) {
        const NKikimrTxDataShard::TEvPeriodicTableStats& record = ev->Get()->Record;
        Cerr << "Captured TEvDataShard::TEvPeriodicTableStats " << record.ShortDebugString() << Endl;

        if (!record.GetFollowerId())
            return;

        if (record.GetDatashardId() != datashardId)
            return;

        if (!condition(record.GetTableStats()))
            return;

        stats = record;
        captured = true;
    };

    WaitTableStatsImpl(runtime, observerFunc, captured);
    return stats;
}


NKikimrTxDataShard::TEvPeriodicTableStats WaitTableStats(TTestActorRuntime& runtime, ui64 datashardId,
    std::function<bool(const NKikimrTableStats::TTableStats& stats)> condition)
{
    NKikimrTxDataShard::TEvPeriodicTableStats stats;
    bool captured = false;

    auto observerFunc = [&](auto& ev) {
        const NKikimrTxDataShard::TEvPeriodicTableStats& record = ev->Get()->Record;
        Cerr << "Captured TEvDataShard::TEvPeriodicTableStats " << record.ShortDebugString() << Endl;

        if (record.GetFollowerId())
            return;

        if (record.GetDatashardId() != datashardId)
            return;

        if (!condition(record.GetTableStats()))
            return;

        stats = record;
        captured = true;
    };

    WaitTableStatsImpl(runtime, observerFunc, captured);
    return stats;
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
             Ydb::StatusIds::StatusCode code,
             NYdb::NUt::TTestContext testCtx,
             const TString& userSID)
{
    auto &runtime = *server->GetRuntime();
    auto request = MakeSQLRequest(sql, dml, userSID);
    runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release(), 0, 0, nullptr));
    auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
    auto& response = ev->Get()->Record;
    auto& issues = response.GetResponse().GetQueryIssues();
    CTX_UNIT_ASSERT_VALUES_EQUAL_C(response.GetYdbStatus(),
                                   code,
                                   issues.empty() ? response.DebugString() : issues.Get(0).DebugString()
    );
}

void ExecSQL(Tests::TServer::TPtr server,
             TActorId sender,
             const TString &sql,
             bool dml,
             const TString &userSID)
{
    ExecSQL(server, sender, sql, dml, Ydb::StatusIds::SUCCESS, NYdb::NUt::TTestContext(), userSID);
}

TRowVersion AcquireReadSnapshot(TTestActorRuntime& runtime, const TString& databaseName, ui32 nodeIndex) {
    TActorId sender = runtime.AllocateEdgeActor(nodeIndex);
    runtime.Send(
        NLongTxService::MakeLongTxServiceID(sender.NodeId()),
        sender,
        new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(databaseName),
        nodeIndex,
        true);
    auto ev = runtime.GrabEdgeEventRethrow<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>(sender);
    const auto& record = ev->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), Ydb::StatusIds::SUCCESS);
    return TRowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());
}

void AddValueToCells(ui64 value, const TString& columnType, TVector<TCell>& cells, TVector<TString>& stringValues) {
    if (columnType == "Uint64") {
        cells.emplace_back(TCell((const char*)&value, sizeof(ui64)));
    } else if (columnType == "Uint32") {
        ui32 value32 = static_cast<ui32>(value);
        cells.emplace_back(TCell((const char*)&value32, sizeof(ui32)));
    } else if (columnType == "Uint16") {
        ui16 value16 = static_cast<ui16>(value);
        cells.emplace_back(TCell((const char*)&value16, sizeof(ui16)));
    } else if (columnType == "Uint8") {
        ui8 value8 = static_cast<ui8>(value);
        cells.emplace_back(TCell((const char*)&value8, sizeof(ui8)));
    } else if (columnType == "Int64") {
        i64 value64 = static_cast<i64>(value);
        cells.push_back(TCell::Make(value64));
    } else if (columnType == "Int32") {
        i32 value32 = static_cast<i32>(value);
        cells.push_back(TCell::Make(value32));
    } else if (columnType == "Int16") {
        i16 value16 = static_cast<i16>(value);
        cells.push_back(TCell::Make(value16));
    } else if (columnType == "Int8") {
        i8 value8 = static_cast<i8>(value);
        cells.push_back(TCell::Make(value8));
    } else if (columnType == "Double") {
        cells.emplace_back(TCell((const char*)&value, sizeof(double)));
    } else if (columnType == "Utf8") {
        stringValues.emplace_back(Sprintf("String_%" PRIu64, value));
        cells.emplace_back(TCell(stringValues.back().c_str(), stringValues.back().size()));
    } else {
        Y_ENSURE(false, "Unsupported column type " << columnType);
    }
}


std::unique_ptr<NEvents::TDataEvents::TEvWrite> MakeWriteRequest(std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWrite_TOperation::EOperationType operationType, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui32 rowCount, ui64 seed) {
    std::vector<ui32> columnIds;
    for (ui32 col = 0; col < columns.size(); ++col) {
        const auto& column = columns[col];
        if (column.IsKey || operationType != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE)
            columnIds.push_back(col + 1);
    }

    TVector<TString> stringValues;
    TVector<TCell> cells;

    for (ui32 row = 0; row < rowCount; ++row) {
        for (ui32 col = 0; col < columns.size(); ++col) {
            const auto& column = columns[col];
            if (!column.IsKey && operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE)
                continue;
            ui64 value = row * columns.size() + col + seed;
            AddValueToCells(value, column.Type, cells, stringValues);
        }
    }

    TSerializedCellMatrix matrix(cells, rowCount, columnIds.size());
    TString blobData = matrix.ReleaseBuffer();

    UNIT_ASSERT(blobData.size() < 8_MB);

    std::unique_ptr<NKikimr::NEvents::TDataEvents::TEvWrite> evWrite = txId ? std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(*txId, txMode) : std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txMode);
    ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(operationType, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

    return evWrite;
}

std::unique_ptr<NEvents::TDataEvents::TEvWrite> MakeWriteRequest(std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWrite_TOperation::EOperationType operationType, const TTableId& tableId, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells, const ui32 defaultFilledColumns) {
    UNIT_ASSERT((cells.size() % columnIds.size()) == 0);

    TSerializedCellMatrix matrix(cells, cells.size() / columnIds.size(), columnIds.size());
    TString blobData = matrix.ReleaseBuffer();

    std::unique_ptr<NKikimr::NEvents::TDataEvents::TEvWrite> evWrite = txId
        ? std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(*txId, txMode)
        : std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txMode);
    ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(operationType, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC, defaultFilledColumns);

    return evWrite;
}

std::unique_ptr<NEvents::TDataEvents::TEvWrite> MakeWriteRequestOneKeyValue(std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWrite_TOperation::EOperationType operationType, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui64 key, ui64 value) {
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);

    std::vector<ui32> columnIds = {1, 2};

    TVector<TString> stringValues;
    TVector<TCell> cells;

    AddValueToCells(key, columns[0].Type, cells, stringValues);
    AddValueToCells(value, columns[1].Type, cells, stringValues);

    TSerializedCellMatrix matrix(cells, 1, 2);
    TString blobData = matrix.ReleaseBuffer();

    std::unique_ptr<NKikimr::NEvents::TDataEvents::TEvWrite> evWrite = txId ? std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(*txId, txMode) : std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txMode);
    ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(operationType, tableId, columnIds, payloadIndex, NKikimrDataEvents::FORMAT_CELLVEC);

    return evWrite;
}

NKikimrDataEvents::TEvWriteResult Write(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, std::unique_ptr<NEvents::TDataEvents::TEvWrite>&& request, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto txMode = request->Record.GetTxMode();
    runtime.SendToPipe(shardId, sender, request.release(), 0, GetPipeConfigWithRetries(), TActorId(), 0, {});

    auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender);
    auto resultRecord = ev->Get()->Record;

    if (expectedStatus == NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED) {
        switch (txMode) {
            case NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE:
                expectedStatus = NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED;
                break;
            case NKikimrDataEvents::TEvWrite::MODE_PREPARE:
            case NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE:
                expectedStatus = NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED;
                break;
            default:
                expectedStatus = NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED;
                UNIT_ASSERT_C(false, "Unexpected txMode: " << txMode);
                break;
        }
    }
    UNIT_ASSERT_C(resultRecord.GetStatus() == expectedStatus, "Status: " << resultRecord.GetStatus() << " Issues: " << resultRecord.GetIssues());

    return resultRecord;
}

NKikimrDataEvents::TEvWriteResult Upsert(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui32 rowCount, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columns, rowCount);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult Upsert(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columnIds, cells);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult UpsertOneKeyValue(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui64 key, ui64 value, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequestOneKeyValue(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columns, key, value);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult Replace(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui32 rowCount, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, tableId, columns, rowCount);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult Delete(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui32 rowCount, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE, tableId, columns, rowCount);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult Insert(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui32 rowCount, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT, tableId, columns, rowCount);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult Increment(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT, tableId, columnIds, cells);
    return Write(runtime, sender, shardId, std::move(request));
}

NKikimrDataEvents::TEvWriteResult Increment(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INCREMENT, tableId, columnIds, cells);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult UpsertIncrement(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT, tableId, columnIds, cells);
    return Write(runtime, sender, shardId, std::move(request));
}

NKikimrDataEvents::TEvWriteResult UpsertIncrement(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT, tableId, columnIds, cells);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult Upsert(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columnIds, cells);
    return Write(runtime, sender, shardId, std::move(request));
}

NKikimrDataEvents::TEvWriteResult UpsertWithDefaultValues(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells, const ui32 defaultFilledColumnCount)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columnIds, cells, defaultFilledColumnCount);
    return Write(runtime, sender, shardId, std::move(request));
}

NKikimrDataEvents::TEvWriteResult UpsertWithDefaultValues(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, const std::vector<ui32>& columnIds, const std::vector<TCell>& cells, const ui32 defaultFilledColumnCount, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columnIds, cells, defaultFilledColumnCount);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult Update(TTestActorRuntime& runtime, TActorId sender, ui64 shardId, const TTableId& tableId, const TVector<TShardedTableOptions::TColumn>& columns, ui32 rowCount, std::optional<ui64> txId, NKikimrDataEvents::TEvWrite::ETxMode txMode, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto request = MakeWriteRequest(txId, txMode, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE, tableId, columns, rowCount);
    return Write(runtime, sender, shardId, std::move(request), expectedStatus);
}

NKikimrDataEvents::TEvWriteResult WaitForWriteCompleted(TTestActorRuntime& runtime, TActorId sender, NKikimrDataEvents::TEvWriteResult::EStatus expectedStatus)
{
    auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvWriteResult>(sender);
    auto resultRecord = ev->Get()->Record;
    UNIT_ASSERT_C(resultRecord.GetStatus() == expectedStatus, "Status: " << resultRecord.GetStatus() << " Issues: " << resultRecord.GetIssues());
    return resultRecord;
}

void UploadRows(TTestActorRuntime& runtime, const TString& database, const TString& tablePath, const TVector<std::pair<TString, Ydb::Type_PrimitiveTypeId>>& types, const TVector<TCell>& keys, const TVector<TCell>& values)
{
    auto txTypes = std::make_shared<NTxProxy::TUploadTypes>();
    std::transform(types.cbegin(), types.cend(), std::back_inserter(*txTypes), [](const auto& iter) {
        const TString& columnName = iter.first;
        Ydb::Type columnType;
        columnType.set_type_id(iter.second);
        return std::make_pair(columnName, columnType);
    });

    auto txRows = std::make_shared<NTxProxy::TUploadRows>();
    TSerializedCellVec serializedKey(keys);
    TString serializedValues(TSerializedCellVec::Serialize(values));
    txRows->emplace_back(serializedKey, serializedValues);

    auto uploadSender = runtime.AllocateEdgeActor();
    auto actor = NTxProxy::CreateUploadRowsInternal(uploadSender, database, tablePath, txTypes, txRows);
    runtime.Register(actor);

    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(uploadSender);
    UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, Ydb::StatusIds::SUCCESS, "Status: " << ev->Get()->Status << " Issues: " << ev->Get()->Issues);
}

void SendProposeToCoordinator(
        TTestActorRuntime& runtime,
        const TActorId& sender,
        const std::vector<ui64>& shards,
        const TSendProposeToCoordinatorOptions& options)
{
    auto req = std::make_unique<TEvTxProxy::TEvProposeTransaction>(
        options.Coordinator, options.TxId, 0, options.MinStep, options.MaxStep);
    auto* tx = req->Record.MutableTransaction();

    auto* affectedSet = tx->MutableAffectedSet();
    affectedSet->Reserve(shards.size());
    for (ui64 shardId : shards) {
        auto* x = affectedSet->Add();
        x->SetTabletId(shardId);
        x->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);
    }

    if (options.Volatile) {
        tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
    }

    SendViaPipeCache(runtime, options.Coordinator, sender, std::move(req));
}

void WaitTabletBecomesOffline(TServer::TPtr server, ui64 tabletId)
{
    struct IsShardStateChange
    {
        IsShardStateChange(ui64 tabletId)
            :
                TabletId(tabletId)
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
        {
        }

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
                    Y_ENSURE(rsParsed.ParseFromString(rsData));
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
            Y_ENSURE(State == EState::PauseSent);
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

            case NYdb::TTypeParser::ETypeKind::Pg:
                PrintPg(out, parser);
                break;

            default:
                Y_ENSURE(false, "Unhandled");
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
            case NYdb::EPrimitiveType::Date32:
                out << parser.GetDate32().time_since_epoch().count();
                break;
            case NYdb::EPrimitiveType::Datetime64:
                out << parser.GetDatetime64().time_since_epoch().count();
                break;
            case NYdb::EPrimitiveType::Timestamp64:
                out << parser.GetTimestamp64().time_since_epoch().count();
                break;
            PRINT_PRIMITIVE(String);
            PRINT_PRIMITIVE(Bool);
            PRINT_PRIMITIVE(Double);
            PRINT_PRIMITIVE(Utf8);
            PRINT_PRIMITIVE(DyNumber);

            default:
                Y_ENSURE(false, "Unhandled");
            }

            #undef PRINT_PRIMITIVE
        }

        static void PrintPg(TStringBuilder& out, const NYdb::TValueParser& parser) {
            auto pg = parser.GetPg();
            if (pg.IsNull()) {
                out << "(pg null)";
            } else {
                out << TString{pg.Content_}.Quote();
            }
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
        TTestActorRuntime& runtime,
        const TString& path,
        TRowVersion snapshot,
        bool pause,
        bool ordered)
{
    auto sender = runtime.AllocateEdgeActor();
    auto worker = runtime.Register(new TReadTableImpl(sender, path, snapshot, pause, ordered));
    auto ev = runtime.GrabEdgeEventRethrow<TReadTableImpl::TEvResult>(sender);
    TString result = ev->Get()->Result;
    if (pause) {
        UNIT_ASSERT_VALUES_EQUAL(result, "PAUSED");
    }
    return { sender, worker, result };
}

TReadShardedTableState StartReadShardedTable(
        Tests::TServer::TPtr server,
        const TString& path,
        TRowVersion snapshot,
        bool pause,
        bool ordered)
{
    return StartReadShardedTable(*server->GetRuntime(), path, snapshot, pause, ordered);
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
        TTestActorRuntime& runtime,
        const TString& path,
        TRowVersion snapshot)
{
    return StartReadShardedTable(runtime, path, snapshot, /* pause = */ false).Result;
}

TString ReadShardedTable(
        Tests::TServer::TPtr server,
        const TString& path,
        TRowVersion snapshot)
{
    return ReadShardedTable(*server->GetRuntime(), path, snapshot);
}

void SendViaPipeCache(
    TTestActorRuntime& runtime,
    ui64 tabletId, const TActorId& sender,
    std::unique_ptr<IEventBase> msg,
    const TSendViaPipeCacheOptions& options)
{
    ui32 nodeIndex = sender.NodeId() - runtime.GetNodeId(0);
    runtime.Send(
        new IEventHandle(
            MakePipePerNodeCacheID(options.Follower),
            sender,
            new TEvPipeCache::TEvForward(msg.release(), tabletId, options.Subscribe),
            options.Flags,
            options.Cookie),
        nodeIndex,
        /* viaActorSystem */ true);
}

void AddKeyQuery(
    TEvDataShard::TEvRead& request,
    const std::vector<ui32>& keys)
{
    // convertion is ugly, but for tests is OK
    auto cells = ToCells(keys);
    request.Keys.emplace_back(cells);
}

void AddFullRangeQuery(TEvDataShard::TEvRead& request) {
    auto fromBuf = TSerializedCellVec::Serialize(TVector<TCell>());
    auto toBuf = TSerializedCellVec::Serialize(TVector<TCell>());
    request.Ranges.emplace_back(fromBuf, toBuf, true, true);
}

std::unique_ptr<TEvDataShard::TEvRead> GetBaseReadRequest(
    const TTableId& tableId,
    const NKikimrSchemeOp::TTableDescription& description,
    ui64 readId,
    NKikimrDataEvents::EDataFormat format,
    const TRowVersion& readVersion)
{
    auto request = std::make_unique<TEvDataShard::TEvRead>();
    auto& record = request->Record;

    record.SetReadId(readId);
    record.MutableTableId()->SetOwnerId(tableId.PathId.OwnerId);
    record.MutableTableId()->SetTableId(tableId.PathId.LocalPathId);

    for (const auto& column: description.GetColumns()) {
        record.AddColumns(column.GetId());
    }

    record.MutableTableId()->SetSchemaVersion(description.GetTableSchemaVersion());

    if (readVersion) {
        readVersion.ToProto(record.MutableSnapshot());
    }

    record.SetResultFormat(format);

    return request;
}

std::unique_ptr<TEvDataShard::TEvReadResult> WaitReadResult(Tests::TServer::TPtr server, TDuration timeout) {
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(handle, timeout);
    if (!handle) {
        return nullptr;
    }
    std::unique_ptr<TEvDataShard::TEvReadResult> event(handle->Release<TEvDataShard::TEvReadResult>().Release());
    return event;
}

void SendReadAsync(
    Tests::TServer::TPtr server,
    ui64 tabletId,
    TEvDataShard::TEvRead* request,
    TActorId sender,
    ui32 node,
    const NTabletPipe::TClientConfig& clientConfig,
    TActorId clientId)
{
    auto &runtime = *server->GetRuntime();
    runtime.SendToPipe(
        tabletId,
        sender,
        request,
        node,
        clientConfig,
        clientId);
}

std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(
    Tests::TServer::TPtr server,
    ui64 tabletId,
    TEvDataShard::TEvRead* request,
    TActorId sender,
    ui32 node,
    const NTabletPipe::TClientConfig& clientConfig,
    TActorId clientId,
    TDuration timeout)
{
    SendReadAsync(server, tabletId, request, sender, node, clientConfig, clientId);

    return WaitReadResult(server, timeout);
}

TString ReadTable(
    Tests::TServer::TPtr server,
    std::span<const ui64> tabletIds,
    const TTableId& tableId,
    ui64 startReadId)
{
    TStringBuilder result;

    auto readId = startReadId;
    for (const auto& tabletId : tabletIds) {
        auto [tablesMap, ownerId] = GetTablesByPathId(server, tabletId);
        const auto& userTable = tablesMap.at(tableId.PathId);
        const auto& description = userTable.GetDescription();

        SendReadTablePart(server, tabletId, tableId, description, readId++, NKikimrDataEvents::FORMAT_CELLVEC);

        std::unique_ptr<TEvDataShard::TEvReadResult> readResult;
        do {
            readResult = WaitReadResult(server);
            UNIT_ASSERT(readResult);
            UNIT_ASSERT_VALUES_EQUAL(readResult->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            PrintTableFromResult(result, *readResult, description);
        } while (!readResult->Record.GetFinished());
    }

    return result;
}

ui64 AsyncCreateSubDomain(
        const Tests::TServer::TPtr& server,
        const TActorId& sender,
        const TString& workingDir,
        const TString& name,
        const TString& schema)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpCreateSubDomain, workingDir);
    auto* m = request->Record.MutableTransaction()->MutableModifyScheme();
    auto* op = m->MutableSubDomain();
    op->SetName(name);
    bool ok = google::protobuf::TextFormat::MergeFromString(schema, op);
    UNIT_ASSERT_C(ok, "failed to parse schema: " << schema);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender, true);
}

ui64 AsyncAlterSubDomain(
        const Tests::TServer::TPtr& server,
        const TActorId& sender,
        const TString& workingDir,
        const TString& name,
        const TString& schema)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpAlterSubDomain, workingDir);
    auto* m = request->Record.MutableTransaction()->MutableModifyScheme();
    auto* op = m->MutableSubDomain();
    op->SetName(name);
    bool ok = google::protobuf::TextFormat::MergeFromString(schema, op);
    UNIT_ASSERT_C(ok, "failed to parse schema: " << schema);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender, true);
}

ui64 AsyncTruncateTable(
        const Tests::TServer::TPtr& server,
        const TActorId& sender,
        const TString& workingDir,
        const TString& tableName)
{
    auto request = SchemeTxTemplate(NKikimrSchemeOp::ESchemeOpTruncateTable, workingDir);
    auto* op = request->Record.MutableTransaction()->MutableModifyScheme()->MutableTruncateTable();
    op->SetTableName(tableName);

    return RunSchemeTx(*server->GetRuntime(), std::move(request), sender);
}

}
