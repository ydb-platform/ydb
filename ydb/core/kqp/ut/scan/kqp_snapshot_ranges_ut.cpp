#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/kqp/session_actor/kqp_snapshot_tables.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NKqp {
namespace {

class TSnapshotPlan {
public:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry =
        NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
    TQueryData Params{FunctionRegistry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()};
    THolder<NKikimrKqp::TPreparedQuery> Proto = MakeHolder<NKikimrKqp::TPreparedQuery>();
    TPreparedQueryHolder::TConstPtr Prepared;
    NKqpProto::TKqpPhyTx* Tx;
    NKqpProto::TKqpPhyTableId TableId;

    explicit TSnapshotPlan(ui32 keyColumns = 1) {
        TableId.SetPath("/Root/Objects");
        TableId.SetOwnerId(1);
        TableId.SetTableId(1);
        Tx = Proto->MutablePhysicalQuery()->AddTransactions();
        Tx->SetType(NKqpProto::TKqpPhyTx::TYPE_SCAN);
        auto* table = Tx->AddTables();
        table->MutableId()->CopyFrom(TableId);
        table->SetKind(NKqpProto::TABLE_KIND_DS);
        for (ui32 i = 0; i < keyColumns; ++i) {
            auto* column = &(*table->MutableColumns())[i + 1];
            column->MutableId()->SetId(i + 1);
            column->MutableId()->SetName("Key" + ToString(i));
            column->SetTypeId(NScheme::NTypeIds::Uint64);
            table->AddKeyColumns()->CopyFrom(column->GetId());
        }
    }

    NKqpProto::TKqpPhyStage* AddStage() {
        auto* stage = Tx->AddStages();
        TStagePredictor().SerializeToKqpSettings(*stage->MutableProgram()->MutableSettings());
        return stage;
    }

    NKqpProto::TKqpPhyKeyRange* AddRead(bool source = true) {
        if (source) {
            auto* read = AddStage()->AddSources()->MutableReadRangesSource();
            read->MutableTable()->CopyFrom(TableId);
            return read->MutableKeyRange();
        }
        auto* read = AddStage()->AddTableOps();
        read->MutableTable()->CopyFrom(TableId);
        return read->MutableReadRange()->MutableKeyRange();
    }

    void AddComputedRead() {
        auto* read = AddStage()->AddSources()->MutableReadRangesSource();
        read->MutableTable()->CopyFrom(TableId);
        read->MutableRanges()->SetParamName("computed_ranges");
    }

    void AddParam(const TString& name, ui64 value) {
        NKikimrMiniKQL::TType type;
        type.SetKind(NKikimrMiniKQL::Data);
        type.MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
        NKikimrMiniKQL::TValue proto;
        proto.SetUint64(value);
        UNIT_ASSERT(Params.AddMkqlParam(name, type, proto));
        auto* binding = Tx->AddParamBindings();
        binding->SetName(name);
        binding->MutableExternalBinding();
    }

    TVector<NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable> Build() {
        if (!Prepared) {
            Prepared = std::make_shared<TPreparedQueryHolder>(Proto.Release(), FunctionRegistry.Get());
        }
        return BuildSnapshotTables(*Prepared, Params);
    }
};

void AddLiteral(NKqpProto::TKqpPhyKeyBound* bound, ui64 value) {
    auto* literal = bound->AddValues()->mutable_literalvalue();
    literal->mutable_type()->set_type_id(Ydb::Type::UINT64);
    literal->mutable_value()->set_uint64_value(value);
}

void SetRange(NKqpProto::TKqpPhyKeyRange* range, ui64 from, ui64 to) {
    AddLiteral(range->MutableFrom(), from);
    range->MutableFrom()->SetIsInclusive(true);
    AddLiteral(range->MutableTo(), to);
    range->MutableTo()->SetIsInclusive(false);
}

const NKikimrTxUserProxy::TKeyRange& GetRange(
    const TVector<NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable>& tables)
{
    UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(tables[0].GetTablePath(), "/Root/Objects");
    UNIT_ASSERT(tables[0].HasKeyRange());
    return tables[0].GetKeyRange();
}

void AssertFullTable(const TVector<NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable>& tables) {
    UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
    UNIT_ASSERT(!tables[0].HasKeyRange());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpSnapshotRanges) {
    Y_UNIT_TEST(LiteralBoundsForBothReadFormats) {
        for (bool source : {false, true}) {
            TSnapshotPlan plan;
            SetRange(plan.AddRead(source), 100, 200);
            const auto tables = plan.Build();
            const auto& range = GetRange(tables);
            UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), 100);
            UNIT_ASSERT_VALUES_EQUAL(range.GetTo().GetValue().GetTuple(0).GetOptional().GetUint64(), 200);
            UNIT_ASSERT(range.GetFromInclusive());
            UNIT_ASSERT(!range.GetToInclusive());
        }
    }

    Y_UNIT_TEST(ExternalParametersArePerExecution) {
        TSnapshotPlan plan;
        plan.AddParam("from", 100);
        plan.AddParam("to", 200);
        auto* range = plan.AddRead();
        range->MutableFrom()->SetIsInclusive(true);
        range->MutableFrom()->AddValues()->MutableParamValue()->SetParamName("from");
        range->MutableTo()->AddValues()->MutableParamValue()->SetParamName("to");
        const auto first = plan.Build();
        UNIT_ASSERT_VALUES_EQUAL(GetRange(first).GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), 100);

        TQueryData other(plan.FunctionRegistry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider());
        NKikimrMiniKQL::TType type;
        type.SetKind(NKikimrMiniKQL::Data);
        type.MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
        NKikimrMiniKQL::TValue value;
        value.SetUint64(300);
        UNIT_ASSERT(other.AddMkqlParam("from", type, value));
        value.SetUint64(400);
        UNIT_ASSERT(other.AddMkqlParam("to", type, value));
        const auto second = BuildSnapshotTables(*plan.Prepared, other);
        UNIT_ASSERT_VALUES_EQUAL(GetRange(second).GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), 300);
        UNIT_ASSERT_VALUES_EQUAL(GetRange(second).GetTo().GetValue().GetTuple(0).GetOptional().GetUint64(), 400);
    }

    Y_UNIT_TEST(ComputedOrMissingParametersUseFullTable) {
        for (bool computed : {false, true}) {
            TSnapshotPlan plan;
            SetRange(plan.AddRead(), 100, 200);
            auto* range = plan.Tx->MutableStages(0)->MutableSources(0)->MutableReadRangesSource()->MutableKeyRange();
            range->MutableFrom()->ClearValues();
            range->MutableFrom()->AddValues()->MutableParamValue()->SetParamName("missing");
            if (computed) {
                // Even an available value must not be used when this physical
                // transaction binds the name to a computation result instead.
                plan.AddParam("missing", 100);
                plan.Tx->MutableParamBindings(0)->MutableTxResultBinding()->SetTxIndex(0);
            } else {
                auto* binding = plan.Tx->AddParamBindings();
                binding->SetName("missing");
                binding->MutableExternalBinding();
            }
            AssertFullTable(plan.Build());
        }
    }

    Y_UNIT_TEST(MultipleReadsProduceOneEntryPerRange) {
        // TxProxy unites the shards of all entries of a table, so ranges are
        // not merged here.
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 300, 400);
        SetRange(plan.AddRead(false), 100, 200);
        const auto tables = plan.Build();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 2);
        TVector<std::pair<ui64, ui64>> ranges;
        for (const auto& table : tables) {
            UNIT_ASSERT_VALUES_EQUAL(table.GetTablePath(), "/Root/Objects");
            UNIT_ASSERT(table.HasKeyRange());
            ranges.emplace_back(
                table.GetKeyRange().GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(),
                table.GetKeyRange().GetTo().GetValue().GetTuple(0).GetOptional().GetUint64());
        }
        Sort(ranges);
        UNIT_ASSERT_VALUES_EQUAL(ranges[0].first, 100);
        UNIT_ASSERT_VALUES_EQUAL(ranges[0].second, 200);
        UNIT_ASSERT_VALUES_EQUAL(ranges[1].first, 300);
        UNIT_ASSERT_VALUES_EQUAL(ranges[1].second, 400);
    }

    Y_UNIT_TEST(TupleParameterElementsAreResolved) {
        TSnapshotPlan plan;
        NKikimrMiniKQL::TType type;
        type.SetKind(NKikimrMiniKQL::Tuple);
        for (ui32 i = 0; i < 2; ++i) {
            auto* item = type.MutableTuple()->AddElement();
            item->SetKind(NKikimrMiniKQL::Data);
            item->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
        }
        NKikimrMiniKQL::TValue value;
        value.AddTuple()->SetUint64(100);
        value.AddTuple()->SetUint64(200);
        UNIT_ASSERT(plan.Params.AddMkqlParam("bounds", type, value));
        auto* binding = plan.Tx->AddParamBindings();
        binding->SetName("bounds");
        binding->MutableExternalBinding();
        auto* range = plan.AddRead();
        range->MutableFrom()->SetIsInclusive(true);
        auto* from = range->MutableFrom()->AddValues()->MutableParamElementValue();
        from->SetParamName("bounds");
        from->SetElementIndex(0);
        auto* to = range->MutableTo()->AddValues()->MutableParamElementValue();
        to->SetParamName("bounds");
        to->SetElementIndex(1);
        const auto tables = plan.Build();
        UNIT_ASSERT_VALUES_EQUAL(GetRange(tables).GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), 100);
        UNIT_ASSERT_VALUES_EQUAL(GetRange(tables).GetTo().GetValue().GetTuple(0).GetOptional().GetUint64(), 200);
    }

    Y_UNIT_TEST(TupleParameterUnrelatedNullElementIsIgnored) {
        TSnapshotPlan plan;
        NKikimrMiniKQL::TType type;
        type.SetKind(NKikimrMiniKQL::Tuple);
        auto* first = type.MutableTuple()->AddElement();
        first->SetKind(NKikimrMiniKQL::Data);
        first->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
        type.MutableTuple()->AddElement()->SetKind(NKikimrMiniKQL::Void);
        NKikimrMiniKQL::TValue value;
        value.AddTuple()->SetUint64(100);
        value.AddTuple();
        UNIT_ASSERT(plan.Params.AddMkqlParam("bounds", type, value));
        auto* binding = plan.Tx->AddParamBindings();
        binding->SetName("bounds");
        binding->MutableExternalBinding();
        auto* range = plan.AddRead();
        range->MutableFrom()->SetIsInclusive(true);
        auto* from = range->MutableFrom()->AddValues()->MutableParamElementValue();
        from->SetParamName("bounds");
        from->SetElementIndex(0);
        AddLiteral(range->MutableTo(), 200);
        range->MutableTo()->SetIsInclusive(true);
        const auto tables = plan.Build();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetRange(tables).GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), 100);
        UNIT_ASSERT_VALUES_EQUAL(GetRange(tables).GetTo().GetValue().GetTuple(0).GetOptional().GetUint64(), 200);
    }

    Y_UNIT_TEST(UnknownReadDominatesKnownRange) {
        for (bool unknownFirst : {false, true}) {
            TSnapshotPlan plan;
            if (unknownFirst) {
                plan.AddComputedRead();
            }
            SetRange(plan.AddRead(), 100, 200);
            if (!unknownFirst) {
                plan.AddComputedRead();
            }
            AssertFullTable(plan.Build());
        }
    }

    Y_UNIT_TEST(ColumnTableUsesFullTable) {
        TSnapshotPlan plan;
        plan.Tx->MutableTables(0)->SetKind(NKqpProto::TABLE_KIND_OLAP);
        SetRange(plan.AddRead(), 100, 200);
        AssertFullTable(plan.Build());
    }

    Y_UNIT_TEST(StreamLookupUsesFullTable) {
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 100, 200);
        plan.AddStage()->AddInputs()->MutableStreamLookup()->MutableTable()->CopyFrom(plan.TableId);
        AssertFullTable(plan.Build());
    }

    Y_UNIT_TEST(FallbackIsPerTable) {
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 100, 200);
        auto* other = plan.Tx->AddTables();
        other->CopyFrom(plan.Tx->GetTables(0));
        other->MutableId()->SetPath("/Root/Other");
        other->MutableId()->SetTableId(2);
        const auto otherId = other->GetId();
        auto* read = plan.AddStage()->AddSources()->MutableReadRangesSource();
        read->MutableTable()->CopyFrom(otherId);
        read->MutableRanges()->SetParamName("computed_ranges");
        const auto tables = plan.Build();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 2);
        for (const auto& table : tables) {
            UNIT_ASSERT_VALUES_EQUAL(table.HasKeyRange(), table.GetTablePath() == "/Root/Objects");
        }
    }

    Y_UNIT_TEST(EmptyRangeUsesFullTable) {
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 200, 100);
        AssertFullTable(plan.Build());
    }

    Y_UNIT_TEST(EmptyRangeNextToOtherRangesUsesFullTable) {
        // The legacy scan sends an empty range to a shard as well, so the
        // snapshot cannot skip it.
        for (bool emptyFirst : {false, true}) {
            TSnapshotPlan plan;
            if (emptyFirst) {
                SetRange(plan.AddRead(), 200, 100);
            }
            SetRange(plan.AddRead(), 100, 200);
            if (!emptyFirst) {
                SetRange(plan.AddRead(), 200, 100);
            }
            AssertFullTable(plan.Build());
        }
    }

    Y_UNIT_TEST(PointReadUsesBothInclusiveBounds) {
        TSnapshotPlan plan;
        auto* point = plan.AddRead();
        point->SetRangeIsPoint(true);
        AddLiteral(point->MutableFrom(), 100);
        point->MutableTo()->CopyFrom(point->GetFrom());
        const auto tables = plan.Build();
        const auto& range = GetRange(tables);
        UNIT_ASSERT(range.GetFromInclusive());
        UNIT_ASSERT(range.GetToInclusive());
        UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().SerializeAsString(), range.GetTo().SerializeAsString());
    }

    Y_UNIT_TEST(PointFlagOnKeyPrefixKeepsBoundedSnapshot) {
        for (bool source : {false, true}) {
            for (bool parameter : {false, true}) {
                TSnapshotPlan plan(2);
                auto* prefix = plan.AddRead(source);
                prefix->SetRangeIsPoint(true);
                prefix->MutableFrom()->SetIsInclusive(true);
                if (parameter) {
                    plan.AddParam("$group", 110);
                    prefix->MutableFrom()->AddValues()->MutableParamValue()->SetParamName("$group");
                } else {
                    AddLiteral(prefix->MutableFrom(), 110);
                }
                // FillKeyRange sets RangeIsPoint when these two bounds match,
                // even though one column of the two-column key is unconstrained.
                prefix->MutableTo()->CopyFrom(prefix->GetFrom());
                const auto tables = plan.Build();
                const auto& range = GetRange(tables);
                UNIT_ASSERT(range.GetFromInclusive());
                UNIT_ASSERT(range.GetToInclusive());
                UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().TupleSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(range.GetTo().GetValue().TupleSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), 110);
                UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().SerializeAsString(), range.GetTo().SerializeAsString());
            }
        }
    }

    Y_UNIT_TEST(ComputedRangeReadForcesFullTable) {
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 100, 200);
        plan.AddComputedRead();
        AssertFullTable(plan.Build());
    }

    Y_UNIT_TEST(NullableKeyIsPreserved) {
        TSnapshotPlan plan;
        auto* range = plan.AddRead();
        auto* literal = range->MutableFrom()->AddValues()->mutable_literalvalue();
        literal->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        literal->mutable_value()->set_null_flag_value(google::protobuf::NULL_VALUE);
        range->MutableFrom()->SetIsInclusive(true);
        AddLiteral(range->MutableTo(), 100);
        const auto tables = plan.Build();
        UNIT_ASSERT(!GetRange(tables).GetFrom().GetValue().GetTuple(0).HasOptional());
    }

    Y_UNIT_TEST(SnapshotLifecycleOnlyContactsSelectedShard) {
        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetWithSampleTables(false));
        kikimr.RunCall([&] {
            kikimr.GetTestClient().CreateTable("/Root", R"(
                Name: "Objects"
                Columns { Name: "Key0", Type: "Uint64" }
                KeyColumnNames: ["Key0"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");
            return true;
        });
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 110, 120);
        const auto tables = plan.Build();
        const TString expectedRange = GetRange(tables).SerializeAsString();
        ui32 proposals = 0;
        ui32 refreshes = 0;
        ui32 discards = 0;
        ui32 checkedRequests = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxUserProxy::TEvProposeTransaction::EventType) {
                const auto& tx = ev->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record.GetTransaction();
                auto check = [&](const auto& request) {
                    UNIT_ASSERT_VALUES_EQUAL(request.TablesSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(request.GetTables(0).GetKeyRange().SerializeAsString(), expectedRange);
                    ++checkedRequests;
                };
                if (tx.HasCreateVolatileSnapshot()) {
                    check(tx.GetCreateVolatileSnapshot());
                } else if (tx.HasRefreshVolatileSnapshot()) {
                    check(tx.GetRefreshVolatileSnapshot());
                } else if (tx.HasDiscardVolatileSnapshot()) {
                    check(tx.GetDiscardVolatileSnapshot());
                }
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType) {
                if (ev->Get<TEvDataShard::TEvProposeTransaction>()->GetTxKind() == NKikimrTxDataShard::TX_KIND_SNAPSHOT) {
                    ++proposals;
                }
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvRefreshVolatileSnapshotRequest::EventType) {
                ++refreshes;
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvDiscardVolatileSnapshotRequest::EventType) {
                ++discards;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        const auto sender = runtime.AllocateEdgeActor();
        const auto manager = runtime.Register(CreateKqpSnapshotManager("/Root", TDuration::Minutes(1)));
        TDispatchOptions bootstrapOptions;
        bootstrapOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(bootstrapOptions);
        runtime.Send(new IEventHandle(manager, sender, new TEvKqpSnapshot::TEvCreateSnapshotRequest(tables, 1)));
        auto response = runtime.GrabEdgeEventRethrow<TEvKqpSnapshot::TEvCreateSnapshotResponse>(sender);
        UNIT_ASSERT_EQUAL_C(response->Get()->Status, NKikimrIssues::TStatusIds::SUCCESS, NKikimrIssues::TStatusIds::EStatusCode_Name(response->Get()->Status));
        UNIT_ASSERT_VALUES_EQUAL(proposals, 1);

        // Trigger refresh directly to avoid depending on wall-clock timing.
        runtime.Send(new IEventHandle(manager, sender, new TEvents::TEvWakeup()));
        TDispatchOptions refreshOptions;
        refreshOptions.CustomFinalCondition = [&] { return refreshes != 0; };
        runtime.DispatchEvents(refreshOptions);
        UNIT_ASSERT_VALUES_EQUAL(refreshes, 1);

        runtime.Send(new IEventHandle(manager, sender, new TEvKqpSnapshot::TEvDiscardSnapshot()));
        TDispatchOptions discardOptions;
        discardOptions.CustomFinalCondition = [&] { return discards != 0; };
        runtime.DispatchEvents(discardOptions);
        UNIT_ASSERT_VALUES_EQUAL(discards, 1);
        UNIT_ASSERT_VALUES_EQUAL(checkedRequests, 3);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ScanQueryPassesRangeToSnapshotManager) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetUseRealThreads(false).SetWithSampleTables(false));
        kikimr.RunCall([&] {
            kikimr.GetTestClient().CreateTable("/Root", R"(
                Name: "Objects"
                Columns { Name: "Key0", Type: "Uint64" }
                KeyColumnNames: ["Key0"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(
                "REPLACE INTO `/Root/Objects` (Key0) VALUES (10u), (110u), (111u), (210u);",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return true;
        });
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        ui32 boundedRequests = 0;
        ui32 proposals = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqpSnapshot::TEvCreateSnapshotRequest::EventType) {
                const auto& tables = ev->Get<TEvKqpSnapshot::TEvCreateSnapshotRequest>()->Tables;
                UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
                UNIT_ASSERT(tables[0].HasKeyRange());
                ++boundedRequests;
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType &&
                ev->Get<TEvDataShard::TEvProposeTransaction>()->GetTxKind() == NKikimrTxDataShard::TX_KIND_SNAPSHOT)
            {
                ++proposals;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        const auto result = kikimr.RunCall([&] {
            auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                SELECT Key0 FROM `/Root/Objects` WHERE Key0 >= 110u AND Key0 < 120u ORDER BY Key0;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            return StreamResultToYson(it);
        });
        CompareYson(R"([[[110u]];[[111u]]])", result);
        UNIT_ASSERT_VALUES_EQUAL(boundedRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(proposals, 1);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }

    Y_UNIT_TEST(ScanQuerySnapshotsKeyPrefixWithDefaultOptimizer) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetUseRealThreads(false).SetWithSampleTables(false));
        const TVector<TString> tableNames = {"Objects", "Parts", "Tags", "Links"};
        kikimr.RunCall([&] {
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            for (const auto& name : tableNames) {
                kikimr.GetTestClient().CreateTable("/Root", TStringBuilder() << R"(
                    Name: ")" << name << R"("
                    Columns { Name: "GroupId", Type: "Uint64" }
                    Columns { Name: "Id", Type: "Uint64" }
                    Columns { Name: "Kind", Type: "Uint64" }
                    KeyColumnNames: ["GroupId", "Id"]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
                )");
                auto result = session.ExecuteDataQuery(TStringBuilder()
                    << "REPLACE INTO `/Root/" << name << "` (GroupId, Id, Kind) VALUES "
                    << "(10u, 1u, 0u), (110u, 1u, 0u), (110u, 2u, 0u), (210u, 1u, 0u);",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
            return true;
        });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        ui32 snapshotRequests = 0;
        ui32 proposals = 0;
        ui64 expectedGroup = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqpSnapshot::TEvCreateSnapshotRequest::EventType) {
                const auto& tables = ev->Get<TEvKqpSnapshot::TEvCreateSnapshotRequest>()->Tables;
                UNIT_ASSERT_VALUES_EQUAL(tables.size(), tableNames.size());
                for (const auto& table : tables) {
                    UNIT_ASSERT_C(table.HasKeyRange(), table.GetTablePath());
                    const auto& range = table.GetKeyRange();
                    UNIT_ASSERT(range.GetFromInclusive());
                    UNIT_ASSERT(range.GetToInclusive());
                    UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().TupleSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(range.GetTo().GetValue().TupleSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), expectedGroup);
                    UNIT_ASSERT_VALUES_EQUAL(range.GetTo().GetValue().GetTuple(0).GetOptional().GetUint64(), expectedGroup);
                }
                ++snapshotRequests;
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType &&
                ev->Get<TEvDataShard::TEvProposeTransaction>()->GetTxKind() == NKikimrTxDataShard::TX_KIND_SNAPSHOT)
            {
                ++proposals;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Several aggregations filtered by a key prefix and joined together,
        // without a pragma that changes how the optimizer extracts key ranges.
        const TString query = R"(
            DECLARE $group AS Uint64;
            SELECT o.cnt, p.cnt, t.cnt, l.cnt
            FROM (
                SELECT COUNT(*) AS cnt, Kind FROM `/Root/Objects`
                WHERE GroupId = $group GROUP BY Kind
            ) AS o FULL JOIN (
                SELECT COUNT(*) AS cnt, Kind FROM `/Root/Parts`
                WHERE GroupId = $group GROUP BY Kind
            ) AS p ON o.Kind = p.Kind FULL JOIN (
                SELECT COUNT(*) AS cnt, Kind FROM `/Root/Tags`
                WHERE GroupId = $group GROUP BY Kind
            ) AS t ON o.Kind = t.Kind FULL JOIN (
                SELECT COUNT(*) AS cnt, Kind FROM `/Root/Links`
                WHERE GroupId = $group GROUP BY Kind
            ) AS l ON o.Kind = l.Kind;
        )";
        for (ui64 group : {110, 210}) {
            expectedGroup = group;
            proposals = 0;
            const auto result = kikimr.RunCall([&] {
                auto params = NYdb::TParamsBuilder().AddParam("$group").Uint64(group).Build().Build();
                auto it = kikimr.GetTableClient().StreamExecuteScanQuery(query, params).GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                return StreamResultToYson(it);
            });
            CompareYson(group == 110 ? R"([[[2u];[2u];[2u];[2u]]])" : R"([[[1u];[1u];[1u];[1u]]])", result);
            UNIT_ASSERT_VALUES_EQUAL(proposals, tableNames.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(snapshotRequests, 2);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }
}

} // namespace NKikimr::NKqp
