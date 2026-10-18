#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/kqp/query_data/kqp_snapshot_tables.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
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

    TVector<NKikimrTxUserProxy::TCreateVolatileSnapshotTx::TTable> Build(
        TVector<TSnapshotTableRangeInfo>* rangeInfo = nullptr)
    {
        if (!Prepared) {
            Prepared = std::make_shared<TPreparedQueryHolder>(Proto.Release(), FunctionRegistry.Get());
        }
        return BuildSnapshotTables(*Prepared, Params, rangeInfo);
    }
};

void AddLiteral(NKqpProto::TKqpPhyKeyBound* bound, ui64 value) {
    auto* literal = bound->AddValues()->MutableLiteralValue();
    literal->MutableType()->SetKind(NKikimrMiniKQL::Data);
    literal->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
    literal->MutableValue()->SetUint64(value);
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

    Y_UNIT_TEST(MultipleReadsUseEnclosingRange) {
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 300, 400);
        SetRange(plan.AddRead(false), 100, 200);
        const auto tables = plan.Build();
        const auto& range = GetRange(tables);
        UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), 100);
        UNIT_ASSERT_VALUES_EQUAL(range.GetTo().GetValue().GetTuple(0).GetOptional().GetUint64(), 400);
    }

    Y_UNIT_TEST(InclusivityIsWidenedAcrossReads) {
        TSnapshotPlan plan;
        auto* first = plan.AddRead();
        SetRange(first, 100, 200);
        first->MutableFrom()->SetIsInclusive(false);
        auto* second = plan.AddRead(false);
        SetRange(second, 100, 200);
        second->MutableTo()->SetIsInclusive(true);
        const auto tables = plan.Build();
        UNIT_ASSERT(GetRange(tables).GetFromInclusive());
        UNIT_ASSERT(GetRange(tables).GetToInclusive());
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

    Y_UNIT_TEST(StringBoundsRemainOwnedAfterMerging) {
        TSnapshotPlan plan;
        (*plan.Tx->MutableTables(0)->MutableColumns())[1].SetTypeId(NScheme::NTypeIds::Utf8);
        auto addRange = [&](const TString& from, const TString& to) {
            auto* range = plan.AddRead();
            auto add = [](NKqpProto::TKqpPhyKeyBound* bound, const TString& text) {
                auto* literal = bound->AddValues()->MutableLiteralValue();
                literal->MutableType()->SetKind(NKikimrMiniKQL::Data);
                literal->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Utf8);
                literal->MutableValue()->SetText(text);
            };
            add(range->MutableFrom(), from);
            range->MutableFrom()->SetIsInclusive(true);
            add(range->MutableTo(), to);
        };
        addRange("middle-key-with-a-long-string", "upper-key-with-a-long-string");
        addRange("lower-key-with-a-long-string", "top-key-with-a-long-string");
        const auto tables = plan.Build();
        UNIT_ASSERT_VALUES_EQUAL(GetRange(tables).GetFrom().GetValue().GetTuple(0).GetOptional().GetText(),
            "lower-key-with-a-long-string");
        UNIT_ASSERT_VALUES_EQUAL(GetRange(tables).GetTo().GetValue().GetTuple(0).GetOptional().GetText(),
            "upper-key-with-a-long-string");
    }

    Y_UNIT_TEST(EmptyRangeUsesFullTable) {
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 200, 100);
        AssertFullTable(plan.Build());
    }

    Y_UNIT_TEST(PointReadUsesBothInclusiveBounds) {
        TSnapshotPlan plan;
        auto* point = plan.AddRead();
        point->SetRangeIsPoint(true);
        AddLiteral(point->MutableFrom(), 100);
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
                    plan.AddParam("$bucket_id", 110);
                    prefix->MutableFrom()->AddValues()->MutableParamValue()->SetParamName("$bucket_id");
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

    Y_UNIT_TEST(FullTableFallbackReportsComputedRanges) {
        TSnapshotPlan plan;
        SetRange(plan.AddRead(), 100, 200);
        plan.AddComputedRead();
        TVector<TSnapshotTableRangeInfo> info;
        AssertFullTable(plan.Build(&info));
        UNIT_ASSERT_VALUES_EQUAL(info.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(info[0].TablePath, "/Root/Objects");
        UNIT_ASSERT_VALUES_EQUAL(info[0].FullTableReason, "computed_source_ranges");
        UNIT_ASSERT_VALUES_EQUAL(info[0].KeyRangeReads, 1);
        UNIT_ASSERT_VALUES_EQUAL(info[0].ComputedRangeReads, 1);
    }

    Y_UNIT_TEST(CompositeKeyPrefixesKeepBoundarySemantics) {
        TSnapshotPlan plan(2);
        auto* first = plan.AddRead();
        SetRange(first, 10, 20);
        first->MutableTo()->SetIsInclusive(true); // upper prefix means (20, +inf)
        auto* second = plan.AddRead(false);
        SetRange(second, 10, 20);
        AddLiteral(second->MutableFrom(), 5); // starts after (10, NULL)
        AddLiteral(second->MutableTo(), 5); // ends before (20, +inf)
        const auto tables = plan.Build();
        const auto& range = GetRange(tables);
        UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().TupleSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(range.GetTo().GetValue().TupleSize(), 1);
        UNIT_ASSERT(range.GetFromInclusive());
        UNIT_ASSERT(range.GetToInclusive());
    }

    Y_UNIT_TEST(NullableKeyIsPreserved) {
        TSnapshotPlan plan;
        auto* range = plan.AddRead();
        auto* literal = range->MutableFrom()->AddValues()->MutableLiteralValue();
        literal->MutableType()->SetKind(NKikimrMiniKQL::Optional);
        auto* type = literal->MutableType()->MutableOptional()->MutableItem();
        type->SetKind(NKikimrMiniKQL::Data);
        type->MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
        literal->MutableValue(); // NULL
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
        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetWithSampleTables(false).SetAppConfig(appConfig));
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
        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetWithSampleTables(false).SetAppConfig(appConfig));
        const TVector<TString> tableNames = {"objects", "inflight_parts", "multipart_uploads", "objects_noncurrent"};
        kikimr.RunCall([&] {
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            for (const auto& name : tableNames) {
                kikimr.GetTestClient().CreateTable("/Root", TStringBuilder() << R"(
                    Name: ")" << name << R"("
                    Columns { Name: "bucket_id", Type: "Uint64" }
                    Columns { Name: "object_id", Type: "Uint64" }
                    Columns { Name: "storage_class", Type: "Uint64" }
                    KeyColumnNames: ["bucket_id", "object_id"]
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                    SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
                )");
                auto result = session.ExecuteDataQuery(TStringBuilder()
                    << "REPLACE INTO `/Root/" << name << "` (bucket_id, object_id, storage_class) VALUES "
                    << "(10u, 1u, 0u), (110u, 1u, 0u), (110u, 2u, 0u), (210u, 1u, 0u);",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
            return true;
        });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        ui32 snapshotRequests = 0;
        ui32 proposals = 0;
        ui64 expectedBucket = 0;
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
                    UNIT_ASSERT_VALUES_EQUAL(range.GetFrom().GetValue().GetTuple(0).GetOptional().GetUint64(), expectedBucket);
                    UNIT_ASSERT_VALUES_EQUAL(range.GetTo().GetValue().GetTuple(0).GetOptional().GetUint64(), expectedBucket);
                }
                ++snapshotRequests;
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvProposeTransaction::EventType &&
                ev->Get<TEvDataShard::TEvProposeTransaction>()->GetTxKind() == NKikimrTxDataShard::TX_KIND_SNAPSHOT)
            {
                ++proposals;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Match the validator query's four filtered aggregations and FULL JOINs,
        // without a pragma that changes how the optimizer extracts key ranges.
        const TString query = R"(
            DECLARE $bucket_id AS Uint64;
            SELECT o.cnt, i.cnt, m.cnt, n.cnt
            FROM (
                SELECT COUNT(*) AS cnt, storage_class FROM `/Root/objects`
                WHERE bucket_id = $bucket_id GROUP BY storage_class
            ) AS o FULL JOIN (
                SELECT COUNT(*) AS cnt, storage_class FROM `/Root/inflight_parts`
                WHERE bucket_id = $bucket_id GROUP BY storage_class
            ) AS i ON o.storage_class = i.storage_class FULL JOIN (
                SELECT COUNT(*) AS cnt, storage_class FROM `/Root/multipart_uploads`
                WHERE bucket_id = $bucket_id GROUP BY storage_class
            ) AS m ON o.storage_class = m.storage_class FULL JOIN (
                SELECT COUNT(*) AS cnt, storage_class FROM `/Root/objects_noncurrent`
                WHERE bucket_id = $bucket_id GROUP BY storage_class
            ) AS n ON o.storage_class = n.storage_class;
        )";
        for (ui64 bucket : {110, 210}) {
            expectedBucket = bucket;
            proposals = 0;
            const auto result = kikimr.RunCall([&] {
                auto params = NYdb::TParamsBuilder().AddParam("$bucket_id").Uint64(bucket).Build().Build();
                auto it = kikimr.GetTableClient().StreamExecuteScanQuery(query, params).GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                return StreamResultToYson(it);
            });
            CompareYson(bucket == 110 ? R"([[[2u];[2u];[2u];[2u]]])" : R"([[[1u];[1u];[1u];[1u]]])", result);
            UNIT_ASSERT_VALUES_EQUAL(proposals, tableNames.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(snapshotRequests, 2);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    }
}

} // namespace NKikimr::NKqp
