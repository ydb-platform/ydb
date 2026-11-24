#include <ydb/core/kqp/query_data/kqp_prepared_query.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

NKqpProto::TKqpPhyStage& AddStage(NKqpProto::TKqpPhyTx& tx) {
    auto& stage = *tx.AddStages();
    TStagePredictor().SerializeToKqpSettings(*stage.MutableProgram()->MutableSettings());
    return stage;
}

void SetWrite(NKqpProto::TKqpInternalSink* sink, bool olap, bool batch = false) {
    NKikimrKqp::TKqpTableSinkSettings settings;
    settings.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_FILL);
    settings.SetIsOlap(olap);
    settings.SetIsBatch(batch);
    sink->MutableSettings()->PackFrom(settings);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpPreparedQueryProperties) {
    Y_UNIT_TEST(FirstTableWriteInQueryAndInStage) {
        for (bool olapFirst : {false, true}) {
            NKikimrKqp::TQueryPhysicalGraph graph;
            auto& query = *graph.MutablePreparedQuery()->MutablePhysicalQuery();
            auto& first = AddStage(*query.AddTransactions());
            first.AddSinks()->MutableExternalSink();
            SetWrite(first.AddSinks()->MutableInternalSink(), olapFirst);
            SetWrite(first.AddOutputTransforms()->MutableInternalSink(), !olapFirst);

            TPreparedQueryHolder firstOnly(new NKikimrKqp::TPreparedQuery(graph.GetPreparedQuery()), nullptr);
            UNIT_ASSERT_VALUES_EQUAL(firstOnly.GetTableAccess().HasOltpWrite, !olapFirst);
            UNIT_ASSERT_VALUES_EQUAL(firstOnly.GetTableAccess().HasOlapWrite, olapFirst);
            UNIT_ASSERT(!firstOnly.IsBatchQuery());

            auto& next = AddStage(*query.AddTransactions());
            SetWrite(next.AddOutputTransforms()->MutableInternalSink(), !olapFirst);
            NKikimrKqp::TQueryPhysicalGraph restored;
            UNIT_ASSERT(restored.ParseFromString(graph.SerializeAsString()));
            TPreparedQueryHolder holder(new NKikimrKqp::TPreparedQuery(restored.GetPreparedQuery()), nullptr);
            UNIT_ASSERT_VALUES_EQUAL(holder.GetTableAccess().HasOltpWrite, !olapFirst);
            UNIT_ASSERT(holder.GetTableAccess().HasOlapWrite);
            UNIT_ASSERT(!holder.GetTableAccess().HasOltpRead);
            UNIT_ASSERT(!holder.GetTableAccess().HasOlapRead);
        }
    }

    Y_UNIT_TEST(BatchCanBeOnALaterSink) {
        auto proto = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto& stage = AddStage(*proto->MutablePhysicalQuery()->AddTransactions());
        SetWrite(stage.AddSinks()->MutableInternalSink(), false);
        SetWrite(stage.AddSinks()->MutableInternalSink(), true, true);
        // Batch detection stops before this otherwise invalid transform.
        SetWrite(stage.AddOutputTransforms()->MutableInternalSink(), true, true);
        TPreparedQueryHolder holder(proto.release(), nullptr);
        UNIT_ASSERT(holder.IsBatchQuery());
        UNIT_ASSERT(holder.GetTableAccess().HasOltpWrite);
        UNIT_ASSERT(!holder.GetTableAccess().HasOlapWrite);
    }

    Y_UNIT_TEST(ReadOperationsAndSources) {
        enum class ERead { RowRange, RowRanges, RowSource, ColumnRange, Mixed };
        for (const auto kind : {ERead::RowRange, ERead::RowRanges, ERead::RowSource, ERead::ColumnRange, ERead::Mixed}) {
            auto proto = std::make_unique<NKikimrKqp::TPreparedQuery>();
            auto& tx = *proto->MutablePhysicalQuery()->AddTransactions();
            tx.AddTables()->SetKind(NKqpProto::TABLE_KIND_DS);
            auto& stage = AddStage(tx);
            switch (kind) {
                case ERead::RowRange:
                    stage.AddTableOps()->MutableReadRange();
                    break;
                case ERead::RowRanges:
                    stage.AddTableOps()->MutableReadRanges();
                    break;
                case ERead::RowSource:
                    stage.AddSources()->MutableReadRangesSource();
                    break;
                case ERead::Mixed:
                    stage.AddSources()->MutableReadRangesSource();
                    [[fallthrough]];
                case ERead::ColumnRange:
                    stage.AddTableOps()->MutableReadOlapRange();
                    break;
            }
            TPreparedQueryHolder holder(proto.release(), nullptr);
            const auto& access = holder.GetTableAccess();
            UNIT_ASSERT_VALUES_EQUAL(access.HasOlapRead, kind == ERead::ColumnRange || kind == ERead::Mixed);
            UNIT_ASSERT_VALUES_EQUAL(access.HasOltpRead, kind != ERead::ColumnRange);
            UNIT_ASSERT(!access.HasOltpWrite && !access.HasOlapWrite);
        }
    }

    Y_UNIT_TEST(SourcePrecedesUnexpectedTableOp) {
        for (bool fullText : {false, true}) {
            auto proto = std::make_unique<NKikimrKqp::TPreparedQuery>();
            auto& tx = *proto->MutablePhysicalQuery()->AddTransactions();
            tx.AddTables()->SetKind(NKqpProto::TABLE_KIND_DS);
            auto& stage = AddStage(tx);
            stage.AddTableOps(); // type not set
            stage.AddTableOps()->MutableReadRange();
            if (fullText) {
                stage.AddSources()->MutableFullTextSource();
            }
            TPreparedQueryHolder holder(proto.release(), nullptr);
            UNIT_ASSERT(!holder.IsBatchQuery());
            if (fullText) {
                UNIT_ASSERT(holder.GetTableAccess().HasOltpRead);
            } else {
                UNIT_ASSERT_EXCEPTION_CONTAINS(holder.GetTableAccess(), yexception, "unexpected type");
            }
        }
    }

    Y_UNIT_TEST(SchemeAndExternalSource) {
        auto proto = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto& query = *proto->MutablePhysicalQuery();
        query.AddTransactions()->SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
        AddStage(*query.AddTransactions()).AddSources()->MutableExternalSource();
        TPreparedQueryHolder holder(proto.release(), nullptr);
        const auto& access = holder.GetTableAccess();
        UNIT_ASSERT(!access.HasOltpRead && !access.HasOlapRead);
        UNIT_ASSERT(!access.HasOltpWrite && !access.HasOlapWrite);
        UNIT_ASSERT(!holder.IsBatchQuery());
    }

    Y_UNIT_TEST(IncompleteHolderSkipsMalformedSettings) {
        auto proto = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto& stage = AddStage(*proto->MutablePhysicalQuery()->AddTransactions());
        auto* sink = stage.AddSinks()->MutableInternalSink();
        SetWrite(sink, false);
        sink->MutableSettings()->set_value("\xff");
        TPreparedQueryHolder holder(proto.release(), nullptr, /* noFillTables */ true);
        UNIT_ASSERT_EXCEPTION_CONTAINS(holder.GetTableAccess(), yexception, "Incomplete prepared query");
        UNIT_ASSERT_EXCEPTION_CONTAINS(holder.IsBatchQuery(), yexception, "Incomplete prepared query");
    }
}

} // namespace NKikimr::NKqp
