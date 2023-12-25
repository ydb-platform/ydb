#include <ydb/library/yql/providers/yt/provider/yql_yt_dq_integration.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

namespace {

struct TTestSetup {
    TTestSetup()
        : TypesCtx(MakeHolder<TTypeAnnotationContext>())
        , State(MakeIntrusive<TYtState>())
    {
        State->Types = TypesCtx.Get();
        State->DqIntegration_ = CreateYtDqIntegration(State.Get());

    }

    THolder<TTypeAnnotationContext> TypesCtx;
    TYtState::TPtr State;
};

}

Y_UNIT_TEST_SUITE(TSchedulerTest) {
    Y_UNIT_TEST(Ranges_table4_table5_test0) {
        TTestSetup setup;
        TDqSettings settings;
        settings.DataSizePerJob = 1000;
        TVector<TString> partitions;
        size_t maxTasks = 4;
        auto astStr = "(\n"
            "(let $4 (Void))\n"
            "(let $5 (YtMeta '('\"CanWrite\" '\"1\") '('\"DoesExist\" '\"1\") '('\"YqlCompatibleScheme\" '\"1\") '('\"InferredScheme\" '\"0\") '('\"IsDynamic\" '\"0\") '('\"Attrs\" '('('\"optimize_for\" '\"lookup\")))))\n"
            "(let $6 '('\"RecordsCount\" '\"100\"))\n"
            "(let $7 '('\"DataSize\" '\"1284\"))\n"
            "(let $8 '('\"ChunkCount\" '\"1\"))\n"
            "(let $9 (YtStat '('\"Id\" '\"1dd1d-fb585-3f40191-36b851ee\") $6 $7 $8 '('\"ModifyTime\" '\"1614862780\") '('\"Revision\" '\"524591601530245\")))\n"
            "(let $10 (YtTable '\"home/yql/test-table5\" $4 $5 $9 '() (Void) (Void) '\"freud\"))\n"
            "(let $11 '('\"key\" '\"value\"))\n"
            "(let $12 (YtPath $10 $11 (Void) (Void)))\n"
            "(let $13 (YtStat '('\"Id\" '\"1dd1d-fb274-3f40191-5a564b01\") $6 $7 $8 '('\"ModifyTime\" '\"1614862777\") '('\"Revision\" '\"524591601529460\")))\n"
            "(let $14 (YtTable '\"home/yql/test-table4\" $4 $5 $13 '() (Void) (Void) '\"freud\"))\n"
            "(let $15 (YtPath $14 $11 (Void) (Void)))\n"
            "(return (YtReadTable! world (DataSource '\"yt\" '\"freud\") '((YtSection '($12 $15) '('('\"unordered\"))))))\n"
        ")\n";
        auto astRes = ParseAst(astStr);
        UNIT_ASSERT(astRes.IsOk());
        TExprContext exprCtx_;
        TExprNode::TPtr exprRoot_;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot_, exprCtx_, nullptr, nullptr));
        TString cluster;
        const auto result = setup.State->DqIntegration_->Partition(settings, maxTasks, *exprRoot_, partitions, &cluster, exprCtx_, false);
        const auto expected = 428;
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 3);
    }

    Y_UNIT_TEST(Ranges_table4_table7_test1) {
        TTestSetup setup;
        TDqSettings settings;
        settings.DataSizePerJob = 1000;
        TVector<TString> partitions;
        size_t maxTasks = 2;
        auto astStr = "(\n"
                "(let $4 (Void))\n"
                "(let $5 (YtMeta '('\"CanWrite\" '\"1\") '('\"DoesExist\" '\"1\") '('\"YqlCompatibleScheme\" '\"1\") '('\"InferredScheme\" '\"0\") '('\"IsDynamic\" '\"0\") '('\"Attrs\" '('('\"optimize_for\" '\"lookup\")))))\n"
                "(let $6 '('\"RecordsCount\" '\"100\"))\n"
                "(let $7 '('\"DataSize\" '\"1284\"))\n"
                "(let $8 '('\"ChunkCount\" '\"1\"))\n"
                "(let $9 (YtStat '('\"Id\" '\"1dd1d-fb585-3f40191-36b851ee\") $6 $7 $8 '('\"ModifyTime\" '\"1614862780\") '('\"Revision\" '\"524591601530245\")))\n"
                "(let $10 (YtTable '\"home/yql/test-table5\" $4 $5 $9 '() (Void) (Void) '\"freud\"))\n"
                "(let $11 '('\"key\" '\"value\"))\n"
                "(let $12 (YtPath $10 $11 (Void) (Void)))\n"
                "(let $13 (YtStat '('\"Id\" '\"1dd1d-fb703-3f40191-46023c3c\") $6 $7 $8 '('\"ModifyTime\" '\"1614862781\") '('\"Revision\" '\"524591601530627\")))\n"
                "(let $14 (YtTable '\"home/yql/test-table6\" $4 $5 $13 '() (Void) (Void) '\"freud\"))\n"
                "(let $15 (YtPath $14 $11 (Void) (Void)))\n"
                "(let $16 (YtStat '('\"Id\" '\"1dd1d-fb274-3f40191-5a564b01\") $6 $7 $8 '('\"ModifyTime\" '\"1614862777\") '('\"Revision\" '\"524591601529460\")))\n"
                "(let $17 (YtTable '\"home/yql/test-table4\" $4 $5 $16 '() (Void) (Void) '\"freud\"))\n"
                "(let $18 (YtPath $17 $11 (Void) (Void)))\n"
                "(let $19 (YtStat '('\"Id\" '\"1dd1d-fb881-3f40191-9d1d7a9a\") $6 $7 $8 '('\"ModifyTime\" '\"1614862782\") '('\"Revision\" '\"524591601531009\")))\n"
                "(let $20 (YtTable '\"home/yql/test-table7\" $4 $5 $19 '() (Void) (Void) '\"freud\"))\n"
                "(let $21 (YtPath $20 $11 (Void) (Void)))\n"
                "(let $22 '($12 $15 $18 $21))\n"
                "(return (YtReadTable! world (DataSource '\"yt\" '\"freud\") '((YtSection $22 '('('\"unordered\"))))))\n"
        ")\n";
        auto astRes = ParseAst(astStr);
        UNIT_ASSERT(astRes.IsOk());
        TExprContext exprCtx_;
        TExprNode::TPtr exprRoot_;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot_, exprCtx_, nullptr, nullptr));
        TString cluster;
        const auto result = setup.State->DqIntegration_->Partition(settings, maxTasks, *exprRoot_, partitions, &cluster, exprCtx_, false);
        const auto expected = 642;
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 2);
    }

    Y_UNIT_TEST(Ranges_table4_table7_test2) {
        TTestSetup setup;
        TDqSettings settings;
        settings.DataSizePerJob = 1000;
        TVector<TString> partitions;
        size_t maxTasks = 10;
        auto astStr = "(\n"
                "(let $4 (Void))\n"
                "(let $5 (YtMeta '('\"CanWrite\" '\"1\") '('\"DoesExist\" '\"1\") '('\"YqlCompatibleScheme\" '\"1\") '('\"InferredScheme\" '\"0\") '('\"IsDynamic\" '\"0\") '('\"Attrs\" '('('\"optimize_for\" '\"lookup\")))))\n"
                "(let $6 '('\"RecordsCount\" '\"100\"))\n"
                "(let $7 '('\"DataSize\" '\"1284\"))\n"
                "(let $8 '('\"ChunkCount\" '\"1\"))\n"
                "(let $9 (YtStat '('\"Id\" '\"1dd1d-fb585-3f40191-36b851ee\") $6 $7 $8 '('\"ModifyTime\" '\"1614862780\") '('\"Revision\" '\"524591601530245\")))\n"
                "(let $10 (YtTable '\"home/yql/test-table5\" $4 $5 $9 '() (Void) (Void) '\"freud\"))\n"
                "(let $11 '('\"key\" '\"value\"))\n"
                "(let $12 (YtPath $10 $11 (Void) (Void)))\n"
                "(let $13 (YtStat '('\"Id\" '\"1dd1d-fb703-3f40191-46023c3c\") $6 $7 $8 '('\"ModifyTime\" '\"1614862781\") '('\"Revision\" '\"524591601530627\")))\n"
                "(let $14 (YtTable '\"home/yql/test-table6\" $4 $5 $13 '() (Void) (Void) '\"freud\"))\n"
                "(let $15 (YtPath $14 $11 (Void) (Void)))\n"
                "(let $16 (YtStat '('\"Id\" '\"1dd1d-fb274-3f40191-5a564b01\") $6 $7 $8 '('\"ModifyTime\" '\"1614862777\") '('\"Revision\" '\"524591601529460\")))\n"
                "(let $17 (YtTable '\"home/yql/test-table4\" $4 $5 $16 '() (Void) (Void) '\"freud\"))\n"
                "(let $18 (YtPath $17 $11 (Void) (Void)))\n"
                "(let $19 (YtStat '('\"Id\" '\"1dd1d-fb881-3f40191-9d1d7a9a\") $6 $7 $8 '('\"ModifyTime\" '\"1614862782\") '('\"Revision\" '\"524591601531009\")))\n"
                "(let $20 (YtTable '\"home/yql/test-table7\" $4 $5 $19 '() (Void) (Void) '\"freud\"))\n"
                "(let $21 (YtPath $20 $11 (Void) (Void)))\n"
                "(let $22 '($12 $15 $18 $21))\n"
                "(return (YtReadTable! world (DataSource '\"yt\" '\"freud\") '((YtSection $22 '('('\"unordered\"))))))\n"
        ")\n";
        auto astRes = ParseAst(astStr);
        UNIT_ASSERT(astRes.IsOk());
        TExprContext exprCtx_;
        TExprNode::TPtr exprRoot_;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot_, exprCtx_, nullptr, nullptr));
        TString cluster;
        const auto result = setup.State->DqIntegration_->Partition(settings, maxTasks, *exprRoot_, partitions, &cluster, exprCtx_, false);
        const auto expected = 214;
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 6);
    }

}

