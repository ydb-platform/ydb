#include "index_info.h"
#include "index_logic_logs.h"

#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/tx/columnshard/columnshard_ut_common.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/yql/core/arrow_kernels/request/request.h>
#include <ydb/library/yql/core/arrow_kernels/registry/registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NOlap;
using namespace NKikimr::NColumnShard;
using namespace NKikimr;
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

namespace {
    static const std::vector<std::pair<TString, TTypeInfo>> testColumns = {
        {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
        {"uid", TTypeInfo(NTypeIds::Utf8) },
        {"sum", TTypeInfo(NTypeIds::Int32) },
        {"vat", TTypeInfo(NTypeIds::Int32) },
    };

    static const std::vector<std::pair<TString, TTypeInfo>> testKey = {
        {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
        {"uid", TTypeInfo(NTypeIds::Utf8) }
    };
}

Y_UNIT_TEST_SUITE(TestProgram) {

    TString BuildRegistry() {
        auto functionRegistry = CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        auto nodeFactory = NMiniKQL::GetBuiltinFactory();
        NYql::TKernelRequestBuilder b(*functionRegistry);

        NYql::TExprContext ctx;
        auto blockInt32Type = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Int32));
        auto index1 = b.AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Add, blockInt32Type, blockInt32Type, blockInt32Type);
        Y_UNUSED(index1);
        return b.Serialize();
    }

    TString SerializeProgram(const NKikimrSSA::TProgram& programProto) {
        NKikimrSSA::TOlapProgram olapProgramProto;
        olapProgramProto.SetKernels(BuildRegistry());
        
        {
            TString str;
            Y_PROTOBUF_SUPPRESS_NODISCARD programProto.SerializeToString(&str);
            olapProgramProto.SetProgram(str);
        }
        TString programSerialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD olapProgramProto.SerializeToString(&programSerialized);
        return programSerialized;
    }
    
    Y_UNIT_TEST(YqlKernel) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("sum");
            functionProto->AddArguments()->SetName("vat");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }
        const auto programSerialized = SerializeProgram(programProto);
       
        TProgramContainer program;
        TString errors;
        UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"sum", TTypeInfo(NTypeIds::Int32) }, {"vat", TTypeInfo(NTypeIds::Int32) }}));
        updates.AddRow().Add(1).Add(1);
        updates.AddRow().Add(100).Add(0);

        auto batch = updates.BuildArrow();
        UNIT_ASSERT(program.ApplyProgram(batch).ok());

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Int32)) }));
        result.AddRow().Add(2);
        result.AddRow().Add(100);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(SimpleFunction) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);;
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            auto* funcArg = functionProto->AddArguments();
            funcArg->SetName("uid");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }
        const auto programSerialized = SerializeProgram(programProto);

        TProgramContainer program;
        TString errors;
        UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema( { std::make_pair("uid", TTypeInfo(NTypeIds::Utf8)) }));
        updates.AddRow().Add("aaa");
        updates.AddRow().Add("b");
        updates.AddRow().Add("");

        auto batch = updates.BuildArrow();
        UNIT_ASSERT(program.ApplyProgram(batch).ok());

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint64)) }));
        result.AddRow().Add<uint64_t>(3);
        result.AddRow().Add<uint64_t>(1);
        result.AddRow().Add<uint64_t>(0);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }
}
