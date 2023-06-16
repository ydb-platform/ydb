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

    class TKernelsWrapper {
        TIntrusivePtr<NMiniKQL::IFunctionRegistry> Reg;
        NYql::TKernelRequestBuilder ReqBuilder;
        public:
            TKernelsWrapper()
                : Reg(CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
                , ReqBuilder((*Reg)) {

            }

            ui32 Add(NYql::TKernelRequestBuilder::EBinaryOp operation) {
                switch (operation) {
                    case NYql::TKernelRequestBuilder::EBinaryOp::Add:
                    {
                        NYql::TExprContext ctx;
                        auto blockInt32Type = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Int32));
                        return ReqBuilder.AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Add, blockInt32Type, blockInt32Type, blockInt32Type);
                    }
                    case NYql::TKernelRequestBuilder::EBinaryOp::StartsWith:
                    {
                        NYql::TExprContext ctx;
                        auto blockStringType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Utf8));
                        auto blockBoolType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
                        return ReqBuilder.AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith, blockStringType, blockStringType, blockBoolType);
                    }
                    case NYql::TKernelRequestBuilder::EBinaryOp::StringContains:
                    {
                        NYql::TExprContext ctx;
                        auto blockStringType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::String));
                        auto blockBoolType = ctx.template MakeType<NYql::TBlockExprType>(ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
                        return ReqBuilder.AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::StringContains, blockStringType, blockStringType, blockBoolType);
                    }
                    default:
                        Y_FAIL("Not implemented");
                
                }
            } 

            TString Serialize() {
                return ReqBuilder.Serialize();
            }            
    };

    TString SerializeProgram(const NKikimrSSA::TProgram& programProto, const TString& kernels = "") {
        NKikimrSSA::TOlapProgram olapProgramProto;
        if (kernels) {
            olapProgramProto.SetKernels(kernels);
        }
        
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

        TKernelsWrapper kernels;
        kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::Add);
        const auto programSerialized = SerializeProgram(programProto, kernels.Serialize());
       
        TProgramContainer program;
        TString errors;
        UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

        TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"sum", TTypeInfo(NTypeIds::Int32) }, {"vat", TTypeInfo(NTypeIds::Int32) }}));
        updates.AddRow().Add<int32_t>(1).Add<int32_t>(1);
        updates.AddRow().Add<int32_t>(100).Add<int32_t>(0);

        auto batch = updates.BuildArrow();
        UNIT_ASSERT(program.ApplyProgram(batch).ok());

        TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Int32)) }));
        result.AddRow().Add<int32_t>(2);
        result.AddRow().Add<int32_t>(100);

        auto expected = result.BuildArrow();
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
    }

    Y_UNIT_TEST(YqlKernelStartsWith) {
        TIndexInfo indexInfo = BuildTableInfo(testColumns, testKey);
        TIndexColumnResolver columnResolver(indexInfo);

        NKikimrSSA::TProgram programProto;
        {
            auto* command = programProto.AddCommand();
            auto* functionProto = command->MutableAssign()->MutableFunction();
            functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
            functionProto->SetKernelIdx(0);
            functionProto->AddArguments()->SetName("string");
            functionProto->AddArguments()->SetName("substring");
            functionProto->SetId(NKikimrSSA::TProgram::TAssignment::EFunction::TProgram_TAssignment_EFunction_FUNC_STR_LENGTH);
        }
        {
            auto* command = programProto.AddCommand();
            auto* prjectionProto = command->MutableProjection();
            auto* column = prjectionProto->AddColumns();
            column->SetName("0");
        }

        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StartsWith);
            const auto programSerialized = SerializeProgram(programProto, kernels.Serialize());
        
            TProgramContainer program;
            TString errors;
            UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"string", TTypeInfo(NTypeIds::Utf8) }, {"substring", TTypeInfo(NTypeIds::Utf8) }}));
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("Lorem");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("amet.");

            auto batch = updates.BuildArrow();
            UNIT_ASSERT(program.ApplyProgram(batch).ok());

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }

        // Test with binary data
        {
            TKernelsWrapper kernels;
            kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::StringContains);
            const auto programSerialized = SerializeProgram(programProto, kernels.Serialize());
        
            TProgramContainer program;
            TString errors;
            UNIT_ASSERT_C(program.Init(columnResolver, NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS, programSerialized, errors), errors);

          
            TTableUpdatesBuilder updates(NArrow::MakeArrowSchema({{"string", TTypeInfo(NTypeIds::Bytes) }, {"substring", TTypeInfo(NTypeIds::Bytes) }}));
            updates.AddRow().Add<std::string>("Lorem ipsum \xC0 dolor\f sit amet.").Add<std::string>("dolor");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit \amet.").Add<std::string>("amet.");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit amet.").Add<std::string>("\amet.");
            updates.AddRow().Add<std::string>("Lorem ipsum dolor sit \amet.").Add<std::string>("\amet.");

            auto batch = updates.BuildArrow();
            Cerr << batch->ToString() << Endl;
            auto res = program.ApplyProgram(batch);
            UNIT_ASSERT_C(res.ok(), res.ToString());

            TTableUpdatesBuilder result(NArrow::MakeArrowSchema( { std::make_pair("0", TTypeInfo(NTypeIds::Uint8)) }));
            result.AddRow().Add<ui8>(1);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(0);
            result.AddRow().Add<ui8>(1);

            auto expected = result.BuildArrow();
            UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), expected->ToString());
        }
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
