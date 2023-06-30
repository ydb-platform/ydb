#include <ydb/library/yql/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_output.h>

#include <util/generic/array_size.h>
#include <util/generic/array_ref.h>

#include <cstring>

namespace NKikimr {
namespace NMiniKQL {

namespace {

TIntrusivePtr<IRandomProvider> CreateRandomProvider() {
    return CreateDeterministicRandomProvider(1);
}

TIntrusivePtr<ITimeProvider> CreateTimeProvider() {
    return CreateDeterministicTimeProvider(10000000);
}

TComputationNodeFactory GetTestFactory(NYql::TMkqlWriterImpl& writer) {
    return [&](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "YtOutput") {
            return NYql::WrapYtOutput(callable, ctx, writer);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

template<bool UseLLVM>
struct TSetup_ {
    TSetup_()
        : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , RandomProvider(CreateRandomProvider())
        , TimeProvider(CreateTimeProvider())
        , Alloc(__LOCATION__)
        , Env(MakeHolder<TTypeEnvironment>(Alloc))
        , PgmBuilder(MakeHolder<TProgramBuilder>(*Env, *FunctionRegistry))
    {}

    TAutoPtr<IComputationGraph> BuildGraph(TRuntimeNode pgm, NYql::TMkqlWriterImpl& writer) {
        Explorer.Walk(pgm.GetNode(), *Env);
        TComputationPatternOpts opts(Alloc.Ref(), *Env, GetTestFactory(writer), FunctionRegistry.Get(),
            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, UseLLVM ? "" : "OFF", EGraphPerProcess::Multi);
        Pattern = MakeComputationPattern(Explorer, pgm, {}, opts);
        return Pattern->Clone(opts.ToComputationOptions(*RandomProvider, *TimeProvider));
    }

    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;

    TScopedAlloc Alloc;
    THolder<TTypeEnvironment> Env;
    THolder<TProgramBuilder> PgmBuilder;

    TExploringNodeVisitor Explorer;
    IComputationPattern::TPtr Pattern;
};

template<bool LLVM>
TRuntimeNode MakeYtWrite(TSetup_<LLVM>& setup, TRuntimeNode item) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    TCallableBuilder callableBuilder(*setup.Env, "YtOutput", pb.NewFlowType(pb.NewVoid().GetStaticType()));
    callableBuilder.Add(item);
    return TRuntimeNode(callableBuilder.Build(), false);
}

} // unnamed

Y_UNIT_TEST_SUITE(YtWriterTests) {
    Y_UNIT_TEST_LLVM(SimpleYson) {
        TSetup_<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto key1 = pb.NewDataLiteral<ui32>(1);
        const auto key2 = pb.NewDataLiteral<ui32>(2);
        const auto key3 = pb.NewDataLiteral<ui32>(3);

        const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("aaa");
        const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("qqq");

        const auto keyType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto payloadType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        auto structType = pb.NewEmptyStructType();
        structType = pb.NewStructType(structType, "payload", payloadType);
        structType = pb.NewStructType(structType, "key", keyType);

        std::vector<std::pair<std::string_view, TRuntimeNode>> map1 = {
            { "key", key1 },
            { "payload", payload1 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map2 = {
            { "key", key2 },
            { "payload", payload2 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map3 = {
            { "key", key3 },
            { "payload", payload3 }
        };

        const auto list = pb.NewList(structType, {
            pb.NewStruct(map2),
            pb.NewStruct(map1),
            pb.NewStruct(map3)
        });

        const auto pgmReturn = pb.Discard(pb.Map(pb.ToFlow(list),
            [&](TRuntimeNode item) {
                return MakeYtWrite(setup, item);
            }
        ));

        const TString spec = R"({
            tables = [{
                "_yql_row_spec" = {
                    "Type" = [
                        "StructType"; [
                            ["key"; ["DataType"; "Uint32"]];
                            ["payload"; ["DataType"; "String"]]
                        ]
                    ]
                }
            }]
        })";

        NYql::NCommon::TCodecContext CodecCtx(*setup.Env, *setup.FunctionRegistry);
        NYql::TMkqlIOSpecs specs;
        specs.Init(CodecCtx, spec);

        TStringStream strm;
        NYql::TMkqlWriterImpl writer(strm, 0ULL, 1ULL << 20ULL);
        writer.SetSpecs(specs);

        const auto graph = setup.BuildGraph(pgmReturn, writer);
        UNIT_ASSERT(graph->GetValue().IsFinish());
        writer.Finish();
        strm.Finish();

        const auto& output = strm.Str();
        const std::string_view expected("{\1\6key=\6\2;\1\x0Epayload=\1\0;};{\1\6key=\6\1;\1\x0Epayload=\1\6aaa;};{\1\6key=\6\3;\1\x0Epayload=\1\6qqq;};", 81);
        UNIT_ASSERT_STRINGS_EQUAL(output, expected);
    }

    Y_UNIT_TEST_LLVM(SimpleSkiff) {
        TSetup_<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto key1 = pb.NewDataLiteral<ui32>(1);
        const auto key2 = pb.NewDataLiteral<ui32>(2);
        const auto key3 = pb.NewDataLiteral<ui32>(3);

        const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("aaa");
        const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("qqq");

        const auto keyType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto payloadType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        auto structType = pb.NewEmptyStructType();
        structType = pb.NewStructType(structType, "payload", payloadType);
        structType = pb.NewStructType(structType, "key", keyType);

        std::vector<std::pair<std::string_view, TRuntimeNode>> map1 = {
            { "key", key1 },
            { "payload", payload1 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map2 = {
            { "key", key2 },
            { "payload", payload2 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map3 = {
            { "key", key3 },
            { "payload", payload3 }
        };

        const auto list = pb.NewList(structType, {
            pb.NewStruct(map2),
            pb.NewStruct(map1),
            pb.NewStruct(map3)
        });

        const auto pgmReturn = pb.Discard(MakeYtWrite(setup, pb.ToFlow(list)));

        const TString spec = R"({
            tables = [{
                "_yql_row_spec" = {
                    "Type" = [
                        "StructType"; [
                            ["key"; ["DataType"; "Uint32"]];
                            ["payload"; ["DataType"; "String"]]
                        ]
                    ]
                }
            }]
        })";

        NYql::NCommon::TCodecContext CodecCtx(*setup.Env, *setup.FunctionRegistry);
        NYql::TMkqlIOSpecs specs;
        specs.SetUseSkiff(LLVM ? "": "OFF");
        specs.Init(CodecCtx, spec);

        TStringStream strm;
        NYql::TMkqlWriterImpl writer(strm, 0ULL, 1ULL << 20ULL);
        writer.SetSpecs(specs);

        const auto graph = setup.BuildGraph(pgmReturn, writer);
        UNIT_ASSERT(graph->GetValue().IsFinish());
        writer.Finish();
        strm.Finish();

        const auto& output = strm.Str();
        const std::string_view expected("\0\0\2\0\0\0\0\0\0\0\0\0\0\0\0\0\1\0\0\0\0\0\0\0\3\0\0\0aaa\0\0\3\0\0\0\0\0\0\0\3\0\0\0qqq", 48);
        UNIT_ASSERT_STRINGS_EQUAL(output, expected);
    }

    Y_UNIT_TEST_LLVM(FlattenSkiff) {
        TSetup_<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto key1 = pb.NewDataLiteral<ui32>(1);
        const auto key2 = pb.NewDataLiteral<ui32>(2);
        const auto key3 = pb.NewDataLiteral<ui32>(3);

        const auto payload1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("aaa");
        const auto payload2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("");
        const auto payload3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("qqq");

        const auto keyType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto payloadType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        auto structType = pb.NewEmptyStructType();
        structType = pb.NewStructType(structType, "payload", payloadType);
        structType = pb.NewStructType(structType, "key", keyType);

        std::vector<std::pair<std::string_view, TRuntimeNode>> map1 = {
            { "key", key1 },
            { "payload", payload1 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map2 = {
            { "key", key2 },
            { "payload", payload2 }
        };

        std::vector<std::pair<std::string_view, TRuntimeNode>> map3 = {
            { "key", key3 },
            { "payload", payload3 }
        };

        const auto list = pb.NewList(structType, {
            pb.NewStruct(map2),
            pb.NewStruct(map1),
            pb.NewStruct(map3)
        });

        const auto flow = pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList {
                return { pb.Member(item, "key"), pb.Member(item, "payload")};
            }
        );

        const auto pgmReturn = pb.Discard(MakeYtWrite(setup, flow));

        const TString spec = R"({
            tables = [{
                "_yql_row_spec" = {
                    "Type" = [
                        "StructType"; [
                            ["key"; ["DataType"; "Uint32"]];
                            ["payload"; ["DataType"; "String"]]
                        ]
                    ]
                }
            }]
        })";

        NYql::NCommon::TCodecContext CodecCtx(*setup.Env, *setup.FunctionRegistry);
        NYql::TMkqlIOSpecs specs;
        specs.SetUseSkiff(LLVM ? "": "OFF");
        specs.Init(CodecCtx, spec);

        TStringStream strm;
        NYql::TMkqlWriterImpl writer(strm, 0ULL, 1ULL << 20ULL);
        writer.SetSpecs(specs);

        const auto graph = setup.BuildGraph(pgmReturn, writer);
        UNIT_ASSERT(graph->GetValue().IsFinish());
        writer.Finish();
        strm.Finish();

        const auto& output = strm.Str();
        const std::string_view expected("\0\0\2\0\0\0\0\0\0\0\0\0\0\0\0\0\1\0\0\0\0\0\0\0\3\0\0\0aaa\0\0\3\0\0\0\0\0\0\0\3\0\0\0qqq", 48);
        UNIT_ASSERT_STRINGS_EQUAL(output, expected);
    }
}

} // NMiniKQL
} // NKikimr
