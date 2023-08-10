#include <ydb/library/yql/core/arrow_kernels/request/request.h>
#include <ydb/library/yql/core/arrow_kernels/registry/registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
using namespace NKikimr::NMiniKQL;

template <typename F>
void TestOne(F&& f) {
    TExprContext ctx;
    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
    FillStaticModules(*functionRegistry);
    auto nodeFactory = GetBuiltinFactory();
    TKernelRequestBuilder b(*functionRegistry);
    auto index = f(b, ctx);
    UNIT_ASSERT_VALUES_EQUAL(index, 0);
    auto s = b.Serialize();
    auto v = LoadKernels(s, *functionRegistry, nodeFactory);
    UNIT_ASSERT_VALUES_EQUAL(v.size(), 1);
}

Y_UNIT_TEST_SUITE(TKernelRegistryTest) {
    Y_UNIT_TEST(TestZeroKernels) {
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        auto nodeFactory = GetBuiltinFactory();
        TKernelRequestBuilder b(*functionRegistry);
        auto s = b.Serialize();
        auto v = LoadKernels(s, *functionRegistry, nodeFactory);
        UNIT_ASSERT_VALUES_EQUAL(v.size(), 0);
    }

    Y_UNIT_TEST(TestTwoKernels) {
        TExprContext ctx;
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        auto nodeFactory = GetBuiltinFactory();
        TKernelRequestBuilder b(*functionRegistry);
        auto blockInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32));
        auto index1 = b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Add, blockInt32Type, blockInt32Type, blockInt32Type);
        UNIT_ASSERT_VALUES_EQUAL(index1, 0);
        auto index2 = b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Sub, blockInt32Type, blockInt32Type, blockInt32Type);
        UNIT_ASSERT_VALUES_EQUAL(index2, 1);
        auto s = b.Serialize();
        auto v = LoadKernels(s, *functionRegistry, nodeFactory);
        UNIT_ASSERT_VALUES_EQUAL(v.size(), 2);
    }

    Y_UNIT_TEST(TestNot) {
        TestOne([](auto& b,auto& ctx) {
            auto blockBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool));
            return b.AddUnaryOp(TKernelRequestBuilder::EUnaryOp::Not, blockBoolType, blockBoolType);
        });
    }

    Y_UNIT_TEST(TestAnd) {
        TestOne([](auto& b,auto& ctx) {
            auto blockBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::And, blockBoolType, blockBoolType, blockBoolType);
        });
    }

    Y_UNIT_TEST(TestOr) {
        TestOne([](auto& b,auto& ctx) {
            auto blockBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Or, blockBoolType, blockBoolType, blockBoolType);
        });
    }

    Y_UNIT_TEST(TestXor) {
        TestOne([](auto& b,auto& ctx) {
            auto blockBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Xor, blockBoolType, blockBoolType, blockBoolType);
        });
    }    

    Y_UNIT_TEST(TestAdd) {
        TestOne([](auto& b,auto& ctx) {
            auto blockInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Add, blockInt32Type, blockInt32Type, blockInt32Type);
        });
    }

    Y_UNIT_TEST(TestSub) {
        TestOne([](auto& b,auto& ctx) {
            auto blockInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Sub, blockInt32Type, blockInt32Type, blockInt32Type);
        });
    }

    Y_UNIT_TEST(TestMul) {
        TestOne([](auto& b,auto& ctx) {
            auto blockInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Mul, blockInt32Type, blockInt32Type, blockInt32Type);
        });
    }

    Y_UNIT_TEST(TestDiv) {
        TestOne([](auto& b,auto& ctx) {
            auto blockInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32));
            auto blockOptInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32)));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Div, blockInt32Type, blockInt32Type, blockOptInt32Type);
        });
    }

    Y_UNIT_TEST(TestStartsWith) {
        TestOne([](auto& b,auto& ctx) {
            auto blockStringType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::String));
            auto blockOptBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool)));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::StartsWith, blockStringType, blockStringType, blockOptBoolType);
        });
    }

    Y_UNIT_TEST(TestEndsWith) {
        TestOne([](auto& b,auto& ctx) {
            auto blockStringType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::String));
            auto blockOptBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool)));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::EndsWith, blockStringType, blockStringType, blockOptBoolType);
        });
    }

    Y_UNIT_TEST(TestStringContains) {
        TestOne([](auto& b,auto& ctx) {
            auto blockStringType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::String));
            auto blockOptBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool)));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::StringContains, blockStringType, blockStringType, blockOptBoolType);
        });
    }

    Y_UNIT_TEST(TestUdf) {
        TestOne([](auto& b,auto& ctx) {
            auto blockOptStringType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::String)));
            return b.Udf("Url.GetHost", false, { blockOptStringType }, blockOptStringType) ;
        });
    }

    Y_UNIT_TEST(TestJsonExists) {
        TestOne([](auto& b,auto& ctx) {
            auto blockOptJsonType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Json)));
            auto scalarUtf8Type = ctx.template MakeType<TScalarExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Utf8));
            auto blockOptBoolType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Bool)));
            return b.JsonExists(blockOptJsonType, scalarUtf8Type, blockOptBoolType);
        });

        TestOne([](auto& b,auto& ctx) {
            auto blockJsonType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Json));
            auto scalarUtf8Type = ctx.template MakeType<TScalarExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Utf8));
            auto blockOptBoolType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Bool)));
            return b.JsonExists(blockJsonType, scalarUtf8Type, blockOptBoolType);
        });

        TestOne([](auto& b,auto& ctx) {
            auto blockOptJsonType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::JsonDocument)));
            auto scalarUtf8Type = ctx.template MakeType<TScalarExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Utf8));
            auto blockOptBoolType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Bool)));
            return b.JsonExists(blockOptJsonType, scalarUtf8Type, blockOptBoolType);
        });

        TestOne([](auto& b,auto& ctx) {
            auto blockJsonType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::JsonDocument));
            auto scalarUtf8Type = ctx.template MakeType<TScalarExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Utf8));
            auto blockOptBoolType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Bool)));
            return b.JsonExists(blockJsonType, scalarUtf8Type, blockOptBoolType);
        });
    }

    void TesJsonValueImpl(EDataSlot jsonType, NYql::EDataSlot resultType) {
        TestOne([&](auto& b,auto& ctx) {
            auto blockJsonType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TDataExprType>(jsonType));
            auto scalarUtf8Type = ctx.template MakeType<TScalarExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::Utf8));
            auto blockOptType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(resultType)));
            return b.JsonValue(blockJsonType, scalarUtf8Type, blockOptType);
        });
    }

    Y_UNIT_TEST(TestJsonValueUtf8) {
        TesJsonValueImpl(EDataSlot::Json, NYql::EDataSlot::Utf8);
        TesJsonValueImpl(EDataSlot::JsonDocument, NYql::EDataSlot::Utf8);
    }

    Y_UNIT_TEST(TestJsonValueBool) {
        TesJsonValueImpl(EDataSlot::Json, NYql::EDataSlot::Bool);
        TesJsonValueImpl(EDataSlot::JsonDocument, NYql::EDataSlot::Bool);
    }

    Y_UNIT_TEST(TestJsonValueInt64) {
        TesJsonValueImpl(EDataSlot::Json, NYql::EDataSlot::Int64);
        TesJsonValueImpl(EDataSlot::JsonDocument, NYql::EDataSlot::Int64);
    }

    Y_UNIT_TEST(TestJsonValueUint64) {
        TesJsonValueImpl(EDataSlot::Json, NYql::EDataSlot::Uint64);
        TesJsonValueImpl(EDataSlot::JsonDocument, NYql::EDataSlot::Uint64);
    }

    Y_UNIT_TEST(TestJsonValueFloat) {;
        TesJsonValueImpl(EDataSlot::Json, NYql::EDataSlot::Float);
        TesJsonValueImpl(EDataSlot::JsonDocument, NYql::EDataSlot::Float);
    }

    Y_UNIT_TEST(TestJsonValueDouble) {;
        TesJsonValueImpl(EDataSlot::Json, NYql::EDataSlot::Double);
        TesJsonValueImpl(EDataSlot::JsonDocument, NYql::EDataSlot::Double);
    }
}
