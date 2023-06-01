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

    Y_UNIT_TEST(TestUdf) {
        TestOne([](auto& b,auto& ctx) {
            auto blockOptStringType = ctx.template MakeType<TBlockExprType>(
                ctx.template MakeType<TOptionalExprType>(
                ctx.template MakeType<TDataExprType>(EDataSlot::String)));
            return b.Udf("Url.GetHost", false, { blockOptStringType }, blockOptStringType) ;
        });
    }

}
