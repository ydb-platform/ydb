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

    Y_UNIT_TEST(TestMod) {
        TestOne([](auto& b,auto& ctx) {
            auto blockInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32));
            auto blockOptInt32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Int32)));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Mod, blockInt32Type, blockInt32Type, blockOptInt32Type);
        });
    }

    Y_UNIT_TEST(TestAddSubMulOps) {
        for (const auto oper : {TKernelRequestBuilder::EBinaryOp::Add, TKernelRequestBuilder::EBinaryOp::Sub, TKernelRequestBuilder::EBinaryOp::Mul}) {
            for (const auto slot : {EDataSlot::Int8, EDataSlot::Int16, EDataSlot::Int32, EDataSlot::Int64, EDataSlot::Uint8, EDataSlot::Uint16, EDataSlot::Uint32, EDataSlot::Uint64, EDataSlot::Float, EDataSlot::Double}) {
                TestOne([slot, oper](auto& b,auto& ctx) {
                    const auto blockUint8Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Uint8));
                    const auto blockType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(slot));
                    return b.AddBinaryOp(oper, blockUint8Type, blockType, blockType);
                });
                TestOne([slot, oper](auto& b,auto& ctx) {
                    const auto blockUint8Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Uint8));
                    const auto blockType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(slot));
                    return b.AddBinaryOp(oper, blockType, blockUint8Type, blockType);
                });
                TestOne([slot, oper](auto& b,auto& ctx) {
                    const auto blockType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(slot));
                    return b.AddBinaryOp(oper, blockType, blockType, blockType);
                });
            }
        }
    }

    Y_UNIT_TEST(TestDivModOps) {
        for (const auto oper : {TKernelRequestBuilder::EBinaryOp::Div, TKernelRequestBuilder::EBinaryOp::Mod}) {
            for (const auto slot : {EDataSlot::Int8, EDataSlot::Int16, EDataSlot::Int32, EDataSlot::Int64, EDataSlot::Uint8, EDataSlot::Uint16, EDataSlot::Uint32, EDataSlot::Uint64, EDataSlot::Float, EDataSlot::Double}) {
                TestOne([slot, oper](auto& b,auto& ctx) {
                    const auto blockUint8Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Uint8));
                    const auto rawType = ctx.template MakeType<TDataExprType>(slot);
                    const auto blockType = ctx.template MakeType<TBlockExprType>(rawType);
                    const auto returnType = EDataSlot::Float != slot && EDataSlot::Double != slot ?
                        ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(rawType)) : blockType;
                    return b.AddBinaryOp(oper, blockUint8Type, blockType, returnType);
                });
                TestOne([slot, oper](auto& b,auto& ctx) {
                    const auto blockUint8Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Uint8));
                    const auto rawType = ctx.template MakeType<TDataExprType>(slot);
                    const auto blockType = ctx.template MakeType<TBlockExprType>(rawType);
                    const auto returnType = EDataSlot::Float != slot && EDataSlot::Double != slot ?
                        ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(rawType)) : blockType;
                    return b.AddBinaryOp(oper, blockType, blockUint8Type, returnType);
                });
                TestOne([slot, oper](auto& b,auto& ctx) {
                    const auto rawType = ctx.template MakeType<TDataExprType>(slot);
                    const auto blockType = ctx.template MakeType<TBlockExprType>(rawType);
                    const auto returnType = EDataSlot::Float != slot && EDataSlot::Double != slot ?
                        ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(rawType)) : blockType;
                    return b.AddBinaryOp(oper, blockType, blockType, returnType);
                });
            }
        }
    }

    Y_UNIT_TEST(TestSize) {
        TestOne([](auto& b,auto& ctx) {
            auto blockStrType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::String));
            auto blockUint32Type = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Uint32));
            return b.AddUnaryOp(TKernelRequestBuilder::EUnaryOp::Size, blockStrType, blockUint32Type);
        });
    }

    Y_UNIT_TEST(TestMinus) {
        for (const auto slot : {EDataSlot::Int8, EDataSlot::Int16, EDataSlot::Int32, EDataSlot::Int64, EDataSlot::Float, EDataSlot::Double}) {
            TestOne([slot](auto& b,auto& ctx) {
                const auto blockType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(slot));
                return b.AddUnaryOp(TKernelRequestBuilder::EUnaryOp::Minus, blockType, blockType);
            });
        }
    }

    Y_UNIT_TEST(TestAbs) {
        for (const auto slot : {EDataSlot::Int8, EDataSlot::Int16, EDataSlot::Int32, EDataSlot::Int64, EDataSlot::Float, EDataSlot::Double}) {
            TestOne([slot](auto& b,auto& ctx) {
                const auto blockType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(slot));
                return b.AddUnaryOp(TKernelRequestBuilder::EUnaryOp::Abs, blockType, blockType);
            });
        }
    }

    Y_UNIT_TEST(TestJust) {
        TestOne([](auto& b,auto& ctx) {
            const auto boolType = ctx.template MakeType<TDataExprType>(EDataSlot::Bool);
            const auto inputType = ctx.template MakeType<TBlockExprType>(boolType);
            const auto outputType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(boolType));
            return b.AddUnaryOp(TKernelRequestBuilder::EUnaryOp::Just, inputType, outputType);
        });
    }

    Y_UNIT_TEST(TestCoalesece) {
        TestOne([](auto& b,auto& ctx) {
            auto blockStringType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::String));
            auto blockOptStringType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TOptionalExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::String)));
            return b.AddBinaryOp(TKernelRequestBuilder::EBinaryOp::Coalesce, blockOptStringType, blockStringType, blockStringType);
        });
    }

    Y_UNIT_TEST(TestIf) {
        TestOne([](auto& b,auto& ctx) {
            auto blockStringType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::String));
            auto blockBoolType = ctx.template MakeType<TBlockExprType>(ctx.template MakeType<TDataExprType>(EDataSlot::Bool));
            return b.AddIf(blockBoolType, blockStringType, blockStringType);
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

    Y_UNIT_TEST(TestScalarApply) {
        TestOne([](auto& b,auto& ctx) {
            const auto stringType = ctx.template MakeType<TDataExprType>(EDataSlot::String);
            const auto uint32Type = ctx.template MakeType<TDataExprType>(EDataSlot::Uint32);
            const auto blockStringType = ctx.template MakeType<TBlockExprType>(stringType);
            const auto blockUint32Type = ctx.template MakeType<TBlockExprType>(uint32Type);
            const TPositionHandle stub;
            auto arg1 = ctx.NewArgument(stub, "str");
            auto arg2 = ctx.NewArgument(stub, "pos");
            auto size = ctx.NewCallable(stub, "Uint32", {ctx.NewAtom(stub, 42U)});
            auto body = ctx.NewCallable(stub, "Substring", {arg1, arg2, size});
            arg1->SetTypeAnn(stringType);
            arg2->SetTypeAnn(uint32Type);
            size->SetTypeAnn(uint32Type);
            body->SetTypeAnn(stringType);
            const auto lambda = ctx.NewLambda(stub, ctx.NewArguments(stub, {std::move(arg1), std::move(arg2)}), std::move(body));
            return b.AddScalarApply(*lambda, { blockStringType, blockUint32Type }, ctx);
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
