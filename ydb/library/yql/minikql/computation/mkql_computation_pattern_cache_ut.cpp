#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql::NUdf;


Y_UNIT_TEST_SUITE(ComputationPatternCache) {
    Y_UNIT_TEST(Smoke) {
        const ui32 cacheSize = 10;
        TComputationPatternLRUCache cache(cacheSize);

        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

        for (ui32 i = 0; i < cacheSize; ++i) {
            std::shared_ptr<TPatternWithEnv> patternEnv = cache.CreateEnv();
            TScopedAlloc& alloc = patternEnv->Alloc;
            TTypeEnvironment& typeEnv = patternEnv->Env;

            TProgramBuilder pb(typeEnv, *functionRegistry);

            auto progReturn  = pb.NewDataLiteral<NYql::NUdf::EDataSlot::String>("qwerty");

            TExploringNodeVisitor explorer;
            explorer.Walk(progReturn.GetNode(), typeEnv);

            TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetBuiltinFactory(),
                functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                "OFF", EGraphPerProcess::Multi);

            {
                auto guard = patternEnv->Env.BindAllocator();
                patternEnv->Pattern = MakeComputationPattern(explorer, progReturn, {}, opts);
            }
            cache.EmplacePattern(TString((char)('a' + i)), patternEnv);
        }

        for (ui32 i = 0; i < cacheSize; ++i) {
            auto key = TString((char)('a' + i));

            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(10000000);
            TScopedAlloc graphAlloc;
            auto patternEnv = cache.Find(key);
            TComputationPatternOpts opts(patternEnv->Alloc.Ref(), patternEnv->Env, GetBuiltinFactory(),
                functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                "OFF", EGraphPerProcess::Multi);

            auto graph = patternEnv->Pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider, &graphAlloc.Ref()));
            auto value = graph->GetValue();
            UNIT_ASSERT_EQUAL(value.AsStringRef(), NYql::NUdf::TStringRef("qwerty"));
        }
    }

    Y_UNIT_TEST(AddPerf) {
        TTimer t("all: ");
        TScopedAlloc alloc;
        TTypeEnvironment typeEnv(alloc);

        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

        TProgramBuilder pb(typeEnv, *functionRegistry);
        auto prog1 = pb.NewDataLiteral<ui64>(123591592ULL);
        auto prog2 = pb.NewDataLiteral<ui64>(323591592ULL);
        auto progReturn = pb.Add(prog1, prog2);

        TExploringNodeVisitor explorer;
        explorer.Walk(progReturn.GetNode(), typeEnv);

        NUdf::EValidateMode validateMode = NUdf::EValidateMode::Lazy;
        TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetBuiltinFactory(),
            functionRegistry.Get(), validateMode, NUdf::EValidatePolicy::Exception,
            "OFF", EGraphPerProcess::Multi);

        auto t_make_pattern = std::make_unique<TTimer>("make_pattern: ");
        auto pattern = MakeComputationPattern(explorer, progReturn, {}, opts);
        t_make_pattern.reset();
        auto randomProvider = CreateDeterministicRandomProvider(1);
        auto timeProvider = CreateDeterministicTimeProvider(10000000);

        auto t_clone = std::make_unique<TTimer>("clone: ");
        auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
        t_clone.reset();

        {
            TTimer t("graph: ");
            ui64 acc = 0;
            for (ui32 i = 0; i < 100'000'000; ++i) {
                acc += graph->GetValue().Get<ui64>();
            }
            Y_DO_NOT_OPTIMIZE_AWAY(acc);
        }
        {
            std::function<ui64(ui64, ui64)> add = [](ui64 a, ui64 b) {
                return a + b;
            };
            Y_DO_NOT_OPTIMIZE_AWAY(add);

            TTimer t("lambda: ");
            ui64 acc = 0;
            for (ui32 i = 0; i < 100'000'000; ++i) {
                acc += add(123591592ULL, 323591592ULL);
            }
            Y_DO_NOT_OPTIMIZE_AWAY(acc);
        }
        {
            std::function<TUnboxedValue(TUnboxedValue&, TUnboxedValue&)> add =
                [](TUnboxedValue& a, TUnboxedValue& b) {
                    return TUnboxedValuePod(a.Get<ui64>() + b.Get<ui64>());
            };
            Y_DO_NOT_OPTIMIZE_AWAY(add);

            TTimer t("lambda unboxed value: ");
            TUnboxedValue acc(TUnboxedValuePod(0));
            TUnboxedValue v1(TUnboxedValuePod(ui64{123591592UL}));
            TUnboxedValue v2(TUnboxedValuePod(ui64{323591592UL}));
            for (ui32 i = 0; i < 100'000'000; ++i) {
                auto r = add(v1, v2);
                acc = add(r, acc);
            }
            Y_DO_NOT_OPTIMIZE_AWAY(acc.Get<ui64>());
        }
    }

TRuntimeNode CreateFilter(TProgramBuilder& pb, size_t vecSize) {
    TTimer t(__func__);
    std::vector<const TRuntimeNode> arr;
    arr.reserve(vecSize);
    for (ui64 i = 0; i < vecSize; ++i) {
        arr.push_back(pb.NewDataLiteral<ui64>((i + 124515) % 6740234));
    }
    TArrayRef<const TRuntimeNode> arrRef(std::move(arr));
    auto arrayNode = pb.AsList(arrRef);
    auto handler = [&](TRuntimeNode node) -> TRuntimeNode {
        return pb.AggrEquals(
                    pb.Mod(node, pb.NewOptional(pb.NewDataLiteral<ui64>(128))),
                pb.NewOptional(pb.NewDataLiteral<ui64>(0)));
    };
    return pb.Filter(arrayNode, handler);
}

    Y_UNIT_TEST(FilterPerf) {
        TTimer t("all: ");
        TScopedAlloc alloc;
        TTypeEnvironment typeEnv(alloc);

        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

        TProgramBuilder pb(typeEnv, *functionRegistry);
        const ui64 vecSize = 1'000'000;
        auto progReturn = CreateFilter(pb, vecSize);

        TExploringNodeVisitor explorer;
        explorer.Walk(progReturn.GetNode(), typeEnv);

        NUdf::EValidateMode validateMode = NUdf::EValidateMode::Lazy;
        TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetBuiltinFactory(),
            functionRegistry.Get(), validateMode, NUdf::EValidatePolicy::Exception,
            "OFF", EGraphPerProcess::Multi);

        auto t_make_pattern = std::make_unique<TTimer>("make_pattern: ");
        auto pattern = MakeComputationPattern(explorer, progReturn, {}, opts);
        t_make_pattern.reset();
        auto randomProvider = CreateDeterministicRandomProvider(1);
        auto timeProvider = CreateDeterministicTimeProvider(10000000);

        auto t_clone = std::make_unique<TTimer>("clone: ");
        auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
        t_clone.reset();

        {
            TTimer t("graph: ");
            TUnboxedValue acc;
            for (ui64 i = 0; i < 20; ++i) {
                TUnboxedValue v = graph->GetValue();
                if (i == 0) {
                    Cerr << "len: " << v.GetListLength() << Endl;
                }
            }
        }
        {
            auto t_prepare = std::make_unique<TTimer>("prepare lambda: ");
            std::vector<ui64> data;
            data.reserve(vecSize);
            for (ui64 i = 0; i < vecSize; ++i) {
                data.push_back((i + 124515) % 6740234);
            }
            std::function<bool(ui64)> predicate = [](ui64 a) {
                return a % 128 == 0;
            };
            Y_DO_NOT_OPTIMIZE_AWAY(predicate);
            t_prepare.reset();

            TTimer t("lambda: ");
            for (ui64 i = 0; i < 20; ++i) {
                std::vector<ui64> acc;
                for (ui64 j = 0; j < data.size(); ++j) {
                    if (predicate(data[j])) {
                        acc.push_back(data[j]);
                    }
                }

                Y_DO_NOT_OPTIMIZE_AWAY(acc);
                if (i == 0) {
                    Cerr << "len: " << acc.size() << Endl;
                }
            }
        }
    }
}

}
}
