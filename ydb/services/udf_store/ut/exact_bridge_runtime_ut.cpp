#include "bridge_test_helpers.h"
using namespace NKikimr::NUdfStore::NWasm;
using namespace NKikimr::NUdfStore::NWasm::NTest;

Y_UNIT_TEST_SUITE(TWasmExactBridgeRuntime) {
    Y_UNIT_TEST(RequiredAndNullableResults) {
        TBridgeEnv env;
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (func (export "null") (param i64) (param $r i64)
                (i64.store (local.get $r) (i64.const 0)))))", {"null"});
        auto required = env.Function("null", {}, env.Type("Int64"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(required->Invoke(&env.ValueBuilder, nullptr), yexception, "non-optional");
        auto nullable = env.Function("null", {}, env.Type("Int64?"));
        UNIT_ASSERT(!nullable->Invoke(&env.ValueBuilder, nullptr));
        UNIT_ASSERT_VALUES_EQUAL(env.Query.BridgeNodes->DebugSize(), 0);
    }
    Y_UNIT_TEST(NestedOptionalStatesRoundtrip) {
        TBridgeEnv env;
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (func (export "echo") (param i64) (param $r i64) (param $a i64)
                (i64.store (local.get $r) (local.get $a)))))", {"echo"});
        for (const auto type : {"Int64??", "Optional<Null>"}) {
            auto fn = env.Function("echo", {env.Type(type)}, env.Type(type));
            TUnboxedValuePod arg;
            UNIT_ASSERT(!fn->Invoke(&env.ValueBuilder, &arg));
            arg = arg.MakeOptional();
            auto justNull = fn->Invoke(&env.ValueBuilder, &arg);
            UNIT_ASSERT(justNull);
            UNIT_ASSERT(!justNull.GetOptionalValue());
        }
        auto fn = env.Function("echo", {env.Type("Int64??")}, env.Type("Int64??"));
        const auto arg = TUnboxedValuePod(i64(42)).MakeOptional().MakeOptional();
        UNIT_ASSERT_VALUES_EQUAL(fn->Invoke(&env.ValueBuilder, &arg).GetOptionalValue().GetOptionalValue().Get<i64>(), 42);
    }
    Y_UNIT_TEST(DecimalAndTopLevelSignature) {
        TBridgeEnv env;
        auto builder = env.Builder();
        for (const auto type : {"Int64", "Int64?", "Decimal(22,9)", "List<Decimal(10,2)>"}) {
            const auto node = env.Type(type);
            TStringBuilder output;
            TTypePrinter printer(*env.TypeHelper, BuildTypeFromWasmTypeNode(*builder, *node));
            printer.Out(output.Out);
            UNIT_ASSERT_VALUES_EQUAL(TString(output), type);
        }
    }
    Y_UNIT_TEST(OptionalGuestTraversalPreservesEmptyLayers) {
        TBridgeEnv env;
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (import "env" "BridgeGetOptional" (func $get (param i64) (result i64)))
            (func (export "unwrap") (param i64) (param $r i64) (param $a i64)
                (i64.store (local.get $r) (call $get (local.get $a))))))", {"unwrap"});
        auto fn = env.Function("unwrap", {env.Type("Int64???")}, env.Type("Int64??"));
        const auto input = TUnboxedValuePod().MakeOptional().MakeOptional();
        const auto output = fn->Invoke(&env.ValueBuilder, &input);
        UNIT_ASSERT(output);
        UNIT_ASSERT(!output.GetOptionalValue());
    }

    Y_UNIT_TEST(ScalarKindsAndDecimalParametersAreExact) {
        TBridgeEnv env;
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (func (export "echo") (param i64) (param $r i64) (param $a i64)
                (i64.store (local.get $r) (local.get $a)))))", {"echo"});
        const TUnboxedValuePod number(ui64(42));
        auto date = env.Function("echo", {env.Type("Uint64")}, env.Type("Date"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(date->Invoke(&env.ValueBuilder, &number), yexception, "scalar type");
        const auto text = env.ValueBuilder.NewString(TStringRef::Of("utf8"));
        auto utf8 = env.Function("echo", {env.Type("String")}, env.Type("Utf8"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(utf8->Invoke(&env.ValueBuilder, &text), yexception, "scalar type");
        const auto decimal = TUnboxedValuePod(NYql::NDecimal::TInt128(123));
        auto same = env.Function("echo", {env.Type("Decimal(22,9)")}, env.Type("Decimal(22,9)"));
        UNIT_ASSERT(same->Invoke(&env.ValueBuilder, &decimal).GetInt128() == 123);
        auto different = env.Function("echo", {env.Type("Decimal(22,9)")}, env.Type("Decimal(22,8)"));
        UNIT_ASSERT_EXCEPTION_CONTAINS(different->Invoke(&env.ValueBuilder, &decimal), yexception, "precision/scale");
    }

    Y_UNIT_TEST(GuestBuildsAndTraversesNestedOptional) {
        TBridgeEnv env;
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (import "env" "BridgeMakeNull" (func $null (result i64)))
            (import "env" "BridgeMakeOptional" (func $just (param i64) (result i64)))
            (import "env" "BridgeGetOptional" (func $get (param i64) (result i64)))
            (func (export "nested") (param i64) (param $r i64)
                (i64.store (local.get $r) (call $just (call $just (call $null)))))
            (func (export "inner") (param i64) (param $r i64)
                (i64.store (local.get $r) (call $get (call $just (call $just (call $null))))))))",
            {"nested", "inner"});
        auto fn = env.Function("nested", {}, env.Type("Int64???"));
        auto result = fn->Invoke(&env.ValueBuilder, nullptr);
        UNIT_ASSERT(result);
        result = result.GetOptionalValue();
        UNIT_ASSERT(result);
        UNIT_ASSERT(!result.GetOptionalValue());
        auto inner = env.Function("inner", {}, env.Type("Int64??"));
        result = inner->Invoke(&env.ValueBuilder, nullptr);
        UNIT_ASSERT(result);
        UNIT_ASSERT(!result.GetOptionalValue());
        UNIT_ASSERT_VALUES_EQUAL(env.Query.BridgeNodes->DebugSize(), 0);
    }

    Y_UNIT_TEST(RejectMissingPayloadBelowLastOptionalLayer) {
        TBridgeEnv env;
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (func (export "echo") (param i64) (param $r i64) (param $a i64)
                (i64.store (local.get $r) (local.get $a)))))", {"echo"});
        auto fn = env.Function("echo", {env.Type("Int64??")}, env.Type("Int64?"));
        const auto input = TUnboxedValuePod().MakeOptional();
        UNIT_ASSERT_EXCEPTION_CONTAINS(fn->Invoke(&env.ValueBuilder, &input), yexception, "non-optional payload");
    }

    Y_UNIT_TEST(NestedOptionalContainerItems) {
        TBridgeEnv env;
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (import "env" "BridgeMakeNull" (func $null (result i64)))
            (import "env" "BridgeMakeOptional" (func $just (param i64) (result i64)))
            (import "env" "BridgeMakeList" (func $list (param i64 i32) (result i64)))
            (func (export "nested") (param i64) (param $r i64)
                (i64.store (i64.const 2048) (call $just (call $just (call $null))))
                (i64.store (local.get $r) (call $list (i64.const 2048) (i32.const 1))))
            (func (export "null_item") (param i64) (param $r i64)
                (i64.store (i64.const 2048) (call $null))
                (i64.store (local.get $r) (call $list (i64.const 2048) (i32.const 1))))))",
            {"nested", "null_item"});
        auto fn = env.Function("nested", {}, env.Type("List<Optional<Optional<Optional<Int64>>>>"));
        const auto result = fn->Invoke(&env.ValueBuilder, nullptr);
        auto iter = result.GetListIterator();
        TUnboxedValue item;
        UNIT_ASSERT(iter.Next(item));
        UNIT_ASSERT(item);
        item = item.GetOptionalValue();
        UNIT_ASSERT(item);
        UNIT_ASSERT(!item.GetOptionalValue());
        auto invalid = env.Function("null_item", {}, env.Type("List<Int64>"));
        UNIT_ASSERT_EXCEPTION(invalid->Invoke(&env.ValueBuilder, nullptr), yexception);
    }

}
