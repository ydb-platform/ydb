#include "bridge_test_helpers.h"
using namespace NKikimr::NUdfStore::NWasm;
using namespace NKikimr::NUdfStore::NWasm::NTest;

Y_UNIT_TEST_SUITE(TWasmSharedCtxSnapshotTest) {
    Y_UNIT_TEST(TwoFiltersThenSnapshot) {
        TBridgeEnv env;
        env.AddModule(R"(
    (module
     (import "env" "memory" (memory i64 8 2097152))
     (import "env" "BridgeGetUint64" (func $get (param i64) (result i64)))
     (import "env" "BridgeMakeUint64" (func $make (param i64) (result i64)))
     (import "env" "BridgeMakeString" (func $str (param i64 i64) (result i64)))
     (global $a (mut i32) (i32.const 48))
     (global $b (mut i32) (i32.const 48))
     (data (i64.const 1024) "a=0;b=0")
     (func (export "create") (param i64) (param $r i64) (param i64)
       (i64.store (local.get $r) (call $make (i64.const 1))))
     (func (export "filter_a") (param i64) (param $r i64) (param $h i64) (param $input i64)
       (if (i64.ne (call $get (local.get $h)) (i64.const 1)) (then unreachable))
       (global.set $a (i32.add (global.get $a) (i32.const 1)))
       (i64.store (local.get $r) (local.get $input)))
     (func (export "filter_b") (param i64) (param $r i64) (param $h i64) (param $input i64)
       (if (i64.ne (call $get (local.get $h)) (i64.const 1)) (then unreachable))
       (global.set $b (i32.add (global.get $b) (i32.const 1)))
       (i64.store (local.get $r) (local.get $input)))
     (func (export "snapshot") (param i64) (param $r i64) (param $h i64)
       (drop (call $get (local.get $h)))
       (i32.store8 (i64.const 1026) (global.get $a))
       (i32.store8 (i64.const 1030) (global.get $b))
       (i64.store (local.get $r) (call $str (i64.const 1024) (i64.const 7))))
    )
    )", {"create", "filter_a", "filter_b", "snapshot"});
        auto create = env.Function("create", {env.Type("String")}, env.Type("Uint64"));
        const auto config = env.ValueBuilder.NewString(TStringRef::Of(""));
        auto object = create->Invoke(&env.ValueBuilder, &config);
        auto a = env.Function("filter_a", {env.Type("Uint64"), env.Type("Int64?")}, env.Type("Int64?"));
        auto b = env.Function("filter_b", {env.Type("Uint64"), env.Type("Int64?")}, env.Type("Int64?"));
        const TUnboxedValuePod args[] = {object, TUnboxedValuePod(i64(5))};
        UNIT_ASSERT_VALUES_EQUAL(a->Invoke(&env.ValueBuilder, args).Get<i64>(), 5);
        a->Invoke(&env.ValueBuilder, args);
        b->Invoke(&env.ValueBuilder, args);
        auto snapshot = env.Function("snapshot", {env.Type("Uint64")}, env.Type("String"));
        const auto text = snapshot->Invoke(&env.ValueBuilder, &object);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(text.AsStringRef()), "a=2;b=1");
        UNIT_ASSERT_VALUES_EQUAL(env.Query.BridgeNodes->DebugSize(), 0);
    }
}
