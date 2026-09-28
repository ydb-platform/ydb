#include "bridge_test_helpers.h"
#include <yql/essentials/minikql/mkql_terminator.h>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NKikimr::NUdfStore::NWasm::NTest;

namespace {
constexpr TStringBuf Objects = R"(
(module
 (import "env" "memory" (memory i64 8 2097152))
 (import "env" "BridgeMakeUint64" (func $make (param i64) (result i64)))
 (import "env" "BridgeGetUint64" (func $get (param i64) (result i64)))
 (import "env" "BridgeGetStringLen" (func $len (param i64) (result i64)))
 (global $created (mut i64) (i64.const 0))
 (global $destroyed (mut i64) (i64.const 0))
 (func (export "create") (param $ctx i64) (param $res i64) (param $config i64)
   (global.set $created (i64.add (global.get $created) (i64.const 1)))
   (i64.store (local.get $res) (call $make (call $len (local.get $config)))))
 (func (export "call") (param $ctx i64) (param $res i64) (param $object i64) (param $arg i64)
   (if (i64.ne (call $get (local.get $object)) (i64.const 3)) (then unreachable))
   (i64.store (local.get $res) (local.get $arg)))
 (func (export "fail") (param i64 i64 i64 i64) unreachable)
 (func (export "destroy") (param $ctx i64) (param $res i64) (param $object i64)
   (drop (call $get (local.get $object)))
   (global.set $destroyed (i64.add (global.get $destroyed) (i64.const 1)))
   (i64.store (local.get $res) (i64.const 0)))
 (func (export "created") (param $ctx i64) (param $res i64)
   (i64.store (local.get $res) (call $make (global.get $created))))
 (func (export "destroyed") (param $ctx i64) (param $res i64)
   (i64.store (local.get $res) (call $make (global.get $destroyed))))
)
)";
}

Y_UNIT_TEST_SUITE(TWasmUdfObjectsAbiTest) {
    Y_UNIT_TEST(ConfiguredContainerCallAndLifecycle) {
        TBridgeEnv env;
        env.AddModule(Objects, {"create", "call", "destroy", "created", "destroyed"});
        const auto parsed = ParseManifest(R"({"module_name":"Test","module_type":"module","module_kind":"wasm",
          "objects":[{"name":"X","create_export":"create","destroy_export":"destroy","methods":[
            {"name":"Apply","export":"call","argument_types":["List<Int64>"],"result_type":"List<Int64>"}]}]})");
        auto created = env.Function("created", {}, env.Type("Uint64"));
        auto destroyed = env.Function("destroyed", {}, env.Type("Uint64"));
        auto builder = env.Builder();
        TWasmConfiguredCallable::Register(*builder, false, env.State, parsed.Functions[1], "cfg");
        TFunctionTypeInfo info;
        builder->Build(&info);
        UNIT_ASSERT(info.Implementation);
        TUnboxedValue function(TUnboxedValuePod(info.Implementation.Release()));
        TUnboxedValue values[] = {TUnboxedValuePod(i64(7)), TUnboxedValuePod(i64(9))};
        const auto input = env.ValueBuilder.NewList(values, 2);
        for (size_t i = 0; i < 3; ++i) {
            const auto output = function.Run(&env.ValueBuilder, &input);
            UNIT_ASSERT_VALUES_EQUAL(output.GetListLength(), 2);
            auto it = output.GetListIterator();
            TUnboxedValue item;
            UNIT_ASSERT(it.Next(item));
            UNIT_ASSERT_VALUES_EQUAL(item.Get<i64>(), 7);
        }
        UNIT_ASSERT_VALUES_EQUAL(created->Invoke(&env.ValueBuilder, nullptr).Get<ui64>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Invoke(&env.ValueBuilder, nullptr).Get<ui64>(), 0);
        function.Clear();
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Invoke(&env.ValueBuilder, nullptr).Get<ui64>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Query.BridgeNodes->DebugSize(), 0);
    }

    Y_UNIT_TEST(GenerationChangeRecreatesObject) {
        TBridgeEnv env;
        env.AddModule(Objects, {"create", "call", "destroy", "created", "destroyed"});
        TWasmUdfDescriptor desc;
        desc.Name = "Apply";
        desc.CreateExport = "create";
        desc.CallExport = "call";
        desc.DestroyExport = "destroy";
        desc.ArgTypes = {env.Type("Int64?")};
        desc.ResultType = env.Type("Int64?");
        auto builder = env.Builder();
        TWasmConfiguredCallable::Register(*builder, false, env.State, desc, "cfg");
        TFunctionTypeInfo info;
        builder->Build(&info);
        TUnboxedValue function(TUnboxedValuePod(info.Implementation.Release()));
        const TUnboxedValuePod null;
        UNIT_ASSERT(!function.Run(&env.ValueBuilder, &null));
        // A new generation never destroys an old guest handle in the new image.
        ++env.Query.Generation;
        UNIT_ASSERT(!function.Run(&env.ValueBuilder, &null));
        auto created = env.Function("created", {}, env.Type("Uint64"));
        auto destroyed = env.Function("destroyed", {}, env.Type("Uint64"));
        UNIT_ASSERT_VALUES_EQUAL(created->Invoke(&env.ValueBuilder, nullptr).Get<ui64>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Invoke(&env.ValueBuilder, nullptr).Get<ui64>(), 0);
    }
    Y_UNIT_TEST(FailureDestroysObjectAndReleasesBridgeHandles) {
        TBridgeEnv env;
        TOnlyThrowingBindTerminator terminator;
        env.AddModule(Objects, {"create", "fail", "destroy", "created", "destroyed"});
        TWasmUdfDescriptor desc;
        desc.Name = "Fail";
        desc.CreateExport = "create";
        desc.CallExport = "fail";
        desc.DestroyExport = "destroy";
        desc.ArgTypes = {env.Type("Int64")};
        desc.ResultType = env.Type("Int64");
        auto builder = env.Builder();
        TWasmConfiguredCallable::Register(*builder, false, env.State, desc, "cfg");
        TFunctionTypeInfo info;
        builder->Build(&info);
        TUnboxedValue function(TUnboxedValuePod(info.Implementation.Release()));
        const TUnboxedValuePod input(i64(1));
        UNIT_ASSERT_EXCEPTION(function.Run(&env.ValueBuilder, &input), TTerminateException);
        auto destroyed = env.Function("destroyed", {}, env.Type("Uint64"));
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Invoke(&env.ValueBuilder, nullptr).Get<ui64>(), 1);
        function.Clear();
        UNIT_ASSERT_VALUES_EQUAL(destroyed->Invoke(&env.ValueBuilder, nullptr).Get<ui64>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Query.BridgeNodes->DebugSize(), 0);
    }

}
