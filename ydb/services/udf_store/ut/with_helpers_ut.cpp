#include "bridge_test_helpers.h"
using namespace NKikimr::NUdfStore::NWasm;
using namespace NKikimr::NUdfStore::NWasm::NTest;

Y_UNIT_TEST_SUITE(TWasmUdfWithHelpersTest) {
    Y_UNIT_TEST(SdkThenHelpersThenModule) {
        TBridgeEnv env;
        const TStringBuf helpers = R"((module
            (func (export "helpers_scale") (param $v i64) (result i64)
                (i64.mul (local.get $v) (i64.const 3)))))";
        const auto compiled = CompileModuleObjectCode(helpers, EBytecodeFormat::HumanReadable);
        AddPrecompiledModule(env.Query.Compartment.get(), MakeModuleBytecode(helpers, compiled, EBytecodeFormat::HumanReadable), "helpers");
        env.AddModule(R"((module
            (import "env" "memory" (memory i64 8 2097152))
            (import "env" "BridgeGetInt64" (func $get (param i64) (result i64)))
            (import "env" "BridgeMakeInt64" (func $make (param i64) (result i64)))
            (import "helpers" "helpers_scale" (func $scale (param i64) (result i64)))
            (func (export "scale") (param i64) (param $r i64) (param $a i64)
                (i64.store (local.get $r) (call $make (call $scale (call $get (local.get $a))))))))", {"scale"});
        auto fn = env.Function("scale", {env.Type("Int64")}, env.Type("Int64"));
        const TUnboxedValuePod arg(i64(7));
        UNIT_ASSERT_VALUES_EQUAL(fn->Invoke(&env.ValueBuilder, &arg).Get<i64>(), 21);
    }
}
