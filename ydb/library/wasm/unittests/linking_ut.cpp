#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NYdb::NWasm {
namespace {

struct TLinkingTestTag {};

void Load(IWebAssemblyCompartment* compartment, const TString& source, bool precompiled)
{
    if (!precompiled) {
        compartment->AddModule(source);
        return;
    }
    WAVM::IR::Module module;
    module.featureSpec.memory64 = true;
    module.featureSpec.table64 = true;
    std::vector<WAVM::WAST::Error> errors;
    ASSERT_TRUE(WAVM::WAST::parseModule(source.c_str(), source.size() + 1, module, errors));
    const auto binary = WAVM::WASM::saveBinaryModule(module);
    const auto compiled = WAVM::Runtime::compileModule(module);
    const auto object = WAVM::Runtime::getObjectCode(compiled);
    compartment->AddPrecompiledModule({
        EBytecodeFormat::Binary, TSharedRef::MakeCopy<TLinkingTestTag>(TRef(binary.data(), binary.size())),
        TSharedRef::MakeCopy<TLinkingTestTag>(TRef(object.data(), object.size())),
    });
}

ui64 Call(IWebAssemblyCompartment* compartment, const TString& name)
{
    auto* previous = GetCurrentCompartment();
    SetCurrentCompartment(compartment);
    Y_DEFER {
        SetCurrentCompartment(previous);
    };
    try {
        return TCompartmentFunction<ui64()>(compartment, name)();
    } catch (WAVM::Runtime::Exception* ex) {
        const auto description = WAVM::Runtime::describeException(ex);
        WAVM::Runtime::destroyException(ex);
        ythrow yexception() << description;
    }
}

void CheckInitializers(const TString& extra)
{
    for (bool precompiled : { false, true }) {
        auto compartment = CreateEmptyImage();
        Load(compartment.get(), TStringBuilder() << R"((module
            (global $state (mut i64) (i64.const 0))
            (func $global_relocs (global.set $state (i64.add (global.get $state) (i64.const 1))))
            (func (export "__wasm_apply_data_relocs")
                (global.set $state (i64.add (i64.mul (global.get $state) (i64.const 10)) (i64.const 2))))
            (func (export "__wasm_call_ctors")
                (global.set $state (i64.add (i64.mul (global.get $state) (i64.const 10)) (i64.const 3))))
            (func (export "state") (result i64) (global.get $state))
        )" << extra << ")", precompiled);
        EXPECT_EQ(Call(compartment.get(), "state"), 123u);
    }
}

TEST(TWasmLinking, UnexportedStartRunsBeforeDataRelocationsAndConstructors)
{
    CheckInitializers("(start $global_relocs)");
}

TEST(TWasmLinking, ExportedStartIsNotCalledTwice)
{
    CheckInitializers(R"((start $global_relocs)
        (export "__wasm_apply_global_relocs" (func $global_relocs)))");
}

TEST(TWasmLinking, LegacyExportedGlobalRelocations)
{
    CheckInitializers(R"((export "__wasm_apply_global_relocs" (func $global_relocs)))");
}

TEST(TWasmLinking, StartTrapIsReported)
{
    for (bool precompiled : { false, true }) {
        auto compartment = CreateEmptyImage();
        EXPECT_THROW(Load(compartment.get(), "(module (func $start unreachable) (start $start))", precompiled), std::exception);
    }
}

const TString Consumer = R"((module
    (type $fill (func (param i64 i32 i64) (result i64)))
    (import "env" "__indirect_function_table" (table i64 1 funcref))
    (import "env" "memory" (memory i64 1))
    (import "GOT.func" "memset" (global $fill (mut i64)))
    (func (export "fill") (result i64)
        (drop (call_indirect (type $fill) (i64.const 1024) (i32.const 42) (i64.const 8) (global.get $fill)))
        (i64.load (i64.const 1024)))
    (func (export "address") (result i64) (global.get $fill))
))";

const TString ProviderPrefix = R"((module
    (import "env" "__indirect_function_table" (table i64 1 funcref))
    (import "env" "memory" (memory i64 1))
    (func $unrelated_debug_name (export "memset") (export "_Z6FillerPvim")
        (param i64 i32 i64) (result i64)
        (memory.fill (local.get 0) (local.get 1) (local.get 2))
        (local.get 0))
)";

TEST(TWasmLinking, GotUsesExportNamesAndActualSparseSegmentOffsets)
{
    for (bool precompiled : { false, true }) {
        auto compartment = CreateEmptyImage();
        Load(compartment.get(), ProviderPrefix + R"(
            (func $other)
            (elem (i64.const 7) func $other)
            (elem (i64.const 3072) func $unrelated_debug_name)
        ))", precompiled);
        Load(compartment.get(), Consumer, precompiled);
        EXPECT_EQ(Call(compartment.get(), "address"), 3072u);
        EXPECT_EQ(Call(compartment.get(), "fill"), 0x2a2a2a2a2a2a2a2aull);
        Load(compartment.get(), R"((module
            (import "GOT.func" "_Z6FillerPvim" (global $alias (mut i64)))
            (func (export "alias_address") (result i64) (global.get $alias))))", precompiled);
        EXPECT_EQ(Call(compartment.get(), "alias_address"), 3072u);
    }
}

TString OccupyTable(const TString& imports, const TString& body)
{
    TStringBuilder source;
    source << R"((module
        (import "env" "__indirect_function_table" (table i64 1 funcref))
        (import "env" "__table_base" (global $base i64)))"
           << imports << R"(
        (func $unrelated)
        (elem (global.get $base) func)";
    for (unsigned i = 0; i < 2050; ++i) {
        source << " $unrelated";
    }
    source << ")" << body << ")";
    return source;
}

TEST(TWasmLinking, GotAllocatedSlotsSurviveCurrentAndLaterModules)
{
    for (bool precompiled : { false, true }) {
        auto compartment = CreateEmptyImage();
        // An exported function without an elem slot must get a new GOT entry.
        Load(compartment.get(), ProviderPrefix + ")", precompiled);
        Load(compartment.get(), OccupyTable(R"(
            (import "GOT.func" "memset" (global $fill (mut i64))))", R"(
            (type $fill_type (func (param i64 i32 i64) (result i64)))
            (func (export "invoke") (result i64)
                (call_indirect (type $fill_type) (i64.const 1024) (i32.const 42) (i64.const 8) (global.get $fill))))"), precompiled);
        EXPECT_EQ(Call(compartment.get(), "invoke"), 1024u);
        Load(compartment.get(), OccupyTable("", ""), precompiled);
        EXPECT_EQ(Call(compartment.get(), "invoke"), 1024u);
    }
}

TEST(TWasmLinking, RejectsOverflowingActiveSegment)
{
    auto compartment = CreateEmptyImage();
    EXPECT_THROW(compartment->AddModule(TString(R"((module
        (import "env" "__indirect_function_table" (table i64 1 funcref))
        (func $f) (elem (i64.const -1) func $f)))")), std::exception);
}

}   // namespace
}   // namespace NYdb::NWasm
