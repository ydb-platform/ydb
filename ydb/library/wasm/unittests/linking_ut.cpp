#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <contrib/restricted/wavm_llvm16/Lib/Runtime/RuntimePrivate.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#if defined(_unix_)
#include <sys/wait.h>

#include <unistd.h>
#endif

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

#if defined(_unix_)
// An outer watchdog turns a missing guest deadline into a bounded test failure.
// Construct WAVM state in the child so no compartment survives across fork.
template <typename TBody>
void CheckInitializationInChild(TBody body)
{
    const auto child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        alarm(15);
        try {
            body();
            _exit(::testing::Test::HasFailure() ? 1 : 0);
        } catch (...) {
            _exit(2);
        }
    }
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status)) << "Initialization exceeded the watchdog: " << status;
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

void ExpectInitializationTimeout(IWebAssemblyCompartment* compartment, const TString& source, bool precompiled)
{
    try {
        Load(compartment, source, precompiled);
        FAIL() << "Expected initialization timeout";
    } catch (const std::exception& ex) {
        EXPECT_TRUE(TStringBuf(ex.what()).Contains("timeoutExpired")) << ex.what();
    }
}

TEST(TWasmLinking, InitializationStartHasDefaultBudget)
{
    for (bool precompiled : { false, true }) {
        SCOPED_TRACE(precompiled);
        CheckInitializationInChild([&] {
            auto compartment = CreateEmptyImage();
            SetCurrentCompartment(compartment.get());
            Y_DEFER {
                SetCurrentCompartment(nullptr);
            };
            ExpectInitializationTimeout(compartment.get(), R"((module
                (func $start (loop $forever (br $forever)))
                (start $start)))", precompiled);
            // The temporary expired budget must not leak into the caller's TLS.
            EXPECT_EQ(GetCurrentCompartment(), compartment.get());
            EXPECT_FALSE(WAVM::Runtime::isCurrentDeadlineReached());
        });
    }
}

TEST(TWasmLinking, InitializationHooksHaveDefaultBudget)
{
    for (bool precompiled : { false, true }) {
        for (const auto* name : { "__wasm_apply_global_relocs", "__wasm_apply_data_relocs", "__wasm_call_ctors" }) {
            SCOPED_TRACE(name);
            SCOPED_TRACE(precompiled);
            CheckInitializationInChild([&] {
                auto compartment = CreateEmptyImage();
                ExpectInitializationTimeout(compartment.get(),
                    TStringBuilder() << "(module (func (export \"" << name << "\") (loop $forever (br $forever))))", precompiled);
                EXPECT_EQ(GetCurrentCompartment(), nullptr);
            });
        }
    }
}

const TString FiniteStart = R"((module
    (func $start (local $i i32)
        (loop $again
            (local.set $i (i32.add (local.get $i) (i32.const 1)))
            (br_if $again (i32.lt_u (local.get $i) (i32.const 1000000)))))
    (start $start)))";

TEST(TWasmLinking, InitializationDoesNotExtendArmedDeadline)
{
    for (bool precompiled : { false, true }) {
        CheckInitializationInChild([&] {
            auto compartment = CreateEmptyImage();
            compartment->SetTimeout(TDuration::Zero());
            compartment->StartDeadlineTimer();
            SetCurrentCompartment(compartment.get());
            Y_DEFER {
                SetCurrentCompartment(nullptr);
            };
            ExpectInitializationTimeout(compartment.get(), FiniteStart, precompiled);
            EXPECT_EQ(GetCurrentCompartment(), compartment.get());
            EXPECT_TRUE(WAVM::Runtime::isCurrentDeadlineReached());
        });
    }
}

TEST(TWasmLinking, InitializationRespectsOuterCompartmentDeadline)
{
    for (bool precompiled : { false, true }) {
        CheckInitializationInChild([&] {
            auto outer = CreateEmptyImage();
            auto inner = CreateEmptyImage();
            outer->SetTimeout(TDuration::Zero());
            outer->StartDeadlineTimer();
            SetCurrentCompartment(outer.get());
            Y_DEFER {
                SetCurrentCompartment(nullptr);
            };
            ExpectInitializationTimeout(inner.get(), FiniteStart, precompiled);
            EXPECT_EQ(GetCurrentCompartment(), outer.get());
            EXPECT_TRUE(WAVM::Runtime::isCurrentDeadlineReached());
            SetCurrentCompartment(inner.get());
            EXPECT_FALSE(WAVM::Runtime::isCurrentDeadlineReached());
        });
    }
}

TEST(TWasmLinking, InitializationPreservesConfiguredInvocationTimeout)
{
    for (bool precompiled : { false, true }) {
        CheckInitializationInChild([&] {
            auto compartment = CreateEmptyImage();
            // A configured, not-yet-started invocation timer must stay dormant.
            compartment->SetTimeout(TDuration::Zero());
            SetCurrentCompartment(compartment.get());
            Y_DEFER {
                SetCurrentCompartment(nullptr);
            };
            Load(compartment.get(), FiniteStart, precompiled);
            EXPECT_FALSE(WAVM::Runtime::isCurrentDeadlineReached());
            compartment->StartDeadlineTimer();
            SetCurrentCompartment(compartment.get());
            EXPECT_TRUE(WAVM::Runtime::isCurrentDeadlineReached());
        });
    }
}
#endif

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

const TString GotProviderFunctions = R"(
    (func $different_debug_name (export "f") (export "alias") (result i64) (i64.const 1))
    (func $g (result i64) (i64.const 2))
)";

const TString GotConsumerBody = R"(
    (type $signature (func (result i64)))
    (import "GOT.func" "f" (global $address (mut i64)))
    (import "GOT.func" "alias" (global $alias (mut i64)))
    (func (export "invoke") (result i64)
        (call_indirect (type $signature) (global.get $address)))
    (func (export "invoke_alias") (result i64)
        (call_indirect (type $signature) (global.get $alias)))
)";

const TString SharedTableImport = R"((import "env" "__indirect_function_table" (table i64 1 funcref)))";

void CheckGotExports(IWebAssemblyCompartment* compartment)
{
    EXPECT_EQ(Call(compartment, "invoke"), 1u);
    EXPECT_EQ(Call(compartment, "invoke_alias"), 1u);
}

TEST(TWasmLinking, PrivateTableDoesNotSupplySharedGotSlot)
{
    for (bool precompiled : { false, true }) {
        SCOPED_TRACE(precompiled);
        auto compartment = CreateEmptyImage();
        Load(compartment.get(), "(module (table i64 8 funcref)" + GotProviderFunctions + "(elem (i64.const 7) func $different_debug_name))",
            precompiled);
        Load(compartment.get(), "(module " + SharedTableImport + GotConsumerBody + ")", precompiled);
        EXPECT_NO_THROW(CheckGotExports(compartment.get()));
    }
}

TEST(TWasmLinking, OverwrittenElementIsNotReusedForExport)
{
    for (bool precompiled : { false, true }) {
        for (const auto* replacement : { "(elem (i64.const 7) func $g)", "(elem (i64.const 7) funcref (ref.null func))" }) {
            SCOPED_TRACE(precompiled);
            SCOPED_TRACE(replacement);
            auto compartment = CreateEmptyImage();
            Load(compartment.get(),
                "(module " + SharedTableImport + GotProviderFunctions + "(elem (i64.const 7) func $different_debug_name)" + replacement + ")",
                precompiled);
            Load(compartment.get(), "(module " + SharedTableImport + GotConsumerBody + ")", precompiled);
            EXPECT_NO_THROW(CheckGotExports(compartment.get()));
        }
    }
}

TEST(TWasmLinking, IncomingSegmentDoesNotOverwriteResolvedGotImport)
{
    for (bool precompiled : { false, true }) {
        for (bool overwriteInConsumer : { false, true }) {
            SCOPED_TRACE(precompiled);
            SCOPED_TRACE(overwriteInConsumer);
            auto compartment = CreateEmptyImage();
            Load(compartment.get(), "(module " + SharedTableImport + GotProviderFunctions + "(elem (i64.const 7) func $different_debug_name))",
                precompiled);
            const TString overwrite = "(func $g (result i64) (i64.const 2)) (elem (i64.const 7) func $g)";
            if (!overwriteInConsumer) {
                Load(compartment.get(), "(module " + SharedTableImport + overwrite + ")", precompiled);
            }
            // The consumer may itself overwrite a slot whose old function it
            // imports. Invalidation after instantiation would be too late.
            Load(compartment.get(), "(module " + SharedTableImport + GotConsumerBody + (overwriteInConsumer ? overwrite : TString()) + ")",
                precompiled);
            EXPECT_NO_THROW(CheckGotExports(compartment.get()));
        }
    }
}

TEST(TWasmLinking, GotCacheChecksContentsAfterGuestTableMutation)
{
    for (bool precompiled : { false, true }) {
        SCOPED_TRACE(precompiled);
        auto compartment = CreateEmptyImage();
        Load(compartment.get(), "(module " + SharedTableImport + GotProviderFunctions + R"(
            (elem (i64.const 7) func $different_debug_name)
            (func $start (table.set (i64.const 7) (ref.null func)))
            (start $start)))", precompiled);
        Load(compartment.get(), "(module " + SharedTableImport + GotConsumerBody + ")", precompiled);
        EXPECT_NO_THROW(CheckGotExports(compartment.get()));
    }
}

TEST(TWasmLinking, GotCacheSurvivesCloneAndProviderDestruction)
{
    for (bool precompiled : { false, true }) {
        SCOPED_TRACE(precompiled);
        auto original = CreateEmptyImage();
        Load(original.get(), "(module " + SharedTableImport + GotProviderFunctions + "(elem (i64.const 7) func $different_debug_name))",
            precompiled);
        auto clone = original->Clone();
        original.reset();
        Load(clone.get(), "(module " + SharedTableImport + GotConsumerBody + ")", precompiled);
        EXPECT_NO_THROW(CheckGotExports(clone.get()));
    }
}

TEST(TWasmLinking, GotForwardExportsArePatchedBeforeUse)
{
    for (bool precompiled : { false, true }) {
        SCOPED_TRACE(precompiled);
        auto compartment = CreateEmptyImage();
        Load(compartment.get(), "(module " + SharedTableImport + GotConsumerBody + GotProviderFunctions + ")", precompiled);
        EXPECT_NO_THROW(CheckGotExports(compartment.get()));
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
