#include <ydb/core/kqp/common/dynamic_function_registry.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/stream/str.h>

#include <atomic>
#include <thread>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NKqp;

namespace {

class TStubUdfModule: public NUdf::IUdfModule {
public:
    explicit TStubUdfModule(
        TString functionName = "Func",
        bool typeAwareness = false,
        TMaybe<TString> polyArgs = Nothing(),
        ui32* cleanupCounter = nullptr)
        : FunctionName_(std::move(functionName))
        , TypeAwareness_(typeAwareness)
        , PolyArgs_(std::move(polyArgs))
        , CleanupCounter_(cleanupCounter)
    {
    }

    void GetAllFunctions(NUdf::IFunctionsSink& sink) const override {
        auto desc = sink.Add(FunctionName_);
        if (TypeAwareness_) {
            desc->SetTypeAwareness();
        }
        if (PolyArgs_) {
            desc->SetPolyArgs(*PolyArgs_);
        }
    }

    void BuildFunctionTypeInfo(
        const NUdf::TStringRef& name,
        NUdf::TType* /*userType*/,
        const NUdf::TStringRef& /*typeConfig*/,
        ui32 /*flags*/,
        NUdf::IFunctionTypeInfoBuilder& builder) const override
    {
        if (name == NUdf::TStringRef(FunctionName_)) {
            builder.SimpleSignature<i32(i32)>();
        }
    }

    void CleanupOnTerminate() const override {
        if (CleanupCounter_) {
            ++(*CleanupCounter_);
        }
    }

private:
    const TString FunctionName_;
    const bool TypeAwareness_;
    const TMaybe<TString> PolyArgs_;
    ui32* CleanupCounter_;
};

//! Blocks inside BuildFunctionTypeInfo until Release_ is set (for remove-during-call tests).
class TBlockingUdfModule: public NUdf::IUdfModule {
public:
    TBlockingUdfModule(
        std::atomic<bool>& entered,
        std::atomic<bool>& release,
        TString functionName = "Bar")
        : Entered_(entered)
        , Release_(release)
        , FunctionName_(std::move(functionName))
    {
    }

    void GetAllFunctions(NUdf::IFunctionsSink& sink) const override {
        sink.Add(FunctionName_);
    }

    void BuildFunctionTypeInfo(
        const NUdf::TStringRef& name,
        NUdf::TType* /*userType*/,
        const NUdf::TStringRef& /*typeConfig*/,
        ui32 /*flags*/,
        NUdf::IFunctionTypeInfoBuilder& builder) const override
    {
        if (name != NUdf::TStringRef(FunctionName_)) {
            return;
        }
        Entered_.store(true, std::memory_order_release);
        while (!Release_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        builder.SimpleSignature<i32(i32)>();
    }

    void CleanupOnTerminate() const override {
    }

private:
    std::atomic<bool>& Entered_;
    std::atomic<bool>& Release_;
    const TString FunctionName_;
};

TIntrusivePtr<IMutableFunctionRegistry> MakeRegistry() {
    auto registry = CreateDynamicFunctionRegistry(CreateBuiltinRegistry());
    UNIT_ASSERT(AsDynamicFunctionRegistry(registry.Get()));
    return registry;
}

IDynamicFunctionRegistry* Dyn(IMutableFunctionRegistry* registry) {
    auto* dyn = AsDynamicFunctionRegistry(registry);
    UNIT_ASSERT(dyn);
    return dyn;
}

TStatus LookupTypeInfo(
    const IFunctionRegistry& registry,
    const TTypeEnvironment& env,
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
    const NYql::TRuntimeSettings& runtimeSettings,
    const TStringBuf& name,
    TFunctionTypeInfo* funcInfo)
{
    return registry.FindFunctionTypeInfo(
        NYql::UnknownLangVersion,
        runtimeSettings,
        env,
        typeInfoHelper,
        /*countersProvider=*/nullptr,
        name,
        /*userType=*/nullptr,
        /*typeConfig=*/TStringBuf(),
        NUdf::IUdfModule::TFlags::TypesOnly,
        NUdf::TSourcePosition(),
        /*secureParamsProvider=*/nullptr,
        /*logProvider=*/nullptr,
        funcInfo);
}

} // namespace

Y_UNIT_TEST_SUITE(TDynamicFunctionRegistryTest) {

Y_UNIT_TEST(RemoveModuleRemovesKnownModule) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule());

    UNIT_ASSERT(registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT(registry->GetAllModuleNames().contains("Foo"));
    UNIT_ASSERT(registry->FindUdfPath("Foo").Defined());
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Foo"), "/lib/foo");

    Dyn(registry.Get())->RemoveModule("Foo");

    UNIT_ASSERT(!registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT(!registry->GetAllModuleNames().contains("Foo"));
    UNIT_ASSERT(!registry->FindUdfPath("Foo").Defined());
}

Y_UNIT_TEST(RemoveModuleMissingIsNoOp) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule());

    Dyn(registry.Get())->RemoveModule("Missing");
    Dyn(registry.Get())->RemoveModule("Missing");

    UNIT_ASSERT(registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Foo"), "/lib/foo");
}

Y_UNIT_TEST(RemoveModuleDropsLibraryEntryWhenLastModule) {
    auto registry = MakeRegistry();
    const TString path = "/lib/shared";
    registry->AddModule(path, "Only", new TStubUdfModule());

    Dyn(registry.Get())->RemoveModule("Only");
    UNIT_ASSERT(!registry->IsLoadedUdfModule("Only"));

    // Path must have been dropped from LoadedLibraries_: re-add under the same
    // path must succeed and reappear in FindUdfPath.
    registry->AddModule(path, "Again", new TStubUdfModule("AgainFunc"));
    UNIT_ASSERT(registry->IsLoadedUdfModule("Again"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Again"), path);
}

Y_UNIT_TEST(RemoveModuleKeepsLibraryEntryWhenOtherModulesRemain) {
    auto registry = MakeRegistry();
    const TString path = "/lib/shared";
    registry->AddModule(path, "ModA", new TStubUdfModule("A"));
    registry->AddModule(path, "ModB", new TStubUdfModule("B"));

    Dyn(registry.Get())->RemoveModule("ModA");

    UNIT_ASSERT(!registry->IsLoadedUdfModule("ModA"));
    UNIT_ASSERT(registry->IsLoadedUdfModule("ModB"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("ModB"), path);

    // Path still tracked: can register another module under the same path.
    registry->AddModule(path, "ModC", new TStubUdfModule("C"));
    UNIT_ASSERT(registry->IsLoadedUdfModule("ModC"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("ModC"), path);
}

Y_UNIT_TEST(CloneIsIndependent) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule());

    auto clone = registry->Clone();
    UNIT_ASSERT(AsDynamicFunctionRegistry(clone.Get()));

    UNIT_ASSERT(clone->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT_VALUES_EQUAL(*clone->FindUdfPath("Foo"), "/lib/foo");

    Dyn(registry.Get())->RemoveModule("Foo");
    UNIT_ASSERT(!registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT(clone->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT_VALUES_EQUAL(*clone->FindUdfPath("Foo"), "/lib/foo");

    Dyn(clone.Get())->RemoveModule("Foo");
    UNIT_ASSERT(!clone->IsLoadedUdfModule("Foo"));
}

Y_UNIT_TEST(FindFunctionTypeInfoResolvesRegisteredUdf) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule("Bar"));

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    auto runtimeSettings = NYql::MakeRuntimeSettings();

    TFunctionTypeInfo funcInfo;
    auto ok = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "Foo.Bar", &funcInfo);
    UNIT_ASSERT_C(ok.IsOk(), ok.GetError());
    UNIT_ASSERT(funcInfo.FunctionType != nullptr);

    TFunctionTypeInfo missingInfo;
    auto missing = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "Foo.Missing", &missingInfo);
    UNIT_ASSERT(!missing.IsOk());

    TFunctionTypeInfo noModuleInfo;
    auto noModule = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "Unknown.Bar", &noModuleInfo);
    UNIT_ASSERT(!noModule.IsOk());
}

// --- IMutableFunctionRegistry surface (parity with TMutableFunctionRegistry usage) ---

Y_UNIT_TEST(GetBuiltinsAndSupportsSizedAllocators) {
    auto builtins = CreateBuiltinRegistry();
    auto* builtinsRaw = builtins.Get();
    auto registry = CreateDynamicFunctionRegistry(std::move(builtins));

    UNIT_ASSERT(registry->GetBuiltins().Get() == builtinsRaw);
    UNIT_ASSERT(registry->SupportsSizedAllocators());
}

Y_UNIT_TEST(GetModuleFunctionsExposesRegisteredMetadata) {
    auto registry = MakeRegistry();
    registry->AddModule(
        "/lib/foo",
        "Foo",
        new TStubUdfModule("Bar", /*typeAwareness=*/true, TString("poly")));

    auto funcs = registry->GetModuleFunctions("Foo");
    UNIT_ASSERT_VALUES_EQUAL(funcs.size(), 1u);
    UNIT_ASSERT(funcs.contains("Bar"));
    UNIT_ASSERT(funcs.at("Bar").IsTypeAwareness);
    UNIT_ASSERT(funcs.at("Bar").PolyArgs.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*funcs.at("Bar").PolyArgs, "poly");

    UNIT_ASSERT(registry->GetModuleFunctions("Missing").empty());
}

Y_UNIT_TEST(SetSystemModulePathsUsedByFindUdfPath) {
    auto registry = MakeRegistry();
    registry->SetSystemModulePaths({{"Sys", "/system/sys.so"}});

    UNIT_ASSERT(!registry->IsLoadedUdfModule("Sys"));
    UNIT_ASSERT(registry->FindUdfPath("Sys").Defined());
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Sys"), "/system/sys.so");

    // Loaded module path takes precedence over system path.
    registry->AddModule("/lib/sys", "Sys", new TStubUdfModule());
    UNIT_ASSERT(registry->IsLoadedUdfModule("Sys"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Sys"), "/lib/sys");

    // RemoveModule unloads the dynamic module but keeps the system catalog entry.
    Dyn(registry.Get())->RemoveModule("Sys");
    UNIT_ASSERT(!registry->IsLoadedUdfModule("Sys"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Sys"), "/system/sys.so");
}

Y_UNIT_TEST(CleanupModulesOnTerminateInvokesModules) {
    ui32 cleanups = 0;
    auto registry = MakeRegistry();
    registry->AddModule("/lib/a", "A", new TStubUdfModule("Fa", false, Nothing(), &cleanups));
    registry->AddModule("/lib/b", "B", new TStubUdfModule("Fb", false, Nothing(), &cleanups));

    registry->CleanupModulesOnTerminate();
    UNIT_ASSERT_VALUES_EQUAL(cleanups, 2u);

    registry->CleanupModulesOnTerminate();
    UNIT_ASSERT_VALUES_EQUAL(cleanups, 4u);
}

Y_UNIT_TEST(AddModuleDuplicateNameFails) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/a", "Foo", new TStubUdfModule("A"));
    UNIT_ASSERT_EXCEPTION(
        registry->AddModule("/lib/b", "Foo", new TStubUdfModule("B")),
        yexception);
}

Y_UNIT_TEST(FindFunctionTypeInfoRejectsInvalidName) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule("Bar"));

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    auto runtimeSettings = NYql::MakeRuntimeSettings();

    TFunctionTypeInfo info;
    auto status = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "NoDelimiter", &info);
    UNIT_ASSERT(!status.IsOk());
}

Y_UNIT_TEST(ClonePreservesFindFunctionTypeInfo) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule("Bar"));

    auto clone = registry->Clone();

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    auto runtimeSettings = NYql::MakeRuntimeSettings();

    TFunctionTypeInfo info;
    auto status = LookupTypeInfo(*clone, env, typeInfoHelper, *runtimeSettings, "Foo.Bar", &info);
    UNIT_ASSERT_C(status.IsOk(), status.GetError());
    UNIT_ASSERT(info.FunctionType != nullptr);

    auto funcs = clone->GetModuleFunctions("Foo");
    UNIT_ASSERT(funcs.contains("Bar"));
}

Y_UNIT_TEST(PrintInfoToDoesNotCrash) {
    auto registry = MakeRegistry();
    TStringStream out;
    registry->PrintInfoTo(out);
}

Y_UNIT_TEST(ConcurrentLookupDuringRemove) {
    // Force RemoveModule while BuildFunctionTypeInfo is in-flight so the
    // shared_ptr lifetime path is actually exercised (not just completed lookups).
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};

    auto registry = MakeRegistry();
    registry->AddModule(
        "/lib/foo",
        "Foo",
        new TBlockingUdfModule(entered, release, "Bar"));

    std::atomic<bool> lookupOk{false};
    std::thread lookupThread([&] {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
        auto runtimeSettings = NYql::MakeRuntimeSettings();

        TFunctionTypeInfo funcInfo;
        auto status = LookupTypeInfo(
            *registry, env, typeInfoHelper, *runtimeSettings, "Foo.Bar", &funcInfo);
        lookupOk.store(status.IsOk() && funcInfo.FunctionType != nullptr,
                       std::memory_order_relaxed);
    });

    while (!entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    Dyn(registry.Get())->RemoveModule("Foo");
    UNIT_ASSERT(!registry->IsLoadedUdfModule("Foo"));

    // No phantom: new lookup after publish must not see the removed module.
    {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
        auto runtimeSettings = NYql::MakeRuntimeSettings();
        TFunctionTypeInfo funcInfo;
        auto status = LookupTypeInfo(
            *registry, env, typeInfoHelper, *runtimeSettings, "Foo.Bar", &funcInfo);
        UNIT_ASSERT(!status.IsOk());
    }

    release.store(true, std::memory_order_release);
    lookupThread.join();

    UNIT_ASSERT(lookupOk.load(std::memory_order_relaxed));
}

Y_UNIT_TEST(ConcurrentAddAndLookup) {
    auto registry = MakeRegistry();
    constexpr size_t moduleCount = 64;
    constexpr size_t writerCount = 4;
    constexpr size_t readerCount = 4;
    constexpr size_t totalThreads = writerCount + readerCount;

    std::atomic<size_t> nextIndex{0};
    std::atomic<size_t> readyCount{0};
    std::atomic<bool> go{false};
    std::atomic<bool> writersDone{false};
    std::atomic<ui64> lookupsWhileWriting{0};

    auto waitStart = [&] {
        readyCount.fetch_add(1, std::memory_order_acq_rel);
        while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    };

    TVector<std::thread> writers;
    writers.reserve(writerCount);
    for (size_t w = 0; w < writerCount; ++w) {
        writers.emplace_back([&] {
            waitStart();
            for (;;) {
                const size_t i = nextIndex.fetch_add(1, std::memory_order_relaxed);
                if (i >= moduleCount) {
                    break;
                }
                const TString name = TStringBuilder() << "Mod" << i;
                registry->AddModule(
                    TStringBuilder() << "/lib/" << i,
                    name,
                    new TStubUdfModule("Func"));
            }
            // Do not leave the writing window until a reader has observed an
            // in-flight add (avoids the flake where all writers finish first).
            while (lookupsWhileWriting.load(std::memory_order_acquire) == 0) {
                std::this_thread::yield();
            }
        });
    }

    TVector<std::thread> readers;
    readers.reserve(readerCount);
    for (size_t r = 0; r < readerCount; ++r) {
        readers.emplace_back([&] {
            waitStart();
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
            auto runtimeSettings = NYql::MakeRuntimeSettings();

            while (!writersDone.load(std::memory_order_acquire)) {
                for (size_t i = 0; i < moduleCount; ++i) {
                    const TString name = TStringBuilder() << "Mod" << i;
                    if (registry->IsLoadedUdfModule(name)) {
                        TFunctionTypeInfo funcInfo;
                        auto status = LookupTypeInfo(
                            *registry, env, typeInfoHelper, *runtimeSettings,
                            TStringBuilder() << name << ".Func", &funcInfo);
                        UNIT_ASSERT_C(status.IsOk(), status.GetError());
                        if (!writersDone.load(std::memory_order_acquire)) {
                            lookupsWhileWriting.fetch_add(1, std::memory_order_release);
                        }
                    }
                }
            }
        });
    }

    while (readyCount.load(std::memory_order_acquire) < totalThreads) {
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);

    for (auto& t : writers) {
        t.join();
    }
    writersDone.store(true, std::memory_order_release);
    for (auto& t : readers) {
        t.join();
    }

    UNIT_ASSERT_GT(lookupsWhileWriting.load(std::memory_order_relaxed), 0u);
    UNIT_ASSERT_VALUES_EQUAL(registry->GetAllModuleNames().size(), moduleCount);
    for (size_t i = 0; i < moduleCount; ++i) {
        const TString name = TStringBuilder() << "Mod" << i;
        UNIT_ASSERT(registry->IsLoadedUdfModule(name));
        UNIT_ASSERT_VALUES_EQUAL(
            *registry->FindUdfPath(name),
            TStringBuilder() << "/lib/" << i);
    }
}

Y_UNIT_TEST(ConcurrentAddRemoveAndLookup) {
    auto registry = MakeRegistry();
    constexpr size_t moduleCount = 64;
    constexpr size_t writerCount = 4;
    constexpr size_t removerCount = 2;
    constexpr size_t readerCount = 4;
    constexpr size_t totalThreads = writerCount + removerCount + readerCount;

    std::atomic<size_t> nextAdd{0};
    std::atomic<size_t> nextRemove{0};
    std::atomic<size_t> readyCount{0};
    std::atomic<bool> go{false};
    std::atomic<bool> mutationsDone{false};
    std::atomic<ui64> successfulLookups{0};

    auto waitStart = [&] {
        readyCount.fetch_add(1, std::memory_order_acq_rel);
        while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    };

    TVector<std::thread> writers;
    writers.reserve(writerCount);
    for (size_t w = 0; w < writerCount; ++w) {
        writers.emplace_back([&] {
            waitStart();
            for (;;) {
                const size_t i = nextAdd.fetch_add(1, std::memory_order_relaxed);
                if (i >= moduleCount) {
                    break;
                }
                registry->AddModule(
                    TStringBuilder() << "/lib/" << i,
                    TStringBuilder() << "Mod" << i,
                    new TStubUdfModule("Func"));
            }
        });
    }

    TVector<std::thread> removers;
    removers.reserve(removerCount);
    for (size_t r = 0; r < removerCount; ++r) {
        removers.emplace_back([&] {
            waitStart();
            for (;;) {
                // Remove even-indexed modules once (and only once) each.
                const size_t i = nextRemove.fetch_add(1, std::memory_order_relaxed) * 2;
                if (i >= moduleCount) {
                    break;
                }
                const TString name = TStringBuilder() << "Mod" << i;
                while (!mutationsDone.load(std::memory_order_acquire) &&
                       !registry->IsLoadedUdfModule(name))
                {
                    std::this_thread::yield();
                }
                Dyn(registry.Get())->RemoveModule(name);
            }
        });
    }

    TVector<std::thread> readers;
    readers.reserve(readerCount);
    for (size_t r = 0; r < readerCount; ++r) {
        readers.emplace_back([&] {
            waitStart();
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
            auto runtimeSettings = NYql::MakeRuntimeSettings();

            while (!mutationsDone.load(std::memory_order_acquire)) {
                for (size_t i = 0; i < moduleCount; ++i) {
                    const TString name = TStringBuilder() << "Mod" << i;
                    TFunctionTypeInfo funcInfo;
                    auto status = LookupTypeInfo(
                        *registry, env, typeInfoHelper, *runtimeSettings,
                        TStringBuilder() << name << ".Func", &funcInfo);
                    if (status.IsOk()) {
                        successfulLookups.fetch_add(1, std::memory_order_relaxed);
                    }
                    // Else: not registered yet, or already removed — both OK.
                }
            }
        });
    }

    while (readyCount.load(std::memory_order_acquire) < totalThreads) {
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);

    for (auto& t : writers) {
        t.join();
    }
    for (auto& t : removers) {
        t.join();
    }
    mutationsDone.store(true, std::memory_order_release);
    for (auto& t : readers) {
        t.join();
    }

    UNIT_ASSERT_GT(successfulLookups.load(std::memory_order_relaxed), 0u);

    // Even modules removed; odd modules remain. No phantom of removed names.
    for (size_t i = 0; i < moduleCount; ++i) {
        const TString name = TStringBuilder() << "Mod" << i;
        if ((i % 2) == 0) {
            UNIT_ASSERT(!registry->IsLoadedUdfModule(name));
        } else {
            UNIT_ASSERT(registry->IsLoadedUdfModule(name));
            UNIT_ASSERT_VALUES_EQUAL(
                *registry->FindUdfPath(name),
                TStringBuilder() << "/lib/" << i);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(registry->GetAllModuleNames().size(), moduleCount / 2);
}

} // Y_UNIT_TEST_SUITE(TDynamicFunctionRegistryTest)
