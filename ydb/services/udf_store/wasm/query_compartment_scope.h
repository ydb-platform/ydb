#pragma once

#include "compartment_manager.h"
#include "module_catalog.h"

#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/system/guard.h>

#include <memory>

namespace NKikimr::NUdfStore::NWasm {

//! TaskParams key for per-stage WASM module list (KQP → CA).
//! Value: newline-separated module names from TKqpPhyStage.WasmUdfModules.
inline constexpr TStringBuf WasmUdfModulesTaskParam = "_WasmUdfModules";

inline TString SerializeWasmUdfModulesTaskParam(const TVector<TString>& modules) {
    return JoinSeq('\n', modules);
}

inline TVector<TString> ParseWasmUdfModulesTaskParam(TStringBuf data) {
    TVector<TString> modules;
    StringSplitter(data).Split('\n').SkipEmpty().Collect(&modules);
    return modules;
}

template <typename TRepeatedString>
inline TVector<TString> WasmUdfModulesFromRepeated(const TRepeatedString& repeated) {
    TVector<TString> modules;
    modules.reserve(repeated.size());
    for (const auto& module : repeated) {
        modules.push_back(module);
    }
    return modules;
}

//! Keep only module names registered in the WASM catalog.
//! Stage predictor records every TCoUdf module (String, Knn, ...);
//! native UDFs must not trigger Acquire / ResolveModules.
inline TVector<TString> FilterLoadedWasmUdfModules(
    const TVector<TString>& modules,
    const TWasmModuleCatalog& catalog = GetWasmModuleCatalog())
{
    TVector<TString> result;
    result.reserve(modules.size());
    for (const auto& module : modules) {
        if (catalog.FindByModuleName(module)) {
            result.push_back(module);
        }
    }
    return result;
}

// Owns a per-query compartment. Install it as the current TLS compartment only
// for the duration of a TLS guard (actor event / task run).
class TQueryCompartmentScope : public TNonCopyable {
public:
    //! `alloc` is the allocator the query's MiniKQL values come from. The
    //! bridge node table and the resident cache keep such values alive across
    //! rows (pinned strings, handles the guest took a ref on), and MiniKQL
    //! frees through TlsAllocState, so the handle can only be released with
    //! that allocator bound. Taking it here rather than at every teardown path
    //! is what keeps the owners from having to remember.
    TQueryCompartmentScope(
        const TVector<TString>& modules,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : Alloc_(std::move(alloc))
    {
        const auto loaded = FilterLoadedWasmUdfModules(modules);
        if (!loaded.empty()) {
            Handle_ = GetWasmCompartmentManager().Acquire(loaded);
        }
    }

    //! Already-acquired handle. Same teardown rules: the destructor binds
    //! `alloc` before the node table and the resident cache die.
    TQueryCompartmentScope(
        TQueryCompartmentHandlePtr handle,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : Alloc_(std::move(alloc))
        , Handle_(std::move(handle))
    {
    }

    ~TQueryCompartmentScope() {
        if (!Handle_) {
            return;
        }
        if (!Alloc_) {
            Handle_.reset();
            return;
        }
        // TScopedAlloc counts attachments, so binding one the caller already
        // holds is harmless -- this works whether or not teardown happens to
        // run inside a BindAllocator scope.
        auto guard = Guard(*Alloc_);
        Handle_.reset();
    }

    bool HasHandle() const {
        return Handle_ != nullptr;
    }

    TCurrentQueryCompartmentGuard MakeTlsGuard() const {
        return TCurrentQueryCompartmentGuard(Handle_.get());
    }

private:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc_;
    TQueryCompartmentHandlePtr Handle_;
};

} // namespace NKikimr::NUdfStore::NWasm
