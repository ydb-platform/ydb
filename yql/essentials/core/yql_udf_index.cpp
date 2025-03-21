#include "yql_udf_index.h"
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NYql {
namespace {

TVector<TResourceInfo::TPtr> ConvertResolveResultToResources(const TResolveResult& resolveResult, const TMap<TString, TString>& pathsWithMd5, bool isTrusted) {
    THashMap<TString, size_t> importIndex; // module => Imports index
    THashMap<TString, size_t> packageIndex; // package => Imports index
    THashMap<TString, TVector<TFunctionInfo>> functionIndex; // package => vector of functions
    for (size_t i = 0; i < resolveResult.ImportsSize(); ++i) {
        auto& import = resolveResult.GetImports(i);
        if (!import.ModulesSize()) {
            continue;
        }

        for (auto& m : import.GetModules()) {
            importIndex.emplace(m, i);
        }

        const TString package = import.GetModules(0);
        packageIndex.emplace(package, i);
        functionIndex.emplace(package, TVector<TFunctionInfo>());
    }

    for (auto& udf : resolveResult.GetUdfs()) {
        const TString module = TString(NKikimr::NMiniKQL::ModuleName(TStringBuf(udf.GetName())));
        const auto& import = resolveResult.GetImports(importIndex.at(module));
        const TString package = import.GetModules(0);

        TFunctionInfo newFunction;
        newFunction.Name = udf.GetName();
        newFunction.IsTypeAwareness = udf.GetIsTypeAwareness();
        newFunction.ArgCount = udf.GetArgCount();
        newFunction.OptionalArgCount = udf.GetOptionalArgCount();
        if (udf.HasCallableType()) {
            newFunction.CallableType = udf.GetCallableType();
        }

        if (udf.HasRunConfigType()) {
            newFunction.RunConfigType = udf.GetRunConfigType();
        }

        if (udf.HasIsStrict()) {
            newFunction.IsStrict = udf.GetIsStrict();
        }

        if (udf.HasSupportsBlocks()) {
            newFunction.SupportsBlocks = udf.GetSupportsBlocks();
        }

        for (const auto& m : udf.GetMessages()) {
            newFunction.Messages.push_back(m);
        }

        functionIndex[package].push_back(newFunction);
    }

    TVector<TResourceInfo::TPtr> result;
    result.reserve(functionIndex.size());
    for (auto& p : functionIndex) {
        const auto& import = resolveResult.GetImports(packageIndex.at(p.first));

        auto info = MakeIntrusive<TResourceInfo>();
        info->IsTrusted = isTrusted;
        auto md5 = pathsWithMd5.FindPtr(import.GetFileAlias());
        info->Link = TDownloadLink::File(import.GetFileAlias(), md5 ? *md5 : "");
        info->Modules.insert(import.GetModules().begin(), import.GetModules().end());
        info->SetFunctions(p.second);

        result.push_back(info);
    }

    return result;
}

void AddResolveResultToRegistry(const TResolveResult& resolveResult, const TMap<TString, TString>& pathsWithMd5, bool isTrusted, TUdfIndex::EOverrideMode mode, TUdfIndex& registry) {
    auto resources = ConvertResolveResultToResources(resolveResult, pathsWithMd5, isTrusted);
    registry.RegisterResources(resources, mode);
}

}

TUdfIndex::TUdfIndex() {
}

void TUdfIndex::SetCaseSentiveSearch(bool caseSensitive) {
    CaseSensitive_ = caseSensitive;
}

TUdfIndex::TUdfIndex(const TMap<TString, TResourceInfo::TPtr>& resources, bool caseSensitive)
    : Resources_(resources)
    , CaseSensitive_(caseSensitive)
{
    for (const auto& x : Resources_) {
        ICaseModules_[to_lower(x.first)].insert(x.first);
    }
}

bool TUdfIndex::ContainsModuleStrict(const TString& moduleName) const {
    return Resources_.contains(moduleName);
}

bool TUdfIndex::CanonizeModule(TString& moduleName) const {
    if (Resources_.contains(moduleName)) {
        return true;
    }

    if (CaseSensitive_) {
        return false;
    }

    auto p = ICaseModules_.FindPtr(to_lower(moduleName));
    if (!p) {
        return false;
    }

    Y_ENSURE(p->size() > 0);
    if (p->size() > 1) {
        return false;
    }

    moduleName = *p->begin();
    return true;
}

TUdfIndex::EStatus TUdfIndex::ContainsModule(const TString& moduleName) const {
    if (Resources_.contains(moduleName)) {
        return EStatus::Found;
    }

    if (CaseSensitive_) {
        return EStatus::NotFound;
    }

    auto p = ICaseModules_.FindPtr(to_lower(moduleName));
    if (!p) {
        return EStatus::NotFound;
    }

    Y_ENSURE(p->size() > 0);
    return p->size() > 1 ? EStatus::Ambigious : EStatus::Found;
}

bool TUdfIndex::ContainsAnyModule(const TSet<TString>& modules) const {
    return AnyOf(modules, [this](auto& m) {
        return Resources_.contains(m);
    });
}

TUdfIndex::EStatus TUdfIndex::FindFunction(const TString& moduleName, const TString& functionName, TFunctionInfo& function) const {
    auto r = Resources_.FindPtr(moduleName);
    if (!r) {
        if (CaseSensitive_) {
            return EStatus::NotFound;
        }

        auto p = ICaseModules_.FindPtr(to_lower(moduleName));
        if (!p) {
            return EStatus::NotFound;
        }

        Y_ENSURE(p->size() > 0);
        if (p->size() > 1) {
            return EStatus::Ambigious;
        }

        r = Resources_.FindPtr(*p->begin());
        Y_ENSURE(r);
    }

    auto f = (*r)->Functions.FindPtr(functionName);
    if (!f) {
        if (CaseSensitive_) {
            return EStatus::NotFound;
        }

        auto p = (*r)->ICaseFuncNames.FindPtr(to_lower(functionName));
        if (!p) {
            return EStatus::NotFound;
        }

        Y_ENSURE(p->size() > 0);
        if (p->size() > 1) {
            return EStatus::Ambigious;
        }

        f = (*r)->Functions.FindPtr(*p->begin());
        Y_ENSURE(f);
    }

    function = *f;
    return EStatus::Found;
}

TResourceInfo::TPtr TUdfIndex::FindResourceByModule(const TString& moduleName) const {
    auto p = Resources_.FindPtr(moduleName);
    if (!p) {
        if (CaseSensitive_) {
            return nullptr;
        }

        auto n = ICaseModules_.FindPtr(to_lower(moduleName));
        if (!n) {
            return nullptr;
        }

        Y_ENSURE(n->size() > 0);
        if (n->size() > 1) {
            return nullptr;
        }

        p = Resources_.FindPtr(*n->begin());
        Y_ENSURE(p);
    }

    return *p;
}

TSet<TResourceInfo::TPtr> TUdfIndex::FindResourcesByModules(const TSet<TString>& modules) const {
    TSet<TResourceInfo::TPtr> result;
    for (auto& m : modules) {
        auto r = FindResourceByModule(m);
        if (r) {
            result.insert(r);
        }
    }
    return result;
}

void TUdfIndex::UnregisterResource(TResourceInfo::TPtr resource) {
    for (auto& m : resource->Modules) {
        Resources_.erase(m);
        auto& names = ICaseModules_[to_lower(m)];
        names.erase(m);
        if (names.empty()) {
            ICaseModules_.erase(to_lower(m));
        }
    }
    // resource pointer should be alive here to avoid problems with erase
}

void TUdfIndex::RegisterResource(const TResourceInfo::TPtr& resource, EOverrideMode mode) {
    Y_ENSURE(resource);
    if (resource->Modules.empty()) {
        // quite strange, but let's ignore
        return;
    }

    // detect conflict first
    if (ContainsAnyModule(resource->Modules)) {
        switch (mode) {
        case EOverrideMode::PreserveExisting:
            return;

        case EOverrideMode::RaiseError:
            // todo: specify module name(s) in intersection
            ythrow yexception() << "Conflict during resource " << resource->Link.Path << " registration";

        case EOverrideMode::ReplaceWithNew: {
            // we have to find resources and remove all related modules:
            // 1. find resources by newModules
            // 2. remove all functions related to found resources

            auto existingResources = FindResourcesByModules(resource->Modules);
            Y_ENSURE(!existingResources.empty());

            for (auto& r : existingResources) {
                UnregisterResource(r);
            }

            break;
        }
        } // switch
    }

    for (auto& m : resource->Modules) {
        Resources_.emplace(m, resource);
        ICaseModules_[to_lower(m)].insert(m);
    }
}

TIntrusivePtr<TUdfIndex> TUdfIndex::Clone() const {
    return new TUdfIndex(Resources_, CaseSensitive_);
}

void TUdfIndex::RegisterResources(const TVector<TResourceInfo::TPtr>& resources, EOverrideMode mode) {
    for (auto& r : resources) {
        RegisterResource(r, mode);
    }
}

void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TVector<TString>& paths, bool isTrusted, TUdfIndex::EOverrideMode mode, TUdfIndex& registry, NUdf::ELogLevel logLevel) {
    TMap<TString, TString> pathsWithMd5;
    for (const auto& path : paths) {
        pathsWithMd5[path] = "";
    }
    LoadRichMetadataToUdfIndex(resolver, pathsWithMd5, isTrusted, mode, registry, logLevel);
}

void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TMap<TString, TString>& pathsWithMd5, bool isTrusted, TUdfIndex::EOverrideMode mode, TUdfIndex& registry, NUdf::ELogLevel logLevel) {
    TVector<TString> paths;
    paths.reserve(pathsWithMd5.size());
    for (const auto& p : pathsWithMd5) {
        paths.push_back(p.first);
    }
    const TResolveResult resolveResult = LoadRichMetadata(resolver, paths, logLevel);
    AddResolveResultToRegistry(resolveResult, pathsWithMd5, isTrusted, mode, registry);
}

void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TVector<TUserDataBlock>& blocks, bool isTrusted, TUdfIndex::EOverrideMode mode, TUdfIndex& registry, NUdf::ELogLevel logLevel) {
    TVector<TUserDataBlock> blocksResolve;
    blocksResolve.reserve(blocks.size());
    // we can work with file path only
    TMap<TString, TString> pathsWithMd5;
    for (auto& b : blocks) {
        TString path;
        switch (b.Type) {
        case EUserDataType::URL:
            if (!b.FrozenFile) {
                ythrow yexception() << "DataBlock for " << b.Data << " is not frozen";
            }
            path = b.FrozenFile->GetPath().GetPath();
            pathsWithMd5.emplace(path, b.FrozenFile->GetMd5());
            break;
        case EUserDataType::PATH:
        {
            TString md5;
            if (b.FrozenFile) {
                md5 = b.FrozenFile->GetMd5();
            }
            path = b.Data;
            pathsWithMd5.emplace(b.Data, md5);
            break;
        }
        default:
            ythrow yexception() << "Unsupport data block type for " << b.Data;
        }

        TUserDataBlock br;
        br.Type = EUserDataType::PATH;
        br.Data = path;
        br.Usage.Set(EUserDataBlockUsage::Udf);
        br.CustomUdfPrefix = b.CustomUdfPrefix;
        blocksResolve.emplace_back(br);
    }
    const TResolveResult resolveResult = LoadRichMetadata(resolver, blocksResolve, logLevel);
    AddResolveResultToRegistry(resolveResult, pathsWithMd5, isTrusted, mode, registry);
}

void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TUserDataBlock& block, TUdfIndex::EOverrideMode mode, TUdfIndex& registry, NUdf::ELogLevel logLevel) {
    TVector<TUserDataBlock> blocks({ block });
    const bool isTrusted = false;
    LoadRichMetadataToUdfIndex(resolver, blocks, isTrusted, mode, registry, logLevel);
}

} // namespace NYql
