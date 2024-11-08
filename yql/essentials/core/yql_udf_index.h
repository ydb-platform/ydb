#pragma once

#include "yql_udf_resolver.h"
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

struct TFunctionInfo {
    TString Name;
    int ArgCount = 0;
    int OptionalArgCount = 0;
    bool IsTypeAwareness = false;
    TString CallableType;
    TString RunConfigType;
    bool IsStrict = false;
    bool SupportsBlocks = false;
};

// todo: specify whether path is frozen
struct TDownloadLink {
    bool IsUrl = false;
    TString Path;
    TString Md5;

    TDownloadLink() {

    }

    TDownloadLink(bool isUrl, const TString& path, const TString& md5)
        : IsUrl(isUrl)
        , Path(path)
        , Md5(md5)
    {
    }

    TDownloadLink(const TDownloadLink&) = default;
    TDownloadLink& operator=(const TDownloadLink&) = default;

    static TDownloadLink Url(const TString& path, const TString& md5 = "") {
        return { true, path, md5 };
    }

    static TDownloadLink File(const TString& path, const TString& md5 = "") {
        return { false, path, md5 };
    }

    bool operator==(const TDownloadLink& other) const {
        return std::tie(IsUrl, Path, Md5) == std::tie(other.IsUrl, other.Path, Md5);
    }

    bool operator!=(const TDownloadLink& other) const {
        return !(*this == other);
    }

    size_t Hash() const {
        return CombineHashes(
            CombineHashes((size_t)IsUrl, ComputeHash(Path)),
            ComputeHash(Md5)
        );
    }
};

struct TResourceInfo : public TThrRefBase {
    typedef TIntrusiveConstPtr<TResourceInfo> TPtr;

    bool IsTrusted = false;
    TDownloadLink Link;
    TSet<TString> Modules;
    TMap<TString, TFunctionInfo> Functions;

    void SetFunctions(const TVector<TFunctionInfo>& functions) {
        for (auto& f : functions) {
            Functions.emplace(f.Name, f);
        }
    }
};

inline bool operator<(const TResourceInfo::TPtr& p1, const TResourceInfo::TPtr& p2) {
    return p1.Get() < p2.Get();
}

class TUdfIndex : public TThrRefBase {
public:
    typedef TIntrusivePtr<TUdfIndex> TPtr;

public:
    // todo: trusted resources should not be replaceble regardless of specified mode
    enum class EOverrideMode {
        PreserveExisting,
        ReplaceWithNew,
        RaiseError
    };

public:
    TUdfIndex();
    bool ContainsModule(const TString& moduleName) const;
    bool FindFunction(const TString& moduleName, const TString& functionName, TFunctionInfo& function) const;
    TResourceInfo::TPtr FindResourceByModule(const TString& moduleName) const;

    /*
    New resource can contain already registered module.
    In this case 'mode' will be used to resolve conflicts.
    For instance, if mode == ReplaceWithNew all functions from old resource will be removed and new functions will be registered.
    It is important to do it atomically because two .so cannot have intersecting module lists
    */
    void RegisterResource(const TResourceInfo::TPtr& resource, EOverrideMode mode);
    void RegisterResources(const TVector<TResourceInfo::TPtr>& resources, EOverrideMode mode);

    TIntrusivePtr<TUdfIndex> Clone() const;

private:
    explicit TUdfIndex(const TMap<TString, TResourceInfo::TPtr>& resources);

    bool ContainsAnyModule(const TSet<TString>& modules) const;
    TSet<TResourceInfo::TPtr> FindResourcesByModules(const TSet<TString>& modules) const;
    void UnregisterResource(TResourceInfo::TPtr resource);

private:
    // module => Resource
    TMap<TString, TResourceInfo::TPtr> Resources_;
};

void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TVector<TString>& paths, bool isTrusted, TUdfIndex::EOverrideMode mode, TUdfIndex& registry);
void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TMap<TString, TString>& pathsWithMd5, bool isTrusted, TUdfIndex::EOverrideMode mode, TUdfIndex& registry);
void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TVector<TUserDataBlock>& blocks, TUdfIndex::EOverrideMode mode, TUdfIndex& registry);
void LoadRichMetadataToUdfIndex(const IUdfResolver& resolver, const TUserDataBlock& block, TUdfIndex::EOverrideMode mode, TUdfIndex& registry);

}
