#include <library/cpp/svnversion/svnversion.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include "version.h"

using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
using TStored = NKikimrConfig::TStoredCompatibilityInfo;

namespace NKikimr {

/////////////////////////////////////////////////////////////
// Global definitions
/////////////////////////////////////////////////////////////

// new version control
std::optional<TCurrent> TCompatibilityInfo::CompatibilityInfo = std::nullopt;
TSpinLock TCompatibilityInfo::LockCurrent = TSpinLock();
const TCurrent* TCompatibilityInfo::GetCurrent() {
    TGuard<TSpinLock> g(TCompatibilityInfo::LockCurrent);

    if (!CompatibilityInfo) {
        // using TYdbVersion = TCompatibilityInfo::TProtoConstructor::TYdbVersion;
        // using TCompatibilityRule = TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;
        using TCurrentCompatibilityInfo = TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;

        auto current = TCurrentCompatibilityInfo{
            .Build = "trunk"
        }.ToPB();

        // Y_VERIFY_DEBUG(CompleteFromTag(current));

        CompatibilityInfo = TCurrent();
        CompatibilityInfo->CopyFrom(current);
    }

    return &*CompatibilityInfo;
}

// obsolete version control
TMaybe<NActors::TInterconnectProxyCommon::TVersionInfo> VERSION = NActors::TInterconnectProxyCommon::TVersionInfo{
    // version of this binary
    "trunk",

    // compatible versions; must include all compatible old ones, including this one; version verification occurs on both
    // peers and connection is accepted if at least one of peers accepts the version of the other peer
    {
        "trunk"
    }
};

/////////////////////////////////////////////////////////////
// Implementation
/////////////////////////////////////////////////////////////

// Last stable YDB release, which doesn't include version control change
// When the compatibility information is not present in component's data,
// we assume component's version to be this version
std::optional<TStored> TCompatibilityInfo::UnknownYdbRelease = std::nullopt;
const TStored* TCompatibilityInfo::GetUnknown() {
    static TSpinLock lock;
    TGuard<TSpinLock> g(lock);

    if (!UnknownYdbRelease) {
        using TYdbVersion = TCompatibilityInfo::TProtoConstructor::TYdbVersion;
        // using TCompatibilityRule = TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;
        using TStoredCompatibilityInfo = TCompatibilityInfo::TProtoConstructor::TStoredCompatibilityInfo;

        UnknownYdbRelease = TStored();
        UnknownYdbRelease->CopyFrom(TStoredCompatibilityInfo{
            .Build = "ydb",
            .YdbVersion = TYdbVersion{ .Year = 22, .Major = 5, .Minor = 7, .Hotfix = 0 }

        }.ToPB());
    }

    return &*UnknownYdbRelease;
}

TStored TCompatibilityInfo::MakeStored(ui32 componentId, const TCurrent* current) {
    Y_VERIFY(current);

    TStored stored;
    stored.SetBuild(current->GetBuild());
    if (current->HasYdbVersion()) {
        stored.MutableYdbVersion()->CopyFrom(current->GetYdbVersion());
    }

    for (ui32 i = 0; i < current->StoresReadableBySize(); i++) {
        auto rule = current->GetStoresReadableBy(i);
        if (!rule.HasComponentId() || rule.GetComponentId() == componentId ||
                rule.GetComponentId() == (ui32)NKikimrConfig::TCompatibilityRule::Any) {
            auto *newRule = stored.AddReadableBy();
            if (rule.HasBuild()) {
                newRule->SetBuild(rule.GetBuild());
            }
            newRule->MutableUpperLimit()->CopyFrom(rule.GetUpperLimit());
            newRule->MutableLowerLimit()->CopyFrom(rule.GetLowerLimit());
            newRule->SetForbidden(rule.GetForbidden());
        }
    }

    return stored;
}

TStored TCompatibilityInfo::MakeStored(NKikimrConfig::TCompatibilityRule::EComponentId componentId) {
    return MakeStored((ui32)componentId, GetCurrent());
}

////////////////////////////////////////////////////////////////////////////////////////
// YDB versions are compared alphabetically, much like strings,
// but instead of chars there are 4 componets: Year, Major, Minor and Hotfix
// Each of the version's components can be absent
// If one is, than all the following components are also considered to be 'absent'
// Absent component is equal to any other, including other absent
// 
// Some examples:
// 22.1.1.1 < 22.1.2.0
// 22.2.1.0 > 22.1._._
// 23.1._._ == 23.1.1.0
//
// Function returns -1 if left < right, 0 if left == right, 1 if left > right
i32 CompareVersions(const NKikimrConfig::TYdbVersion& left, const NKikimrConfig::TYdbVersion& right) {
    if (!left.HasYear() || !right.HasYear()) {
        return 0;
    }
    if (left.GetYear() < right.GetYear()) {
        return -1; 
    } else if (left.GetYear() > right.GetYear()) {
        return 1;
    }

    if (!left.HasMajor() || !right.HasMajor()) {
        return 0;
    }
    if (left.GetMajor() < right.GetMajor()) {
        return -1; 
    } else if (left.GetMajor() > right.GetMajor()) {
        return 1;
    }

    if (!left.HasMinor() || !right.HasMinor()) {
        return 0;
    }
    if (left.GetMinor() < right.GetMinor()) {
        return -1; 
    } else if (left.GetMinor() > right.GetMinor()) {
        return 1;
    }

    if (!left.HasHotfix() || !right.HasHotfix()) {
        return 0;
    }
    if (left.GetHotfix() < right.GetHotfix()) {
        return -1; 
    } else if (left.GetHotfix() > right.GetHotfix()) {
        return 1;
    }

    return 0;
}

// If stored CompatibilityInfo is not present, we:
// - compare current to UnknownYdbRelease if current is stable version
// - consider versions compatible otherwise
bool CheckNonPresent(const TCurrent* current, ui32 componentId, TString& errorReason) {
    Y_VERIFY(current);
    if (!current->HasYdbVersion()) {
        return true;
    }
    const auto* lastUnsupported = TCompatibilityInfo::GetUnknown();
    Y_VERIFY(lastUnsupported);
    TString errorReason1;
    if (!TCompatibilityInfo::CheckCompatibility(lastUnsupported, componentId, errorReason1)) {
        errorReason = "No stored YDB version found, last unsupported release is incompatible: " + errorReason1;
        return false;
    } else {
        return true;
    }
}

// By default two stable versions are considered compatible, if their Year is the same 
// and Major differ for no more, than 1, regardless of their Build
// Two unstable versions are compatible only if they have the same Build
// Stable and non-stable versions are not compatible by default
bool CheckDefaultRules(TString currentBuild, const NKikimrConfig::TYdbVersion* currentYdbVersion, 
        TString storedBuild, const NKikimrConfig::TYdbVersion* storedYdbVersion) {
    if (!currentYdbVersion && !storedYdbVersion) {
        return currentBuild == storedBuild;
    }
    if (currentYdbVersion && storedYdbVersion) {
        if (!currentYdbVersion->HasYear() || !storedYdbVersion->HasYear()) {
            return true;
        }
        if (currentYdbVersion->GetYear() != storedYdbVersion->GetYear()) {
            return false;
        }
        if (!currentYdbVersion->HasMajor() || !storedYdbVersion->HasMajor()) {
            return true;
        }
        return std::abs((i32)currentYdbVersion->GetMajor() - (i32)storedYdbVersion->GetMajor()) <= 1;
    }

    return false;
}

bool CheckRule(std::optional<TString> build, const NKikimrConfig::TYdbVersion* version, const NKikimrConfig::TCompatibilityRule& rule) {
    if (build) {
        if (rule.HasBuild()) {
            if (rule.GetBuild() != *build) {
                return false;
            }
            if (version == nullptr) {
                return true;
            }
        } else {
            // non-stable build is incompatible with stable
            if (version == nullptr) {
                return false;
            }
        }
    } else {
        if (version == nullptr) {
            return false;
        }
        if (rule.HasBuild()) {
            return false;
        }
    }
    
    return (!rule.HasLowerLimit() || CompareVersions(*version, rule.GetLowerLimit()) > -1) &&
            (!rule.HasUpperLimit() || CompareVersions(*version, rule.GetUpperLimit()) < 1);
}

bool TCompatibilityInfo::CheckCompatibility(const TCurrent* current, const TStored* stored, ui32 componentId, TString& errorReason) {
    Y_VERIFY(current);
    if (stored == nullptr) {
        // version record is not found
        return CheckNonPresent(current, componentId, errorReason);
    }

    const auto currentBuild = current->GetBuild();
    const auto storedBuild = stored->GetBuild();
    const auto* currentYdbVersion = current->HasYdbVersion() ? &current->GetYdbVersion() : nullptr;
    const auto* storedYdbVersion = stored->HasYdbVersion() ? &stored->GetYdbVersion() : nullptr;

    bool permitted = false;
    bool useDefault = true;

    for (ui32 i = 0; i < current->CanLoadFromSize(); ++i) {
        const auto rule = current->GetCanLoadFrom(i);
        if (!rule.HasComponentId() || rule.GetComponentId() == componentId ||
                rule.GetComponentId() == (ui32)NKikimrConfig::TCompatibilityRule::Any) {
            useDefault = false;
            if (CheckRule(storedBuild, storedYdbVersion, rule)) {
                if (rule.HasForbidden() && rule.GetForbidden()) {
                    errorReason = "Stored version is explicitly prohibited";
                    return false;
                } else {
                    permitted = true;
                }
            }
        }
    }

    for (ui32 i = 0; i < stored->ReadableBySize(); ++i) {
        const auto rule = stored->GetReadableBy(i);
        if (!rule.HasComponentId() || rule.GetComponentId() == componentId ||
                rule.GetComponentId() == (ui32)NKikimrConfig::TCompatibilityRule::Any) {
            if (CheckRule(currentBuild, currentYdbVersion, rule)) {
                useDefault = false;
                if (rule.HasForbidden() && rule.GetForbidden()) {
                    errorReason = "Current version is explicitly prohibited";
                    return false;
                } else {
                    permitted = true;
                }
            }
        }
    }

    if (permitted) {
        return true;
    } else {
        if (useDefault) {
            if (CheckDefaultRules(currentBuild, currentYdbVersion, storedBuild, storedYdbVersion)) {
                return true;
            } else {
                errorReason = "Versions are not compatible by default rules";
                return false;
            }
        }
        errorReason = "Versions are not compatible by given rule sets";
        return false;
    }
}

bool TCompatibilityInfo::CheckCompatibility(const TStored* stored, ui32 componentId, TString& errorReason) {
    return CheckCompatibility(GetCurrent(), stored, componentId, errorReason);
}

void TCompatibilityInfo::Reset(TCurrent* newCurrent) {
    TGuard<TSpinLock> g(TCompatibilityInfo::LockCurrent);
    if (!CompatibilityInfo) {
        CompatibilityInfo = TCurrent();
    }
    CompatibilityInfo->CopyFrom(*newCurrent);
}

TString GetBranchName(TString url) {
    bool found = false;
    for (const char *prefix : {"arcadia.yandex.ru/arc/", "arcadia/arc/", "arcadia.arc.yandex.ru/arc/"}) {
        const char *base = url.data();
        const char *p = strstr(base, prefix);
        if (p) {
            url = url.substr(p + strlen(prefix) - base);
            found = true;
            break;
        }
    }
    if (!found) {
        return TString();
    }

    static TString suffix("/arcadia");
    if (url.EndsWith(suffix)) {
        url = url.substr(0, url.length() - suffix.length());
    } else if (url.EndsWith("/arc/trunk")) {
        url = "trunk";
    } else {
        return TString();
    }

    return url;
}

std::optional<NKikimrConfig::TYdbVersion> ParseYdbVersionFromTag(TString tag, TString delimiter = "-") {
    NKikimrConfig::TYdbVersion version;
    TVector<TString> splitted;
    Split(tag, delimiter , splitted);
    TDeque<TString> parts(splitted.begin(), splitted.end());

    if (parts.empty()) {
        // empty tag
        return std::nullopt;
    }

    // Skip "stable" if present
    if (parts.front() == "stable") {
        parts.pop_front();
    }

    // parse Major version
    ui32 year;
    ui32 major;
    if (parts.size() < 2 || !TryIntFromString<10, ui32>(parts[0], year) ||
            !TryIntFromString<10, ui32>(parts[1], major)) {
        // non-stable version, example: trunk
        return std::nullopt;
    }

    parts.pop_front();
    parts.pop_front();
    version.SetYear(year);
    version.SetMajor(major);

    if (parts.empty()) {
        // example: stable-22-1 == 22.1.1.0
        version.SetMinor(1);
        version.SetHotfix(0);
        return version;
    }

    // parse Minor version
    ui32 minor;
    if (!TryIntFromString<10, ui32>(parts.front(), minor)) {
        // example: stable-22-1-testing == 22.1.1.0
        version.SetMinor(1);
        version.SetHotfix(0);
        return version;
    }
    parts.pop_front();
    version.SetMinor(minor);

    // parse Hotfix
    ui32 hotfix;
    if (parts.empty()) {
        // example: stable-23-1-1 == 23.1.1.0
        version.SetHotfix(0);
        return version;
    }
    
    if (TryIntFromString<10, ui32>(parts.front(), hotfix)) {
        // example: stable-23-1-1-4 == 23.1.1.4
        version.SetHotfix(hotfix);
        return version;
    }
    
    if (parts.front() == "hotfix" || parts.front() == "fix") {
        parts.pop_front();
    }

    if (parts.empty()) {
        // example: stable-23-1-1-hotfix == 23.1.1.1
        version.SetHotfix(1);
        return version;
    }
    
    if (TryIntFromString<10, ui32>(parts.front(), hotfix)) {
        // example: stable-23-1-1-hotfix-7 == 23.1.1.7
        version.SetHotfix(hotfix);
        return version;
    }
    
    if (TryIntFromString<10, ui32>(parts.back(), hotfix)) {
        // example: stable-23-1-1-fix-something-important-2 == 23.1.1.2
        version.SetHotfix(hotfix);
        return version;
    } 

    // example: stable-23-1-1-whatever == 23.1.1.0
    version.SetHotfix(0);
    return version;
}

TString GetBranchString() {
    const char* begin = GetBranch();
    const char* end = begin + strlen(begin);

    while (begin != end && std::isspace(begin[0])) {
        ++begin;
    }
    while (begin != end && std::isspace(end[-1])) {
        --end;
    }

    return TString(begin, end);
}

TString GetTagString() {
    TString tag = GetTag();
    if (!tag) {
        TString branch = GetBranchString();
        for (const char* prefix : { "releases/", "tags/releases/experimental/", "tags/releases/" }) {
            if (branch.StartsWith(prefix)) {
                branch = TString(branch.begin() + strlen(prefix), branch.end());
                break;
            }
        }
        branch = tag;
    }

    for (const char* prefix : { "ydb/", "nbs/", "releases/nbs/", "releases/ydb/" , "releases/" }) {
        if (tag.StartsWith(prefix)) {
            tag = TString(tag.begin() + strlen(prefix), tag.end());
            break;
        }
    }

    return std::move(tag);
}

bool TCompatibilityInfo::CompleteFromTag(NKikimrConfig::TCurrentCompatibilityInfo& current) {
    TString tag = GetTagString();
    for (TString delim : {"-", "."}) {
        auto tryParse = ParseYdbVersionFromTag(tag, delim);
        if (tryParse) {
            auto versionFromTag = *tryParse;
            auto version = current.MutableYdbVersion();
            if (version->HasYear()) {
                Y_VERIFY_DEBUG(version->GetYear() == versionFromTag.GetYear());
            } else {
                version->SetYear(versionFromTag.GetYear());
            }
            
            if (version->HasMajor()) {
                Y_VERIFY_DEBUG(version->GetMajor() == versionFromTag.GetMajor());
            } else {
                version->SetYear(versionFromTag.GetYear());
            }

            if (versionFromTag.HasMinor()) {
                if (version->HasMinor()) {
                    Y_VERIFY_DEBUG(version->GetMinor() == versionFromTag.GetMinor());
                } else {
                    version->SetYear(versionFromTag.GetYear());
                }
            }

            if (versionFromTag.HasHotfix()) {
                if (version->HasYear()) {
                    Y_VERIFY_DEBUG(version->GetYear() == versionFromTag.GetYear());
                } else {
                    version->SetYear(versionFromTag.GetYear());
                }
            }

            return true;
        }
    }

    return false;
}

////////////////////////////////////////////
// Old Format
////////////////////////////////////////////

void CheckVersionTag() {
    if (VERSION) {
        TString branch = GetBranchString();
        const char* arcadia_url = GetArcadiaSourceUrl();

        if ((branch.StartsWith("releases/") || branch.StartsWith("tags/releases/")) && VERSION->Tag == "trunk") {
            Y_FAIL("release branch %s with ARCADIA_SOURCE_URL# %s contains VersionTag# trunk", branch.data(), arcadia_url);
        }
    }
}

using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;

bool TCompatibilityInfo::CheckCompatibility(const NKikimrConfig::TCurrentCompatibilityInfo* current,
        const TOldFormat& stored, ui32 componentId, TString& errorReason) {
    Y_VERIFY(current);

    std::optional<TString> storedBuild;

    auto storedVersion = ParseYdbVersionFromTag(stored.Tag);
    if (!storedVersion) {
        // non-stable version is stored
        if (current->GetBuild() == stored.Tag) {
            return true;
        }
        storedBuild = stored.Tag;
    }

    bool permitted = false;
    bool useDefault = true;

    for (ui32 i = 0; i < current->CanLoadFromSize(); ++i) {
        const auto rule = current->GetCanLoadFrom(i);
        if (!rule.HasComponentId() || rule.GetComponentId() == componentId ||
                rule.GetComponentId() == (ui32)NKikimrConfig::TCompatibilityRule::Any) {
            if (!rule.HasBuild()) {
                useDefault = false;
            }
            if (CheckRule(storedBuild, &*storedVersion, rule)) {
                if (rule.HasForbidden() && rule.GetForbidden()) {
                    errorReason = "Stored version is explicitly prohibited";
                    return false;
                } else {
                    permitted = true;
                }
            }
        }
    }

    if (permitted) {
        return true;
    }

    const auto* currentVersion = current->HasYdbVersion() ? &current->GetYdbVersion() : nullptr;
    for (const auto& tag : stored.AcceptedTags) {
        auto version = ParseYdbVersionFromTag(tag);
        if (storedVersion && currentVersion) {
            if (version->GetYear() == currentVersion->GetYear() &&
                    version->GetMajor() == currentVersion->GetMajor() &&
                    version->GetMinor() == currentVersion->GetMinor() &&
                    version->GetHotfix() == currentVersion->GetHotfix()) {
                return true;
            }
        } else if (!storedVersion && !currentVersion)  {
            if (current->GetBuild() == tag) {
                return true;
            }
        }
    }

    if (useDefault) {
        if (current->HasYdbVersion() && storedVersion) {
            auto currentYdbVersion = current->GetYdbVersion();
            if (!currentYdbVersion.HasYear() || !storedVersion->HasYear()) {
                return true;
            }
            if (currentYdbVersion.GetYear() != storedVersion->GetYear()) {
                errorReason = "Default rules used, stored's and current's Year differ";
                return false;
            }
            if (!currentYdbVersion.HasMajor() || !storedVersion->HasMajor()) {
                return true;
            }
            if (std::abs((i32)currentYdbVersion.GetMajor() - (i32)storedVersion->GetMajor()) <= 1) {
                return true;
            } else {
                errorReason = "Default rules used, stored's and current's Major difference is more than 1";
                return false;
            }
        } else if (!current->HasYdbVersion() && !storedVersion) {
            if (*storedBuild == current->GetBuild()) {
                return true;
            } else {
                errorReason = "Default rules used, both versions are non-stable, stored's and current's Build differ";
                return false;
            }
        } else {
            errorReason = "Default rules used, stable and non-stable versions are incompatible";
            return false;
        }
    }

    errorReason = "Version tag doesn't match any current compatibility rule, current version is not in accepted tags list";
    return false;
}

bool TCompatibilityInfo::CheckCompatibility(const TOldFormat& stored, ui32 componentId, TString& errorReason) {
    return CheckCompatibility(GetCurrent(), stored, componentId, errorReason);
}

}
