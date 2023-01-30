#include <library/cpp/svnversion/svnversion.h>
#include "version.h"

using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
using TStored = NKikimrConfig::TStoredCompatibilityInfo;


/////////////////////////////////////////////////////////////
// Global definitions
/////////////////////////////////////////////////////////////

// new version control
TCurrent* TCompatibilityInfo::CompatibilityInfo = nullptr;
TSpinLock TCompatibilityInfo::LockCurrent = TSpinLock();
const TCurrent* TCompatibilityInfo::GetCurrent() {
    TGuard<TSpinLock> g(TCompatibilityInfo::LockCurrent);

    if (!CompatibilityInfo) {
        CompatibilityInfo = new TCurrent();
        // Look for protobuf message format in ydb/core/protos/config.proto
        // To be changed in new release:
        CompatibilityInfo->SetBuild("trunk");
    }

    return CompatibilityInfo;
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
TStored* TCompatibilityInfo::UnknownYdbRelease = nullptr;
const TStored* TCompatibilityInfo::GetUnknown() {
    static TSpinLock lock;
    TGuard<TSpinLock> g(lock);

    if (!UnknownYdbRelease) {
        UnknownYdbRelease = new TStored();
        UnknownYdbRelease->SetBuild("ydb");

        auto* version = UnknownYdbRelease->MutableYdbVersion();
        version->SetYear(22);
        version->SetMajor(5);
        version->SetMinor(7);
        version->SetHotfix(0);
    }

    return UnknownYdbRelease;
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
            newRule->MutableBottomLimit()->CopyFrom(rule.GetBottomLimit());
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

// If StoredCompatibilityInfo is not present, we:
// compare current to UnknownYdbRelease, if current version is stable, otherwise
// we consider versions compatible
bool CheckNonPresent(const TCurrent* current, ui32 componentId, TString& errorReason) {
    Y_VERIFY(current);
    if (!current->HasYdbVersion()) {
        return true;
    }
    const auto* lastUnsupported = TCompatibilityInfo::GetUnknown();
    Y_VERIFY(lastUnsupported);
    TString errorReason1;
    if (!TCompatibilityInfo::CheckCompatibility(current, lastUnsupported, componentId, errorReason1)) {
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
    
    return (!rule.HasBottomLimit() || CompareVersions(*version, rule.GetBottomLimit()) > -1) &&
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
    CompatibilityInfo = newCurrent;
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

void CheckVersionTag() {
    if (VERSION) {
        const char* begin = GetBranch();
        const char* end = begin + strlen(begin);

        while (begin != end && std::isspace(begin[0])) {
            ++begin;
        }
        while (begin != end && std::isspace(end[-1])) {
            --end;
        }

        TString branch(begin, end);
        const char* arcadia_url = GetArcadiaSourceUrl();

        if ((branch.StartsWith("releases/") || branch.StartsWith("tags/releases/")) && VERSION->Tag == "trunk") {
            Y_FAIL("release branch %s with ARCADIA_SOURCE_URL# %s contains VersionTag# trunk", branch.data(), arcadia_url);
        }
    }
}

using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;

std::optional<NKikimrConfig::TYdbVersion> ParseYdbVersionFromTag(TString tag) {
    NKikimrConfig::TYdbVersion version;
    TVector<TString> splitted;
    ui32 partsCount = Split(tag, "-", splitted);

    ui32 year;
    ui32 major;

    // at least "stable", year and major should be present
    if (partsCount < 3 || splitted[0] != "stable" ||
            !TryIntFromString<10, ui32>(splitted[1], year) ||
            !TryIntFromString<10, ui32>(splitted[2], major)) {
        // if we cannot parse ydb version, we assume that tag name is build name and version is non-stable
        return std::nullopt;
    }
    version.SetYear(year);
    version.SetMajor(major);

    if (partsCount == 3) {
        version.SetMinor(1);
        version.SetHotfix(0);
        return version;
    }

    ui32 minor;
    if (!TryIntFromString<10, ui32>(splitted[3], minor)) {
        version.SetMinor(1);
        version.SetHotfix(0);
        return version;
    }
    version.SetMinor(minor);

    ui32 hotfix;

    if (partsCount == 4) {
        // example: stable-23-1-1 == 23.1.1.0
        version.SetHotfix(0);
    } else if (partsCount == 5 && TryIntFromString<10, ui32>(splitted[4], hotfix)) {
        // example: stable-23-1-1-4 == 23.1.1.4
        version.SetHotfix(hotfix);
    } else if (splitted[4] == "hotfix" || splitted[4] == "fix") {
        if (partsCount == 5) {
            // example: stable-23-1-1-hotfix == 23.1.1.1
            version.SetHotfix(1);
        } else if (partsCount == 6 && TryIntFromString<10, ui32>(splitted[5], hotfix)) {
            // example: stable-23-1-1-hotfix-7 == 23.1.1.7
            version.SetHotfix(hotfix);
        } else {
            // stable-23-1-1-hotfix-some-bug == 23.1.1.1
            version.SetHotfix(1);
        }
    } else {
        // example: stable-23-1-1-cool-release == 23.1.1.0
        version.SetHotfix(0);
    }

    return version;
}

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
