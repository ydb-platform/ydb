#include <library/cpp/svnversion/svnversion.h>
#include "version.h"

NKikimrConfig::TCurrentCompatibilityInfo* TCompatibilityInfo::CompatibilityInfo = nullptr;
TSpinLock TCompatibilityInfo::LockCurrent = TSpinLock();
const NKikimrConfig::TCurrentCompatibilityInfo* TCompatibilityInfo::GetCurrent() {
    TGuard<TSpinLock> g(TCompatibilityInfo::LockCurrent);

    if (!CompatibilityInfo) {
        CompatibilityInfo = new NKikimrConfig::TCurrentCompatibilityInfo();
        // Look for protobuf message format in ydb/core/protos/config.proto
        // To be changed in new release:
        CompatibilityInfo->SetBuild("trunk");
    }

    return CompatibilityInfo;
}

// Last stable YDB release, which doesn't include version control change
// When the compatibility information is not present in component's data,
// we assume component's version to be this version
NKikimrConfig::TStoredCompatibilityInfo* TCompatibilityInfo::UnknownYdbRelease = nullptr;
const NKikimrConfig::TStoredCompatibilityInfo* TCompatibilityInfo::GetUnknown() {
    static TSpinLock lock;
    TGuard<TSpinLock> g(lock);

    if (!UnknownYdbRelease) {
        UnknownYdbRelease = new NKikimrConfig::TStoredCompatibilityInfo();
        UnknownYdbRelease->SetBuild("ydb");

        auto* version = UnknownYdbRelease->MutableYdbVersion();
        version->SetYear(22);
        version->SetMajor(5);
        version->SetMinor(7);
        version->SetHotfix(0);
    }

    return UnknownYdbRelease;
}

NKikimrConfig::TStoredCompatibilityInfo TCompatibilityInfo::MakeStored(ui32 componentId,
        const NKikimrConfig::TCurrentCompatibilityInfo* current) {
    Y_VERIFY(current);

    NKikimrConfig::TStoredCompatibilityInfo stored;
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

NKikimrConfig::TStoredCompatibilityInfo TCompatibilityInfo::MakeStored(
        NKikimrConfig::TCompatibilityRule::EComponentId componentId) {
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
bool CheckNonPresent(const NKikimrConfig::TCurrentCompatibilityInfo* current,
        ui32 componentId, TString& errorReason) {
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
        if (!currentYdbVersion->HasMajor() || !storedYdbVersion->HasMajor()) {
            return true;
        }
        return currentYdbVersion->GetYear() == storedYdbVersion->GetYear() && 
                std::abs((i32)currentYdbVersion->GetMajor() - (i32)storedYdbVersion->GetMajor()) <= 1;
    }

    return false;
}

bool CheckRule(TString build, const NKikimrConfig::TYdbVersion* version, 
        const NKikimrConfig::TCompatibilityRule& rule) {
    if (rule.HasBuild()) {
        if (rule.GetBuild() != build) {
            return false;
        }
        if (version == nullptr) {
            return true;
        }
    } else {
        if (version == nullptr) {
            return false;
        }
    }
    
    return (!rule.HasBottomLimit() || CompareVersions(*version, rule.GetBottomLimit()) > -1) &&
            (!rule.HasUpperLimit() || CompareVersions(*version, rule.GetUpperLimit()) < 1);
}

bool TCompatibilityInfo::CheckCompatibility(const NKikimrConfig::TCurrentCompatibilityInfo* current,
        const NKikimrConfig::TStoredCompatibilityInfo* stored, ui32 componentId, TString& errorReason) {
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

bool TCompatibilityInfo::CheckCompatibility(const NKikimrConfig::TStoredCompatibilityInfo* stored,
        ui32 componentId, TString& errorReason) {
    return CheckCompatibility(GetCurrent(), stored, componentId, errorReason);
}

void TCompatibilityInfo::Reset(NKikimrConfig::TCurrentCompatibilityInfo* newCurrent) {
    TGuard<TSpinLock> g(TCompatibilityInfo::LockCurrent);
    CompatibilityInfo = newCurrent;
}

void TCompatibilityInfoTest::Reset(NKikimrConfig::TCurrentCompatibilityInfo* newCurrent) {
    TCompatibilityInfo::Reset(newCurrent);
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
