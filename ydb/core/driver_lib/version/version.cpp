#include <google/protobuf/text_format.h>
#include <library/cpp/svnversion/svnversion.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include "version.h"

namespace NKikimr {

TCompatibilityInfo CompatibilityInfo = TCompatibilityInfo{};

using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
using TStored = NKikimrConfig::TStoredCompatibilityInfo;
using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;

using EComponentId = NKikimrConfig::TCompatibilityRule;
using TComponentId = NKikimrConfig::TCompatibilityRule::EComponentId;

TCompatibilityInfo::TCompatibilityInfo() {
    using TCurrentConstructor = TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;
    using TStoredConstructor = TCompatibilityInfo::TProtoConstructor::TStoredCompatibilityInfo;
    using TYdbVersionConstructor = TCompatibilityInfo::TProtoConstructor::TYdbVersion;

    /////////////////////////////////////////////////////////
    // Current CompatibilityInfo
    /////////////////////////////////////////////////////////
    auto current = TCurrentConstructor{
        .Build = "trunk"
    }.ToPB();

    // bool success = CompleteFromTag(current);
    // Y_VERIFY(success);

    CurrentCompatibilityInfo.CopyFrom(current);

    /////////////////////////////////////////////////////////
    // Default CompatibilityInfo
    /////////////////////////////////////////////////////////
    DefaultCompatibilityInfo = TDefaultCompatibilityInfo{};
#define EMPLACE_DEFAULT_COMPATIBILITY_INFO(componentName, build, year, major, minor, hotfix)    \
    do {                                                                                        \
        auto& defaultInfo = DefaultCompatibilityInfo[(ui32)EComponentId::componentName];        \
        defaultInfo.emplace();                                                                  \
        defaultInfo->CopyFrom(                                                                  \
            TStoredConstructor{                                                                 \
                .Build = build,                                                                 \
                .YdbVersion = TYdbVersionConstructor{                                           \
                    .Year = year,                                                               \
                    .Major = major,                                                             \
                    .Minor = minor,                                                             \
                    .Hotfix = hotfix,                                                           \
                },                                                                              \
            }.ToPB()                                                                            \
        );                                                                                      \
    } while (false)

    EMPLACE_DEFAULT_COMPATIBILITY_INFO(PDisk, "ydb", 23, 2, 12, 0);
    EMPLACE_DEFAULT_COMPATIBILITY_INFO(VDisk, "ydb", 23, 2, 12, 0);
    EMPLACE_DEFAULT_COMPATIBILITY_INFO(BlobStorageController, "ydb", 23, 2, 12, 0);

#undef EMPLACE_DEFAULT_COMPATIBILITY_INFO
}

const TCurrent* TCompatibilityInfo::GetCurrent() const {
    return &CurrentCompatibilityInfo;
}

const TStored* TCompatibilityInfo::GetDefault(TComponentId componentId) const {
    const auto& info = DefaultCompatibilityInfo[componentId];
    Y_VERIFY_S(info, "Default version is not defined for component# " << NKikimrConfig::TCompatibilityRule::EComponentId_Name(componentId));
    return &*info;
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

// Auxiliary output functions
TString PrintStoredAndCurrent(const TStored* stored, const TCurrent* current) {
    TString storedStr;
    TString currentStr;
    google::protobuf::TextFormat::PrintToString(*stored, &storedStr);
    google::protobuf::TextFormat::PrintToString(*current, &currentStr);
    return TStringBuilder() << "Stored CompatibilityInfo# { " << storedStr << " } "
            "Current CompatibilityInfo# { " << currentStr << " } ";
}

TString PrintStoredAndCurrent(const TOldFormat& stored, const TCurrent* current) {
    TStringStream str;
    str << "Stored CompatibilityInfo# { ";
    str << "Tag# " << stored.Tag;
    str << "AcceptedTag# { ";
    for (const TString& tag : stored.AcceptedTags) {
        str << tag << " ";
    }
    str << " } } ";
    TString currentStr;
    google::protobuf::TextFormat::PrintToString(*current, &currentStr);
    str << "Currrent CompatibilityInfo# { " << currentStr << " }";
    return str.Str();
}

TStored TCompatibilityInfo::MakeStored(TComponentId componentId, const TCurrent* current) const {
    Y_VERIFY(current);

    TStored stored;
    stored.SetBuild(current->GetBuild());
    if (current->HasYdbVersion()) {
        stored.MutableYdbVersion()->CopyFrom(current->GetYdbVersion());
    }

    for (ui32 i = 0; i < current->StoresReadableBySize(); i++) {
        auto rule = current->GetStoresReadableBy(i);
        const auto ruleComponentId = TComponentId(rule.GetComponentId());
        if (!rule.HasComponentId() || ruleComponentId == componentId || ruleComponentId == EComponentId::Any) {
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

TStored TCompatibilityInfo::MakeStored(TComponentId componentId) const {
    return MakeStored(componentId, GetCurrent());
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
// - compare current to DefaultCompatibilityInfo if current is stable version
// - consider versions compatible otherwise
bool CheckNonPresent(const TCurrent* current, TComponentId componentId, TString& errorReason) {
    Y_VERIFY(current);
    if (!current->HasYdbVersion()) {
        return true;
    }
    const auto* lastUnsupported = CompatibilityInfo.GetDefault(componentId);
    Y_VERIFY(lastUnsupported);

    TString errorReason1;
    if (!CompatibilityInfo.CheckCompatibility(lastUnsupported, componentId, errorReason1)) {
        errorReason = "No stored YDB version found, default version is incompatible: " + errorReason1;
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

bool TCompatibilityInfo::CheckCompatibility(const TCurrent* current, const TStored* stored, TComponentId componentId, TString& errorReason) const {
    Y_VERIFY(current);
    if (stored == nullptr) {
        // version record is not found
        return CheckNonPresent(current, componentId, errorReason);
    }

    const auto& currentBuild = current->GetBuild();
    const auto& storedBuild = stored->GetBuild();
    const auto* currentYdbVersion = current->HasYdbVersion() ? &current->GetYdbVersion() : nullptr;
    const auto* storedYdbVersion = stored->HasYdbVersion() ? &stored->GetYdbVersion() : nullptr;

    bool permitted = false;
    bool useDefault = true;

    for (ui32 i = 0; i < current->CanLoadFromSize(); ++i) {
        const auto rule = current->GetCanLoadFrom(i);
        const auto ruleComponentId = TComponentId(rule.GetComponentId());
        if (!rule.HasComponentId() || ruleComponentId == componentId || ruleComponentId == EComponentId::Any) {
            bool isForbidding = rule.HasForbidden() && rule.GetForbidden();
            if ((!rule.HasBuild() || rule.GetBuild() == storedBuild) && !isForbidding) {
                useDefault = false;
            }
            if (CheckRule(storedBuild, storedYdbVersion, rule)) {
                if (isForbidding) {
                    errorReason = "Stored version is explicitly prohibited, " + PrintStoredAndCurrent(stored, current);
                    return false;
                } else {
                    permitted = true;
                }
            }
        }
    }

    for (ui32 i = 0; i < stored->ReadableBySize(); ++i) {
        const auto rule = stored->GetReadableBy(i);
        const auto ruleComponentId = TComponentId(rule.GetComponentId());
        if (!rule.HasComponentId() || ruleComponentId == componentId || ruleComponentId == EComponentId::Any) {
            if (CheckRule(currentBuild, currentYdbVersion, rule)) {
                useDefault = false;
                if (rule.HasForbidden() && rule.GetForbidden()) {
                    errorReason = "Current version is explicitly prohibited, " + PrintStoredAndCurrent(stored, current);
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
                errorReason = "Versions are not compatible by default rules, " + PrintStoredAndCurrent(stored, current);
                return false;
            }
        }
        errorReason = "Versions are not compatible by given rule sets, " + PrintStoredAndCurrent(stored, current);
        return false;
    }
}

bool TCompatibilityInfo::CheckCompatibility(const TStored* stored, TComponentId componentId, TString& errorReason) const {
    return CheckCompatibility(GetCurrent(), stored, componentId, errorReason);
}

void TCompatibilityInfo::Reset(TCurrent* newCurrent) {
    CurrentCompatibilityInfo.CopyFrom(*newCurrent);
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
        // example: stable-22-1 == 22.1
        // major version, from which minor tags are formed
        return version;
    }

    // parse Minor version
    ui32 minor;
    if (!TryIntFromString<10, ui32>(parts.front(), minor)) {
        // example: stable-22-1-testing == 22.1
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
        tag = branch;
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
    if (current.GetBuild() == "trunk") {
        Y_FAIL("Cannot complete trunk version");
    }

    TString tag = GetTagString();
    for (TString delim : {"-", "."}) {
        auto tryParse = ParseYdbVersionFromTag(tag, delim);
        if (tryParse) {
            auto versionFromTag = *tryParse;
            auto version = current.MutableYdbVersion();
            if (version->HasYear()) {
                Y_VERIFY(version->GetYear() == versionFromTag.GetYear());
            } else {
                version->SetYear(versionFromTag.GetYear());
            }
            
            if (version->HasMajor()) {
                Y_VERIFY(version->GetMajor() == versionFromTag.GetMajor());
            } else {
                version->SetMajor(versionFromTag.GetMajor());
            }

            if (versionFromTag.HasMinor()) {
                if (version->HasMinor()) {
                    Y_VERIFY(version->GetMinor() == versionFromTag.GetMinor());
                } else {
                    version->SetMinor(versionFromTag.GetMinor());
                }
            }

            if (versionFromTag.HasHotfix()) {
                if (version->HasHotfix()) {
                    Y_VERIFY(version->GetHotfix() == versionFromTag.GetHotfix());
                } else {
                    version->SetHotfix(versionFromTag.GetHotfix());
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

bool TCompatibilityInfo::CheckCompatibility(const TCurrent* current, const TOldFormat& stored, TComponentId componentId, TString& errorReason) const {
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
        const auto ruleComponentId = TComponentId(rule.GetComponentId());
        if (!rule.HasComponentId() || ruleComponentId == componentId || ruleComponentId == EComponentId::Any) {
            bool isForbidding = rule.HasForbidden() && rule.GetForbidden();
            if (!rule.HasBuild() && !isForbidding) {
                useDefault = false;
            }
            if (CheckRule(storedBuild, &*storedVersion, rule)) {
                if (isForbidding) {
                    errorReason = "Stored version is explicitly prohibited, " + PrintStoredAndCurrent(stored, current);
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
                errorReason = "Default rules used, stored's and current's Year differ, "
                        + PrintStoredAndCurrent(stored, current);
                return false;
            }
            if (!currentYdbVersion.HasMajor() || !storedVersion->HasMajor()) {
                return true;
            }
            if (std::abs((i32)currentYdbVersion.GetMajor() - (i32)storedVersion->GetMajor()) <= 1) {
                return true;
            } else {
                errorReason = "Default rules used, stored's and current's Major difference is more than 1, "
                        + PrintStoredAndCurrent(stored, current);
                return false;
            }
        } else if (!current->HasYdbVersion() && !storedVersion) {
            if (*storedBuild == current->GetBuild()) {
                return true;
            } else {
                errorReason = "Default rules used, both versions are non-stable, stored's and current's Build differ, "
                        + PrintStoredAndCurrent(stored, current);
                return false;
            }
        } else {
            errorReason = "Default rules used, stable and non-stable versions are incompatible, "
                    + PrintStoredAndCurrent(stored, current);
            return false;
        }
    }

    errorReason = "Version tag doesn't match any current compatibility rule, current version is not in accepted tags list, "
            + PrintStoredAndCurrent(stored, current);
    return false;
}

bool TCompatibilityInfo::CheckCompatibility(const TOldFormat& stored, TComponentId componentId, TString& errorReason) const {
    return CheckCompatibility(GetCurrent(), stored, componentId, errorReason);
}

}
