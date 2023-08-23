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
    using TVersionConstructor = TCompatibilityInfo::TProtoConstructor::TVersion;

    /////////////////////////////////////////////////////////
    // Current CompatibilityInfo
    /////////////////////////////////////////////////////////
    auto current = TCurrentConstructor{
        .Application = "trunk"
    }.ToPB();

    // bool success = CompleteFromTag(current);
    // Y_VERIFY(success);

    CurrentCompatibilityInfo.CopyFrom(current);

    /////////////////////////////////////////////////////////
    // Default CompatibilityInfo
    /////////////////////////////////////////////////////////
    DefaultCompatibilityInfo = TDefaultCompatibilityInfo{};
#define EMPLACE_DEFAULT_COMPATIBILITY_INFO(componentName, app, year, major, minor, hotfix)      \
    do {                                                                                        \
        auto& defaultInfo = DefaultCompatibilityInfo[(ui32)EComponentId::componentName];        \
        defaultInfo.emplace();                                                                  \
        defaultInfo->CopyFrom(                                                                  \
            TStoredConstructor{                                                                 \
                .Application = app,                                                             \
                .Version = TVersionConstructor{                                                 \
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

TString PrintStoredAndCurrent(const TOldFormat& peer, const TCurrent* current) {
    TStringStream str;
    str << "Peer CompatibilityInfo# { ";
    str << "Tag# " << peer.Tag;
    str << "AcceptedTag# { ";
    for (const TString& tag : peer.AcceptedTags) {
        str << tag << " ";
    }
    str << " } } ";
    TString currentStr;
    google::protobuf::TextFormat::PrintToString(*current, &currentStr);
    str << "Currrent CompatibilityInfo# { " << currentStr << " }";
    return str.Str();
}

bool CheckComponentId(const NKikimrConfig::TCompatibilityRule& rule, TComponentId componentId) {
    if (!rule.HasComponentId()) {
        return true;
    }
    const auto ruleComponentId = TComponentId(rule.GetComponentId());
    return ruleComponentId == EComponentId::Any || ruleComponentId == componentId;
}

TStored TCompatibilityInfo::MakeStored(TComponentId componentId, const TCurrent* current) const {
    Y_VERIFY(current);

    TStored stored;
    stored.SetApplication(current->GetApplication());
    if (current->HasVersion()) {
        stored.MutableVersion()->CopyFrom(current->GetVersion());
    }

    auto copyFromList = [&](const auto& current, auto& stored) {
        for (const auto& rule : current) {
            if (CheckComponentId(rule, componentId)) {
                auto *newRule = stored.AddReadableBy();
                if (rule.HasApplication()) {
                    newRule->SetApplication(rule.GetApplication());
                }
                newRule->MutableUpperLimit()->CopyFrom(rule.GetUpperLimit());
                newRule->MutableLowerLimit()->CopyFrom(rule.GetLowerLimit());
                newRule->SetForbidden(rule.GetForbidden());
            }
        }
    };

    if (componentId == EComponentId::Interconnect) {
        copyFromList(current->GetCanConnectTo(), stored);
    } else {
        copyFromList(current->GetStoresReadableBy(), stored);
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
    if (!current->HasVersion()) {
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
// and Major differ for no more, than 1, regardless of their Application
// Two unstable versions are compatible only if they have the same Application
// Stable and non-stable versions are not compatible by default
bool CheckDefaultRules(TString currentApplication, const NKikimrConfig::TYdbVersion* currentVersion, 
        TString storedApplication, const NKikimrConfig::TYdbVersion* storedVersion) {
    if (!currentVersion && !storedVersion) {
        return currentApplication == storedApplication;
    }
    if (currentVersion && storedVersion) {
        if (!currentVersion->HasYear() || !storedVersion->HasYear()) {
            return true;
        }
        if (currentVersion->GetYear() != storedVersion->GetYear()) {
            return false;
        }
        if (!currentVersion->HasMajor() || !storedVersion->HasMajor()) {
            return true;
        }
        return std::abs((i32)currentVersion->GetMajor() - (i32)storedVersion->GetMajor()) <= 1;
    }

    return false;
}

bool CheckRule(std::optional<TString> app, const NKikimrConfig::TYdbVersion* version, const NKikimrConfig::TCompatibilityRule& rule) {
    if (app) {
        if (rule.HasApplication()) {
            if (rule.GetApplication() != *app) {
                return false;
            }
            if (version == nullptr) {
                return true;
            }
        } else {
            // non-stable app is incompatible with stable
            if (version == nullptr) {
                return false;
            }
        }
    } else {
        if (version == nullptr) {
            return false;
        }
        if (rule.HasApplication()) {
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

    const auto& currentApplication = current->GetApplication();
    const auto& storedApplication = stored->GetApplication();
    const auto* currentVersion = current->HasVersion() ? &current->GetVersion() : nullptr;
    const auto* storedVersion = stored->HasVersion() ? &stored->GetVersion() : nullptr;

    bool permitted = false;

    auto checkRuleList = [&](const auto& rules, const TString& application, const NKikimrConfig::TYdbVersion* version, const TString& errorPrefix) {
        for (const auto& rule : rules) {
            if (CheckComponentId(rule, componentId)) {
                if (CheckRule(application, version, rule)) {
                    if (rule.HasForbidden() && rule.GetForbidden()) {
                        errorReason = errorPrefix + PrintStoredAndCurrent(stored, current);
                        return false;
                    } else {
                        permitted = true;
                    }
                }
            }
        }
        return true;
    };
    
    if (componentId == EComponentId::Interconnect) {
        if (!checkRuleList(current->GetCanConnectTo(), storedApplication, storedVersion, "Peer version is explicitly prohibited, ")) {
            return false;
        }
    } else {
        if (!checkRuleList(current->GetCanLoadFrom(), storedApplication, storedVersion, "Stored version is explicitly prohibited, ")) {
            return false;
        }
    }

    if (!checkRuleList(stored->GetReadableBy(), currentApplication, currentVersion, "Current version is explicitly prohibited, ")) {
        return false;
    }

    if (permitted) {
        return true;
    } else {
        if (CheckDefaultRules(currentApplication, currentVersion, storedApplication, storedVersion)) {
            return true;
        } else {
            errorReason = "Versions are not compatible neither by common rule nor by provided rule sets, "
                    + PrintStoredAndCurrent(stored, current);
            return false;
        }
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

std::optional<NKikimrConfig::TYdbVersion> ParseVersionFromTag(TString tag, TString delimiter = "-") {
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
    if (current.GetApplication() == "trunk") {
        Y_FAIL("Cannot complete trunk version");
    }

    TString tag = GetTagString();
    for (TString delim : {"-", "."}) {
        auto tryParse = ParseVersionFromTag(tag, delim);
        if (tryParse) {
            auto versionFromTag = *tryParse;
            auto version = current.MutableVersion();
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

bool TCompatibilityInfo::CheckCompatibility(const TCurrent* current, const TOldFormat& peer, TComponentId componentId, TString& errorReason) const {
    // stored version is peer version in terms of Interconnect
    Y_VERIFY(current);
    Y_VERIFY(componentId == EComponentId::Interconnect); // old version control is only implemented in IC

    std::optional<TString> peerApplication;

    auto peerVersion = ParseVersionFromTag(peer.Tag);
    if (!peerVersion) {
        // non-stable version is peer
        if (current->GetApplication() == peer.Tag) {
            return true;
        }
        peerApplication = peer.Tag;
    }

    bool permitted = false;

    for (const auto& rule : current->GetCanConnectTo()) {
        if (CheckComponentId(rule, componentId)) {
            if (CheckRule(peerApplication, &*peerVersion, rule)) {
                if (rule.HasForbidden() && rule.GetForbidden()) {
                    errorReason = "Peer version is explicitly prohibited, " + PrintStoredAndCurrent(peer, current);
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

    for (const auto& tag : peer.AcceptedTags) {
        auto version = ParseVersionFromTag(tag);
        if (version && current->HasVersion()) {
            if (CompareVersions(*version, current->GetVersion()) == 0) {
                return true;
            }
        } else if (!version && !current->HasVersion())  {
            if (current->GetApplication() == tag) {
                return true;
            }
        }
    }

    // use common rule
    if (current->HasVersion() && peerVersion) {
        const auto& currentVersion = current->GetVersion();
        if (!currentVersion.HasYear() || !peerVersion->HasYear()) {
            return true;
        }
        if (currentVersion.GetYear() != peerVersion->GetYear()) {
            errorReason = "Incompatible by common rule: peer's and current's Year differ, "
                    + PrintStoredAndCurrent(peer, current);
            return false;
        }
        if (!currentVersion.HasMajor() || !peerVersion->HasMajor()) {
            return true;
        }
        if (std::abs((i32)currentVersion.GetMajor() - (i32)peerVersion->GetMajor()) <= 1) {
            return true;
        } else {
            errorReason = "Incompatible by common rule: peer's and current's Major differ by more than 1, "
                    + PrintStoredAndCurrent(peer, current);
            return false;
        }
    } else if (!current->HasVersion() && !peerVersion) {
        if (*peerApplication == current->GetApplication()) {
            return true;
        } else {
            errorReason = "Incompatible by common rule: both versions are non-stable, peer's and current's Build differ, "
                    + PrintStoredAndCurrent(peer, current);
            return false;
        }
    } else {
        errorReason = "Incompatible by common rule: one tag is stable and other is non-stable, "
                + PrintStoredAndCurrent(peer, current);
        return false;
    }

    errorReason = "Peer version tag doesn't match any current compatibility rule, current version is not in accepted tags list, "
            + PrintStoredAndCurrent(peer, current);
    return false;
}

bool TCompatibilityInfo::CheckCompatibility(const TOldFormat& peer, TComponentId componentId, TString& errorReason) const {
    return CheckCompatibility(GetCurrent(), peer, componentId, errorReason);
}

}
