#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>
#include <library/cpp/svnversion/svnversion.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/viewer/json/json.h>
#include "version.h"

namespace NKikimr {

TCompatibilityInfo CompatibilityInfo = TCompatibilityInfo{};

using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
using TStored = NKikimrConfig::TStoredCompatibilityInfo;
using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;

using EComponentId = NKikimrConfig::TCompatibilityRule;
using TComponentId = NKikimrConfig::TCompatibilityRule::EComponentId;

TCompatibilityInfo::TCompatibilityInfo() {
    using TStoredConstructor = TCompatibilityInfo::TProtoConstructor::TStoredCompatibilityInfo;
    using TVersionConstructor = TCompatibilityInfo::TProtoConstructor::TVersion;

    /////////////////////////////////////////////////////////
    // Current CompatibilityInfo
    /////////////////////////////////////////////////////////

    auto current = MakeCurrent();

    bool success = CompleteFromTag(current);
    Y_UNUSED(success);
    // Y_ABORT_UNLESS(success);

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

    EMPLACE_DEFAULT_COMPATIBILITY_INFO(PDisk, "ydb", 23, 3, 13, 0);
    EMPLACE_DEFAULT_COMPATIBILITY_INFO(VDisk, "ydb", 23, 2, 12, 0);
    EMPLACE_DEFAULT_COMPATIBILITY_INFO(BlobStorageController, "ydb", 23, 3, 13, 0);

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
    "stable-24-2",

    // compatible versions; must include all compatible old ones, including this one; version verification occurs on both
    // peers and connection is accepted if at least one of peers accepts the version of the other peer
    {
        "stable-24-1",
        "stable-24-2"
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
    Y_ABORT_UNLESS(current);

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

bool IsVersionComplete(const NKikimrConfig::TYdbVersion& version) {
    return version.HasYear() && version.HasMajor() && version.HasMinor() && version.HasHotfix();
}

// If stored CompatibilityInfo is not present, we:
// - compare current to DefaultCompatibilityInfo if current is stable version
// - consider versions compatible otherwise
bool CheckNonPresent(const TCurrent* current, TComponentId componentId, TString& errorReason) {
    Y_ABORT_UNLESS(current);
    if (!current->HasVersion()) {
        return true;
    }
    const auto* lastUnsupported = CompatibilityInfo.GetDefault(componentId);
    Y_ABORT_UNLESS(lastUnsupported);

    TString errorReason1;
    if (!CompatibilityInfo.CheckCompatibility(lastUnsupported, componentId, errorReason1)) {
        errorReason = "No stored YDB version found, default version is incompatible: " + errorReason1;
        return false;
    } else {
        return true;
    }
}

// Default rules:
// Two stable versions are compatible if their Year's are the same and their Major's differ for no more, than 1.
// Two unstable versions are compatible.
// Stable and non-stable versions are not compatible.
bool CheckDefaultRules(const NKikimrConfig::TYdbVersion* currentVersion, const NKikimrConfig::TYdbVersion* storedVersion) {
    if (!currentVersion && !storedVersion) {
        return true;
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
                // this rule is not applicable to different application
                return false;
            }
            if (!version) {
                // rule for stable versions is not applicable to trunk
                return true;
            }
        } else {
            if (!version) {
                // rule for stable versions is not applicable to trunk
                return false;
            }
        }
    } else {
        if (!version) {
            // neither application nor version is set, should not reach here
            return false;
        }
        if (rule.HasApplication()) {
            // only rules, which are common to all applications, apply to version with no application info
            return false;
        }
    }
    
    return (!rule.HasLowerLimit() || CompareVersions(*version, rule.GetLowerLimit()) > -1) &&
            (!rule.HasUpperLimit() || CompareVersions(*version, rule.GetUpperLimit()) < 1);
}

bool TCompatibilityInfo::CheckCompatibility(const TCurrent* current, const TStored* stored, TComponentId componentId, TString& errorReason) const {
    Y_ABORT_UNLESS(current);
    if (!stored) {
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
        if (CheckDefaultRules(currentVersion, storedVersion)) {
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
        Y_ABORT("Cannot complete trunk version");
    }

    TString tag = GetTagString();
    for (TString delim : {"-", "."}) {
        auto tryParse = ParseVersionFromTag(tag, delim);
        if (tryParse) {
            auto versionFromTag = *tryParse;
            auto version = current.MutableVersion();
            if (version->HasYear()) {
                Y_ABORT_UNLESS(version->GetYear() == versionFromTag.GetYear());
            } else {
                version->SetYear(versionFromTag.GetYear());
            }
            
            if (version->HasMajor()) {
                //Y_ABORT_UNLESS(version->GetMajor() == versionFromTag.GetMajor());
            } else {
                version->SetMajor(versionFromTag.GetMajor());
            }

            if (versionFromTag.HasMinor()) {
                if (version->HasMinor()) {
                    Y_ABORT_UNLESS(version->GetMinor() == versionFromTag.GetMinor());
                } else {
                    version->SetMinor(versionFromTag.GetMinor());
                }
            }

            if (versionFromTag.HasHotfix()) {
                if (version->HasHotfix()) {
                    Y_ABORT_UNLESS(version->GetHotfix() == versionFromTag.GetHotfix());
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
            Y_ABORT("release branch %s with ARCADIA_SOURCE_URL# %s contains VersionTag# trunk", branch.data(), arcadia_url);
        }
    }
}

bool TCompatibilityInfo::CheckCompatibility(const TCurrent* current, const TOldFormat& peer, TComponentId componentId, TString& errorReason) const {
    // stored version is peer version in terms of Interconnect
    Y_ABORT_UNLESS(current);
    Y_ABORT_UNLESS(componentId == EComponentId::Interconnect); // old version control is only implemented in IC

    std::optional<TString> peerApplication;

    auto peerVersion = ParseVersionFromTag(peer.Tag);
    if (!peerVersion) {
        if (!current->HasVersion()) {
            // both peer and current versions are non-stable
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
}

bool TCompatibilityInfo::CheckCompatibility(const TOldFormat& peer, TComponentId componentId, TString& errorReason) const {
    return CheckCompatibility(GetCurrent(), peer, componentId, errorReason);
}

void PrintVersion(IOutputStream& out, const NKikimrConfig::TYdbVersion& version, bool printSuffix = false) {
    if (!version.HasYear() || !version.HasMajor()) {
        out << "invalid";
    } else {
        out << version.GetYear() << '-' << version.GetMajor();
        if (version.HasMinor()) {
            out << "-" << version.GetMinor();
        } else if (printSuffix) {
            out << "-*";
            return;
        }

        if (version.HasHotfix()) {
            const ui32 hotfix = version.GetHotfix();
            if (hotfix == 1) {
                out << "-hotfix";
            } else if (hotfix > 1) {
                out << "-hotfix-" << hotfix;
            }
        } else if (printSuffix) {
            out << "-*";
        }
    }
}

void PrintCompatibilityRule(IOutputStream& out, const NKikimrConfig::TCompatibilityRule& rule) {
    if (rule.HasForbidden() && rule.GetForbidden()) {
        out << "Forbid";
    } else {
        out << "Allow";
    }

    if (rule.HasApplication()) {
        out << " application: " << rule.GetApplication() << ",";
    }

    if (rule.HasComponentId()) {
        out << " component: " << NKikimrConfig::TCompatibilityRule::EComponentId_Name(rule.GetComponentId()) << ",";
    }

    if (rule.HasLowerLimit() && rule.HasUpperLimit() && google::protobuf::util::MessageDifferencer::Equals(rule.GetLowerLimit(), rule.GetUpperLimit())) {
        if (IsVersionComplete(rule.GetLowerLimit())) {
            out << " version: ";
        } else {
            out << " versions: ";
        }
        PrintVersion(out, rule.GetLowerLimit(), true);
    } else {
        out << " bounds: ";
        if (rule.HasLowerLimit()) {
            out << "[";
            PrintVersion(out, rule.GetLowerLimit(), true);
        } else {
            out << "(-inf";
        }
        out << "; ";
        if (rule.HasUpperLimit()) {
            PrintVersion(out, rule.GetUpperLimit(), true);
            out << "]";
        } else {
            out << "+inf)";
        }
    }
}

TString TCompatibilityInfo::PrintHumanReadable(const NKikimrConfig::TCurrentCompatibilityInfo* current) const {
    TStringStream str("Current compatibility info:\n");

    // print application name
    str << "    Application: " << current->GetApplication() << "\n";

    // print version
    str << "    Version: ";
    if (current->HasVersion()) {
        PrintVersion(str, current->GetVersion());
    } else {
        str << "trunk";
    }
    str << "\n";

    // print common rule
    if (current->HasVersion() && current->GetVersion().HasYear() && current->GetVersion().HasMajor()) {
        const auto& version = current->GetVersion();
        str << "    Compatible by default with versions in range ";
        ui32 year = version.GetYear();
        ui32 major = version.GetMajor();
    
        str << "[" << year << "-";
        if (major > 1) {
            str << major - 1;
        } else {
            str << major;
        }
        str << "-*; " << year << "-" << major + 1 << "-*]\n";
    }

    // print compatibility rules
    if (current->StoresReadableBySize() > 0) {
        str << "    Additional StoresReadableBy rules:\n";
        for (const auto& rule : current->GetStoresReadableBy()) {
            str << "        ";
            PrintCompatibilityRule(str, rule);
            str << "\n";
        }
    }

    if (current->CanLoadFromSize() > 0) {
        str << "    Additional CanLoadFrom rules:\n";
        for (const auto& rule : current->GetCanLoadFrom()) {
            str << "        ";
            PrintCompatibilityRule(str, rule);
            str << "\n";
        }
    }

    if (current->CanConnectToSize() > 0) {
        str << "    Additional CanConnectTo rules:\n";
        for (const auto& rule : current->GetCanConnectTo()) {
            str << "        ";
            PrintCompatibilityRule(str, rule);
            str << "\n";
        }
    }

    return str.Str();
}

TString TCompatibilityInfo::PrintHumanReadable() const {
    return PrintHumanReadable(GetCurrent());
}

TString TCompatibilityInfo::PrintJson(const NKikimrConfig::TCurrentCompatibilityInfo* current) const {
    TStringStream str;
    TProtoToJson::ProtoToJson(str, *current);
    return str.Str();
}

TString TCompatibilityInfo::PrintJson() const {
    return PrintJson(GetCurrent());
}

} // namespace NKikimr
