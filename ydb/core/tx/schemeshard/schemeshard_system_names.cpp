#include <util/string/builder.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/core/base/auth.h>

#include "schemeshard_system_names.h"


namespace NKikimr::NSchemeShard {

// ReservedNames and ReservedPrefixes are the lists of names and prefixes
// that are reserved exclusively for the system's use.
//
//NOTE: Updates to the both lists are regulated by the project committee and must be explicitly approved.
//
const TVector<TString> ReservedNames = {
    // Database, column store or a column table can have the directory with the system- or meta- data.
    //TODO: real name are taken from the config NKikimr::NMetadata::NProvider::TConfig
    ".metadata",

    // Database has this place for the system views.
    // (Feature flag EnableSystemViews manages it's use.)
    ".sys",

    // Database has this directory for keeping kqp-session bound temporary objects (tables etc)
    ".tmp",

    // Database has this directory for keeping backups and backup related objects
    ".backups",
};
const TVector<TString> ReservedPrefixes = {
    ".",
    "__ydb",
};

// Temporary exceptions from the ReservedNames (note: no prefix exceptions).
// The goal is to have no exceptions.
const TVector<TString> ReservedNamesExceptions = {
    // external agent checking liveness of the database creates this directory
    ".sys_health",
    // sqs/yqm employs schema dirs and objects starting with the dot
    ".AtomicCounter",
    ".Events",
    ".FIFO",
    ".Queues",
    ".Quoter",
    ".RemovedQueues",
    ".Settings",
    ".STD",
};

bool CheckReservedNameImpl(const TString& name, bool isSystemUser, bool isAdministrator, TString& explain) {
    // System reserved names can't be created by ordinary users.
    // They can only be created:
    // - by the system itself
    // - by the admin (allowing amendments if necessary)
    const bool nameIsReserved = [&name]() {
        auto it = std::find(ReservedNames.begin(), ReservedNames.end(), name);
        return (it != ReservedNames.end());
    }();
    if (nameIsReserved && !(isSystemUser || isAdministrator)) {
        explain += TStringBuilder()
            << "path part '" << name << "', name is reserved by the system: '" << name << "'"
            << "(subject: system user " << isSystemUser << ", cluster admin " << isAdministrator << ")";
        return false;
    }

    // Temporary exceptions from the ReservedNames.
    const bool nameIsException = [&name]() {
        auto it = std::find(ReservedNamesExceptions.begin(), ReservedNamesExceptions.end(), name);
        return (it != ReservedNamesExceptions.end());
    }();
    if (nameIsException) {
        return true;
    }

    // Names that aren't reserved but start with a reserved prefix can't be created at all,
    // not even by admins or the system.
    // Such names must be explicitly added to the ReservedNames list before creation is possible.
    const auto prefixFound = std::find_if(ReservedPrefixes.begin(), ReservedPrefixes.end(), [&name](const auto& prefix) {
        return name.StartsWith(prefix);
    });
    if (!nameIsReserved && (prefixFound != ReservedPrefixes.end())) {
        explain += TStringBuilder()
            << "path part '" << name << "', prefix is reserved by the system: '" << *prefixFound << "'";
        return false;
    }

    return true;
}

bool IsSystemUser(const NACLib::TUserToken* userToken) {
    return userToken && userToken->IsSystemUser();
}

bool CheckReservedName(const TString& name, const NACLib::TUserToken* userToken, const TVector<TString>& allowedSids, TString& explain) {
    return CheckReservedNameImpl(name, IsSystemUser(userToken), IsTokenAllowed(userToken, allowedSids), explain);
}

bool CheckReservedName(const TString& name, const TAppData* appData, const NACLib::TUserToken* userToken, TString& explain) {
    return CheckReservedNameImpl(name, IsSystemUser(userToken), IsAdministrator(appData, userToken), explain);
}

}  // namespace NKikimr::NSchemeShard


// For tests
namespace NSchemeShardUT_Private {

const TVector<TString>& GetReservedNames() {
    return NKikimr::NSchemeShard::ReservedNames;
}
const TVector<TString>& GetReservedPrefixes() {
    return NKikimr::NSchemeShard::ReservedPrefixes;
}
const TVector<TString>& GetReservedNamesExceptions() {
    return NKikimr::NSchemeShard::ReservedNamesExceptions;
}

}  // namespace NSchemeShardUT_Private

