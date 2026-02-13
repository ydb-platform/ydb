#include "static_credentials_provider.h"

namespace NKikimr::NSasl {

TStaticCredentialsProvider::TStaticCredentialsProvider()
    : StaticCredsStorage(new std::unordered_map<std::string, TDatabaseUsersHashes>())
{
}

TStaticCredentialsProvider& TStaticCredentialsProvider::GetInstance() {
    static TStaticCredentialsProvider instance;
    return instance;
}

std::pair<TStaticCredentialsProvider::ELookupResultCode, std::unordered_map<NLoginProto::EHashType::HashType, std::string>>
TStaticCredentialsProvider::GetUserHashInitParams(const std::string& database, const std::string& username) const
{
    TTrueAtomicSharedPtr staticCredsStorage(StaticCredsStorage);
    auto itDb = staticCredsStorage->find(database);
    if (itDb == staticCredsStorage->end()) {
        return {ELookupResultCode::UnknownDatabase, {}};
    }

    TTrueAtomicSharedPtr databaseUsersHashes(itDb->second);
    auto itUser = databaseUsersHashes->find(username);
    if (itUser == databaseUsersHashes->end()) {
        return {ELookupResultCode::UnknownUser, {}};
    }

    return {ELookupResultCode::Success, itUser->second};
}

// Updates user credentials for a specific database.
// Thread-safety is achieved through copy-on-write semantics using TTrueAtomicSharedPtr.
// When copying TTrueAtomicSharedPtr, only reference counters are incremented, not the underlying data.
// New TTrueAtomicSharedPtr objects are created to avoid corrupting memory that may be accessed
// by other threads still holding references to the old data.
void TStaticCredentialsProvider::UpdateDatabaseUsers(const NLoginProto::TSecurityState& securityState) {
    const auto& database = securityState.GetAudience();
    // Create a new map for updated user hashes
    TTrueAtomicSharedPtr usersHashes(new std::unordered_map<std::string, THashInitParams>());
    usersHashes->reserve(securityState.SidsSize());
    for (const auto& sid : securityState.GetSids()) {
        if (sid.GetType() == NLoginProto::ESidType::USER) {
            THashInitParams userHashesInitParams;
            userHashesInitParams.reserve(sid.HashesInitParamsSize());
            for (const auto& hashInitParams : sid.GetHashesInitParams()) {
                userHashesInitParams.emplace(hashInitParams.GetHashType(), hashInitParams.GetInitParams());
            }

            usersHashes->emplace(sid.GetName(), std::move(userHashesInitParams));
        }
    }

    // Copy the shared pointer (only increments reference counter, doesn't copy data)
    TTrueAtomicSharedPtr staticCredsStorage(StaticCredsStorage);
    auto itDb = staticCredsStorage->find(database);
    if (itDb == staticCredsStorage->end()) {
        // Database doesn't exist - create a new storage map to avoid modifying the existing one
        // that other threads might be reading. This ensures thread-safe copy-on-write behavior.
        TTrueAtomicSharedPtr newStaticCredsStorage(new std::unordered_map<std::string, TDatabaseUsersHashes>);
        // Copy all existing databases (only increments reference counters for nested shared pointers)
        for (const auto& [databaseName, databaseUsersHashes] : *staticCredsStorage) {
            newStaticCredsStorage->emplace(databaseName, databaseUsersHashes);
        }
        // Add the new database
        newStaticCredsStorage->emplace(database, usersHashes);

        // Atomically swap the storage pointer
        StaticCredsStorage.swap(newStaticCredsStorage);
    } else {
        // Database exists - swap the user hashes for this database only
        itDb->second.swap(usersHashes);
    }
}

// Deletes all user credentials for a specific database.
// Thread-safety is achieved through copy-on-write semantics using TTrueAtomicSharedPtr.
// When copying TTrueAtomicSharedPtr, only reference counters are incremented, not the underlying data.
// A new TTrueAtomicSharedPtr object is created to avoid corrupting memory that may be accessed
// by other threads still holding references to the old data.
void TStaticCredentialsProvider::DeleteDatabaseUsers(const std::string& database) {
    // Copy the shared pointer (only increments reference counter, doesn't copy data)
    TTrueAtomicSharedPtr staticCredsStorage(StaticCredsStorage);
    auto itDb = staticCredsStorage->find(database);
    if (itDb != staticCredsStorage->end()) {
        // Database exists - create a new storage map to avoid modifying the existing one
        // that other threads might be reading. This ensures thread-safe copy-on-write behavior.
        TTrueAtomicSharedPtr newStaticCredsStorage(new std::unordered_map<std::string, TDatabaseUsersHashes>);
        // Copy all databases except the one being deleted (only increments reference counters for nested shared pointers)
        for (const auto& [databaseName, databaseUsersHashes] : *staticCredsStorage) {
            if (databaseName != database) {
                newStaticCredsStorage->emplace(databaseName, databaseUsersHashes);
            }
        }

        // Atomically swap the storage pointer
        StaticCredsStorage.swap(newStaticCredsStorage);
    }
}

} // namespace NKikimr::NSasl
