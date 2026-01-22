#pragma once

#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

#include <ydb/library/login/protos/login.pb.h>

namespace NKikimr::NSasl {

// TTrueAtomicSharedPtr provides thread-safe operations with different guarantees
// depending on the platform architecture:
//
// x86_64 (amd64): Wait-free guarantee is provided for copying and destroying
// shared pointers. The implementation uses 48 usable address bits and stores
// reference counters in the high bits of 64-bit pointers.
//
// aarch64 (ARM64): Wait-free guarantee is NOT provided. The implementation uses
// 52 usable address bits but relies on atomic operations that are only lock-free
// (not wait-free) on this architecture. This requires more sophisticated algorithms
// to achieve wait-free guarantees using lock-free atomic operations.
//
// When wait-free guarantees are not provided, update operations
// for user credentials may experience slowdowns. This means that delays may be
// observed during:
// - User creation/deletion
// - User credentials modification
// - Database creation/deletion
//
// These delays occur because the underlying atomic operations may need to retry
// in case of contention, rather than completing in a bounded number of steps.

class TStaticCredentialsProvider {
    using THashInitParams = std::unordered_map<NLoginProto::EHashType::HashType, std::string>;
    using TDatabaseUsersHashes = TTrueAtomicSharedPtr<std::unordered_map<std::string, THashInitParams>>;

public:
    enum ELookupResultCode {
        Success,
        UnknownDatabase,
        UnknownUser,
    };

public:
    TStaticCredentialsProvider(const TStaticCredentialsProvider&) = delete;
    TStaticCredentialsProvider(TStaticCredentialsProvider&&) = delete;
    TStaticCredentialsProvider& operator=(const TStaticCredentialsProvider&) = delete;
    TStaticCredentialsProvider& operator=(TStaticCredentialsProvider&&) = delete;

    static TStaticCredentialsProvider& GetInstance();

    std::pair<ELookupResultCode, THashInitParams> GetUserHashInitParams(const std::string& database,
        const std::string& username) const;
    void UpdateDatabaseUsers(const NLoginProto::TSecurityState& securityState);
    void DeleteDatabaseUsers(const std::string& database);

private:
    TStaticCredentialsProvider();

private:
    TTrueAtomicSharedPtr<std::unordered_map<std::string, TDatabaseUsersHashes>> StaticCredsStorage;

};

} // namespace NKikimr::NSasl
