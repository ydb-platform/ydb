#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::inline Dev {
namespace NSecret {

class TDescribeSecretResult;

using TAsyncDescribeSecretResult = NThreading::TFuture<TDescribeSecretResult>;

struct TExportSecretSettings : public TOperationRequestSettings<TExportSecretSettings> {};

class TSecretClient {
    class TImpl;

public:
    TSecretClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    // Publish one secret to partition 0 of an existing cluster administrator-owned topic.
    // The response contains status only; retries in the same topic are deduplicated.
    TAsyncStatus ExportSecret(const std::string& secretPath, const std::string& topicPath,
        const TExportSecretSettings& settings = TExportSecretSettings());

    TAsyncDescribeSecretResult DescribeSecret(const std::string& path,
        const NScheme::TDescribePathSettings& settings = NScheme::TDescribePathSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

class TDescribeSecretResult : public TStatus {
public:
    TDescribeSecretResult(TStatus&& status, NScheme::TSchemeEntry&& entry, uint64_t version);
    const NScheme::TSchemeEntry& GetEntry() const;
    const std::string& GetName() const;
    uint64_t GetVersion() const;

    void Out(IOutputStream& out) const;

    void SerializeTo(::Ydb::Scheme::Entry* proto) const;

private:
    NScheme::TSchemeEntry Entry_;
    uint64_t Version_ = 0;
};

} // namespace NSecret
} // namespace NYdb
