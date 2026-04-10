#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::inline Dev {
namespace NSecret {

class TDescribeSecretResult;

using TAsyncDescribeSecretResult = NThreading::TFuture<TDescribeSecretResult>;

class TSecretClient {
    class TImpl;

public:
    TSecretClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

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
