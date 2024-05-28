#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/vector.h>

namespace Ydb::Replication {
    class ConnectionParams;
    class DescribeReplicationResult;
}

namespace NYdb {
    class TProtoAccessor;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NReplication {

class TDescribeReplicationResult;
using TAsyncDescribeReplicationResult = NThreading::TFuture<TDescribeReplicationResult>;
struct TDescribeReplicationSettings: public TOperationRequestSettings<TDescribeReplicationSettings> {};

struct TStaticCredentials {
    TString User;
    TString PasswordSecretName;
};

struct TOAuthCredentials {
    TString TokenSecretName;
};

class TConnectionParams: private TCommonClientSettings {
public:
    enum class ECredentials {
        Static,
        OAuth,
    };

    explicit TConnectionParams(const Ydb::Replication::ConnectionParams& params);

    const TString& GetDiscoveryEndpoint() const;
    const TString& GetDatabase() const;

    ECredentials GetCredentials() const;
    const TStaticCredentials& GetStaticCredentials() const;
    const TOAuthCredentials& GetOAuthCredentials() const;

private:
    std::variant<
        TStaticCredentials,
        TOAuthCredentials
    > Credentials_;
};

struct TRunningState {};
struct TDoneState {};

class TErrorState {
    class TImpl;

public:
    explicit TErrorState(NYql::TIssues&& issues);

    const NYql::TIssues& GetIssues() const;

private:
    std::shared_ptr<TImpl> Impl_;
};

class TReplicationDescription {
public:
    struct TItem {
        TString SrcPath;
        TString DstPath;
    };

    enum class EState {
        Running,
        Error,
        Done,
    };

    explicit TReplicationDescription(const Ydb::Replication::DescribeReplicationResult& desc);

    const TConnectionParams& GetConnectionParams() const;
    const TVector<TItem> GetItems() const;

    EState GetState() const;
    const TRunningState& GetRunningState() const;
    const TErrorState& GetErrorState() const;
    const TDoneState& GetDoneState() const;

private:
    TConnectionParams ConnectionParams_;
    TVector<TItem> Items_;
    std::variant<
        TRunningState,
        TErrorState,
        TDoneState
    > State_;
};

class TDescribeReplicationResult: public NScheme::TDescribePathResult {
    friend class NYdb::TProtoAccessor;
    const Ydb::Replication::DescribeReplicationResult& GetProto() const;

public:
    TDescribeReplicationResult(TStatus&& status, Ydb::Replication::DescribeReplicationResult&& desc);
    const TReplicationDescription& GetReplicationDescription() const;

private:
    TReplicationDescription ReplicationDescription_;
    std::unique_ptr<Ydb::Replication::DescribeReplicationResult> Proto_;
};

class TReplicationClient {
    class TImpl;

public:
    TReplicationClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncDescribeReplicationResult DescribeReplication(const TString& path,
        const TDescribeReplicationSettings& settings = TDescribeReplicationSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NReplication
