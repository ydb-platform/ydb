#pragma once

#include <yt/yt/client/api/connection.h>

#include <yt/yt/client/hive/transaction_participant.h>

#include <yt/yt/client/ypath/rich.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TMockConnection
    : public IConnection
{
public:
    MOCK_METHOD(TClusterTag, GetClusterTag, (), (const, override));
    MOCK_METHOD(const TString&, GetLoggingTag, (), (const, override));
    MOCK_METHOD(const TString&, GetClusterId, (), (const, override));
    MOCK_METHOD(const std::optional<std::string>&, GetClusterName, (), (const, override));

    MOCK_METHOD(IInvokerPtr, GetInvoker, (), (override));

    MOCK_METHOD(bool, IsSameCluster, (const IConnectionPtr&), (const, override));

    MOCK_METHOD(IClientPtr, CreateClient, (const TClientOptions& options), (override));

    MOCK_METHOD(NHiveClient::ITransactionParticipantPtr, CreateTransactionParticipant,
        (NHiveClient::TCellId,
        const TTransactionParticipantOptions&),
        (override));

    MOCK_METHOD(void, ClearMetadataCaches, (), (override));
    MOCK_METHOD(void, Terminate, (), (override));

    MOCK_METHOD(NYson::TYsonString, GetConfigYson, (), (const, override));
};

DEFINE_REFCOUNTED_TYPE(TMockConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
