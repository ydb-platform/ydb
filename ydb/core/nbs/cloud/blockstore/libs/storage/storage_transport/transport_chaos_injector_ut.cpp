#include "transport_chaos_injector.h"

#include "storage_transport_mock.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NKikimrBlobStorage::NDDisk;

namespace {

////////////////////////////////////////////////////////////////////////////////

THostConnection MakeConnection(ui32 nodeId)
{
    return {
        .ConnectionType = THostConnection::EConnectionType::DDisk,
        .DDiskId = {nodeId, 1, 1},
    };
}

void AssertUndelivered(const auto& result)
{
    UNIT_ASSERT(result.GetStatus() == TReplyStatus::ERROR);
    UNIT_ASSERT_STRINGS_EQUAL(UndeliveryErrorMessage, result.GetErrorReason());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTransportChaosInjectorTest)
{
    Y_UNIT_TEST(ShouldDelegateRequestsToEnabledNodes)
    {
        auto underlying = std::make_shared<TStorageTransportMock>();
        underlying->WriteToDDiskStatus = TReplyStatus::OUTDATED;
        auto injector = CreateTransportChaosInjector(std::move(underlying));

        const auto result = injector->WriteToDDisk(
            MakeConnection(42),
            {},
            NKikimr::NDDisk::TWriteInstruction(0),
            {},
            nullptr);

        UNIT_ASSERT(
            result.GetValueSync().GetStatus() == TReplyStatus::OUTDATED);
    }

    Y_UNIT_TEST(ShouldReturnUndeliveredForDisabledNodes)
    {
        auto injector = CreateTransportChaosInjector(
            std::make_shared<TStorageTransportMock>());
        injector->DisableNode(42);
        UNIT_ASSERT(injector->IsNodeDisabled(42));

        const auto connection = MakeConnection(42);
        AssertUndelivered(
            injector->Connect(connection).ConnectFuture.GetValueSync());
        AssertUndelivered(
            injector->ReadFromDDisk(connection, {}, {}, {}, nullptr)
                .GetValueSync());
        AssertUndelivered(injector
                              ->WriteToDDisk(
                                  connection,
                                  {},
                                  NKikimr::NDDisk::TWriteInstruction(0),
                                  {},
                                  nullptr)
                              .GetValueSync());
        AssertUndelivered(
            injector->DeleteTabletChunks(connection).GetValueSync());
    }

    Y_UNIT_TEST(ShouldDelegateAgainAfterNodeIsEnabled)
    {
        auto injector = CreateTransportChaosInjector(
            std::make_shared<TStorageTransportMock>());
        injector->DisableNode(42);
        injector->EnableNode(42);
        UNIT_ASSERT(!injector->IsNodeDisabled(42));

        const auto result = injector->WriteToDDisk(
            MakeConnection(42),
            {},
            NKikimr::NDDisk::TWriteInstruction(0),
            {},
            nullptr);

        UNIT_ASSERT(result.GetValueSync().GetStatus() == TReplyStatus::OK);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
