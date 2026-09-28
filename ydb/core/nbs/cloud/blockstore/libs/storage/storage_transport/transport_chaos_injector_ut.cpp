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

TDDiskId MakePersistentBufferId(ui32 nodeId)
{
    TDDiskId id;
    id.SetNodeId(nodeId);
    id.SetPDiskId(1);
    id.SetDDiskSlotId(1);
    return id;
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

    Y_UNIT_TEST(ShouldReplaceRepliesFromDisabledPersistentBufferNodes)
    {
        auto underlying = std::make_shared<TStorageTransportMock>();
        auto injector = CreateTransportChaosInjector(underlying);
        injector->DisableNode(43);

        const TVector<TDDiskId> persistentBufferIds = {
            MakePersistentBufferId(42),
            MakePersistentBufferId(43),
            MakePersistentBufferId(44),
        };

        ui32 callbackCount = 0;
        IStorageTransport::TEvWriteToManyPersistentBuffersResult response;
        injector->WriteToManyPBuffers(
            MakeConnection(42),
            {},
            1,
            NKikimr::NDDisk::TWriteInstruction(0),
            persistentBufferIds,
            TDuration::Seconds(1),
            {},
            nullptr,
            [&callbackCount, &response](const auto& result, auto)
            {
                ++callbackCount;
                response = result;
            });

        UNIT_ASSERT_VALUES_EQUAL(1, callbackCount);
        UNIT_ASSERT_VALUES_EQUAL(
            persistentBufferIds.size(),
            underlying->LastWriteToManyPBuffersDiskIds.size());
        for (size_t i = 0; i < persistentBufferIds.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                persistentBufferIds[i].GetNodeId(),
                underlying->LastWriteToManyPBuffersDiskIds[i].GetNodeId());
        }

        UNIT_ASSERT_VALUES_EQUAL(3, response.ResultSize());
        UNIT_ASSERT(
            response.GetResult(0).GetResult().GetStatus() == TReplyStatus::OK);
        AssertUndelivered(response.GetResult(1).GetResult());
        UNIT_ASSERT(
            response.GetResult(2).GetResult().GetStatus() == TReplyStatus::OK);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
