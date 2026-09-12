#include "ic_storage_transport_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>

#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {
namespace {

using namespace NActors;
using namespace NKikimr;
using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

// Exercises the wire handshake while retaining control of each PB reply.
struct TRegistrationFixture
{
    TTestActorSystem Runtime{1};
    TActorId Peer;
    TActorId Transport;
    TActorId Service = MakeBlobStoragePersistentBufferId(1, 1, 1);
    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult> Future;

    TRegistrationFixture()
    {
        Runtime.Start();
        Peer = Runtime.AllocateEdgeActor(1, __FILE__, __LINE__);
        Runtime.RegisterService(Service, Peer);
        Transport =
            Runtime.Register(new TICStorageTransportActor({}, 7, true), 1);
        auto request = std::make_unique<TEvTransportPrivate::TEvConnect>(
            Service,
            NDDisk::TQueryCredentials::ToPersistentBuffer(
                42,
                3,
                std::nullopt,
                7));
        Future = request->ConnectPromise.GetFuture();
        Runtime.Send(new IEventHandle(Transport, Peer, request.release()), 1);
        auto connect =
            Runtime.WaitForEdgeActorEvent<NDDisk::TEvConnect>(Peer, false);
        auto reply = std::make_unique<NDDisk::TEvConnectResult>();
        reply->Record.SetStatus(TStatus::OK);
        reply->Record.SetDDiskInstanceGuid(123);
        Runtime.Send(
            new IEventHandle(
                Transport,
                Peer,
                reply.release(),
                0,
                connect->Cookie),
            1);
    }

    ~TRegistrationFixture()
    {
        Runtime.Stop();
    }

    void Complete(TStatus::E registrationStatus, TStatus::E listStatus)
    {
        auto registration =
            Runtime.WaitForEdgeActorEvent<NDDisk::TEvRegisterPersistentBuffer>(
                Peer,
                false);
        UNIT_ASSERT(!Future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            registration->Get()->Record.GetTimestampMicroseconds(),
            Runtime.GetClock().MicroSeconds());
        Runtime.Send(
            new IEventHandle(
                Transport,
                Peer,
                new NDDisk::TEvRegisterPersistentBufferResult(
                    registrationStatus),
                0,
                registration->Cookie),
            1);
        if (registrationStatus == TStatus::OK ||
            registrationStatus == TStatus::INCORRECT_REQUEST)
        {
            auto list =
                Runtime.WaitForEdgeActorEvent<NDDisk::TEvListPersistentBuffer>(
                    Peer,
                    false);
            UNIT_ASSERT(!Future.HasValue());
            auto reply =
                std::make_unique<NDDisk::TEvListPersistentBufferResult>();
            reply->Record.SetStatus(listStatus);
            Runtime.Send(
                new IEventHandle(
                    Transport,
                    Peer,
                    reply.release(),
                    0,
                    list->Cookie),
                1);
        }
        Runtime.Sim([&] { return !Future.HasValue(); });
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TICStorageTransportRegistrationTest)
{
    Y_UNIT_TEST(WaitsForRegistrationAndProbe)
    {
        TRegistrationFixture fixture;
        fixture.Complete(TStatus::OK, TStatus::OK);
        UNIT_ASSERT(fixture.Future.GetValue().GetStatus() == TStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(
            fixture.Future.GetValue().GetDDiskInstanceGuid(),
            123);
    }

    Y_UNIT_TEST(ExistingRegistrationMustStillBeServed)
    {
        for (auto status: {TStatus::OK, TStatus::OUTDATED}) {
            TRegistrationFixture fixture;
            fixture.Complete(TStatus::INCORRECT_REQUEST, status);
            UNIT_ASSERT(fixture.Future.GetValue().GetStatus() == status);
        }
    }

    Y_UNIT_TEST(PropagatesRegistrationFailure)
    {
        TRegistrationFixture fixture;
        fixture.Complete(TStatus::ERROR, TStatus::UNKNOWN);
        UNIT_ASSERT(fixture.Future.GetValue().GetStatus() == TStatus::ERROR);
    }

    Y_UNIT_TEST(RetriesBusyRegistrationWithoutReconnecting)
    {
        TRegistrationFixture fixture;
        auto request =
            fixture.Runtime
                .WaitForEdgeActorEvent<NDDisk::TEvRegisterPersistentBuffer>(
                    fixture.Peer,
                    false);
        fixture.Runtime.Send(
            new IEventHandle(
                fixture.Transport,
                fixture.Peer,
                new NDDisk::TEvRegisterPersistentBufferResult(TStatus::BUSY),
                0,
                request->Cookie),
            1);
        fixture.Complete(TStatus::OK, TStatus::OK);
        UNIT_ASSERT(fixture.Future.GetValue().GetStatus() == TStatus::OK);
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
