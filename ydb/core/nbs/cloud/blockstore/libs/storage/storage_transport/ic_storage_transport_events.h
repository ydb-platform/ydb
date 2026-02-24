#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

struct TEvICStorageTransportPrivate
{
    struct TConnect
    {
        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        NThreading::TPromise<NKikimrBlobStorage::NDDisk::TEvConnectResult>
            Promise;

        TConnect(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials credentials,
            NThreading::TPromise<NKikimrBlobStorage::NDDisk::TEvConnectResult>
                promise)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Promise(std::move(promise))
        {}
    };

    struct TWritePersistentBuffer
    {
        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const ui64 Lsn;
        const NKikimr::NDDisk::TWriteInstruction Instruction;
        TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<
            NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>
            Promise;

        TWritePersistentBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials credentials,
            const NKikimr::NDDisk::TBlockSelector selector,
            const ui64 lsn,
            const NKikimr::NDDisk::TWriteInstruction instruction,
            TGuardedSgList data,
            NWilson::TTraceId traceId,
            NThreading::TPromise<
                NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>
                promise)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Lsn(lsn)
            , Instruction(instruction)
            , Data(std::move(data))
            , TraceId(std::move(traceId))
            , Promise(std::move(promise))
        {}
    };

    struct TErasePersistentBuffer
    {
        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const ui64 Lsn;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<
            NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>
            Promise;

        TErasePersistentBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials credentials,
            const NKikimr::NDDisk::TBlockSelector selector,
            const ui64 lsn,
            NWilson::TTraceId traceId,
            NThreading::TPromise<
                NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>
                promise)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Lsn(lsn)
            , TraceId(std::move(traceId))
            , Promise(std::move(promise))
        {}
    };

    struct TReadPersistentBuffer
    {
        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const ui64 Lsn;
        const NKikimr::NDDisk::TReadInstruction Instruction;
        TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<
            NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>
            Promise;

        TReadPersistentBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials credentials,
            const NKikimr::NDDisk::TBlockSelector selector,
            const ui64 lsn,
            const NKikimr::NDDisk::TReadInstruction instruction,
            TGuardedSgList data,
            NWilson::TTraceId traceId,
            NThreading::TPromise<
                NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>
                promise)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Lsn(lsn)
            , Instruction(instruction)
            , Data(std::move(data))
            , TraceId(std::move(traceId))
            , Promise(std::move(promise))
        {}
    };

    struct TRead
    {
        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const NKikimr::NDDisk::TReadInstruction Instruction;
        TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<NKikimrBlobStorage::NDDisk::TEvReadResult> Promise;

        TRead(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials credentials,
            const NKikimr::NDDisk::TBlockSelector selector,
            const NKikimr::NDDisk::TReadInstruction instruction,
            TGuardedSgList data,
            NWilson::TTraceId traceId,
            NThreading::TPromise<NKikimrBlobStorage::NDDisk::TEvReadResult>
                promise)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Instruction(instruction)
            , Data(std::move(data))
            , TraceId(std::move(traceId))
            , Promise(std::move(promise))
        {}
    };

    struct TSyncWithPersistentBuffer
    {
        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const ui64 Lsn;
        const std::tuple<ui32, ui32, ui32> DDiskId;
        const ui64 DDiskInstanceGuid;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<
            NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>
            Promise;

        TSyncWithPersistentBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials credentials,
            const NKikimr::NDDisk::TBlockSelector selector,
            const ui64 lsn,
            const std::tuple<ui32, ui32, ui32> ddiskId,
            const ui64 ddiskInstanceGuid,
            NWilson::TTraceId traceId,
            NThreading::TPromise<
                NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>
                promise)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Lsn(lsn)
            , DDiskId(ddiskId)
            , DDiskInstanceGuid(ddiskInstanceGuid)
            , TraceId(std::move(traceId))
            , Promise(std::move(promise))
        {}
    };

    struct TListPersistentBuffer
    {
        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const ui64 RequestId;
        NThreading::TPromise<NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult> Promise;

        TListPersistentBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials credentials,
            const ui64 requestId,
            NThreading::TPromise<NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult> promise)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , RequestId(requestId)
            , Promise(std::move(promise))
        {}
    };

    enum EEvents
    {
        EvConnect,
        EvWritePersistentBuffer,
        EvErasePersistentBuffer,
        EvReadPersistentBuffer,
        EvRead,
        EvSyncWithPersistentBuffer,
        EvListPersistentBuffer,
    };

    using TEvConnect = TRequestEvent<TConnect, EEvents::EvConnect>;

    using TEvWritePersistentBuffer =
        TRequestEvent<TWritePersistentBuffer, EEvents::EvWritePersistentBuffer>;

    using TEvErasePersistentBuffer =
        TRequestEvent<TErasePersistentBuffer, EEvents::EvErasePersistentBuffer>;

    using TEvReadPersistentBuffer =
        TRequestEvent<TReadPersistentBuffer, EEvents::EvReadPersistentBuffer>;

    using TEvRead = TRequestEvent<TRead, EEvents::EvRead>;

    using TEvSyncWithPersistentBuffer = TRequestEvent<
        TSyncWithPersistentBuffer,
        EEvents::EvSyncWithPersistentBuffer>;

    using TEvListPersistentBuffer = TRequestEvent<
        TListPersistentBuffer,
        EEvents::EvListPersistentBuffer>;

};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
