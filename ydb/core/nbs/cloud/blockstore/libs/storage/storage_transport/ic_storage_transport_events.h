#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

struct TEvTransportPrivate
{
    struct TConnect: TDisableCopyMove
    {
        using TResult = NKikimrBlobStorage::NDDisk::TEvConnectResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TConnect(
            const NActors::TActorId& serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials)
            : ServiceId(serviceId)
            , Credentials(credentials)
        {}

        ~TConnect();
    };

    struct TWriteToPBuffer: TDisableCopyMove
    {
        using TResult =
            NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const ui64 Lsn;
        const NKikimr::NDDisk::TWriteInstruction Instruction;
        const TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TWriteToPBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials,
            const NKikimr::NDDisk::TBlockSelector& selector,
            const ui64 lsn,
            const NKikimr::NDDisk::TWriteInstruction instruction,
            const TGuardedSgList& data,
            NWilson::TTraceId traceId)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Lsn(lsn)
            , Instruction(instruction)
            , Data(data)
            , TraceId(std::move(traceId))

        {}

        ~TWriteToPBuffer();
    };

    struct TWriteToDDisk: TDisableCopyMove
    {
        using TResult = NKikimrBlobStorage::NDDisk::TEvWriteResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const NKikimr::NDDisk::TWriteInstruction Instruction;
        const TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TWriteToDDisk(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials,
            const NKikimr::NDDisk::TBlockSelector& selector,
            const NKikimr::NDDisk::TWriteInstruction instruction,
            const TGuardedSgList& data,
            NWilson::TTraceId traceId)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Instruction(instruction)
            , Data(data)
            , TraceId(std::move(traceId))

        {}

        ~TWriteToDDisk();
    };

    struct TEraseFromPBuffer: TDisableCopyMove
    {
        using TResult =
            NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const TVector<NKikimr::NDDisk::TBlockSelector> Selectors;
        const TVector<ui64> Lsns;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TEraseFromPBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials,
            TVector<NKikimr::NDDisk::TBlockSelector> selectors,
            TVector<ui64> lsns,
            NWilson::TTraceId traceId)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selectors(std::move(selectors))
            , Lsns(std::move(lsns))
            , TraceId(std::move(traceId))
        {}

        ~TEraseFromPBuffer();
    };

    struct TReadFromPBuffer: TDisableCopyMove
    {
        using TResult =
            NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const ui64 Lsn;
        const NKikimr::NDDisk::TReadInstruction Instruction;
        TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TReadFromPBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials,
            const NKikimr::NDDisk::TBlockSelector& selector,
            const ui64 lsn,
            const NKikimr::NDDisk::TReadInstruction instruction,
            const TGuardedSgList& data,
            NWilson::TTraceId traceId)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Lsn(lsn)
            , Instruction(instruction)
            , Data(data)
            , TraceId(std::move(traceId))

        {}

        ~TReadFromPBuffer();
    };

    struct TReadFromDDisk: TDisableCopyMove
    {
        using TResult = NKikimrBlobStorage::NDDisk::TEvReadResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const NKikimr::NDDisk::TReadInstruction Instruction;
        TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TReadFromDDisk(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials,
            const NKikimr::NDDisk::TBlockSelector& selector,
            const NKikimr::NDDisk::TReadInstruction instruction,
            TGuardedSgList data,
            NWilson::TTraceId traceId)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Instruction(instruction)
            , Data(std::move(data))
            , TraceId(std::move(traceId))
        {}

        ~TReadFromDDisk();
    };

    struct TSyncWithPBuffer: TDisableCopyMove
    {
        using TResult =
            NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const TVector<NKikimr::NDDisk::TBlockSelector> Selectors;
        const TVector<ui64> Lsns;
        const NKikimr::NBsController::TDDiskId PBufferId;
        const NKikimr::NDDisk::TQueryCredentials PBufferCredentials;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TSyncWithPBuffer(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials,
            TVector<NKikimr::NDDisk::TBlockSelector> selectors,
            TVector<ui64> lsns,
            const NKikimr::NBsController::TDDiskId& pBufferId,
            const NKikimr::NDDisk::TQueryCredentials& pBufferCredentials,
            NWilson::TTraceId traceId)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selectors(std::move(selectors))
            , Lsns(std::move(lsns))
            , PBufferId(pBufferId)
            , PBufferCredentials(pBufferCredentials)
            , TraceId(std::move(traceId))
        {}

        ~TSyncWithPBuffer();
    };

    struct TListPBufferEntries: TDisableCopyMove
    {
        using TResult =
            NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TListPBufferEntries(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials)
            : ServiceId(serviceId)
            , Credentials(credentials)
        {}

        ~TListPBufferEntries();
    };

    // TODO delete this 'using' after name's fix on the YDB's side.
    using TProtoEvWriteToManyPersistentBuffersResult =
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffersResult;
    using TProtoEvWriteToManyPersistentBuffers =
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffers;

    struct TWriteToManyPBuffers: TDisableCopyMove
    {
        using TResult = TProtoEvWriteToManyPersistentBuffersResult;

        const NActors::TActorId ServiceId;
        const NKikimr::NDDisk::TQueryCredentials Credentials;
        const NKikimr::NDDisk::TBlockSelector Selector;
        const ui64 Lsn;
        const NKikimr::NDDisk::TWriteInstruction Instruction;
        const TVector<NKikimrBlobStorage::NDDisk::TDDiskId> PersistentBufferIds;
        const TDuration ReplyTimeout;

        const TGuardedSgList Data;
        NWilson::TTraceId TraceId;
        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        TWriteToManyPBuffers(
            const NActors::TActorId serviceId,
            const NKikimr::NDDisk::TQueryCredentials& credentials,
            const NKikimr::NDDisk::TBlockSelector& selector,
            const ui64 lsn,
            const NKikimr::NDDisk::TWriteInstruction instruction,
            TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
            const TDuration replyTimeout,
            const TGuardedSgList& data,
            NWilson::TTraceId traceId)
            : ServiceId(serviceId)
            , Credentials(credentials)
            , Selector(selector)
            , Lsn(lsn)
            , Instruction(instruction)
            , PersistentBufferIds(std::move(persistentBufferIds))
            , ReplyTimeout(replyTimeout)
            , Data(data)
            , TraceId(std::move(traceId))

        {}

        ~TWriteToManyPBuffers();
    };

    enum EEvents
    {
        EvConnect,
        EvWriteToPBuffer,
        EvWriteToDDisk,
        EvEraseFromPBuffer,
        EvReadFromPBuffer,
        EvReadFromDDisk,
        EvSyncWithPBuffer,
        EvListPBufferEntries,
        EvWriteToManyPBuffers,
    };

    using TEvConnect = TRequestEvent<TConnect, EEvents::EvConnect>;

    using TEvReadFromPBuffer =
        TRequestEvent<TReadFromPBuffer, EEvents::EvReadFromPBuffer>;

    using TEvReadFromDDisk =
        TRequestEvent<TReadFromDDisk, EEvents::EvReadFromDDisk>;

    using TEvWriteToPBuffer =
        TRequestEvent<TWriteToPBuffer, EEvents::EvWriteToPBuffer>;

    using TEvWriteToDDisk =
        TRequestEvent<TWriteToDDisk, EEvents::EvWriteToDDisk>;

    using TEvSyncWithPBuffer =
        TRequestEvent<TSyncWithPBuffer, EEvents::EvSyncWithPBuffer>;

    using TEvEraseFromPBuffer =
        TRequestEvent<TEraseFromPBuffer, EEvents::EvEraseFromPBuffer>;

    using TEvListPBufferEntries =
        TRequestEvent<TListPBufferEntries, EEvents::EvListPBufferEntries>;

    using TEvWriteToManyPBuffers =
        TRequestEvent<TWriteToManyPBuffers, EEvents::EvWriteToManyPBuffers>;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
