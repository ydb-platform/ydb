#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <functional>

namespace NKikimr::NReplication {

class TTopicMessage;
struct TTransferReadStats;
struct TTransferWriteStats;

enum class EWorkerOperation {
    NONE = 0,
    READ = 1,
    DECOMPRESS = 2,
    PROCESS = 3,
    WRITE = 4
};

struct TWorkerDetailedStats {
    EWorkerOperation CurrentOperation;
    std::unique_ptr<TTransferReadStats> ReaderStats;
    std::unique_ptr<TTransferWriteStats> WriterStats;
};


namespace NService {

struct TEvWorker {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_REPLICATION_WORKER),

        EvHandshake,
        EvPoll,
        EvData,
        EvGone,
        EvStatus,
        EvDataEnd,
        EvCommit,
        EvTerminateWriter,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_WORKER));

    struct TEvHandshake: public TEventLocal<TEvHandshake, EvHandshake> {};

    struct TEvPoll: public TEventLocal<TEvPoll, EvPoll> {
        bool SkipCommit;

        explicit TEvPoll(bool skipCommit = false);
        TString ToString() const override;
    };

    struct TEvCommit: public TEventLocal<TEvCommit, EvCommit> {
        size_t Offset;

        explicit TEvCommit(size_t offset);
        TString ToString() const override;
    };

    struct TEvData: public TEventLocal<TEvData, EvData> {
        ui32 PartitionId;
        TString Source;
        TVector<TTopicMessage> Records;
        std::unique_ptr<TWorkerDetailedStats> Stats;

        explicit TEvData(ui32 partitionId, const TString& source, const TVector<TTopicMessage>& records);
        explicit TEvData(ui32 partitionId, const TString& source, TVector<TTopicMessage>&& records);
        TString ToString() const override;
    };

    struct TEvGone: public TEventLocal<TEvGone, EvGone> {
        enum EStatus {
            DONE,
            S3_ERROR,
            SCHEME_ERROR,
            UNAVAILABLE,
            OVERLOAD
        };

        EStatus Status;
        TString ErrorDescription;

        explicit TEvGone(EStatus status, const TString& errorDescription = {});
        TString ToString() const override;
    };

    struct TEvStatus: public TEventLocal<TEvStatus, EvStatus> {
        TDuration Lag;
        std::unique_ptr<TWorkerDetailedStats> DetailedStats;

        explicit TEvStatus(TDuration lag);
        explicit TEvStatus(std::unique_ptr<TWorkerDetailedStats>&& detailedStats);
        TString ToString() const override;
        static TEvStatus* FromOperation(EWorkerOperation operation);
    };

    struct TEvDataEnd: public TEventLocal<TEvDataEnd, EvDataEnd> {
        ui64 PartitionId;
        TVector<ui64> AdjacentPartitionsIds;
        TVector<ui64> ChildPartitionsIds;

        TEvDataEnd(ui64 partitionId, TVector<ui64>&& adjacentPartitionsIds, TVector<ui64>&& childPartitionsIds);
        TString ToString() const override;
    };

    struct TEvTerminateWriter: public TEventLocal<TEvTerminateWriter, EvTerminateWriter> {
        ui64 PartitionId;

        explicit TEvTerminateWriter(ui64 partitionId);
        TString ToString() const override;
    };

};

IActor* CreateWorker(
    const TActorId& parent,
    std::function<IActor*(void)>&& createReaderFn,
    std::function<IActor*(void)>&& createWriterFn);

} // NService
} // NKikimr::NReplication
