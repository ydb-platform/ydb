#pragma once

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

namespace NKikimr::NKqp {

struct TLocksInfo {
    TVector<NKikimrDataEvents::TLock> Locks;
    TVector<NKikimrDataEvents::TLock> BrokenLocks;
};

struct TEvKqpCompute {
    struct TEvRemoteScanData : public TEventPB<TEvRemoteScanData, NKikimrKqp::TEvRemoteScanData,
        TKqpComputeEvents::EvRemoteScanData> {};

    class IShardScanStats {
    public:
        virtual ~IShardScanStats() = default;
        virtual THashMap<TString, ui64> GetMetrics() const = 0;
    };

    /*
     * Scan communications.
     *
     * TEvScanData is intentionally preserved as a local event for performance reasons: leaf compute
     * actors are communicating with shard scans using this message, so big amount of unfiltered data
     * is expected. However, it is possible that after query planning datashard would migrate to other
     * node. To support scans in this case we provide serialization routines. For now such remote scan
     * is considered as rare event and not worth of some fast serialization, so we just use protobuf.
     *
     * TEvScanDataAck follows the same pattern mostly for symmetry reasons.
     */
    struct TEvScanData : public NActors::TEventLocal<TEvScanData, TKqpComputeEvents::EvScanData> {
        TEvScanData(const ui32 scanId, const ui32 generation = 0)
            : ScanId(scanId)
            , Generation(generation)
            , Finished(false) {}

        std::optional<ui32> AvailablePacks;
        ui32 ScanId;
        ui32 Generation;
        TVector<TOwnedCellVec> Rows;
        std::shared_ptr<arrow::Table> ArrowBatch;
        std::vector<std::vector<ui32>> SplittedBatches;
        
        TOwnedCellVec LastKey;
        TDuration CpuTime;
        TDuration WaitTime;
        ui32 PageFaults = 0; // number of page faults occurred when filling in this message
        bool RequestedBytesLimitReached = false;
        bool Finished = false;
        bool PageFault = false; // page fault was the reason for sending this message
        mutable THolder<TEvRemoteScanData> Remote;
        std::shared_ptr<IShardScanStats> StatsOnFinished;
        TLocksInfo LocksInfo;

        template <class T>
        const T& GetStatsAs() const {
            Y_ABORT_UNLESS(!!StatsOnFinished);
            return VerifyDynamicCast<const T&>(*StatsOnFinished);
        }

        template <class T>
        bool CheckStatsIs() const {
            auto p = dynamic_cast<const T*>(StatsOnFinished.get());
            return p;
        }

        ui32 GetRowsCount() const {
            if (ArrowBatch) {
                return ArrowBatch->num_rows();
            } else {
                return Rows.size();
            }
        }

        bool IsEmpty() const {
            return GetRowsCount() == 0;
        }

        bool IsSerializable() const override {
            return true;
        }

        ui32 CalculateSerializedSize() const override {
            InitRemote();
            return Remote->CalculateSerializedSizeCached();
        }

        bool SerializeToArcadiaStream(NActors::TChunkSerializer* chunker) const override {
            InitRemote();
            return Remote->SerializeToArcadiaStream(chunker);
        }

        NKikimrDataEvents::EDataFormat GetDataFormat() const {
            if (ArrowBatch != nullptr || SplittedBatches.size()) {
                return NKikimrDataEvents::FORMAT_ARROW;
            }
            return NKikimrDataEvents::FORMAT_CELLVEC;
        }


        static NActors::IEventBase* Load(TEventSerializedData* data) {
            auto pbEv = THolder<TEvRemoteScanData>(static_cast<TEvRemoteScanData *>(TEvRemoteScanData::Load(data)));
            auto ev = MakeHolder<TEvScanData>(pbEv->Record.GetScanId(), pbEv->Record.GetGeneration());

            ev->CpuTime = TDuration::MicroSeconds(pbEv->Record.GetCpuTimeUs());
            ev->WaitTime = TDuration::MilliSeconds(pbEv->Record.GetWaitTimeMs());
            ev->PageFault = pbEv->Record.GetPageFault();
            ev->PageFaults = pbEv->Record.GetPageFaults();
            ev->Finished = pbEv->Record.GetFinished();
            ev->RequestedBytesLimitReached = pbEv->Record.GetRequestedBytesLimitReached();
            ev->LastKey = TOwnedCellVec(TSerializedCellVec(pbEv->Record.GetLastKey()).GetCells());
            if (pbEv->Record.HasAvailablePacks()) {
                ev->AvailablePacks = pbEv->Record.GetAvailablePacks();
            }

            auto rows = pbEv->Record.GetRows();
            ev->Rows.reserve(rows.size());
            for (const auto& row: rows) {
                ev->Rows.emplace_back(TSerializedCellVec(row).GetCells());
            }

            if (pbEv->Record.HasArrowBatch()) {
                auto batch = pbEv->Record.GetArrowBatch();
                auto schema = NArrow::DeserializeSchema(batch.GetSchema());
                ev->ArrowBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({NArrow::DeserializeBatch(batch.GetBatch(), schema)}));
            }
            return ev.Release();
        }

    private:
        void InitRemote() const {
            if (!Remote) {
                Remote = MakeHolder<TEvRemoteScanData>();

                Remote->Record.SetScanId(ScanId);
                Remote->Record.SetGeneration(Generation);
                Remote->Record.SetCpuTimeUs(CpuTime.MicroSeconds());
                Remote->Record.SetWaitTimeMs(WaitTime.MilliSeconds());
                Remote->Record.SetPageFaults(PageFaults);
                Remote->Record.SetFinished(Finished);
                Remote->Record.SetRequestedBytesLimitReached(RequestedBytesLimitReached);
                Remote->Record.SetPageFaults(PageFaults);
                Remote->Record.SetPageFault(PageFault);
                Remote->Record.SetLastKey(TSerializedCellVec::Serialize(LastKey));
                if (AvailablePacks) {
                    Remote->Record.SetAvailablePacks(*AvailablePacks);
                }

                switch (GetDataFormat()) {
                    case NKikimrDataEvents::FORMAT_UNSPECIFIED:
                    case NKikimrDataEvents::FORMAT_CELLVEC: {
                        Remote->Record.MutableRows()->Reserve(Rows.size());
                        for (const auto& row: Rows) {
                            Remote->Record.AddRows(TSerializedCellVec::Serialize(row));
                        }
                        break;
                    }
                    case NKikimrDataEvents::FORMAT_ARROW: {
                        Y_DEBUG_ABORT_UNLESS(ArrowBatch != nullptr);
                        auto* protoArrowBatch = Remote->Record.MutableArrowBatch();
                        protoArrowBatch->SetSchema(NArrow::SerializeSchema(*ArrowBatch->schema()));
                        protoArrowBatch->SetBatch(NArrow::SerializeBatchNoCompression(NArrow::ToBatch(ArrowBatch, true)));
                        break;
                    }
                }
            }
        }
    };

    struct TEvRemoteScanDataAck: public NActors::TEventPB<TEvRemoteScanDataAck, NKikimrKqp::TEvRemoteScanDataAck,
        TKqpComputeEvents::EvRemoteScanDataAck> {
    };

    struct TEvScanDataAck : public NActors::TEventLocal<TEvScanDataAck, TKqpComputeEvents::EvScanDataAck> {
        explicit TEvScanDataAck(ui64 freeSpace, ui32 generation = 0, const ui32 maxChunksCount = Max<ui32>())
            : FreeSpace(freeSpace)
            , Generation(generation)
            , MaxChunksCount(maxChunksCount)
        {}

        const ui64 FreeSpace;
        const ui32 Generation;
        const ui32 MaxChunksCount;
        mutable THolder<TEvRemoteScanDataAck> Remote;

        bool IsSerializable() const override {
            return true;
        }

        ui32 CalculateSerializedSize() const override {
            InitRemote();
            return Remote->CalculateSerializedSizeCached();
        }

        bool SerializeToArcadiaStream(NActors::TChunkSerializer* chunker) const override {
            InitRemote();
            return Remote->SerializeToArcadiaStream(chunker);
        }

        static NActors::IEventBase* Load(TEventSerializedData* data) {
            auto pbEv = THolder<TEvRemoteScanDataAck>(static_cast<TEvRemoteScanDataAck *>(TEvRemoteScanDataAck::Load(data)));
            ui32 maxChunksCount = Max<ui32>();
            if (pbEv->Record.HasMaxChunksCount()) {
                maxChunksCount = pbEv->Record.GetMaxChunksCount();
            }
            return new TEvScanDataAck(pbEv->Record.GetFreeSpace(), pbEv->Record.GetGeneration(), maxChunksCount);
        }

    private:
        void InitRemote() const {
            if (!Remote) {
                Remote.Reset(new TEvRemoteScanDataAck);
                Remote->Record.SetFreeSpace(FreeSpace);
                Remote->Record.SetGeneration(Generation);
                Remote->Record.SetMaxChunksCount(MaxChunksCount);
            }
        }
    };

    struct TEvScanError : public NActors::TEventPB<TEvScanError, NKikimrKqp::TEvScanError,
        TKqpComputeEvents::EvScanError>
    {
        TEvScanError() = default;
        TEvScanError(const ui32 generation, const ui64 tabletId) {
            Record.SetGeneration(generation);
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvScanInitActor : public NActors::TEventPB<TEvScanInitActor, NKikimrKqp::TEvScanInitActor,
        TKqpComputeEvents::EvScanInitActor>
    {
        TEvScanInitActor() = default;

        TEvScanInitActor(ui64 scanId, const NActors::TActorId& scanActor, ui32 generation, const ui64 tabletId) {
            Record.SetScanId(scanId);
            ActorIdToProto(scanActor, Record.MutableScanActorId());
            Record.SetGeneration(generation);
            Record.SetTabletId(tabletId);
        }
    };

    struct TEvKillScanTablet : public NActors::TEventPB<TEvKillScanTablet, NKikimrKqp::TEvKillScanTablet,
        TKqpComputeEvents::EvKillScanTablet> {};

};

} // namespace NKikimr::NKqp
