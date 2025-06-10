#include "query_stathuge.h"
#include "query_statalgo.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THugeStatActor
    // Output info about given TabletId
    ////////////////////////////////////////////////////////////////////////////
    class THugeStatActor : public TActorBootstrapped<THugeStatActor> {
        friend class TActorBootstrapped<TThis>;

        void Bootstrap(const TActorContext &ctx) {
            TStringStream str;
            const bool prettyPrint = Ev->Get()->Record.GetPrettyPrint();

            CalculateUsedHugeChunks(str, prettyPrint);

            Result->SetResult(str.Str());
            SendVDiskResponse(ctx, Ev->Sender, Result.release(), 0, HullCtx->VCtx, {});
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            TThis::Die(ctx);
        }

        void CalculateUsedHugeChunks(IOutputStream &str, bool pretty);

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_STAT_QUERY;
        }

        THugeStatActor(
                TIntrusivePtr<THullCtx> hullCtx,
                const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
                const TActorId &parentId,
                THullDsSnap &&fullSnap,
                TEvBlobStorage::TEvVDbStat::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
            : TActorBootstrapped<THugeStatActor>()
            , HullCtx(std::move(hullCtx))
            , HugeBlobCtx(hugeBlobCtx)
            , ParentId(parentId)
            , FullSnap(std::move(fullSnap))
            , Ev(ev)
            , Result(std::move(result))
        {}

    private:
        // class aggregates info about LogoBlobs
        class TAggr;

        TIntrusivePtr<THullCtx> HullCtx;
        std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        const TActorId ParentId;
        THullDsSnap FullSnap;
        TEvBlobStorage::TEvVDbStat::TPtr Ev;
        std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> Result;
    };

    ////////////////////////////////////////////////////////////////////////////
    // THugeStatActor::TAggr
    ////////////////////////////////////////////////////////////////////////////
    class THugeStatActor::TAggr {
    public:
        using TLevelSegment = ::NKikimr::TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

    private:
        // Info gathered per chunk
        struct TChunkInfo {
            // counter for used slots in chunk
            ui32 UsedSlots = 0;
            // counter for used bytes in chunk
            ui32 UsedBytes = 0;
            // const: huge slot size in chunk
            ui32 SlotSize = 0;
            // const: number of huge slots of size SlotSize in chunk
            ui32 NumberOfSlotsInChunk = 0;

            TChunkInfo(ui32 slotSize, ui32 numberOfSlotsInChunk)
                : SlotSize(slotSize)
                , NumberOfSlotsInChunk(numberOfSlotsInChunk)
            {}
        };

        // Aggregated info gathered per slotSize
        struct TAggrSlotInfo {
            // counter for used slots in chunk
            ui64 UsedSlots = 0;
            // counter for used bytes in chunk
            ui64 UsedBytes = 0;
            // counter for used chunks per this slotSize
            ui32 UsedChunks = 0;
            // const: number of huge slots of size SlotSize in chunk
            ui32 NumberOfSlotsInChunk = 0;

            TAggrSlotInfo(ui32 numberOfSlotsInChunk)
                : NumberOfSlotsInChunk(numberOfSlotsInChunk)
            {}
        };

        class TChunksMap {
        private:
            // chunkIdx -> TChunkInfo
            using TPerChunkMap = THashMap<ui32, TChunkInfo>;
            // slotSize -> TAggrSlotInfo
            using TAggrBySlotSize = TMap<ui32, TAggrSlotInfo>;
            IOutputStream &Str;
            const std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
            const bool Pretty;
            const ui32 ChunkSize;
            TPerChunkMap Map;
            TAggrBySlotSize Aggr;

            void OutputNum(ui64 val, ESizeFormat format) {
                Str << "<td data-text='" << val << "'><small>";
                if (Pretty) {
                    Str << HumanReadableSize(val, format);
                } else {
                    Str << val;
                }
                Str << "</small></td>";
            }

            void AggregatePerSlotSize() {
                for (const auto &x : Map) {
                    TAggrBySlotSize::iterator it = Aggr.find(x.second.SlotSize);
                    if (it == Aggr.end()) {
                        TAggrSlotInfo aggr(x.second.NumberOfSlotsInChunk);

                        auto insertRes = Aggr.insert(TAggrBySlotSize::value_type(x.second.SlotSize, aggr));
                        Y_ABORT_UNLESS(insertRes.second);
                        it = insertRes.first;
                    }
                    it->second.UsedSlots += x.second.UsedSlots;
                    it->second.UsedBytes += x.second.UsedBytes;
                    ++(it->second.UsedChunks);
                }
            }

            void RenderTotalStat() {
                // total used bytes by all huge blobs stored in huge heap
                ui64 totalUsedBytes = 0;
                // total used bytes with respect to slot alignment (always >= totalUsedBytes)
                ui64 totalUsedBytesSlotAlignment = 0;
                // total used slots
                ui64 totalUsedSlots = 0;
                // number of slots allocated (of different sizes)
                ui64 totalAllocatedSlots = 0;
                // allocated chunks
                ui64 allocatedChunks = 0;
                for (auto it = Aggr.begin(); it != Aggr.end(); ++it) {
                    totalUsedBytes += it->second.UsedBytes;
                    allocatedChunks += it->second.UsedChunks;

                    // calculate totalUsedBytesSlotAlignment
                    const ui64 slotSize = it->first;
                    totalUsedBytesSlotAlignment += slotSize * it->second.UsedSlots;
                    // calculate free/used slots
                    const ui64 slotsInChunk = (ui64)ChunkSize / slotSize;
                    totalUsedSlots += it->second.UsedSlots;
                    totalAllocatedSlots += it->second.UsedChunks * slotsInChunk;
                }

                auto renderOneRecord = [] (IOutputStream &str, const char *name, ui64 value) {
                    str << name << ": " << value << " " << HumanReadableSize(value, SF_QUANTITY) << "<br>";
                };

                HTML(Str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            Str << "HugeHeap Aggregated Statistics";
                        }
                        DIV_CLASS("panel-body") {
                            DIV_CLASS("well well-sm") {
                                const ui64 allocatedBytes = allocatedChunks * ChunkSize;
                                // WRT = with respect to slot alignment
                                const ui64 garbageBytesWRTSlotAlignment = allocatedBytes - totalUsedBytesSlotAlignment;
                                unsigned garbageToStoredPercentage = 0;
                                unsigned garbageOverheadPercentage = 0;
                                if (allocatedBytes && totalUsedBytesSlotAlignment) {
                                    garbageToStoredPercentage =
                                        double(garbageBytesWRTSlotAlignment) / allocatedBytes * 100u;
                                    garbageOverheadPercentage =
                                        double(garbageBytesWRTSlotAlignment) / totalUsedBytesSlotAlignment * 100u;
                                }

                                // render info about bytes
                                renderOneRecord(Str, "TotalStoredBytes", totalUsedBytes);
                                renderOneRecord(Str, "TotalStoredBytesWithRespectToSlotAlignment",
                                    totalUsedBytesSlotAlignment);
                                renderOneRecord(Str, "AllocatedBytes", allocatedBytes);
                                renderOneRecord(Str, "GarbageToStoredWithRespectToSlotAlignmentPercentage",
                                    garbageToStoredPercentage);
                                renderOneRecord(Str, "GarbageOverheadWithRespectToSlotAlignmentPercentage",
                                    garbageOverheadPercentage);
                                Str << "<br>";

                                // render info about slots
                                renderOneRecord(Str, "TotalUsedSlots", totalUsedSlots);
                                renderOneRecord(Str, "TotalAllocatedSlots", totalAllocatedSlots);

                                // render info about chunks
                                Str << "<br>";
                                renderOneRecord(Str, "AllocatedChunks", allocatedChunks);
                            }
                        }
                    }
                }
            }

            void RenderSlotsStat_RenderRow(ui32 slotSize, const TAggrSlotInfo &aggr) {
                HTML(Str) {
                    TABLER() {
                        TABLED() {Str << slotSize;}
                        OutputNum(aggr.UsedBytes, SF_QUANTITY);
                        ui64 freeBytes = ui64(ChunkSize) * aggr.UsedChunks - aggr.UsedBytes;
                        OutputNum(freeBytes, SF_QUANTITY);
                        OutputNum(aggr.UsedSlots, SF_QUANTITY);
                        ui64 totalSlots = ui64(aggr.UsedChunks) * aggr.NumberOfSlotsInChunk;
                        ui64 freeSlots = totalSlots - aggr.UsedSlots;
                        OutputNum(freeSlots, SF_QUANTITY);
                        OutputNum(aggr.UsedChunks, SF_QUANTITY);
                        unsigned usedPercentage = double(aggr.UsedSlots) / totalSlots * 100;
                        OutputNum(usedPercentage, SF_QUANTITY);
                    }
                }
            }

            void RenderSlotsStat() {
                HTML(Str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            Str << "HugeHeap Per Slot Statistics";
                        }
                        DIV_CLASS("panel-body") {
                            TABLE_SORTABLE_CLASS ("table table-condensed") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {Str << "SlotSize";}
                                        TABLEH() {Str << "UsedBytes";}
                                        TABLEH() {Str << "FreeBytes";}
                                        TABLEH() {Str << "UsedSlots";}
                                        TABLEH() {Str << "FreeSlots";}
                                        TABLEH() {Str << "UsedChunks";}
                                        TABLEH() {Str << "PercentageUsed";}
                                    }
                                }
                                TABLEBODY() {
                                    for (auto it = Aggr.begin(); it != Aggr.end(); ++it) {
                                        RenderSlotsStat_RenderRow(it->first, it->second);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            void RenderRawChunks_RenderRow(ui32 chunkIdx, const TChunkInfo &chunkInfo) {
                HTML(Str) {
                    TABLER() {
                        TABLED() {Str << chunkIdx;}
                        OutputNum(chunkInfo.UsedBytes, SF_QUANTITY);
                        OutputNum(ChunkSize - chunkInfo.UsedBytes, SF_QUANTITY);
                        OutputNum(chunkInfo.UsedSlots, SF_QUANTITY);
                        ui32 freeSlots = chunkInfo.NumberOfSlotsInChunk - chunkInfo.UsedSlots;
                        OutputNum(freeSlots, SF_QUANTITY);
                        unsigned usedPercentage = double(chunkInfo.UsedSlots) / chunkInfo.NumberOfSlotsInChunk * 100;
                        OutputNum(usedPercentage, SF_QUANTITY);
                    }
                }
            }

            void RenderRawChunks() {
                // render output
                HTML(Str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            Str << "HugeHeap Per Chunk Statistics";
                        }
                        DIV_CLASS("panel-body") {
                            TABLE_SORTABLE_CLASS ("table table-condensed") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {Str << "ChunkIdx";}
                                        TABLEH() {Str << "UsedBytes";}
                                        TABLEH() {Str << "FreeBytes";}
                                        TABLEH() {Str << "UsedSlots";}
                                        TABLEH() {Str << "FreeSlots";}
                                        TABLEH() {Str << "PercentageUsed";}
                                    }
                                }
                                TABLEBODY() {
                                    for (auto it = Map.begin(); it != Map.end(); ++it) {
                                        RenderRawChunks_RenderRow(it->first, it->second);
                                    }
                                }
                            }
                        }
                    }
                }
            }

        public:
            TChunksMap(
                    IOutputStream &str,
                    const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
                    const bool pretty,
                    ui32 chunkSize)
                : Str(str)
                , HugeBlobCtx(hugeBlobCtx)
                , Pretty(pretty)
                , ChunkSize(chunkSize)
            {}

            void Add(const TDiskPart &part) {
                TPerChunkMap::iterator it = Map.find(part.ChunkIdx);
                if (it == Map.end()) {
                    const THugeSlotsMap::TSlotInfo *slotInfo = HugeBlobCtx->HugeSlotsMap->GetSlotInfo(part.Size);
                    Y_ABORT_UNLESS(slotInfo, "size# %" PRIu32, part.Size);
                    TChunkInfo chunkInfo(slotInfo->SlotSize, slotInfo->NumberOfSlotsInChunk);

                    auto insertRes = Map.insert(TPerChunkMap::value_type(part.ChunkIdx, chunkInfo));
                    Y_ABORT_UNLESS(insertRes.second);
                    it = insertRes.first;
                }
                ++(it->second.UsedSlots);
                it->second.UsedBytes += part.Size;
            }

            void RenderHTML() {
                // Aggregate
                AggregatePerSlotSize();
                // Render
                RenderTotalStat();
                RenderSlotsStat();
                RenderRawChunks();
            }
        };

        // store DataExtractor as a field to avoid recreating it on every record
        TDiskDataExtractor DataExtractor;
        TChunksMap ChunksMap;

    public:
        TAggr(IOutputStream &str, const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx, bool pretty, ui32 chunkSize)
            : ChunksMap(str, hugeBlobCtx, pretty, chunkSize)
        {}

        void UpdateFresh(const char *segName,
                         const TKeyLogoBlob &key,
                         const TMemRecLogoBlob &memRec) {
            Y_UNUSED(segName);
            Y_UNUSED(key);
            Update(memRec, nullptr);
        }

        void UpdateLevel(const TLevelSstPtr &sstPtr,
                         const TKeyLogoBlob &key,
                         const TMemRecLogoBlob &memRec) {
            Y_UNUSED(key);
            Update(memRec, sstPtr.SstPtr->GetOutbound());
        }

        void Finish() {
            ChunksMap.RenderHTML();
        }
    private:
        void Update(const TMemRecLogoBlob &memRec, const TDiskPart *outbound) {
            TBlobType::EType t = memRec.GetType();
            if (t == TBlobType::HugeBlob || t == TBlobType::ManyHugeBlobs) {
                memRec.GetDiskData(&DataExtractor, outbound);
                for (const TDiskPart *hb = DataExtractor.Begin; hb != DataExtractor.End; ++hb) {
                    ChunksMap.Add(*hb);
                }
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // THugeStatActor::CalculateUsedHugeChunks
    ////////////////////////////////////////////////////////////////////////////
    void THugeStatActor::CalculateUsedHugeChunks(IOutputStream &str, bool pretty) {
        TAggr aggr(str, HugeBlobCtx, pretty, HullCtx->ChunkSize);
        TraverseDbWithoutMerge(HullCtx, &aggr, FullSnap.LogoBlobsSnap);
    }

    ////////////////////////////////////////////////////////////////////////////
    // CreateTabletStatActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateHugeStatActor(
            TIntrusivePtr<THullCtx> hullCtx,
            const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
            const TActorId &parentId,
            THullDsSnap &&fullSnap,
            TEvBlobStorage::TEvVDbStat::TPtr &ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result) {
        return new THugeStatActor(std::move(hullCtx), hugeBlobCtx, parentId, std::move(fullSnap), ev, std::move(result));
    }

} // NKikimr
