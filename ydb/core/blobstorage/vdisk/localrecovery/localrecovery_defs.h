#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

namespace NKikimr {
    namespace NLocRecovery {

        ////////////////////////////////////////////////////////////////////////
        // Info about one PDisk read log result
        ////////////////////////////////////////////////////////////////////////
        struct TReadLogReply {
            ui64 Records = 0;
            ui64 FirstLsn = 0;
            ui64 LastLsn = 0;

            TReadLogReply(ui64 recs, ui64 firstLsn, ui64 lastLsn)
                : Records(recs)
                , FirstLsn(firstLsn)
                , LastLsn(lastLsn)
            {}

            void Output(IOutputStream &str) const {
                str << "{RecsN# " << Records << " Lsns# [" << FirstLsn << " " << LastLsn << "]}";
            }
        };

        ////////////////////////////////////////////////////////////////////////
        // Record dispatcher that tracks applied records
        ////////////////////////////////////////////////////////////////////////
        class TRecordDispatcherStat {
        public:
            void NewRecord(NMonGroup::TLocalRecoveryGroup *mon, ui32 size);
            void SetApplyFlag();
            void SetSkipFlag();
            void Finish(NMonGroup::TLocalRecoveryGroup *mon);

        private:
            // total number of bytes dispatched
            ui64 TotalBytesDispatched = 0;
            // total number of records dispatched
            ui64 TotalRecordsDispatched = 0;
            // total number of bytes applied
            ui64 TotalBytesApplied = 0;
            // total number of records applied
            ui64 TotalRecordsApplied = 0;

            // current state
            ui32 CurRecSize = 0;
            bool CurRecApplied = false;

            void FinishRecord(NMonGroup::TLocalRecoveryGroup *mon, ui32 newRecSize);
        };

    } // NLocRecovery

    ////////////////////////////////////////////////////////////////////////////
    // TLocalRecoveryInfo
    ////////////////////////////////////////////////////////////////////////////
    class TLocalRecoveryInfo : public TThrRefBase {
    public:
        bool SuccessfulRecovery = false;
        bool EmptyLogoBlobsDb = true;
        bool EmptyBlocksDb = true;
        bool EmptyBarriersDb = true;
        bool EmptySyncLog = true;
        bool EmptySyncer = true;
        bool EmptyHuge = true;

    private:
        // Dispatched Log Records
        ui64 LogRecLogoBlob = 0;
        ui64 LogRecBlock = 0;
        ui64 LogRecGC = 0;
        ui64 LogRecSyncLogIdx = 0;
        ui64 LogRecLogoBlobsDB = 0;
        ui64 LogRecBlocksDB = 0;
        ui64 LogRecBarriersDB = 0;
        ui64 LogRecCutLog = 0;
        ui64 LogRecLocalSyncData = 0;
        ui64 LogRecSyncerState = 0;
        ui64 LogRecHandoffDel = 0;
        ui64 LogRecHugeBlobAllocChunk = 0;
        ui64 LogRecHugeBlobFreeChunk = 0;
        ui64 LogRecHugeBlobEntryPoint = 0;
        ui64 LogRecHugeLogoBlob = 0;
        ui64 LogRecLogoBlobOpt = 0;
        ui64 LogRecPhantomBlob = 0;
        ui64 LogRecAnubisOsirisPut = 0;
        ui64 LogRecAddBulkSst = 0;
        ui64 LogRecScrub = 0;


        // statistics for record dispatching
        NLocRecovery::TRecordDispatcherStat RecDispatcherStat;
        // diapason of applied log records [FirstLsn, LastLsn]
        ui64 RecoveryLogFirstLsn = Max<ui64>();
        ui64 RecoveryLogLastLsn = 0;
        // some info for every log request to Yard that has been made
        TVector<NLocRecovery::TReadLogReply> ReadLogReplies;
        // lsn we starting with after local recovery and lsn shift
        ui64 RecoveredLogStartLsn = 0;
        // found starting points
        using TSignatureToLsn = TMap<TLogSignature, ui64>;
        TSignatureToLsn StartingPoints;

        TInstant LocalRecoveryStartTime;
        TInstant LocalRecoveryFinishTime;
        NMonGroup::TLocalRecoveryGroup MonGroup;

    public:
        using TRec = const NPDisk::TLogRecord&;
#define DISPATCH_SIGNATURE_FUNC_GEN(signatureName, counter)     \
    void DispatchSignature##signatureName(TRec r)               \
    {                                                           \
        Disp(r);                                                \
        counter++;                                              \
    }

        DISPATCH_SIGNATURE_FUNC_GEN(LogoBlob, LogRecLogoBlob)
        DISPATCH_SIGNATURE_FUNC_GEN(Block, LogRecBlock)
        DISPATCH_SIGNATURE_FUNC_GEN(GC, LogRecGC)
        DISPATCH_SIGNATURE_FUNC_GEN(SyncLogIdx, LogRecSyncLogIdx)
        DISPATCH_SIGNATURE_FUNC_GEN(HullLogoBlobsDB, LogRecLogoBlobsDB)
        DISPATCH_SIGNATURE_FUNC_GEN(HullBlocksDB, LogRecBlocksDB)
        DISPATCH_SIGNATURE_FUNC_GEN(HullBarriersDB, LogRecBarriersDB)
        DISPATCH_SIGNATURE_FUNC_GEN(HullCutLog, LogRecCutLog)
        DISPATCH_SIGNATURE_FUNC_GEN(LocalSyncData, LogRecLocalSyncData)
        DISPATCH_SIGNATURE_FUNC_GEN(SyncerState, LogRecSyncerState)
        DISPATCH_SIGNATURE_FUNC_GEN(HandoffDelLogoBlob, LogRecHandoffDel)
        DISPATCH_SIGNATURE_FUNC_GEN(HugeBlobAllocChunk, LogRecHugeBlobAllocChunk)
        DISPATCH_SIGNATURE_FUNC_GEN(HugeBlobFreeChunk, LogRecHugeBlobFreeChunk)
        DISPATCH_SIGNATURE_FUNC_GEN(HugeBlobEntryPoint, LogRecHugeBlobEntryPoint)
        DISPATCH_SIGNATURE_FUNC_GEN(HugeLogoBlob, LogRecHugeLogoBlob)
        DISPATCH_SIGNATURE_FUNC_GEN(LogoBlobOpt, LogRecLogoBlobOpt)
        DISPATCH_SIGNATURE_FUNC_GEN(PhantomBlobs, LogRecPhantomBlob)
        DISPATCH_SIGNATURE_FUNC_GEN(AnubisOsirisPut, LogRecAnubisOsirisPut)
        DISPATCH_SIGNATURE_FUNC_GEN(AddBulkSst, LogRecAddBulkSst)
        DISPATCH_SIGNATURE_FUNC_GEN(Scrub, LogRecScrub)

        ///////////////////////////////////////////////////////////////////////////////
        // Log Applied/Skipped items
        ///////////////////////////////////////////////////////////////////////////////
    private:
        // LogoBlob
        ui64 LogoBlobFreshApply = 0;
        ui64 LogoBlobFreshSkip = 0;
        ui64 LogoBlobsBatchFreshApply = 0;
        ui64 LogoBlobsBatchFreshSkip = 0;
        ui64 LogoBlobSyncLogApply = 0;
        ui64 LogoBlobSyncLogSkip = 0;
        // Huge LogoBlob
        ui64 HugeLogoBlobFreshApply = 0;
        ui64 HugeLogoBlobFreshSkip = 0;
        ui64 HugeLogoBlobSyncLogApply = 0;
        ui64 HugeLogoBlobSyncLogSkip = 0;
        // Block
        ui64 BlockFreshApply = 0;
        ui64 BlockFreshSkip = 0;
        ui64 BlocksBatchFreshApply = 0;
        ui64 BlocksBatchFreshSkip = 0;
        ui64 BlockSyncLogApply = 0;
        ui64 BlockSyncLogSkip = 0;
        // Barrier from SyncLog
        ui64 BarrierFreshApply = 0;
        ui64 BarrierFreshSkip = 0;
        ui64 BarriersBatchFreshApply = 0;
        ui64 BarriersBatchFreshSkip = 0;
        ui64 BarrierSyncLogApply = 0;
        ui64 BarrierSyncLogSkip = 0;
        // GC Cmd
        ui64 GCBarrierFreshApply = 0;
        ui64 GCBarrierFreshSkip = 0;
        ui64 GCLogoBlobFreshApply = 0;
        ui64 GCLogoBlobFreshSkip = 0;
        ui64 GCSyncLogApply = 0;
        ui64 GCSyncLogSkip = 0;
        // Local Sync Data
        ui64 TryPutLogoBlobSyncData = 0;
        ui64 TryPutBlockSyncData = 0;
        ui64 TryPutBarrierSyncData = 0;
        // HandoffDel
        ui64 HandoffDelFreshApply = 0;
        ui64 HandoffDelFreshSkip = 0;
        // HugeBlobAllocChunk
        ui64 HugeBlobAllocChunkApply = 0;
        ui64 HugeBlobAllocChunkSkip = 0;
        // HugeBlobFreeChunk
        ui64 HugeBlobFreeChunkApply = 0;
        ui64 HugeBlobFreeChunkSkip = 0;
        // HugeLogoBlobToHeap
        ui64 HugeLogoBlobToHeapApply = 0;
        ui64 HugeLogoBlobToHeapSkip = 0;
        // HugeSlotsDelGeneric
        ui64 HugeSlotsDelGenericApply = 0;
        ui64 HugeSlotsDelGenericSkip = 0;
        // Phantom
        ui64 TryPutLogoBlobPhantom = 0;

    public:

#define REC_APPLY       RecDispatcherStat.SetApplyFlag()
#define REC_SKIP        RecDispatcherStat.SetSkipFlag()

        // LogoBlob
        void FreshApplyLogoBlob()                   { REC_APPLY; LogoBlobFreshApply++;       }
        void FreshSkipLogoBlob()                    { REC_SKIP;  LogoBlobFreshSkip++;        }
        void FreshApplyLogoBlobsBatch()             { REC_APPLY; LogoBlobsBatchFreshApply++; }
        void FreshSkipLogoBlobsBatch()              { REC_SKIP;  LogoBlobsBatchFreshSkip++;  }
        void SyncLogApplyLogoBlob()                 { REC_APPLY; LogoBlobSyncLogApply++;     }
        void SyncLogSkipLogoBlob()                  { REC_SKIP;  LogoBlobSyncLogSkip++;      }
        // Huge LogoBlob
        void FreshApplyHugeLogoBlob()               { REC_APPLY; HugeLogoBlobFreshApply++;   }
        void FreshSkipHugeLogoBlob()                { REC_SKIP;  HugeLogoBlobFreshSkip++;    }
        void SyncLogApplyHugeLogoBlob()             { REC_APPLY; HugeLogoBlobSyncLogApply++; }
        void SyncLogSkipHugeLogoBlob()              { REC_SKIP;  HugeLogoBlobSyncLogSkip++;  }
        // Block
        void FreshApplyBlock()                      { REC_APPLY; BlockFreshApply++;          }
        void FreshSkipBlock()                       { REC_SKIP;  BlockFreshSkip++;           }
        void FreshApplyBlocksBatch()                { REC_APPLY; BlocksBatchFreshApply++;    }
        void FreshSkipBlocksBatch()                 { REC_SKIP;  BlocksBatchFreshSkip++;     }
        void SyncLogApplyBlock()                    { REC_APPLY; BlockSyncLogApply++;        }
        void SyncLogSkipBlock()                     { REC_SKIP;  BlockSyncLogSkip++;         }
        // Barrier from SyncLog
        void FreshApplyBarrier()                    { REC_APPLY; BarrierFreshApply++;        }
        void FreshSkipBarrier()                     { REC_SKIP;  BarrierFreshSkip++;         }
        void FreshApplyBarriersBatch()              { REC_APPLY; BarriersBatchFreshApply++;  }
        void FreshSkipBarriersBatch()               { REC_SKIP;  BarriersBatchFreshSkip++;   }
        void SyncLogApplyBarrier()                  { REC_APPLY; BarrierSyncLogApply++;      }
        void SyncLogSkipBarrier()                   { REC_SKIP;  BarrierSyncLogSkip++;       }
        // GC Cmd
        void FreshApplyGCBarrier()                  { REC_APPLY; GCBarrierFreshApply++;      }
        void FreshSkipGCBarrier()                   { REC_SKIP;  GCBarrierFreshSkip++;       }
        void FreshApplyGCLogoBlob()                 { REC_APPLY; GCLogoBlobFreshApply++;     }
        void FreshSkipGCLogoBlob()                  { REC_SKIP;  GCLogoBlobFreshSkip++;      }
        void SyncLogApplyGC()                       { REC_APPLY; GCSyncLogApply++;           }
        void SyncLogSkipGC()                        { REC_SKIP;  GCSyncLogSkip++;            }
        // Local Sync Data
        void SyncDataTryPutLogoBlob()               { TryPutLogoBlobSyncData++;              }
        void SyncDataTryPutBlock()                  { TryPutBlockSyncData++;                 }
        void SyncDataTryPutBarrier()                { TryPutBarrierSyncData++;               }
        // HandoffDel
        void FreshApplyHandoffDel()                 { REC_APPLY; HandoffDelFreshApply++;     }
        void FreshSkipHandoffDel()                  { REC_SKIP;  HandoffDelFreshSkip++;      }
        // HugeBlobAllocChunk
        void ApplyHugeBlobAllocChunk()              { REC_APPLY; HugeBlobAllocChunkApply++;  }
        void SkipHugeBlobAllocChunk()               { REC_SKIP;  HugeBlobAllocChunkSkip++;   }
        // HugeBlobFreeChunk
        void ApplyHugeBlobFreeChunk()               { REC_APPLY; HugeBlobFreeChunkApply++;   }
        void SkipHugeBlobFreeChunk()                { REC_SKIP;  HugeBlobFreeChunkSkip++;    }
        // HugeLogoBlobToHeap
        void ApplyHugeLogoBlobToHeap()              { REC_APPLY; HugeLogoBlobToHeapApply++;  }
        void SkipHugeLogoBlobToHeap()               { REC_SKIP;  HugeLogoBlobToHeapSkip++;   }
        // HugeSlotsDelGeneric
        void ApplyHugeSlotsDelGeneric()             { REC_APPLY; HugeSlotsDelGenericApply++; }
        void SkipHugeSlotsDelGeneric()              { REC_SKIP;  HugeSlotsDelGenericSkip++;  }
        // Phantom
        void PhantomTryPutLogoBlob()                { TryPutLogoBlobPhantom++;               }


        ///////////////////////////////////////////////////////////////////////////////
        // Other methods
        ///////////////////////////////////////////////////////////////////////////////
        TLocalRecoveryInfo(const NMonGroup::TLocalRecoveryGroup &monGroup);
        void FillIn(NKikimrBlobStorage::TLocalRecoveryInfo *proto) const;
        void Output(IOutputStream &str) const;
        void OutputHtml(IOutputStream &str) const;
        TString ToString() const;
        void SetStartingPoint(TLogSignature signature, ui64 lsn);
        void HandleReadLogResult(const NPDisk::TEvReadLogResult::TResults &results);
        void SetRecoveredLogStartLsn(ui64 lsn);
        void CheckConsistency();
        // finish dispatching
        void FinishDispatching();

    private:
        void Disp(TRec r);
        void OutputCounters(IOutputStream &str,
                            const TString &prefix,
                            const TString &suffix,
                            const TString &hr) const;
    };

} // NKikimr
