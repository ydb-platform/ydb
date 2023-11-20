#include "localrecovery_defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_internal_interface.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>


namespace NKikimr {

    namespace NLocRecovery {

        ////////////////////////////////////////////////////////////////////////
        // Record dispatcher that tracks applied records
        ////////////////////////////////////////////////////////////////////////
        void TRecordDispatcherStat::NewRecord(NMonGroup::TLocalRecoveryGroup *mon, ui32 size) {
            // finish with prev rec
            FinishRecord(mon, size);
            // update total stat
            TotalBytesDispatched += size;
            ++TotalRecordsDispatched;
            mon->LocalRecovBytesDispatched() += size;
            ++mon->LocalRecovRecsDispatched();
        }

        void TRecordDispatcherStat::SetApplyFlag() {
            CurRecApplied = true;
        }

        void TRecordDispatcherStat::SetSkipFlag() {
        }

        void TRecordDispatcherStat::Finish(NMonGroup::TLocalRecoveryGroup *mon) {
            // finish prev record
            FinishRecord(mon, 0);
        }

        void TRecordDispatcherStat::FinishRecord(NMonGroup::TLocalRecoveryGroup *mon, ui32 newRecSize) {
            // finish with prev record
            if (CurRecApplied) {
                TotalBytesApplied += CurRecSize;
                ++TotalRecordsApplied;
                mon->LocalRecovBytesApplied() += CurRecSize;
                ++mon->LocalRecovRecsApplied();
            }
            // setup cur record
            CurRecSize = newRecSize;
            CurRecApplied = false;
        }

    } // NLocRecovery

    ////////////////////////////////////////////////////////////////////////////
    // TLocalRecoveryInfo
    ////////////////////////////////////////////////////////////////////////////
    TLocalRecoveryInfo::TLocalRecoveryInfo(const NMonGroup::TLocalRecoveryGroup &monGroup)
        : LocalRecoveryStartTime(TAppData::TimeProvider->Now())
        , MonGroup(monGroup)
    {}

    void TLocalRecoveryInfo::FillIn(NKikimrBlobStorage::TLocalRecoveryInfo *proto) const {
        proto->SetStartLsn(RecoveredLogStartLsn);

        proto->SetSuccessfulRecovery(SuccessfulRecovery);
        proto->SetEmptyLogoBlobsDb(EmptyLogoBlobsDb);
        proto->SetEmptyBlocksDb(EmptyBlocksDb);
        proto->SetEmptyBarriersDb(EmptyBarriersDb);
        proto->SetEmptySyncLog(EmptySyncLog);
        proto->SetEmptySyncer(EmptySyncer);
        proto->SetEmptyHuge(EmptyHuge);

        proto->SetRecoveryLogFirstLsn(RecoveryLogFirstLsn);
        proto->SetRecoveryLogLastLsn(RecoveryLogLastLsn);
    }


    void TLocalRecoveryInfo::OutputCounters(IOutputStream &str,
                                            const TString &prefix,
                                            const TString &suffix,
                                            const TString &hr) const {
        auto boolToStr = [] (bool x) { return x ? "true" : "false"; };
        str << "RecoveredLogStartLsn# " << RecoveredLogStartLsn << suffix
            << prefix << "SuccessfulRecovery# " << boolToStr(SuccessfulRecovery) << suffix
            << prefix << "EmptyLogoBlobsDb# " << boolToStr(EmptyLogoBlobsDb) << suffix
            << prefix << "EmptyBlocksDb# " << boolToStr(EmptyBlocksDb) << suffix
            << prefix << "EmptyBarriersDb# " << boolToStr(EmptyBarriersDb) << suffix
            << prefix << "EmptySyncLog# " << boolToStr(EmptySyncLog) << suffix
            << prefix << "EmptySyncer# " << boolToStr(EmptySyncer) << suffix
            << prefix << "EmptyHuge# " << boolToStr(EmptyHuge) << suffix;
        str << hr;
        // Dispatched Log Records
        str << prefix << "LogRecLogoBlob# " << LogRecLogoBlob << suffix
            << prefix << "LogRecBlock# " << LogRecBlock << suffix
            << prefix << "LogRecGC# " << LogRecGC << suffix
            << prefix << "LogRecSyncLogIdx# " << LogRecSyncLogIdx << suffix
            << prefix << "LogRecLogoBlobsDB# " << LogRecLogoBlobsDB << suffix
            << prefix << "LogRecBlocksDB# " << LogRecBlocksDB << suffix
            << prefix << "LogRecBarriersDB# " << LogRecBarriersDB << suffix
            << prefix << "LogRecCutLog# " << LogRecCutLog << suffix
            << prefix << "LogRecLocalSyncData# " << LogRecLocalSyncData << suffix
            << prefix << "LogRecSyncerState# " << LogRecSyncerState << suffix
            << prefix << "LogRecHandoffDel# " << LogRecHandoffDel << suffix
            << prefix << "LogRecHugeBlobAllocChunk# " << LogRecHugeBlobAllocChunk << suffix
            << prefix << "LogRecHugeBlobFreeChunk# " << LogRecHugeBlobFreeChunk << suffix
            << prefix << "LogRecHugeBlobEntryPoint# " << LogRecHugeBlobEntryPoint << suffix
            << prefix << "LogRecHugeLogoBlob# " << LogRecHugeLogoBlob << suffix
            << prefix << "LogRecLogoBlobOpt# " << LogRecLogoBlobOpt << suffix
            << prefix << "LogRecPhantomBlob# " << LogRecPhantomBlob << suffix
            << prefix << "LogRecAnubisOsirisPut# " << LogRecAnubisOsirisPut << suffix
            << prefix << "LogRecAddBulkSst# " << LogRecAddBulkSst << suffix;
        str << hr;
        // Applied/Skip records
        str << prefix << "LogoBlobFreshApply# " << LogoBlobFreshApply << suffix
            << prefix << "LogoBlobFreshSkip# " << LogoBlobFreshSkip << suffix
            << prefix << "LogoBlobsBatchFreshApply# " << LogoBlobsBatchFreshApply << suffix
            << prefix << "LogoBlobsBatchFreshSkip#" << LogoBlobsBatchFreshSkip << suffix
            << prefix << "LogoBlobSyncLogApply# " << LogoBlobSyncLogApply << suffix
            << prefix << "LogoBlobSyncLogSkip# " << LogoBlobSyncLogSkip << suffix
            << prefix << "HugeLogoBlobFreshApply# " << HugeLogoBlobFreshApply << suffix
            << prefix << "HugeLogoBlobFreshSkip# " << HugeLogoBlobFreshSkip << suffix
            << prefix << "HugeLogoBlobSyncLogApply# " << HugeLogoBlobSyncLogApply << suffix
            << prefix << "HugeLogoBlobSyncLogSkip# " << HugeLogoBlobSyncLogSkip << suffix
            << prefix << "BlockFreshApply# " << BlockFreshApply << suffix
            << prefix << "BlockFreshSkip# " << BlockFreshSkip << suffix
            << prefix << "BlocksBatchFreshApply# " << BlocksBatchFreshApply << suffix
            << prefix << "BlocksBatchFreshSkip# " << BlocksBatchFreshSkip << suffix
            << prefix << "BlockSyncLogApply# " << BlockSyncLogApply << suffix
            << prefix << "BlockSyncLogSkip# " << BlockSyncLogSkip << suffix
            << prefix << "BarrierFreshApply# " << BarrierFreshApply << suffix
            << prefix << "BarrierFreshSkip# " << BarrierFreshSkip << suffix
            << prefix << "BarriersBatchFreshApply# " << BarriersBatchFreshApply << suffix
            << prefix << "BarriersBatchFreshSkip# " << BarriersBatchFreshSkip << suffix
            << prefix << "BarrierSyncLogApply# " << BarrierSyncLogApply << suffix
            << prefix << "BarrierSyncLogSkip# " << BarrierSyncLogSkip << suffix
            << prefix << "GCBarrierFreshApply# " << GCBarrierFreshApply << suffix
            << prefix << "GCBarrierFreshSkip# " << GCBarrierFreshSkip << suffix
            << prefix << "GCLogoBlobFreshApply# " << GCLogoBlobFreshApply << suffix
            << prefix << "GCLogoBlobFreshSkip# " << GCLogoBlobFreshSkip << suffix
            << prefix << "GCSyncLogApply# " << GCSyncLogApply << suffix
            << prefix << "GCSyncLogSkip# " << GCSyncLogSkip << suffix
            << prefix << "TryPutLogoBlobSyncData# " << TryPutLogoBlobSyncData << suffix
            << prefix << "TryPutBlockSyncData# " << TryPutBlockSyncData << suffix
            << prefix << "TryPutBarrierSyncData# " << TryPutBarrierSyncData << suffix
            << prefix << "HandoffDelFreshApply# " << HandoffDelFreshApply << suffix
            << prefix << "HandoffDelFreshSkip# " << HandoffDelFreshSkip << suffix
            << prefix << "HugeBlobAllocChunkApply# " << HugeBlobAllocChunkApply << suffix
            << prefix << "HugeBlobAllocChunkSkip# " << HugeBlobAllocChunkSkip << suffix
            << prefix << "HugeBlobFreeChunkApply# " << HugeBlobFreeChunkApply << suffix
            << prefix << "HugeBlobFreeChunkSkip# " << HugeBlobFreeChunkSkip << suffix
            << prefix << "HugeLogoBlobToHeapApply# " << HugeLogoBlobToHeapApply << suffix
            << prefix << "HugeLogoBlobToHeapSkip# " << HugeLogoBlobToHeapSkip << suffix
            << prefix << "HugeSlotsDelGenericApply# " << HugeSlotsDelGenericApply << suffix
            << prefix << "HugeSlotsDelGenericSkip# " << HugeSlotsDelGenericSkip << suffix
            << prefix << "TryPutLogoBlobPhantom# " << TryPutLogoBlobPhantom << suffix;
        str << hr;
        // other
        str << prefix << "RecoveryLogDiapason# [" << RecoveryLogFirstLsn
            << " " << RecoveryLogLastLsn << "]" << suffix;
    }


    void TLocalRecoveryInfo::Output(IOutputStream &str) const {
        str << "{"; // border
        if (LocalRecoveryFinishTime.GetValue() == 0) {
            str << "RecoveryDuration# INPROGRESS";
        } else {
            str << "RecoveryDuration# " << (LocalRecoveryFinishTime - LocalRecoveryStartTime);
        }
        str << " ";
        OutputCounters(str, " ", "", "");
        str << " StartingPoints# {";
        for (const auto &x : StartingPoints) {
            str << "[" << x.first.ToString()
                << " " << x.second << "]";
        }
        str << "}";
        str << " ReadLogReplies# {";
        for (const auto &x : ReadLogReplies) {
            x.Output(str);
        }
        str << "}";
        str << "}"; // border
    }

    void TLocalRecoveryInfo::OutputHtml(IOutputStream &str) const {
        str << "\n";
        HTML(str) {
            DIV_CLASS("panel panel-default") {
                DIV_CLASS("panel-heading") {str << "Local Recovery Info";}
                DIV_CLASS("panel-body") {
                    str << "LocalRecoveryStartTime: " << LocalRecoveryStartTime << "<br>";
                    if (LocalRecoveryFinishTime.GetValue() == 0) {
                        // recovery is in progress
                        str << "LocalRecoveryFinishTime: INPROGRESS<br>";
                        str << "LocalRecoveryDuration: INPROGRESS<br>";
                    } else {
                        str << "LocalRecoveryFinishTime: " << LocalRecoveryFinishTime << "<br>";
                        auto duration = LocalRecoveryFinishTime - LocalRecoveryStartTime;
                        str << "LocalRecoveryDuration: " << duration << "<br>";
                    }
                    OutputCounters(str, "", "<br>", "<hr>\n");
                }
            }
        }
        str << "\n";
    }

    TString TLocalRecoveryInfo::ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    void TLocalRecoveryInfo::Disp(TLocalRecoveryInfo::TRec r) {
        // update dispatcher stat
        RecDispatcherStat.NewRecord(&MonGroup, r.Data.size());
        // set up lsn diapason
        if (RecoveryLogFirstLsn == Max<ui64>())
            RecoveryLogFirstLsn = r.Lsn;
        RecoveryLogLastLsn = r.Lsn;
    }

    void TLocalRecoveryInfo::SetStartingPoint(TLogSignature signature, ui64 lsn) {
        bool success = StartingPoints.insert(TSignatureToLsn::value_type(signature, lsn)).second;
        Y_ABORT_UNLESS(success);
    }

    void TLocalRecoveryInfo::HandleReadLogResult(
            const NPDisk::TEvReadLogResult::TResults &results)
    {
        ui64 firstSegLsn = 0;
        ui64 lastSegLsn = 0;
        if (!results.empty()) {
            firstSegLsn = results.begin()->Lsn;
            lastSegLsn = (results.end() - 1)->Lsn;
        }
        ReadLogReplies.emplace_back(results.size(), firstSegLsn, lastSegLsn);
    }

    void TLocalRecoveryInfo::SetRecoveredLogStartLsn(ui64 lsn) {
        RecoveredLogStartLsn = lsn;
    }

    void TLocalRecoveryInfo::CheckConsistency() {
        if (SuccessfulRecovery) {
            bool emptyLog = (RecoveryLogFirstLsn == Max<ui64>()) && RecoveryLogLastLsn == 0;
            if (!StartingPoints.empty() && emptyLog) {
                Y_ABORT("Empty log with none empty entry points; State# %s", ToString().data());
            }
        }
    }

    void TLocalRecoveryInfo::FinishDispatching() {
        LocalRecoveryFinishTime = TAppData::TimeProvider->Now();
        RecDispatcherStat.Finish(&MonGroup);
    }

} // NKikimr
