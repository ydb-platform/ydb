#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>

namespace NKikimr {

    template <class TSubsIterator, class TDbIterator>
    TString MergeIteratorWithWholeDbDefaultCrashReport(const TString &logPrefix, const TSubsIterator &subsIt, const TDbIterator &dbIt) {
        TStringStream str;
        str << logPrefix << "SubsIt:  ";
        if (subsIt.Valid())
            str << subsIt.GetCurKey().ToString();
        else
            str << "not_valid";

        str << " dbIt: ";
        if (dbIt.Valid())
            str << dbIt.GetCurKey().ToString();
        else
            str << "not_valid";

        // in case of error dump database for exploration
        str << "\n";
        dbIt.DumpAll(str);
        str << "\n";

        return str.Str();
    }

    ////////////////////////////////////////////////////////////////////////////
    // MergeIteratorWithWholeDb
    // The caller must implement:
    //   newItem(subsIt, subsMerger);
    //   doMerge(subsIt, dbIt, subsMerger, dbMerger);
    //   crashReport(subsIt, dbIt);
    ////////////////////////////////////////////////////////////////////////////
    template <
        bool CollectStats,
        class TSubsIterator,
        class TDbIterator,
        class TRecordMerger,
        class TNewItem,
        class TDoMerge,
        class TCrash>
    void MergeIteratorWithWholeDbImpl(
            const TBlobStorageGroupType &gtype,
            TSubsIterator &subsIt,
            TDbIterator &dbIt,
            TNewItem &newItem,
            TDoMerge &doMerge,
            TCrash &crashReport,
            ui32 skipBeforeSeek,
            ui64 *keysProcessedCounter,
            ui64 *sourceRecordsProcessedCounter,
            ui64 *dbRecordsMergedCounter,
            ui64 *seekCounter,
            ui64 *nextCounter)
    {
        // the subset we processing
        TRecordMerger subsMerger(gtype);

        // for the whole level index
        TRecordMerger dbMerger(gtype);
        auto finishDbMerge = [&] {
            dbMerger.Finish();
            if constexpr (CollectStats) {
                ++*keysProcessedCounter;
                *dbRecordsMergedCounter += dbMerger.GetNumMergedRecords();
            }
        };

        bool first = true;
        while (subsIt.Valid()) {
            subsIt.PutToMerger(&subsMerger);
            subsMerger.Finish();
            if constexpr (CollectStats) {
                *sourceRecordsProcessedCounter += subsMerger.GetNumMergedRecords();
            }
            newItem(subsIt, subsMerger);

            if (first) {
                // initialize level iterator
                first = false;
                if constexpr (CollectStats) {
                    ++*seekCounter;
                }
                dbIt.Seek(subsIt.GetCurKey());
                Y_ABORT_UNLESS(dbIt.Valid(), "%s", crashReport(subsIt, dbIt).data());
                dbIt.PutToMerger(&dbMerger);
                finishDbMerge();
            }

            ui32 seenItems = 0;
            while (dbIt.GetCurKey() < subsIt.GetCurKey()) {
                seenItems++;
                dbMerger.Clear();

                if (seenItems < skipBeforeSeek) {
                    if constexpr (CollectStats) {
                        ++*nextCounter;
                    }
                    dbIt.Next();
                } else {
                    if constexpr (CollectStats) {
                        ++*seekCounter;
                    }
                    dbIt.Seek(subsIt.GetCurKey());
                }
                Y_ABORT_UNLESS(dbIt.Valid(), "%s", crashReport(subsIt, dbIt).data());

                dbIt.PutToMerger(&dbMerger);
                finishDbMerge();
            }

            Y_ABORT_UNLESS(subsIt.GetCurKey() == dbIt.GetCurKey(), "%s", crashReport(subsIt, dbIt).data());

            doMerge(subsIt, dbIt, subsMerger, dbMerger);

            // next
            subsMerger.Clear();
            subsIt.Next();
        }
    }

    template <class TSubsIterator, class TDbIterator, class TRecordMerger, class TNewItem, class TDoMerge, class TCrash>
    void MergeIteratorWithWholeDb(const TBlobStorageGroupType &gtype, TSubsIterator &subsIt,
                                  TDbIterator &dbIt, TNewItem &newItem, TDoMerge &doMerge,
                                  TCrash &crashReport, ui32 skipBeforeSeek = 6) {
        MergeIteratorWithWholeDbImpl<false, TSubsIterator, TDbIterator, TRecordMerger>(
            gtype,
            subsIt,
            dbIt,
            newItem,
            doMerge,
            crashReport,
            skipBeforeSeek,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr);
    }

    template <class TSubsIterator, class TDbIterator, class TRecordMerger, class TNewItem, class TDoMerge, class TCrash>
    void MergeIteratorWithWholeDbWithStat(const TBlobStorageGroupType &gtype, TSubsIterator &subsIt,
                                          TDbIterator &dbIt, TNewItem &newItem, TDoMerge &doMerge,
                                          TCrash &crashReport, ui64 &keysProcessedCounter,
                                          ui64 &sourceRecordsProcessedCounter, ui64 &dbRecordsMergedCounter,
                                          ui64 &seekCounter, ui64 &nextCounter,
                                          ui32 skipBeforeSeek = 6) {
        MergeIteratorWithWholeDbImpl<true, TSubsIterator, TDbIterator, TRecordMerger>(
            gtype,
            subsIt,
            dbIt,
            newItem,
            doMerge,
            crashReport,
            skipBeforeSeek,
            &keysProcessedCounter,
            &sourceRecordsProcessedCounter,
            &dbRecordsMergedCounter,
            &seekCounter,
            &nextCounter);
    }

} // NKikimr
