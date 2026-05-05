#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NDataShard {
    enum class EBuildIndexEventType : ui32 {
        Unused = 0, 
        SecondaryIndexResponseFinal = 1,
        SecondaryIndexProgressResponse = 2
    };

    inline constexpr TStringBuf IndexBuildScanResponseTypeFinal =
        TStringBuf("TEvBuildIndexProgressResponseFinal");
    inline constexpr TStringBuf IndexProgressResponse = TStringBuf("TEvBuildIndexProgressResponse");

    class TBuildIndexScanManager {
    public:
        struct TScanInfo {
            ui64 BuildId = 0;
            ui64 SeqNoGeneration = 0;
            ui64 SeqNoRound = 0;
            ui32 ResponseType = 0;
            TString FinalProgressRecordSerialized;
        };

    public:
        bool Load(NIceDb::TNiceDb& db);

        void PersistAdd(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound,
                        ui32 responseType);

        void PersistRemove(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound);

        void PersistMarkFinalResponse(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound,
                                      const TString& serializedRecord);

        const THashMap<ui64, TScanInfo>& GetScans() const {
            return Scans;
        }

        void Reset();

    private:
        THashMap<ui64, TScanInfo> Scans;
    };

} // namespace NKikimr::NDataShard
