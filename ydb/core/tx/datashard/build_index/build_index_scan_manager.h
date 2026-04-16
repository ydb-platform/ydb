#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NDataShard {

    inline constexpr TStringBuf IndexBuildScanResponseTypeFinal =
        TStringBuf("TEvBuildIndexProgressResponseFinal");

    class TBuildIndexScanManager {
    public:
        struct TScanInfo {
            ui64 BuildId = 0;
            ui64 SeqNoGeneration = 0;
            ui64 SeqNoRound = 0;
            TString ResponseType;
            TString FinalProgressRecordSerialized;
        };

    public:
        bool Load(NIceDb::TNiceDb& db);

        void PersistAdd(NIceDb::TNiceDb& db, ui64 buildId, ui64 seqNoGeneration, ui64 seqNoRound,
                        TStringBuf responseType);

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
