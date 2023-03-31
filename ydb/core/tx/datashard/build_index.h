#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/util/intrusive_heap.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/map.h>

namespace NKikimr::NDataShard {

class TDataShard;

struct TBuildIndexRecord {

    struct TSeqNo {
        ui64 Generation = 0;
        ui64 Round = 0;

        bool operator == (const TSeqNo& x) const {
            return Generation == x.Generation && Round == x.Round;
        }

        bool operator < (const TSeqNo& x) const {
            return Generation != x.Generation ? Generation < x.Generation : Round < x.Round;
        }
    };

    ui64 ScanId = 0;
    TSeqNo SeqNo = {0, 0};

};

class TBuildIndexManager {
public:
    bool Contains(ui64 id) {
        return Records.contains(id);
    }

    void Set(ui64 id, TBuildIndexRecord record) {
        Records.emplace(id, record);
    }

    TBuildIndexRecord Get(ui64 id) const {
        return Records.at(id);
    }

    void Drop(ui64 id) {
        Records.erase(id);
    }

private:
    using TBuildIndexIdToScanIdMap = TMap<ui64, TBuildIndexRecord>;

    TBuildIndexIdToScanIdMap Records;
};

}
