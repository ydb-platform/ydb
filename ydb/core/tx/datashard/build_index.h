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
    const TBuildIndexRecord* Get(ui64 id) const {
        Y_ABORT_UNLESS(id != 0);
        if (BuildIndexId == id) {
            return &Record;
        }
        Y_ABORT_UNLESS(BuildIndexId == 0);
        return nullptr;
    }

    void Set(ui64 id, TBuildIndexRecord record) {
        Y_ABORT_UNLESS(id != 0);
        Y_ABORT_UNLESS(BuildIndexId == 0);
        BuildIndexId = id;
        Record = record;
    }

    void Drop(ui64 id) {
        Y_ABORT_UNLESS(Get(id) == &Record);
        BuildIndexId = 0;
        Record = {};
    }

private:
    // Only single shard scan for build index possible now
    ui64 BuildIndexId = 0;
    TBuildIndexRecord Record;
};

}
