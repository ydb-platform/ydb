#pragma once

#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/util/intrusive_heap.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/map.h>

namespace NKikimr::NDataShard {

using TIndexBuildScanSettings = NKikimrIndexBuilder::TIndexBuildScanSettings;
class TDataShard;
struct TUserTable;

struct TScanRecord {
    struct TSeqNo {
        ui64 Generation = 0;
        ui64 Round = 0;

        bool operator==(const TSeqNo& x) const noexcept = default;
        auto operator<=>(const TSeqNo& x) const noexcept = default;
    };
    using TScanIds = std::vector<ui64>;

    TSeqNo SeqNo;
    TScanIds ScanIds;
};

class TScanManager {
public:
    const TScanRecord* Get(ui64 id) const {
        Y_ENSURE(id != 0);
        if (Id == id) {
            return &Record;
        }
        Y_ENSURE(Id == 0);
        return nullptr;
    }

    TScanRecord::TScanIds& Set(ui64 id, TScanRecord::TSeqNo seqNo) {
        Y_ENSURE(id != 0);
        Y_ENSURE(Id == 0);
        Id = id;
        Record.SeqNo = seqNo;
        return Record.ScanIds;
    }

    void Drop(ui64 id) {
        Y_ENSURE(Get(id) == &Record);
        Id = 0;
        Record = {};
    }

private:
    // Only single shard scan, that use ScanManager possible now
    ui64 Id = 0;
    TScanRecord Record;
};

using TColumnsTags = THashMap<TString, NTable::TTag>;
using TTags = TVector<NTable::TTag>;
using TProtoColumnsCRef = const google::protobuf::RepeatedPtrField<TString>&;

TColumnsTags GetAllTags(const TUserTable& tableInfo);

void AddTags(TTags& tags, const TColumnsTags& allTags, TProtoColumnsCRef columns);

template <typename... Args>
TTags BuildTags(const TUserTable& tableInfo, Args&&... columns) {
    auto allTags = GetAllTags(tableInfo);

    TTags tags;
    tags.reserve((0 + ... + columns.size()));

    (..., AddTags(tags, allTags, columns));

    return tags;
}

using TColumnsTypes = THashMap<TString, NScheme::TTypeInfo>;

TColumnsTypes GetAllTypes(const TUserTable& tableInfo);

ui64 CountRowCellBytes(TConstArrayRef<TCell> key, TConstArrayRef<TCell> value);

inline TDuration GetRetryWakeupTimeoutBackoff(ui32 attempt) {
    const ui32 maxBackoffExponent = 3;

    return TDuration::Seconds(1u << Min(attempt, maxBackoffExponent));
}

}
