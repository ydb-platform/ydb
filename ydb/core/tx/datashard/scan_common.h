#pragma once

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/util/intrusive_heap.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/map.h>

namespace NKikimr::NDataShard {

class TDataShard;
struct TUserTable;

struct TScanRecord {
    struct TSeqNo {
        ui64 Generation = 0;
        ui64 Round = 0;

        bool operator==(const TSeqNo& x) const noexcept = default;
        auto operator<=>(const TSeqNo& x) const noexcept = default;
    };

    ui64 ScanId = 0;
    TSeqNo SeqNo;
};

class TScanManager {
public:
    const TScanRecord* Get(ui64 id) const {
        Y_ABORT_UNLESS(id != 0);
        if (Id == id) {
            return &Record;
        }
        Y_ABORT_UNLESS(Id == 0);
        return nullptr;
    }

    void Set(ui64 id, TScanRecord record) {
        Y_ABORT_UNLESS(id != 0);
        Y_ABORT_UNLESS(Id == 0);
        Id = id;
        Record = record;
    }

    void Drop(ui64 id) {
        Y_ABORT_UNLESS(Get(id) == &Record);
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

}
