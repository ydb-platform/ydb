#pragma once

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/vector.h>

namespace NKikimr::NSQS {

struct TColumn {
    TString Name;
    NScheme::TTypeId TypeId;
    bool Key;
    size_t Partitions;

    TColumn(const TString& name,
            const NScheme::TTypeId type,
            const bool key = false,
            const size_t partitions = 0)
        : Name(name)
        , TypeId(type)
        , Key(key)
        , Partitions(partitions)
    { }
};

struct TTable {
    TString Name;
    TVector<TColumn> Columns;
    bool InMemory = false;
    bool Sequential = false;
    bool Small = false;
    bool OnePartitionPerShard = false; // <queue_name>/State table - has one datashard per SQS-shard
    i64  Shard = 0;
    bool HasLeaderTablet = false;
    bool EnableAutosplit = false;
    ui64 SizeToSplit = 0;

    TTable() = default;

    TTable(const TString& name)
        : Name(name)
    { }

    TTable& SetColumns(const TVector<TColumn>& columns) {
        Columns = columns;
        return *this;
    }

    TTable& SetInMemory(bool value) {
        InMemory = value;
        return *this;
    }

    TTable& SetSequential(bool value) {
        Sequential = value;
        return *this;
    }

    TTable& SetSmall(bool value) {
        Small = value;
        return *this;
    }

    TTable& SetOnePartitionPerShard(bool value) {
        OnePartitionPerShard = value;
        return *this;
    }

    TTable& SetShard(i64 value) {
        Shard = value;
        return *this;
    }

    TTable& SetHasLeaderTablet(bool value = true) {
        HasLeaderTablet = value;
        return *this;
    }

    TTable& SetAutosplit(bool enableAutosplit, ui64 sizeToSplit) {
        EnableAutosplit = enableAutosplit;
        SizeToSplit = sizeToSplit;
        return *this;
    }
};

} // namespace NKikimr::NSQS
