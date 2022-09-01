#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/actors/core/actor.h>

#include <util/system/types.h>

#include <util/generic/fwd.h>
#include <util/generic/hash.h>
#include <util/generic/ylimits.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <vector>

namespace NKikimr::NKqp::NTopic {

class TOffsetsRangeIntersectExpection : public yexception {
};

struct TPartition {
    void AddRange(const Ydb::Topic::OffsetsRange &range);
    void SetTabletId(ui64 value);

    void Merge(const TPartition& rhs);

private:
    void AddRangeImpl(ui64 begin, ui64 end);

    TDisjointIntervalTree<ui64> Offsets_;
    ui64 TabletId_ = Max<ui64>();
};

struct TTopic {
    TPartition& AddPartition(ui32 id);
    TPartition* GetPartition(ui32 id);

    void Merge(const TTopic& rhs);

    THashMap<ui32, TPartition> Partitions_;
};

struct TOffsetsInfo {
    TTopic& AddTopic(const TString &path);
    TTopic* GetTopic(const TString &path);

    void FillSchemeCacheNavigate(NSchemeCache::TSchemeCacheNavigate& navigate) const;
    bool ProcessSchemeCacheNavigate(const NSchemeCache::TSchemeCacheNavigate::TResultSet& results,
                                    Ydb::StatusIds_StatusCode& status,
                                    TString& message);

    void Merge(const TOffsetsInfo& rhs);

    THashMap<TString, TTopic> Topics_;
};

}
