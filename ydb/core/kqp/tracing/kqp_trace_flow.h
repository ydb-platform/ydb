#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr::NKqp {

// Unmarked children are sequential calls. Spawned children progress independently.
inline constexpr TStringBuf ParentRelationAttribute = "parent.relation";
inline constexpr TStringBuf SpawnRelation = "spawn";
inline constexpr TStringBuf RelationAttribute = "link.relation";
// A waiting span requires every linked result before continuing.
inline constexpr TStringBuf DependsOnRelation = "depends_on";
// Completion of the linked span enables this span to start.
inline constexpr TStringBuf FollowsFromRelation = "follows_from";

inline void MarkSpawn(NWilson::TSpan& span) {
    if (span) {
        span.Attribute(TString(ParentRelationAttribute), TString(SpawnRelation));
    }
}

inline void LinkDependsOn(NWilson::TSpan& span, const NWilson::TTraceId& dependency) {
    if (span && dependency) {
        span.Link(dependency, {{TString(RelationAttribute), TString(DependsOnRelation)}});
    }
}

inline void LinkFollowsFrom(NWilson::TSpan& span, const NWilson::TTraceId& predecessor) {
    if (span && predecessor) {
        span.Link(predecessor, {{TString(RelationAttribute), TString(FollowsFromRelation)}});
    }
}

} // namespace NKikimr::NKqp
