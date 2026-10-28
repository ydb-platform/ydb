#pragma once

#include <ydb/core/kqp/tracing/kqp_trace_flow.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

#include <algorithm>
#include <ranges>

namespace NKikimr::NKqp::NTest {

using NWilson::TFakeWilsonUploader;
using NWilson::TTraceSnapshot;
namespace NTraceProto = NWilson::NTraceProto;

inline void ClearUploader(TFakeWilsonUploader& uploader) {
    uploader.Clear();
}

inline bool SpanNameMatches(TStringBuf actual, TStringBuf expected) {
    return expected == "Task: " || expected == "Stage: " ? actual.StartsWith(expected) : actual == expected;
}

inline const TTraceSnapshot::TOtelSpan* FindSpan(const TTraceSnapshot& uploader, TStringBuf name) {
    for (const auto& span : uploader.Spans) {
        if (SpanNameMatches(span.name(), name)) {
            return &span;
        }
    }
    return nullptr;
}

inline const TTraceSnapshot::TOtelSpan* FindSpanByPrefix(const TTraceSnapshot& uploader, TStringBuf prefix) {
    const auto it = std::ranges::find_if(uploader.Spans, [&](const auto& span) {
        return span.name().StartsWith(prefix);
    });
    return it == uploader.Spans.end() ? nullptr : &*it;
}

inline const TTraceSnapshot::TOtelSpan* FindParent(const TTraceSnapshot& uploader, const TTraceSnapshot::TOtelSpan& child) {
    const auto it = std::ranges::find_if(uploader.Spans, [&](const auto& span) {
        return span.trace_id() == child.trace_id() && span.span_id() == child.parent_span_id();
    });
    return it == uploader.Spans.end() ? nullptr : &*it;
}

template<class T>
const opentelemetry::proto::common::v1::KeyValue* FindAttribute(
        const T& span, TStringBuf name) {
    for (const auto& attr : span.attributes()) {
        if (attr.key() == name) {
            return &attr;
        }
    }
    return nullptr;
}

inline auto StageSpans(const TTraceSnapshot& uploader) {
    return uploader.Spans | std::views::filter([](const auto& span) {
        return span.name().StartsWith("Stage: ");
    });
}

inline void AssertStatus(const TTraceSnapshot& uploader, TStringBuf name,
        NTraceProto::Status::StatusCode status) {
    const auto* span = FindSpan(uploader, name);
    UNIT_ASSERT_C(span, "missing span " << name << ": " << uploader.PrintTraces());
    UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(span->status().code()), static_cast<int>(status), span->DebugString());
}

inline void AssertDescendant(const TTraceSnapshot& uploader, TStringBuf childName, TStringBuf parentName) {
    const auto* child = FindSpan(uploader, childName);
    UNIT_ASSERT_C(child, uploader.PrintTraces());
    TString parentId = child->parent_span_id();
    for (size_t hop = 0; hop < uploader.Spans.size(); ++hop) {
        const auto it = std::ranges::find_if(uploader.Spans, [&](const auto& span) {
            return span.trace_id() == child->trace_id() && span.span_id() == parentId;
        });
        if (it == uploader.Spans.end()) {
            break;
        }
        if (SpanNameMatches(it->name(), parentName)) {
            return;
        }
        parentId = it->parent_span_id();
    }
    UNIT_FAIL(childName << " is not under " << parentName << ": " << uploader.PrintTraces());
}

// Checks the lanes of logical flows (see kqp_trace_flow.h) under the first span
// with the given name. A span is on the lane of its parent unless it starts a
// flow of its own or runs on another node. Spans of a lane form a stack: each
// one nests in its parent, and children of a span on its lane do not overlap.
// Returns the violations: an unmarked concurrent span or a misordered clock.
inline TVector<TString> FindFlowNestingViolations(const TTraceSnapshot& snapshot, TStringBuf rootName) {
    using TOtelSpan = TTraceSnapshot::TOtelSpan;
    const auto key = [](const TString& traceId, const TString& spanId) {
        return traceId + spanId;
    };
    const auto stringAttribute = [](const TOtelSpan& span, TStringBuf name) -> TString {
        const auto* attr = FindAttribute(span, name);
        return attr ? attr->value().string_value() : TString();
    };
    const auto nodeId = [](const TOtelSpan& span) -> i64 {
        const auto* attr = FindAttribute(span, "node_id");
        return attr ? attr->value().int_value() : -1;
    };

    THashMap<TString, const TOtelSpan*> spans;
    for (const auto& span : snapshot.Spans) {
        spans[key(span.trace_id(), span.span_id())] = &span;
    }
    const auto parentOf = [&](const TOtelSpan& span) -> const TOtelSpan* {
        const auto it = spans.find(key(span.trace_id(), span.parent_span_id()));
        return it == spans.end() ? nullptr : it->second;
    };

    const auto* root = FindSpan(snapshot, rootName);
    UNIT_ASSERT_C(root, "missing span " << rootName << ": " << snapshot.PrintTraces());
    const auto underRoot = [&](const TOtelSpan& span) {
        for (const auto* current = &span; current; current = parentOf(*current)) {
            if (current == root) {
                return true;
            }
        }
        return false;
    };

    TVector<TString> violations;
    THashMap<const TOtelSpan*, TVector<const TOtelSpan*>> laneChildren;
    for (const auto& span : snapshot.Spans) {
        const auto* parent = parentOf(span);
        if (!parent || &span == root || !underRoot(span) || stringAttribute(span, FlowAttribute) == AsyncFlow
            || nodeId(span) != nodeId(*parent)) {
            continue;
        }
        if (span.start_time_unix_nano() < parent->start_time_unix_nano()
            || span.end_time_unix_nano() > parent->end_time_unix_nano()) {
            violations.push_back(TStringBuilder() << span.name() << " sticks out of its parent " << parent->name());
        }
        laneChildren[parent].push_back(&span);
    }
    for (auto& [parent, children] : laneChildren) {
        std::ranges::sort(children, {}, [](const TOtelSpan* span) {
            return std::pair(span->start_time_unix_nano(), span->end_time_unix_nano());
        });
        // Compare with the child that ends last so far, not only the previous one: a
        // long child can overlap several later ones.
        const TOtelSpan* latest = nullptr;
        for (const auto* child : children) {
            if (latest && child->start_time_unix_nano() < latest->end_time_unix_nano()) {
                violations.push_back(TStringBuilder() << child->name() << " overlaps "
                    << latest->name() << " in " << parent->name());
            }
            if (!latest || child->end_time_unix_nano() > latest->end_time_unix_nano()) {
                latest = child;
            }
        }
    }
    return violations;
}

inline void AssertFlowsNest(const TTraceSnapshot& snapshot, TStringBuf rootName) {
    const auto violations = FindFlowNestingViolations(snapshot, rootName);
    TStringBuilder message;
    for (const auto& violation : violations) {
        message << violation << "\n";
    }
    UNIT_ASSERT_C(violations.empty(), message << snapshot.PrintTraces());
}

} // namespace NKikimr::NKqp::NTest
