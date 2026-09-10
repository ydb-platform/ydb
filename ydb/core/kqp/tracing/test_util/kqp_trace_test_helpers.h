#pragma once

#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <ranges>

namespace NKikimr::NKqp::NTest {

using NWilson::TFakeWilsonUploader;
using NWilson::TTraceSnapshot;
namespace NTraceProto = NWilson::NTraceProto;

inline void ClearUploader(TFakeWilsonUploader& uploader) {
    uploader.Spans.clear();
    uploader.Traces.clear();
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

} // namespace NKikimr::NKqp::NTest
