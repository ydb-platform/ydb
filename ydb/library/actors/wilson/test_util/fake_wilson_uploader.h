#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>

#include <library/cpp/threading/future/future.h>

#include <util/stream/str.h>
#include <util/system/mutex.h>

#include <functional>
#include <optional>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace NWilson {

    class TTraceSnapshot {
    public:
        using TOtelSpan = opentelemetry::proto::trace::v1::Span;

        class Span {
        public:
            Span(TString name, TString parentSpanId, ui64 startTime) : Name(name), ParentSpanId(parentSpanId), StartTime(startTime) {}

            std::optional<std::reference_wrapper<Span>> FindOne(TString targetName) {
                for (const auto childRef : Children) {
                    if (childRef.get().Name == targetName) {
                        return childRef;
                    }
                }

                return {};
            }

            std::vector<std::reference_wrapper<Span>> FindAll(TString targetName) {
                std::vector<std::reference_wrapper<Span>> res;

                for (const auto childRef : Children) {
                    if (childRef.get().Name == targetName) {
                        res.emplace_back(childRef);
                    }
                }

                return res;
            }

            std::optional<std::reference_wrapper<Span>> BFSFindOne(TString targetName) {
                std::queue<std::reference_wrapper<Span>> bfsQueue;
                bfsQueue.push(std::ref(*this));

                while (!bfsQueue.empty()) {
                    Span &currentSpan = bfsQueue.front().get();
                    bfsQueue.pop();

                    if (currentSpan.Name == targetName) {
                        return currentSpan;
                    }

                    for (const auto childRef : currentSpan.Children) {
                        bfsQueue.push(childRef);
                    }
                }

                return {};
            }

            static bool CompareByStartTime(const std::reference_wrapper<Span>& span1, const std::reference_wrapper<Span>& span2) {
                return span1.get().StartTime < span2.get().StartTime;
            }

            TString Name;
            TString ParentSpanId;
            ui64 StartTime;
            std::set<std::reference_wrapper<Span>, decltype(&CompareByStartTime)> Children{&CompareByStartTime};
        };

        class Trace {
        public:
            std::string ToString() const {
                std::string result;

                for (const auto& spanPair : Spans) {
                    const Span& span = spanPair.second;
                    if (span.ParentSpanId.empty()) {
                        result += ToStringHelper(span);
                    }
                }

                return result;
            }
        private:
            std::string ToStringHelper(const Span& span) const {
                std::string result = "(" + span.Name;

                if (!span.Children.empty()) {
                    result += " -> [";
                    auto it = span.Children.begin();
                    while (it != span.Children.end()) {
                        const Span& childSpan = it->get();
                        result += ToStringHelper(childSpan);
                        ++it;

                        if (it != span.Children.end()) {
                            result += " , ";
                        }
                    }
                    result += "]";
                }

                result += ")";

                return result;
            }
        public:
            TMutex SpansMutex;
            std::unordered_map<TString, Span> Spans;

            Span Root{"Root", "", 0};
        };

    public:
        void AddSpan(const TOtelSpan& span) {
            TOtelSpan spanCopy;
            spanCopy.CopyFrom(span);
            Spans.push_back(std::move(spanCopy));
            const TString &traceId = span.trace_id();
            const TString &spanId = span.span_id();
            const TString &parentSpanId = span.parent_span_id();
            const TString &spanName = span.name();
            ui64 startTime = span.start_time_unix_nano();

            Trace &trace = Traces[traceId];
            TGuard lock(trace.SpansMutex);

            trace.Spans.try_emplace(spanId, spanName, parentSpanId, startTime);
        }

        [[nodiscard]] bool BuildTraceTrees() {
            for (auto& tracePair : Traces) {
                Trace& trace = tracePair.second;

                TGuard lock(trace.SpansMutex);
                for (auto& spanPair : trace.Spans) {
                    Span& span = spanPair.second;

                    const TString& parentSpanId = span.ParentSpanId;

                    // Check if the span has a parent
                    if (!parentSpanId.empty()) {
                        auto parentSpanIt = trace.Spans.find(parentSpanId);
                        if (parentSpanIt == trace.Spans.end()) {
                            return false;
                        }
                        parentSpanIt->second.Children.insert(std::ref(span));
                    } else {
                        trace.Root.Children.insert(std::ref(span));
                    }
                }
            }
            return true;
        }

        void Clear() {
            Traces.clear();
        }

    public:
        std::unordered_map<TString, Trace> Traces;
        std::vector<TOtelSpan> Spans;

        TString PrintTraces() const {
            TStringStream str;
            for (const auto& [_, trace] : Traces) {
                str << "{ " << trace.ToString() << " } ";
            }
            return str.Str();
        }
    };

    class TFakeWilsonUploader : public NActors::TActorBootstrapped<TFakeWilsonUploader>, public TTraceSnapshot {
    public:
        struct TEvGetSnapshot : NActors::TEventLocal<TEvGetSnapshot, EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 1> {
            explicit TEvGetSnapshot(NThreading::TPromise<std::vector<TOtelSpan>> promise)
                : Promise(std::move(promise))
            {}

            NThreading::TPromise<std::vector<TOtelSpan>> Promise;
        };

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        void Handle(NWilson::TEvWilson::TPtr ev) {
            AddSpan(ev->Get()->Span);
        }

        void Handle(TEvGetSnapshot::TPtr ev) {
            ev->Get()->Promise.SetValue(Spans);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NWilson::TEvWilson, Handle);
            hFunc(TEvGetSnapshot, Handle);
        );
    };

} // namespace NWilson
