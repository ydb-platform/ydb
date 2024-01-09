#pragma once

#include "wilson_span.h"

#include <memory>

namespace NWilson {

    class TSpanStack {
    public:
        TSpanStack(TSpan&& root) {
            if (root) {
                Top = std::make_unique<TStackNode>(std::move(root));
            }
        }

        template<class T>
        TSpan* Push(ui8 verbosity, T&& name, TFlags flags = EFlags::NONE) {
            auto span = CreateChild(verbosity, std::forward<T>(name), flags);
            if (span) {
                Top = std::make_unique<TStackNode>(std::move(span), std::move(Top));
                return &Top->Span;
            }
            return nullptr;
        }

        template<class T>
        TSpan CreateChild(ui8 verbosity, T&& name, TFlags flags = EFlags::NONE) const {
            if (auto span = PeekTop()) {
                return span->CreateChild(verbosity, std::forward<T>(name), flags);
            }
            return {};
        }

        TTraceId GetTraceId() const {
            if (auto span = PeekTop()) {
                return span->GetTraceId();
            }
            return {};
        }

        TSpan Pop() {
            if (!Top) {
                return {};
            }
            auto prevTop = std::exchange(Top, std::move(Top->Next));
            return std::move(prevTop->Span);
        }

        void PopOk() {
            Pop().EndOk();
        }

        template<class T>
        void PopError(T&& error) {
            Pop().EndError(std::forward<T>(error));
        }

        TSpan* PeekTop() const {
            if (!Top) {
                return nullptr;
            }
            return &Top->Span;
        }

    private:
        struct TStackNode {
            TStackNode(TSpan&& span, std::unique_ptr<TStackNode> next)
                : Span(std::move(span))
                , Next(std::move(next))
            {}

            TStackNode(TSpan&& span)
                : TStackNode(std::move(span), {})
            {}

            TSpan Span;
            std::unique_ptr<TStackNode> Next;
        };

        std::unique_ptr<TStackNode> Top;
    };

} // NWilson
