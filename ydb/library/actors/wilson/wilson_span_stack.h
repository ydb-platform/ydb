#pragma once

#include "wilson_span.h"

#include <util/generic/vector.h>
#include <stack>

namespace NWilson {

    class TSpanStack {
    public:
        TSpanStack(TSpan&& root) {
            if (root) {
                Stack.push(std::move(root));
            }
        }

        template<class T>
        TSpan* Push(ui8 verbosity, T&& name, TFlags flags = EFlags::NONE) {
            if (auto span = CreateChild(verbosity, std::forward<T>(name), flags)) {
                Stack.push(std::move(span));
                return &Stack.top();
            }
            return nullptr;
        }

        template<class T>
        TSpan CreateChild(ui8 verbosity, T&& name, TFlags flags = EFlags::NONE) const {
            return PeekTopConst().CreateChild(verbosity, std::forward<T>(name), flags);
        }

        TTraceId GetTraceId() const {
            return PeekTopConst().GetTraceId();
        }

        TSpan Pop() {
            if (Stack.empty()) {
                return {};
            }
            auto top = std::move(Stack.top());
            Stack.pop();
            return top;
        }

        void PopOk() {
            Pop().EndOk();
        }

        template<class T>
        void PopError(T&& error) {
            Pop().EndError(std::forward<T>(error));
        }

        TSpan* PeekTop() {
            if (Stack.empty()) {
                return nullptr;
            }
            return &Stack.top();
        }

        const TSpan& PeekTopConst() const {
            if (Stack.empty()) {
                return TSpan::Empty;
            }
            return Stack.top();
        }

    private:
        std::stack<TSpan, TVector<TSpan>> Stack;
    };

} // NWilson
