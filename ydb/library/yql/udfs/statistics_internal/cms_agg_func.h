#pragma once

#include "common.h"

#include <yql/essentials/core/minsketch/count_min_sketch.h>

namespace NKikimr::NStat::NAggFuncs {

class TCMSAggFunc {
public:
    static constexpr std::string_view GetName() { return "CountMinSketch"; }

    using TState = std::unique_ptr<NKikimr::TCountMinSketch>;

    static constexpr size_t ParamsCount = 2;

    static TState CreateState(
            TTypeId /*columnTypeId*/, const std::span<const TValue, ParamsCount>& params) {
        return TState(NKikimr::TCountMinSketch::Create(
            params[0].Get<ui64>(), params[1].Get<ui64>()));
    }
    static auto CreateStateUpdater(TTypeId columnTypeId) {
        return [columnTypeId](TState& state, const TValue& val) {
            Y_ENSURE(state);
            VisitValue(columnTypeId, val, RawDataVisitor(
                [&state](const char* data, size_t size) {
                    state->Count(data, size);
                }));
        };
    }
    static void MergeStates(const TState& left, TState& right) {
        Y_ENSURE(left && right);
        *left += *right;
    }
    static TString SerializeState(const TState& state) {
        Y_ENSURE(state);
        return TString(state->AsStringBuf());
    }
    static TState DeserializeState(const char* data, size_t size) {
        return TState(NKikimr::TCountMinSketch::FromString(data, size));
    }
    static TString FinalizeState(const TState& state) {
        return SerializeState(state);
    }
};

} // NKikimr::NStat::NAggFuncs
