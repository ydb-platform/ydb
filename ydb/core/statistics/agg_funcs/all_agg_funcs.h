#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>

namespace NKikimr::NStat::NAggFuncs {

class TCMSAggFunc {
public:
    static constexpr std::string_view GetName() { return "CountMinSketch"; }

    using TState = std::unique_ptr<NKikimr::TCountMinSketch>;

    static constexpr size_t ParamsCount = 2;

    static TState CreateState(const std::array<ui64, ParamsCount>& params) {
        return TState(NKikimr::TCountMinSketch::Create(params[0], params[1]));
    }
    static auto CreateStateUpdater() {
        return [](TState& state, const TCell& cell) {
            state->Count(cell.Data(), cell.Size());
        };
    }
    static void MergeStates(const TState& left, TState& right) {
        *right += *left;
    }
    static TString SerializeState(const TState& state) {
        return TString(state->AsStringBuf());
    }
    static TState DeserializeState(const char* data, size_t size) {
        return TState(NKikimr::TCountMinSketch::FromString(data, size));
    }
    static TString FinalizeState(const TState& state) {
        return SerializeState(state);
    }
};

using TAllAggFuncsList = TTypeList<
    TCMSAggFunc
>;

} // NKikimr::NStat::NAggFuncs
