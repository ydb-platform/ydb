#pragma once

#include "common.h"

#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>

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

class TEWHAggFunc {
public:
    static constexpr std::string_view GetName() { return "EquiWidthHistogram"; }

    using TState = std::unique_ptr<NKikimr::TEqWidthHistogram>;

    static constexpr size_t ParamsCount = 3;

    static TState CreateState(
            TTypeId columnTypeId, const std::span<const TValue, ParamsCount>& params) {
        TState histogram;

        switch (columnTypeId) {
#define CREATE_HISTOGRAM_CASE(type, layout)                                                                 \
            case NUdf::TDataType<type>::Id: {                                                               \
                auto valType = NKikimr::GetHistogramValueType<type>();                                      \
                Y_ENSURE(valType, "Unsupported histogram data type");                                       \
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui32>(), *valType);  \
                histogram->InitializeBuckets(params[1].Get<layout>(), params[2].Get<layout>());             \
                break;                                                                                      \
            }
            KNOWN_FIXED_VALUE_TYPES(CREATE_HISTOGRAM_CASE)
#undef CREATE_HISTOGRAM_CASE

            default:
                Y_ENSURE(false, "Unsupported histogram column type id");
        }

        return histogram;
    }

    static auto CreateStateUpdater(TTypeId columnTypeId) {
        return [columnTypeId](TState& state, const TValue& val) {
            Y_ENSURE(state);
            switch (columnTypeId) {
#define MAKE_PRIMITIVE_VISITOR(type, layout)                                                                \
                case NUdf::TDataType<type>::Id: {                                                           \
                    auto valType = NKikimr::GetHistogramValueType<type>();                                  \
                    Y_ENSURE(valType, "Unsupported histogram data type");                                   \
                    state->AddElement(val.Get<layout>());                                                   \
                    break;                                                                                  \
                }
                KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_VISITOR)
#undef MAKE_PRIMITIVE_VISITOR

                default:
                    Y_ENSURE(false, "Unsupported histogram column type id");
            }
        };
    }

    static void MergeStates(const TState& left, TState& right) {
        Y_ENSURE(left && right);
        left->Aggregate(*right);
    }

    static TString SerializeState(const TState& state) {
        Y_ENSURE(state);
        auto [binaryData, binarySize] = state->Serialize();
        return TString(binaryData.get(), binarySize);
    }

    static TState DeserializeState(const char* data, size_t size) {
        return std::make_unique<NKikimr::TEqWidthHistogram>(data, size);
    }

    static TString FinalizeState(const TState& state) {
        return SerializeState(state);
    }
};

using TAllAggFuncsList = TTypeList<
    TCMSAggFunc,
    TEWHAggFunc
>;

} // NKikimr::NStat::NAggFuncs
