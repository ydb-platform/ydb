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
            VisitValue(columnTypeId, val, RawDataVisitor(
                [&state](const char* data, size_t size) {
                    state->Count(data, size);
                }));
        };
    }
    static void MergeStates(const TState& left, TState& right) {
        Y_ASSERT(left && right);
        *left += *right;
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

class TEWHAggFunc {
public:
    static constexpr std::string_view GetName() { return "EquiWidthHistogram"; }

    using TState = std::unique_ptr<NKikimr::TEqWidthHistogram>;

    static constexpr size_t ParamsCount = 3;

    static TState CreateState(
            TTypeId columnTypeId, const std::span<const TValue, ParamsCount>& params) {
        TState histogram;
        switch (columnTypeId) {
            case NUdf::TDataType<i16>::Id: {
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui64>(), NKikimr::EHistogramValueType::Int16);
                histogram->InitializeBuckets(params[1].Get<i16>(), params[2].Get<i16>());
                break;
            }
            case NUdf::TDataType<i32>::Id: {
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui64>(), NKikimr::EHistogramValueType::Int32);
                histogram->InitializeBuckets(params[1].Get<i32>(), params[2].Get<i32>());
                break;
            }
            case NUdf::TDataType<i64>::Id: {
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui64>(), NKikimr::EHistogramValueType::Int64);
                histogram->InitializeBuckets(params[1].Get<i64>(), params[2].Get<i64>());
                break;
            }
            case NUdf::TDataType<ui16>::Id: {
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui64>(), NKikimr::EHistogramValueType::Uint16);
                histogram->InitializeBuckets(params[1].Get<ui16>(), params[2].Get<ui16>());
                break;
            }
            case NUdf::TDataType<ui32>::Id: {
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui64>(), NKikimr::EHistogramValueType::Uint32);
                histogram->InitializeBuckets(params[1].Get<ui64>(), params[2].Get<ui64>());
                break;
            }
            case NUdf::TDataType<ui64>::Id: {
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui64>(), NKikimr::EHistogramValueType::Uint64);
                histogram->InitializeBuckets(params[1].Get<ui64>(), params[2].Get<ui64>());
                break;
            }
            case NUdf::TDataType<double>::Id: {
                histogram = std::make_unique<NKikimr::TEqWidthHistogram>(params[0].Get<ui64>(), NKikimr::EHistogramValueType::Double);
                histogram->InitializeBuckets(params[1].Get<double>(), params[2].Get<double>());
                break;
            }
            default: Y_ABORT("Unsupported histogram type");
        }
        return histogram;
    }

    static auto CreateStateUpdater(TTypeId columnTypeId) {
        return [columnTypeId](TState& state, const TValue& val) {
            VisitValue(columnTypeId, val, RawDataVisitor(
                [&state](const char* data, size_t size) {
                    state->AddElement(data, size);
                }));
        };
    }

    static void MergeStates(const TState& left, TState& right) {
        Y_ASSERT(left && right);
        left->Aggregate(*right);
    }

    static TString SerializeState(const TState& state) {
        auto [binaryData, binarySize] = state->Serialize();
        Y_ASSERT(binaryData && binarySize);
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
