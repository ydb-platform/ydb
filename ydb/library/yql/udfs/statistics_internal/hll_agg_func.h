#pragma once

#include "common.h"

#include <library/cpp/hyperloglog/hyperloglog.h>

#include <util/digest/murmur.h>
#include <util/generic/hash_set.h>
#include <util/stream/mem.h>
#include <util/string/cast.h>

#include <variant>

namespace NKikimr::NStat::NAggFuncs {

// Adapted from yql/essentials/udfs/common/hyperloglog/hyperloglog_udf.cpp
template<template<typename> typename TAlloc>
class THybridHyperLogLog {
private:
    using THybridSet = THashSet<ui64, std::hash<ui64>, std::equal_to<ui64>, TAlloc<ui64>>;
    using THybridHll = THyperLogLogWithAlloc<TAlloc<ui8>>;

    explicit THybridHyperLogLog(ui32 precision)
        : Var_(THybridSet())
        , SizeLimit_((1u << precision) / 8)
        , Precision_(precision)
    {
    }

    THybridHll ConvertToHyperLogLog() const {
        auto res = THybridHll::Create(Precision_);
        for (auto& el : GetSetRef()) {
            res.Update(el);
        }
        return res;
    }

    bool IsSet() const {
        return Var_.index() == 1;
    }

    const THybridSet& GetSetRef() const {
        return std::get<1>(Var_);
    }

    THybridSet& GetMutableSetRef() {
        return std::get<1>(Var_);
    }

    const THybridHll& GetHllRef() const {
        return std::get<0>(Var_);
    }

    THybridHll& GetMutableHllRef() {
        return std::get<0>(Var_);
    }

public:
    THybridHyperLogLog(THybridHyperLogLog&&) = default;

    THybridHyperLogLog& operator=(THybridHyperLogLog&&) = default;

    void Update(ui64 hash) {
        if (IsSet()) {
            GetMutableSetRef().insert(hash);
            if (GetSetRef().size() >= SizeLimit_) {
                Var_ = ConvertToHyperLogLog();
            }
        } else {
            GetMutableHllRef().Update(hash);
        }
    }

    void Merge(const THybridHyperLogLog& rh) {
        if (IsSet() && rh.IsSet()) {
            GetMutableSetRef().insert(rh.GetSetRef().begin(), rh.GetSetRef().end());
            if (GetSetRef().size() >= SizeLimit_) {
                Var_ = ConvertToHyperLogLog();
            }
        } else {
            if (IsSet()) {
                Var_ = ConvertToHyperLogLog();
            }
            if (rh.IsSet()) {
                GetMutableHllRef().Merge(rh.ConvertToHyperLogLog());
            } else {
                GetMutableHllRef().Merge(rh.GetHllRef());
            }
        }
    }

    void Save(IOutputStream& out) const {
        out.Write(static_cast<char>(Var_.index()));
        out.Write(static_cast<char>(Precision_));
        if (IsSet()) {
            ::Save(&out, GetSetRef());
        } else {
            GetHllRef().Save(out);
        }
    }

    ui64 Estimate() const {
        if (IsSet()) {
            return GetSetRef().size();
        }
        return GetHllRef().Estimate();
    }

    static THybridHyperLogLog Create(unsigned precision) {
        Y_ENSURE(precision >= THyperLogLog::PRECISION_MIN && precision <= THyperLogLog::PRECISION_MAX);
        return THybridHyperLogLog(precision);
    }

    static THybridHyperLogLog Load(IInputStream& in) {
        char type;
        Y_ENSURE(in.ReadChar(type));
        char precision;
        Y_ENSURE(in.ReadChar(precision));
        auto res = Create(precision);
        if (type) {
            ::Load(&in, res.GetMutableSetRef());
        } else {
            res.Var_ = THybridHll::Load(in);
        }
        return res;
    }

private:
    std::variant<THybridHll, THybridSet> Var_;

    size_t SizeLimit_;

    ui32 Precision_;
};

// UDAF to calculate count of distinct column values.
class THLLAggFunc {
public:
    static constexpr std::string_view GetName() { return "HLL"; }

    using TState = THybridHyperLogLog<NYql::NUdf::TStdAllocatorForUdf>;

    static constexpr ui32 DEFAULT_PRECISION = 14;
    static constexpr size_t ParamsCount = 0;

    static TState CreateState(
            TTypeId /*columnTypeId*/, const std::span<const TValue, ParamsCount>&) {
        return TState::Create(DEFAULT_PRECISION);
    }
    static auto CreateStateUpdater(TTypeId columnTypeId) {
        return [columnTypeId](TState& state, const TValue& val) {
            VisitValue(columnTypeId, val, RawDataVisitor(
                [&state](const char* data, size_t size) {
                    state.Update(MurmurHash<ui64>(data, size));
                }));
        };
    }
    static void MergeStates(const TState& left, TState& right) {
        right.Merge(left);
    }
    static TString SerializeState(const TState& state) {
        TStringStream os;
        state.Save(os);
        return std::move(os).Str();
    }
    static TState DeserializeState(const char* data, size_t size) {
        TMemoryInput is(data, size);
        return TState::Load(is);
    }
    static TString FinalizeState(const TState& state) {
        return ToString(state.Estimate());
    }
};

} // NKikimr::NStat::NAggFuncs
