#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>

#include <util/string/printf.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

// Common types used by both IBlockLayoutConverter and IScalarLayoutConverter
struct TPackResult {
    std::vector<ui8, TMKQLAllocator<ui8>> PackedTuples;
    std::vector<ui8, TMKQLAllocator<ui8>> Overflow;
    i64 NTuples{ 0 };
    i64 AllocatedBytes() const;
    TPackResult() = default;
    ~TPackResult() = default;

    TPackResult(TPackResult&& other)
        : PackedTuples(std::move(other.PackedTuples))
        , Overflow(std::move(other.Overflow))
        , NTuples(other.NTuples)
    {
        other.NTuples = 0;
    }

    TPackResult& operator=(TPackResult&& other) {
        PackedTuples = std::move(other.PackedTuples);
        Overflow = std::move(other.Overflow);
        NTuples = other.NTuples;
        other.NTuples = 0;
        return *this;
    }

    TPackResult(const TPackResult& other) = delete;
    TPackResult& operator=(const TPackResult& other) = delete;

    bool Empty() const {
        bool allFieldEmpty = NTuples == 0 && PackedTuples.empty();
        bool haveOneFieldEmpty = NTuples == 0 || PackedTuples.empty();
        MKQL_ENSURE(allFieldEmpty == haveOneFieldEmpty, "inconsistent state");
        if (allFieldEmpty) {
            MKQL_ENSURE(Overflow.empty(), "sanity check");
        }
        return allFieldEmpty;
    }
    struct Iterator{
        using difference_type = std::ptrdiff_t;
        using element_type = const TSingleTuple;
        using pointer = element_type *;
        using reference = element_type &;
    private:
        const TPackResult* base;
        i32 index;
        i32 Width() const {
            return base->PackedTuples.size() / base->NTuples;
        }
    public:
        Iterator() { MKQL_ENSURE(false,"Not implemented"); }
        Iterator(const TPackResult& pack, i32 idx) : base(&pack), index(idx) {}
        element_type operator*() const { return {.PackedData = base->PackedTuples.data() + Width()*index, .OverflowBegin = base->Overflow.data()}; }
        auto &operator++() { index++; return *this; }
        auto operator++(int) { auto tmp = *this; ++(*this); return tmp; }
        auto begin() {return Iterator(*base, 0);}
        auto end() {return Iterator(*base, base->NTuples);}
        bool operator==(const Iterator& other) const = default;
    };

    auto begin() const {return Iterator(*this, 0);}
    auto end() const {return Iterator(*this, this->NTuples);}


    void Clear() {
        *this = TPackResult{};
        MKQL_ENSURE(Empty(), "sanity check");
    }

    void AppendTuple(TSingleTuple tuple, const NPackedTuple::TTupleLayout* layout);
};

using TPackedTuple = std::vector<ui8, TMKQLAllocator<ui8>>;
using TOverflow = std::vector<ui8, TMKQLAllocator<ui8>>;

}   // namespace NKikimr::NMiniKQL
