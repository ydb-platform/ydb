#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <vector>

namespace NKikimr {
namespace NMiniKQL {

struct TWideUnboxedEqual {
    TWideUnboxedEqual(const TKeyTypes& types)
        : Types(types)
    {}

    bool operator()(const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right) const {
        for (ui32 i = 0U; i < Types.size(); ++i) {
            if (CompareValues(Types[i].first, true, Types[i].second, left[i], right[i])) {
                return false;
            }
        }
        return true;
    }

    const TKeyTypes& Types;
};

template<bool FibonacciOptimization>
struct TWideUnboxedHasherFib {
    TWideUnboxedHasherFib(const TKeyTypes& types, const ui64 seed = 0ULL)
        : Seed(seed)
        , Types(types)
    {}

    NUdf::THashType operator()(const NUdf::TUnboxedValuePod* values) const {
        NUdf::THashType hash;

        if (Types.size() == 1U) {
            if (const auto v = *values) {
                hash = CombineHashes(Seed, NUdf::GetValueHash(Types.front().first, v));
            } else {
                hash = HashOfNull;
            }
        } else {
            hash = Seed;

            for (const auto& type : Types) {
                if (const auto v = *values++) {
                    hash = CombineHashes(hash, NUdf::GetValueHash(type.first, v));
                } else {
                    hash = CombineHashes(hash, HashOfNull);
                }
            }
        }

        // https://web.archive.org/web/20180620004325/https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        if constexpr (FibonacciOptimization) {
            return hash * 11400714819323198485ull;
        } else {
            return hash;
        }
    }

private:
    const ui64 Seed;
    const TKeyTypes& Types;
};

using TWideUnboxedHasher = TWideUnboxedHasherFib<false>;

bool UnwrapBlockTypes(const TArrayRef<TType* const>& typeComponents, std::vector<TType*>& result);

void WrapArrayBlockTypes(std::vector<TType*>& types, const TProgramBuilder& pb);

int ArrowScalarAsInt(const TArrowBlock& scalar);

bool ForceLeftOptional(EJoinKind kind);

// Left join causes all right columns to be nullable
bool ForceRightOptional(EJoinKind kind);

constexpr bool SemiOrOnlyJoin(EJoinKind kind) {
    switch (kind) {
        using enum EJoinKind;
    case RightOnly:
    case RightSemi:
    case LeftOnly:
    case LeftSemi:
        return true;
    default:
        return false;
    }
}

constexpr bool
ContainsRowsFromInnerJoin(EJoinKind kind) { // true if kind is a join that contains all rows from inner join output.
    switch (kind) {
        using enum EJoinKind;
    case Inner:
    case Full:
    case Left:
    case Right:
    case Cross:
        return true;
    default:
        return false;
    }
}

constexpr bool LeftSemiOrOnly(EJoinKind kind) {
    switch (kind) {
        using enum EJoinKind;
    case LeftOnly:
    case LeftSemi:
        return true;
    default:
        return false;
    }
}

constexpr bool RightSemiOrOnly(EJoinKind kind) {
    switch (kind) {
        using enum EJoinKind;
    case RightSemi:
    case RightOnly:
        return true;
    default:
        return false;
    }
}

struct Yield {};

struct Finish {};

template <typename Payload> struct One {
    Payload Data;
};

template <typename Payload> using FetchResult = std::variant<Finish, Yield, One<Payload>>;

template <typename Payload> Payload& GetPayload(FetchResult<Payload>& res) {
    auto* p = std::get_if<One<Payload>>(&res);
    MKQL_ENSURE(p, "precondition failed");
    return p->Data;
}

template <typename Payload> const Payload& GetPayload(const FetchResult<Payload>& res) {
    auto* p = std::get_if<One<Payload>>(&res);
    MKQL_ENSURE(p, "precondition failed");
    return p->Data;
}

template <typename Payload> EFetchResult AsResult(const FetchResult<Payload>& var) {
    return static_cast<EFetchResult>(int(var.index()) - 1);
}

template <typename Payload> NYql::NUdf::EFetchStatus AsStatus(const FetchResult<Payload>& var) {
    int index = var.index();
    switch (index) {
    case 0:
        return NYql::NUdf::EFetchStatus::Finish;
    case 1:
        return NYql::NUdf::EFetchStatus::Yield;
    case 2:
        return NYql::NUdf::EFetchStatus::Ok;
    }
    MKQL_ENSURE(false, "fetchresult is valueless?");
}

enum class EJoinSide { kLeft, kRight };

template <typename SideEnum> struct TIndexAndSide {
    int Index;
    SideEnum Side;
};

template <typename SideEnum> using TDqRenames = std::vector<TIndexAndSide<SideEnum>>;

using TDqUserRenames = TDqRenames<EJoinSide>;

void ValidateRenames(const TDqUserRenames& renames, EJoinKind kind, int leftTypesWidth, int rightTypesWidth);

struct TGraceJoinRenames {
    TVector<ui32> Left;
    TVector<ui32> Right;
    static TGraceJoinRenames FromRuntimeNodes(TRuntimeNode left, TRuntimeNode right);
    static TGraceJoinRenames FromDq(const TDqUserRenames& dqJoinRenames);
};

TDqUserRenames FromGraceFormat(const TGraceJoinRenames& graceJoinRenames);

} // namespace NMiniKQL
} // namespace NKikimr