#include "type_utils.h"
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {
bool UnwrapBlockTypes(const TArrayRef<TType* const>& typeComponents, std::vector<TType*>& result) {
    bool hasBlock = false;
    bool hasNonBlock = false;

    result.reserve(typeComponents.size());
    for (TType* type : typeComponents) {
        if (type->GetKind() == TType::EKind::Block) {
            hasBlock = true;
            type = static_cast<const TBlockType*>(type)->GetItemType();
        } else {
            hasNonBlock = true;
        }
        result.push_back(type);
    }
    MKQL_ENSURE(hasBlock != hasNonBlock, "Inconsistent wide item types: mixing of blocks and non-blocks detected");
    return hasBlock;
}

void WrapArrayBlockTypes(std::vector<TType*>& types, const TProgramBuilder& pb) {
    std::transform(types.begin(), types.end(), types.begin(),
                   [&](TType* type) { return pb.NewBlockType(type, TBlockType::EShape::Many); });
}

int ArrowScalarAsInt(const TArrowBlock& scalar) {
    return scalar.GetDatum().scalar_as<arrow::UInt64Scalar>().value;
}

bool ForceRightOptional(EJoinKind kind) {
    switch (kind) {
    case EJoinKind::Left:
    case EJoinKind::Exclusion:
    case EJoinKind::Full:
        return true;
    default:
        return false;
    }
}

bool ForceLeftOptional(EJoinKind kind) {
    switch (kind) {
    case EJoinKind::Right:
    case EJoinKind::Exclusion:
    case EJoinKind::Full:
        return true;
    default:
        return false;
    }
}

void ValidateRenames(const TDqRenames& renames, EJoinKind kind, int leftTypesWidth, int rightTypesWidth) {
    if (LeftSemiOrOnly(kind)) {
        MKQL_ENSURE(std::find_if(renames.begin(), renames.end(), [&](const TIndexAndSide& data) {
                        return data.Side == JoinSide::kRight;
                    }) == renames.end(), "right side tuple in left semi or inner join renames?");
    }
    if (RightSemiOrOnly(kind)) {
        MKQL_ENSURE(std::find_if(renames.begin(), renames.end(), [&](const TIndexAndSide& data) {
                        return data.Side == JoinSide::kLeft;
                    }) == renames.end(), "right side tuple in right semi or inner join renames?");
    }

    for (TIndexAndSide rename : renames) {
        MKQL_ENSURE(rename.Index >= 0, "column index negative");
        if (rename.Side == JoinSide::kLeft) {
            MKQL_ENSURE(rename.Index < leftTypesWidth, "column index too big");
        } else {
            MKQL_ENSURE(rename.Index < rightTypesWidth, "column index too big");
        }
    }
}

void Print(auto arr) {
    for (auto& val : arr) {
        Cout << val << " ";
    }
    Cout << Endl;
}

TDqRenames FromGraceFormat(const TGraceJoinRenames& graceJoinRenames) {
    TDqRenames map;
    map.resize((graceJoinRenames.Left.size() + graceJoinRenames.Right.size()) / 2, {-1, JoinSide::kLeft});
    MKQL_ENSURE(graceJoinRenames.Left.size() % 2 == 0,
                "grace join renames arrays go in pairs, left array has incorrect size");
    MKQL_ENSURE(graceJoinRenames.Right.size() % 2 == 0,
                "grace join renames arrays go in pairs, right array has incorrect size");
    for (int index = 0; index < std::ssize(graceJoinRenames.Left); index += 2) {
        MKQL_ENSURE(map[graceJoinRenames.Left[index + 1]].Index == -1, "duplicate output column in grace join renames");
        map[graceJoinRenames.Left[index + 1]] = {int(graceJoinRenames.Left[index]), JoinSide::kLeft};
    }
    for (int index = 0; index < std::ssize(graceJoinRenames.Right); index += 2) {
        MKQL_ENSURE(map[graceJoinRenames.Right[index + 1]].Index == -1,
                    "duplicate output column in grace join renames");
        map[graceJoinRenames.Right[index + 1]] = {int(graceJoinRenames.Right[index]), JoinSide::kRight};
    }
    return map;
}

TGraceJoinRenames TGraceJoinRenames::FromDq(const TDqRenames& dqJoinRenames) {
    TGraceJoinRenames renames;
    for (int index = 0; index < std::ssize(dqJoinRenames); ++index) {
        auto& dest = dqJoinRenames[index];
        if (dest.Side == JoinSide::kLeft) {
            renames.Left.push_back(dest.Index);
            renames.Left.push_back(index);
        } else {
            renames.Right.push_back(dest.Index);
            renames.Right.push_back(index);
        }
    }
    return renames;
}

TGraceJoinRenames TGraceJoinRenames::FromRuntimeNodes(TRuntimeNode left, TRuntimeNode right) {
    TGraceJoinRenames GJRenames;
    auto asVector = [](TRuntimeNode tuple) {
        TVector<ui32> vec;
        auto renamesTuple = AS_VALUE(TTupleLiteral, tuple);
        for (ui32 i = 0; i < renamesTuple->GetValuesCount(); ++i) {
            vec.emplace_back(AS_VALUE(TDataLiteral, renamesTuple->GetValue(i))->AsValue().Get<ui32>());
        }
        return vec;
    };
    GJRenames.Left = asVector(left);
    GJRenames.Right = asVector(right);
    return GJRenames;
}

} // namespace NKikimr::NMiniKQL