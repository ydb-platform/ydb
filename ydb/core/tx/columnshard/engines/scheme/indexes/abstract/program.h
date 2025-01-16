#pragma once
#include <ydb/core/tx/program/program.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

class TLikePart {
public:
    enum class EOperation {
        StartsWith,
        EndsWith,
        Contains
    };

private:
    YDB_READONLY(EOperation, Operation, EOperation::Contains);
    YDB_READONLY_DEF(TString, Value);

public:
    TLikePart(const EOperation op, const TString& value)
        : Operation(op)
        , Value(value) {
    }

    static TLikePart MakeStart(const TString& value) {
        return TLikePart(EOperation::StartsWith, value);
    }
    static TLikePart MakeEnd(const TString& value) {
        return TLikePart(EOperation::EndsWith, value);
    }
    static TLikePart MakeContains(const TString& value) {
        return TLikePart(EOperation::Contains, value);
    }

    TString ToString() const {
        if (Operation == EOperation::StartsWith) {
            return '%' + Value;
        }
        if (Operation == EOperation::EndsWith) {
            return Value + '%';
        }
        if (Operation == EOperation::Contains) {
            return Value;
        }
        AFL_VERIFY(false);
        return "";
    }
};

class TLikeDescription {
private:
    THashMap<TString, TLikePart> LikeSequences;

public:
    TLikeDescription(const TLikePart& likePart) {
        LikeSequences.emplace(likePart.ToString(), likePart);
    }

    const THashMap<TString, TLikePart>& GetLikeSequences() const {
        return LikeSequences;
    }

    void Merge(const TLikeDescription& d) {
        for (auto&& i : d.LikeSequences) {
            LikeSequences.emplace(i.first, i.second);
        }
    }

    TString ToString() const {
        TStringBuilder sb;
        sb << "[";
        for (auto&& i : LikeSequences) {
            sb << i.first << ",";
        }
        sb << "];";
        return sb;
    }
};

class TBranchCoverage {
private:
    THashMap<TString, std::shared_ptr<arrow::Scalar>> Equals;
    THashMap<TString, TLikeDescription> Likes;
    YDB_ACCESSOR_DEF(std::vector<std::shared_ptr<IIndexChecker>>, Indexes);

public:
    TBranchCoverage(const THashMap<TString, std::shared_ptr<arrow::Scalar>>& equals, const THashMap<TString, TLikeDescription>& likes)
        : Equals(equals)
        , Likes(likes) {
    }

    const THashMap<TString, std::shared_ptr<arrow::Scalar>>& GetEquals() const {
        return Equals;
    }

    const THashMap<TString, TLikeDescription>& GetLikes() const {
        return Likes;
    }

    std::shared_ptr<IIndexChecker> GetAndChecker() const;
};

class TDataForIndexesCheckers {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TBranchCoverage>>, Branches);

public:
    void AddBranch(const THashMap<TString, std::shared_ptr<arrow::Scalar>>& equalsData, const THashMap<TString, TLikeDescription>& likesData) {
        Branches.emplace_back(std::make_shared<TBranchCoverage>(equalsData, likesData));
    }

    static std::shared_ptr<TDataForIndexesCheckers> Build(const TProgramContainer& program);

    TIndexCheckerContainer GetCoverChecker() const;
};

}   // namespace NKikimr::NOlap::NIndexes::NRequest
