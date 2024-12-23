#pragma once
#include <ydb/core/tx/program/program.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

class TLikeDescription {
private:
    THashMap<TString, std::vector<TString>> LikeSequences;

public:
    TLikeDescription(const TString& likeEquation) {
        LikeSequences.emplace(likeEquation, StringSplitter(likeEquation).Split('%').ToList<TString>());

    }

    const THashMap<TString, std::vector<TString>>& GetLikeSequences() const {
        return LikeSequences;
    }

    void Merge(const TLikeDescription& d) {
        for (auto&& i : d.LikeSequences) {
            LikeSequences.emplace(i.first, i.second);
        }
    }

    TString ToString() const {
        TStringBuilder sb;
        for (auto&& i : LikeSequences) {
            sb << "{" << i.first << ":[";
            for (auto&& s: i.second) {
                sb << s << ",";
            }
            sb << "];}";
        }
        return sb;
    }
};

class TBranchCoverage {
private:
    THashMap<TString, std::shared_ptr<arrow::Scalar>> Equals;
    THashMap<TString, TLikeDescription> Likes;
    YDB_ACCESSOR_DEF(std::vector<std::shared_ptr<IIndexChecker>>, Indexes);

public:
    TBranchCoverage(
        const THashMap<TString, std::shared_ptr<arrow::Scalar>>& equals, const THashMap<TString, TLikeDescription>& likes)
        : Equals(equals)
        , Likes(likes)
    {

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