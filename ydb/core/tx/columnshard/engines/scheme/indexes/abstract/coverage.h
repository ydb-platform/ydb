#pragma once
#include "checker.h"
#include "common.h"
#include "like.h"

#include <ydb/core/tx/program/program.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

class TBranchCoverage {
private:
    THashMap<TOriginalDataAddress, std::shared_ptr<arrow::Scalar>> Equals;
    THashMap<TOriginalDataAddress, TLikeDescription> Likes;
    YDB_ACCESSOR_DEF(std::vector<std::shared_ptr<IIndexChecker>>, Indexes);

public:
    TBranchCoverage(const THashMap<TOriginalDataAddress, std::shared_ptr<arrow::Scalar>>& equals,
        const THashMap<TOriginalDataAddress, TLikeDescription>& likes)
        : Equals(equals)
        , Likes(likes) {
    }

    const THashMap<TOriginalDataAddress, std::shared_ptr<arrow::Scalar>>& GetEquals() const {
        return Equals;
    }

    const THashMap<TOriginalDataAddress, TLikeDescription>& GetLikes() const {
        return Likes;
    }

    std::shared_ptr<IIndexChecker> GetAndChecker() const;

    TString DebugString() const {
        return DebugJson().GetStringRobust();
    }

    NJson::TJsonValue DebugJson() const;
};

class TDataForIndexesCheckers {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TBranchCoverage>>, Branches);

public:
    TString DebugString() const {
        return DebugJson().GetStringRobust();
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& jsonBranches = result.InsertValue("branches", NJson::JSON_ARRAY);
        for (auto&& i : Branches) {
            jsonBranches.AppendValue(i->DebugJson());
        }
        return result;
    }

    void AddBranch(const THashMap<TOriginalDataAddress, std::shared_ptr<arrow::Scalar>>& equalsData,
        const THashMap<TOriginalDataAddress, TLikeDescription>& likesData) {
        Branches.emplace_back(std::make_shared<TBranchCoverage>(equalsData, likesData));
    }

    static std::shared_ptr<TDataForIndexesCheckers> Build(const TProgramContainer& program);

    TIndexCheckerContainer GetCoverChecker() const;
};

}   // namespace NKikimr::NOlap::NIndexes::NRequest
