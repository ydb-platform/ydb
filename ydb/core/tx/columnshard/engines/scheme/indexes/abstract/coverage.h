#pragma once
#include "checker.h"
#include "like.h"

#include <ydb/core/tx/program/program.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

class TBranchCoverage {
private:
    THashMap<ui32, std::shared_ptr<arrow::Scalar>> Equals;
    THashMap<ui32, TLikeDescription> Likes;
    YDB_ACCESSOR_DEF(std::vector<std::shared_ptr<IIndexChecker>>, Indexes);

public:
    TBranchCoverage(const THashMap<ui32, std::shared_ptr<arrow::Scalar>>& equals, const THashMap<ui32, TLikeDescription>& likes)
        : Equals(equals)
        , Likes(likes) {
    }

    const THashMap<ui32, std::shared_ptr<arrow::Scalar>>& GetEquals() const {
        return Equals;
    }

    const THashMap<ui32, TLikeDescription>& GetLikes() const {
        return Likes;
    }

    std::shared_ptr<IIndexChecker> GetAndChecker() const;

    TString DebugString() const {
        return DebugJson().GetStringRobust();
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        if (Equals.size()) {
            auto& jsonEquals = result.InsertValue("equals", NJson::JSON_MAP);
            for (auto&& i : Equals) {
                jsonEquals.InsertValue(::ToString(i.first), i.second ? i.second->ToString() : "NULL");
            }
        }
        if (Likes.size()) {
            auto& jsonLikes = result.InsertValue("likes", NJson::JSON_MAP);
            for (auto&& i : Likes) {
                jsonLikes.InsertValue(::ToString(i.first), i.second.DebugJson());
            }
        }
        return result;
    }
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

    void AddBranch(const THashMap<ui32, std::shared_ptr<arrow::Scalar>>& equalsData, const THashMap<ui32, TLikeDescription>& likesData) {
        Branches.emplace_back(std::make_shared<TBranchCoverage>(equalsData, likesData));
    }

    static std::shared_ptr<TDataForIndexesCheckers> Build(const TProgramContainer& program);

    TIndexCheckerContainer GetCoverChecker() const;
};

}   // namespace NKikimr::NOlap::NIndexes::NRequest
