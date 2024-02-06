#pragma once
#include <ydb/core/tx/program/program.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

class TBranchCoverage {
private:
    THashMap<TString, std::shared_ptr<arrow::Scalar>> Equals;
    YDB_ACCESSOR_DEF(std::vector<std::shared_ptr<IIndexChecker>>, Indexes);
public:
    TBranchCoverage(const THashMap<TString, std::shared_ptr<arrow::Scalar>>& equals)
        : Equals(equals)
    {

    }

    const THashMap<TString, std::shared_ptr<arrow::Scalar>>& GetEquals() const {
        return Equals;
    }

    std::shared_ptr<IIndexChecker> GetAndChecker() const;
};

class TDataForIndexesCheckers {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TBranchCoverage>>, Branches);
public:
    void AddBranch(const THashMap<TString, std::shared_ptr<arrow::Scalar>>& equalsData) {
        Branches.emplace_back(std::make_shared<TBranchCoverage>(equalsData));
    }

    static std::shared_ptr<TDataForIndexesCheckers> Build(const TProgramContainer& program);

    TIndexCheckerContainer GetCoverChecker() const;
};

}   // namespace NKikimr::NOlap::NIndexes::NRequest