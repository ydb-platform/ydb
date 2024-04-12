#pragma once

#include "yql_statistics.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <set>

/**
 * The cost function for cost based optimizer currently consists of methods for computing
 * both the cost and cardinalities of individual plan operators
*/
namespace NYql {

struct IProviderContext;

namespace NDq {    
/**
 * Join column is a struct that records the relation label and 
 * attribute name, used in join conditions
*/
struct TJoinColumn {
    TString RelName;
    TString AttributeName;

    TJoinColumn(TString relName, TString attributeName) : RelName(relName), 
        AttributeName(attributeName) {}

    bool operator == (const TJoinColumn& other) const {
        return RelName == other.RelName && AttributeName == other.AttributeName;
    }

    struct HashFunction
    {
        size_t operator()(const TJoinColumn& c) const
        {
            return THash<TString>{}(c.RelName) ^ THash<TString>{}(c.AttributeName);
        }
    };
};

bool operator < (const TJoinColumn& c1, const TJoinColumn& c2);

}

enum class EJoinAlgoType {
    Undefined,
    LookupJoin,
    MapJoin,
    GraceJoin,
    StreamLookupJoin //Right part can be updated during an operation. Used mainly for joining streams with lookup tables. Currently impplemented in Dq by LookupInputTransform
};

//StreamLookupJoin is not a subject for CBO and not not included here
static constexpr auto AllJoinAlgos = { EJoinAlgoType::MapJoin, EJoinAlgoType::GraceJoin, EJoinAlgoType::LookupJoin };

TOptimizerStatistics ComputeJoinStats(const TOptimizerStatistics& leftStats, const TOptimizerStatistics& rightStats, 
    const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& joinConditions, EJoinAlgoType joinAlgo, const IProviderContext& ctx);

TOptimizerStatistics ComputeJoinStats(const TOptimizerStatistics& leftStats, const TOptimizerStatistics& rightStats, 
    const TVector<TString>& leftJoinKeys, const TVector<TString>& rightJoinKeys, EJoinAlgoType joinAlgo, const IProviderContext& ctx);

}

