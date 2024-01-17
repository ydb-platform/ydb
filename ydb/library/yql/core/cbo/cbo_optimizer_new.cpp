#include "cbo_optimizer_new.h"

#include <array>

#include <util/string/builder.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <library/cpp/disjoint_sets/disjoint_sets.h>

namespace NYql {

using namespace NYql::NDq;

namespace { 

    THashMap<TString,EJoinKind> JoinKindMap = {
        {"Inner",EJoinKind::InnerJoin}, 
        {"Left",EJoinKind::LeftJoin}, 
        {"Right",EJoinKind::RightJoin}, 
        {"Full",EJoinKind::OuterJoin},
        {"LeftOnly",EJoinKind::LeftOnly},
        {"RightOnly",EJoinKind::RightOnly},
        {"Exclusion",EJoinKind::Exclusion},
        {"LeftSemi",EJoinKind::LeftSemi},
        {"RightSemi",EJoinKind::RightSemi},
        {"Cross",EJoinKind::Cross}};
}

EJoinKind ConvertToJoinKind(const TString& joinString) {
    auto maybeKind = JoinKindMap.find(joinString);
    Y_ENSURE(maybeKind != JoinKindMap.end());

    return maybeKind->second;
}

TString ConvertToJoinString(const EJoinKind kind) {
    for (auto [k,v] : JoinKindMap) {
        if (v == kind) {
            return k;
        }
    }

    Y_ENSURE(false,"Unknown join kind");
}

TVector<TString> TRelOptimizerNode::Labels()  {
    TVector<TString> res;
    res.emplace_back(Label);
    return res;
}

void TRelOptimizerNode::Print(std::stringstream& stream, int ntabs) {
    for (int i = 0; i < ntabs; i++){
        stream << "\t";
    }
    stream << "Rel: " << Label << "\n";

    for (int i = 0; i < ntabs; i++){
        stream << "\t";
    }
    stream << *Stats << "\n";
}

TJoinOptimizerNode::TJoinOptimizerNode(const std::shared_ptr<IBaseOptimizerNode>& left, const std::shared_ptr<IBaseOptimizerNode>& right, 
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions, const EJoinKind joinType, bool nonReorderable) :
    IBaseOptimizerNode(JoinNodeType), 
    LeftArg(left), 
    RightArg(right), 
    JoinConditions(joinConditions), 
    JoinType(joinType) {
        IsReorderable = (JoinType==EJoinKind::InnerJoin) && (nonReorderable==false);
    }

TVector<TString> TJoinOptimizerNode::Labels() {
    auto res = LeftArg->Labels();
    auto rightLabels = RightArg->Labels();
    res.insert(res.begin(),rightLabels.begin(),rightLabels.end());
    return res;
}

void TJoinOptimizerNode::Print(std::stringstream& stream, int ntabs) {
    for (int i = 0; i < ntabs; i++){
        stream << "\t";
    }

    stream << "Join: (" << JoinType << ") ";
    for (auto c : JoinConditions){
        stream << c.first.RelName << "." << c.first.AttributeName 
            << "=" << c.second.RelName << "." 
            << c.second.AttributeName << ", ";
    }
    stream << "\n";

    for (int i = 0; i < ntabs; i++){
        stream << "\t";
    }

    stream << *Stats << "\n";

    LeftArg->Print(stream, ntabs+1);
    RightArg->Print(stream, ntabs+1);
}

} // namespace NYql
