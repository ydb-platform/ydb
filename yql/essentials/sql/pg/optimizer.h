#pragma once

#include <yql/essentials/parser/pg_wrapper/interface/optimizer.h>

struct Var;
struct RelOptInfo;
struct List;
struct EquivalenceClass;
struct Path;
struct RestrictInfo;

namespace NYql {

class TPgOptimizer: public IOptimizer {
public:
    TPgOptimizer(const TInput& input, const std::function<void(const TString&)>& log = {});
    ~TPgOptimizer();

    TOutput JoinSearch() override;

private:
    TInput Input_;
    std::function<void(const TString&)> Log_;
    std::map<TVarId, Var*> Vars_;
    std::vector<std::vector<RestrictInfo*>> RestrictInfos_;
    std::vector<RestrictInfo*> LeftRestriction_;
    std::vector<RestrictInfo*> RightRestriction_;

    void MakeLeftOrRightRestrictions(std::vector<RestrictInfo*>& dst, const std::vector<TEq>& src);
    void LogNode(const TString& prefix, void* node);
    RelOptInfo* JoinSearchInternal();
    List* MakeEqClasses();
    EquivalenceClass* MakeEqClass(int eqId);
    Var* MakeVar(TVarId);
    TOutput MakeOutput(Path*);
    int MakeOutputJoin(TOutput& output, Path*);
    void LogOutput(const TString& prefix, const TOutput& output, int id);
    TString PrettyPrintVar(TVarId varId);
};

// export for tests
Var* MakeVar(int relno, int varno);
RelOptInfo* MakeRelOptInfo(const IOptimizer::TRel& r, int relno);
List* MakeRelOptInfoList(const IOptimizer::TInput& input);

} // namespace NYql

