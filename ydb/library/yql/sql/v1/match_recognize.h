#pragma once
#include "node.h"
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include <util/generic/ptr.h>

namespace NSQLTranslationV1 {

struct TNamedFunction {
    TNodePtr callable; //Callable with some free args
    TString name;
};

enum class ERowsPerMatch {
    OneRow,
    AllRows
};

enum class EAfterMatchSkipTo {
    NextRow,
    PastLastRow,
    ToFirst,
    ToLast,
    To
};

struct TAfterMatchSkipTo {
    TAfterMatchSkipTo(EAfterMatchSkipTo to, const TStringBuf var = TStringBuf())
        : To(to)
        , Var(var)
    {}
    EAfterMatchSkipTo To;
    TString Var;
};

class TMatchRecognizeBuilder: public TSimpleRefCount<TMatchRecognizeBuilder> {
public:
    TMatchRecognizeBuilder(
            TPosition clausePos,
            std::pair<TPosition, TVector<TNamedFunction>>&& partitioners,
            std::pair<TPosition, TVector<TSortSpecificationPtr>>&& sortSpecs,
            std::pair<TPosition, TVector<TNamedFunction>>&& measures,
            std::pair<TPosition, ERowsPerMatch>&& rowsPerMatch,
            std::pair<TPosition, TAfterMatchSkipTo>&& skipTo,
            std::pair<TPosition, NYql::NMatchRecognize::TRowPattern>&& pattern,
            std::pair<TPosition, TNodePtr>&& subset,
            std::pair<TPosition, TVector<TNamedFunction>>&& definitions
            )
            : Pos(clausePos)
            , Partitioners(std::move(partitioners))
            , SortSpecs(std::move(sortSpecs))
            , Measures(std::move(measures))
            , RowsPerMatch(std::move(rowsPerMatch))
            , SkipTo(std::move(skipTo))
            , Pattern(std::move(pattern))
            , Subset(std::move(subset))
            , Definitions(definitions)

    {}
    TNodePtr Build(TContext& ctx, TString&& inputTable, ISource* source);
private:
    TPosition Pos;
    std::pair<TPosition, TVector<TNamedFunction>> Partitioners;
    std::pair<TPosition, TVector<TSortSpecificationPtr>> SortSpecs;
    std::pair<TPosition, TVector<TNamedFunction>> Measures;
    std::pair<TPosition, ERowsPerMatch> RowsPerMatch;
    std::pair<TPosition, TAfterMatchSkipTo> SkipTo;
    std::pair<TPosition, NYql::NMatchRecognize::TRowPattern> Pattern;
    std::pair<TPosition, TNodePtr> Subset;
    std::pair<TPosition, TVector<TNamedFunction>> Definitions;
};

using TMatchRecognizeBuilderPtr=TIntrusivePtr<TMatchRecognizeBuilder> ;

class TMatchRecognizeVarAccessNode: public INode {
public:
    TMatchRecognizeVarAccessNode(TPosition pos, const TString& var, const TString& column, bool theSameVar)
        : INode(pos)
        , Var(var)
        , TheSameVar(theSameVar)
        , Column(column)
    {
    }

    TString GetVar() const {
        return Var;
    }

    bool IsTheSameVar() const {
        return TheSameVar;
    }

    TString GetColumn() const {
        return Column;
    }

    bool DoInit(TContext& ctx, ISource* src) override;

    TAstNode* Translate(TContext& ctx) const override {
        return Node->Translate(ctx);
    }

    TPtr DoClone() const override {
        YQL_ENSURE(!Node, "TMatchRecognizeVarAccessNode::Clone: Node must not be initialized");
        auto copy = new TMatchRecognizeVarAccessNode(Pos, Var, Column, TheSameVar);
        return copy;
    }

protected:
    void DoUpdateState() const override {
        YQL_ENSURE(Node);
    }

    void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final {
        Y_DEBUG_ABORT_UNLESS(Node);
        Node->VisitTree(func, visited);
    }

private:
    TNodePtr Node;
    const TString Var;
    const bool TheSameVar; //reference the same var as being defined by this expression;
    const TString Column;
};

class TMatchRecognizeNavigate: public TAstListNode {
public:
    TMatchRecognizeNavigate(TPosition pos, const TString& name, const TVector<TNodePtr>& args)
        : TAstListNode(pos)
        , Name(name)
        , Args(args)
    {
    }

private:
    TNodePtr DoClone() const override {
        return new TMatchRecognizeNavigate(GetPos(), Name, CloneContainer(Args));
    }

    bool DoInit(TContext& ctx, ISource* src) override;

private:
    const TString Name;
    const TVector<TNodePtr> Args;
};

} // namespace NSQLTranslationV1

