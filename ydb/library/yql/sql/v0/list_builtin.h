#pragma once

#include "node.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_type_string.h>

#include <library/cpp/charset/ci_string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/util.h>

using namespace NYql;

namespace NSQLTranslationV0 {

class TListBuiltin: public TCallNode {
public:
    TListBuiltin(TPosition pos,
                     const TString& opName,
                     const TVector<TNodePtr>& args)
        : TCallNode(pos, opName, args.size(), args.size(), args)
        , OpName(opName)
        , Args(args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override = 0;

    TAstNode* Translate(TContext& ctx) const override;

protected:
    const TString OpName;
    TVector<TNodePtr> Args;
    TNodePtr Node;

    inline TNodePtr GetIdentityLambda();

    inline TNodePtr SkipEmpty(TNodePtr arg);

    void DoUpdateState() const override {
        State.Set(ENodeState::Aggregated, Args[0]->IsAggregated());
    }
};

class TListSortBuiltin final: public TListBuiltin {
public:
    TListSortBuiltin(TPosition pos, const TVector<TNodePtr>& args, bool asc)
        : TListBuiltin(pos, "Sort", args)
        , Asc(asc)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListSortBuiltin(Pos, CloneContainer(Args), Asc);
    }

private:
    bool Asc;
};

class TListExtractBuiltin final: public TListBuiltin {
public:
    TListExtractBuiltin(TPosition pos, const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "OrderedExtract", args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListExtractBuiltin(Pos, CloneContainer(Args));
    }
};

class TListProcessBuiltin: public TListBuiltin {
protected:
    TListProcessBuiltin(TPosition pos,
                 const TString& opName,
                 const TVector<TNodePtr>& args)
        : TListBuiltin(pos, opName, args)
        , OpLiteral(nullptr)
    {}

    bool CheckArgs(TContext& ctx, ISource* src);

    TNodePtr PrepareResult();

    const TString* OpLiteral;
};

class TListMapBuiltin final: public TListProcessBuiltin {
public:
    TListMapBuiltin(TPosition pos,
                    const TVector<TNodePtr>& args,
                    bool flat)
        : TListProcessBuiltin(pos, flat ? "OrderedFlatMap" : "OrderedMap", args)
        , Flat(flat)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListMapBuiltin(Pos, CloneContainer(Args), Flat);
    }
protected:
    virtual TNodePtr GetMapLambda();
private:
    bool Flat;
};

class TListFilterBuiltin final: public TListProcessBuiltin {
public:
    TListFilterBuiltin(TPosition pos,
                       const TVector<TNodePtr>& args)
        : TListProcessBuiltin(pos, "OrderedFilter", args)
    {}


    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListFilterBuiltin(Pos, CloneContainer(Args));
    }
protected:
    virtual TNodePtr GetFilterLambda();
};

class TListFoldBuiltin: public TListBuiltin {
public:
    TListFoldBuiltin(TPosition pos,
                     const TString& opName,
                     const TString& stateType,
                     const TString& stateValue,
                     const ui32 argCount,
                     const TVector<TNodePtr>& args)
        : TListBuiltin(pos, opName, args)
        , StateType(stateType)
        , StateValue(stateValue)
        , ArgCount(argCount)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override;
protected:
    const TString StateType;
    const TString StateValue;
    const ui32 ArgCount;

    virtual TNodePtr GetInitialState();

    virtual TNodePtr GetUpdateLambda();
};

class TListFoldBuiltinImpl final: public TListFoldBuiltin {
public:
    TListFoldBuiltinImpl(TPosition pos, const TString& opName, const TString& stateType, const TString& stateValue,
                     const ui32 argCount, const TVector<TNodePtr>& args)
        : TListFoldBuiltin(pos, opName, stateType, stateValue, argCount, args)
    {}

    TNodePtr DoClone() const final {
        return new TListFoldBuiltinImpl(Pos, OpName, StateType, StateValue, ArgCount, CloneContainer(Args));
    }
};

class TListCountBuiltin final: public TListFoldBuiltin {
public:
    TListCountBuiltin(TPosition pos, const TVector<TNodePtr>& args)
        : TListFoldBuiltin(pos, "Inc", "Uint64", "0", 1, args)
    {}

    TNodePtr DoClone() const final {
        return new TListCountBuiltin(Pos, CloneContainer(Args));
    }
private:
    virtual TNodePtr GetUpdateLambda();
};

class TListAvgBuiltin final: public TListFoldBuiltin {
public:
    TListAvgBuiltin(TPosition pos, const TVector<TNodePtr>& args)
        : TListFoldBuiltin(pos, "Avg", "", "", 1, args)
    {
    }

    TNodePtr DoClone() const final {
        return new TListAvgBuiltin(Pos, CloneContainer(Args));
    }
private:
    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr GetInitialState() override;

    TNodePtr GetUpdateLambda() override;
};

class TListHasBuiltin final: public TListFoldBuiltin {
public:
    TListHasBuiltin(TPosition pos, const TVector<TNodePtr>& args)
        : TListFoldBuiltin(pos, "==", "Bool", "false", 2, args)
    {
    }

    TNodePtr DoClone() const final {
        return new TListHasBuiltin(Pos, CloneContainer(Args));
    }
private:
    TNodePtr GetUpdateLambda() override;

    void DoUpdateState() const override {
        bool isAggregated = true;
        for (const auto& arg: Args) {
            if (!arg->IsAggregated()) {
                isAggregated = false;
                break;
            }
        }
        State.Set(ENodeState::Aggregated, isAggregated);
    }
};

class TListFold1Builtin final: public TListBuiltin {
public:
    TListFold1Builtin(TPosition pos,
                     const TString& opName,
                     const TVector<TNodePtr>& args)
        : TListBuiltin(pos, opName, args)
    {
    }


    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListFold1Builtin(Pos, OpName, CloneContainer(Args));
    }

protected:
    virtual TNodePtr GetInitLambda();

    virtual TNodePtr GetUpdateLambda();
};

class TListUniqBuiltin final: public TListBuiltin {
public:
    TListUniqBuiltin(TPosition pos,
                     const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "ListUniq", args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListUniqBuiltin(Pos, CloneContainer(Args));
    }
};

class TListCreateBuiltin final: public TListBuiltin {
public:
    TListCreateBuiltin(TPosition pos,
                     const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "ListCreate", args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListCreateBuiltin(Pos, CloneContainer(Args));
    }
};

class TDictCreateBuiltin final: public TListBuiltin {
public:
    TDictCreateBuiltin(TPosition pos,
                     const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "DictCreate", args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TDictCreateBuiltin(Pos, CloneContainer(Args));
    }
};

} // namespace NSQLTranslationV0
