#pragma once

#include "node.h"
#include "context.h"

#include <yql/essentials/ast/yql_type_string.h>

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
        , OpName_(opName)
        , Args_(args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override = 0;

    TAstNode* Translate(TContext& ctx) const override;

protected:
    const TString OpName_;
    TVector<TNodePtr> Args_;
    TNodePtr Node_;

    inline TNodePtr GetIdentityLambda();

    inline TNodePtr SkipEmpty(TNodePtr arg);

    void DoUpdateState() const override {
        State_.Set(ENodeState::Aggregated, Args_[0]->IsAggregated());
    }
};

class TListSortBuiltin final: public TListBuiltin {
public:
    TListSortBuiltin(TPosition pos, const TVector<TNodePtr>& args, bool asc)
        : TListBuiltin(pos, "Sort", args)
        , Asc_(asc)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListSortBuiltin(Pos_, CloneContainer(Args_), Asc_);
    }

private:
    bool Asc_;
};

class TListExtractBuiltin final: public TListBuiltin {
public:
    TListExtractBuiltin(TPosition pos, const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "OrderedExtract", args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListExtractBuiltin(Pos_, CloneContainer(Args_));
    }
};

class TListProcessBuiltin: public TListBuiltin {
protected:
    TListProcessBuiltin(TPosition pos,
                 const TString& opName,
                 const TVector<TNodePtr>& args)
        : TListBuiltin(pos, opName, args)
        , OpLiteral_(nullptr)
    {}

    bool CheckArgs(TContext& ctx, ISource* src);

    TNodePtr PrepareResult();

    const TString* OpLiteral_;
};

class TListMapBuiltin final: public TListProcessBuiltin {
public:
    TListMapBuiltin(TPosition pos,
                    const TVector<TNodePtr>& args,
                    bool flat)
        : TListProcessBuiltin(pos, flat ? "OrderedFlatMap" : "OrderedMap", args)
        , Flat_(flat)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListMapBuiltin(Pos_, CloneContainer(Args_), Flat_);
    }
protected:
    virtual TNodePtr GetMapLambda();
private:
    bool Flat_;
};

class TListFilterBuiltin final: public TListProcessBuiltin {
public:
    TListFilterBuiltin(TPosition pos,
                       const TVector<TNodePtr>& args)
        : TListProcessBuiltin(pos, "OrderedFilter", args)
    {}


    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListFilterBuiltin(Pos_, CloneContainer(Args_));
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
        , StateType_(stateType)
        , StateValue_(stateValue)
        , ArgCount_(argCount)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override;
protected:
    const TString StateType_;
    const TString StateValue_;
    const ui32 ArgCount_;

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
        return new TListFoldBuiltinImpl(Pos_, OpName_, StateType_, StateValue_, ArgCount_, CloneContainer(Args_));
    }
};

class TListCountBuiltin final: public TListFoldBuiltin {
public:
    TListCountBuiltin(TPosition pos, const TVector<TNodePtr>& args)
        : TListFoldBuiltin(pos, "Inc", "Uint64", "0", 1, args)
    {}

    TNodePtr DoClone() const final {
        return new TListCountBuiltin(Pos_, CloneContainer(Args_));
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
        return new TListAvgBuiltin(Pos_, CloneContainer(Args_));
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
        return new TListHasBuiltin(Pos_, CloneContainer(Args_));
    }
private:
    TNodePtr GetUpdateLambda() override;

    void DoUpdateState() const override {
        bool isAggregated = true;
        for (const auto& arg: Args_) {
            if (!arg->IsAggregated()) {
                isAggregated = false;
                break;
            }
        }
        State_.Set(ENodeState::Aggregated, isAggregated);
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
        return new TListFold1Builtin(Pos_, OpName_, CloneContainer(Args_));
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
        return new TListUniqBuiltin(Pos_, CloneContainer(Args_));
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
        return new TListCreateBuiltin(Pos_, CloneContainer(Args_));
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
        return new TDictCreateBuiltin(Pos_, CloneContainer(Args_));
    }
};

} // namespace NSQLTranslationV0
