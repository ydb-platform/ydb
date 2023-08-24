#pragma once

#include "node.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_type_string.h>

#include <library/cpp/charset/ci_string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/util.h>

using namespace NYql;

namespace NSQLTranslationV1 {

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
};

class TListSortBuiltin final: public TListBuiltin {
public:
    TListSortBuiltin(TPosition pos, const TVector<TNodePtr>& args, bool asc)
        : TListBuiltin(pos, "ListSort", args)
        , Asc(asc)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListSortBuiltin(Pos, CloneContainer(Args), Asc);
    }

private:
    const bool Asc;
};

class TListExtractBuiltin final: public TListBuiltin {
public:
    TListExtractBuiltin(TPosition pos, const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "ListExtract", args)
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
    {}

    bool CheckArgs(TContext& ctx, ISource* src);
};

class TListMapBuiltin final: public TListProcessBuiltin {
public:
    TListMapBuiltin(TPosition pos,
                    const TVector<TNodePtr>& args,
                    bool flat)
        : TListProcessBuiltin(pos, flat ? "ListFlatMap" : "ListMap", args)
        , Flat(flat)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListMapBuiltin(Pos, CloneContainer(Args), Flat);
    }
private:
    bool Flat;
};

class TListFilterBuiltin final: public TListProcessBuiltin {
public:
    TListFilterBuiltin(TPosition pos, const TString& opName,
                       const TVector<TNodePtr>& args)
        : TListProcessBuiltin(pos, opName, args)
    {}


    bool DoInit(TContext& ctx, ISource* src) override;

    TNodePtr DoClone() const final {
        return new TListFilterBuiltin(Pos, OpName, CloneContainer(Args));
    }
protected:
    virtual TNodePtr GetFilterLambda();
};

class TListCreateBuiltin final: public TListBuiltin {
public:
    TListCreateBuiltin(TPosition pos,
                     const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "ListCreate", args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;
    void DoUpdateState() const override;

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
    void DoUpdateState() const override;

    TNodePtr DoClone() const final {
        return new TDictCreateBuiltin(Pos, CloneContainer(Args));
    }
};

class TSetCreateBuiltin final: public TListBuiltin {
public:
    TSetCreateBuiltin(TPosition pos,
                     const TVector<TNodePtr>& args)
        : TListBuiltin(pos, "SetCreate", args)
    {}

    bool DoInit(TContext& ctx, ISource* src) override;
    void DoUpdateState() const override;

    TNodePtr DoClone() const final {
        return new TSetCreateBuiltin(Pos, CloneContainer(Args));
    }
};

} // namespace NSQLTranslationV1
