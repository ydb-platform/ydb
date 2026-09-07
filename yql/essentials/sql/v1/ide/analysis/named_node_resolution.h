#pragma once

#include <yql/essentials/sql/v1/ide/core/environment.h>
#include <yql/essentials/sql/v1/ide/core/position.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NSQLPureAST {

using TNamedNode = std::variant<
    SQLv1::ExprContext*,
    NYT::TNode,
    std::monostate>;

struct TNamedNodeRef {
    TString Name;
    TPosition Position;

    bool IsWildcard() const;

    friend bool operator==(const TNamedNodeRef& lhs, const TNamedNodeRef& rhs) = default;
    friend bool operator!=(const TNamedNodeRef& lhs, const TNamedNodeRef& rhs) = default;

    static TNamedNodeRef Wildcard(TPosition position);
};

class INamedNodeDef;

class INamedNodeScope {
public:
    using TPtr = std::shared_ptr<INamedNodeScope>;
    using TEntry = std::variant<TNamedNodeRef, std::shared_ptr<INamedNodeDef>, TPtr>;

    virtual ~INamedNodeScope() = default;

    virtual const TNamedNodeRef& Owner() const = 0;

    virtual TPosition Position() const = 0;

    virtual const TSet<TEntry>& Entries() const = 0;
};

bool operator<(const INamedNodeScope::TEntry& lhs, const INamedNodeScope::TEntry& rhs);

class INamedNodeDef {
public:
    using TPtr = std::shared_ptr<INamedNodeDef>;

    virtual ~INamedNodeDef() = default;

    virtual const TNamedNodeRef& Decl() const = 0;

    virtual const TNamedNode& Value() const = 0;

    virtual const TVector<TNamedNodeRef>& References() const = 0;
};

class INamedNodes {
public:
    using TPtr = std::shared_ptr<INamedNodes>;

    virtual ~INamedNodes() = default;

    virtual INamedNodeScope::TPtr TopLevel() const = 0;

    virtual INamedNodeDef::TPtr Declaration(const TNamedNodeRef& ref) const = 0;

    virtual INamedNodeDef::TPtr Definition(const TNamedNodeRef& ref) const = 0;
};

TMaybe<TNamedNodeRef> GetNamedNodeRef(SQLv1::Bind_parameterContext* ctx);

INamedNodes::TPtr ResolveNamedNodes(IParseTree::TPtr input, const TEnvironment& env);

} // namespace NSQLPureAST

template <>
struct THash<NSQLPureAST::TNamedNodeRef> {
    inline size_t operator()(const NSQLPureAST::TNamedNodeRef& x) const {
        return THash<std::tuple<TString, NSQLPureAST::TPosition>>()(
            std::tie(x.Name, x.Position));
    }
};
