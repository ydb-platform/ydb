#pragma once

#include <yql/essentials/sql/v1/ide/core/environment.h>
#include <yql/essentials/sql/v1/ide/core/position.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLPureAST {

using TNamedNode = std::variant<
    SQLv1::ExprContext*,
    NYT::TNode,
    std::monostate>;

struct TNamedNodeRef {
    TString Name;
    TPosition Position;

    friend bool operator==(const TNamedNodeRef& lhs, const TNamedNodeRef& rhs) = default;
    friend bool operator!=(const TNamedNodeRef& lhs, const TNamedNodeRef& rhs) = default;
};

class INamedNodes {
public:
    using TPtr = THolder<INamedNodes>;

    virtual ~INamedNodes() = default;

    [[nodiscard]] virtual const TNamedNode* Resolve(const TNamedNodeRef& ref) const = 0;
    virtual void Dump(IOutputStream& out) const = 0;
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
