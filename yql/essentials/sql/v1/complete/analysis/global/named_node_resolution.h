#pragma once

#include "input.h"

#include <yql/essentials/sql/v1/complete/core/environment.h>
#include <yql/essentials/sql/v1/complete/core/position.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

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

    virtual const TNamedNode* Resolve(const TNamedNodeRef& ref) const = 0;
    virtual void Dump(IOutputStream& out) const = 0;
};

TMaybe<TNamedNodeRef> GetNamedNodeRef(SQLv1::Bind_parameterContext* ctx);

INamedNodes::TPtr ResolveNamedNodes(TParsedInput input, const TEnvironment& env);

} // namespace NSQLComplete

template <>
struct THash<NSQLComplete::TNamedNodeRef> {
    inline size_t operator()(const NSQLComplete::TNamedNodeRef& x) const {
        return THash<std::tuple<TString, NSQLComplete::TPosition>>()(
            std::tie(x.Name, x.Position));
    }
};
