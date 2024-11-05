#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <functional>
#include <variant>

namespace NYql {

class IArrowResolver : public TThrRefBase {
public:
    using TPtr = TIntrusiveConstPtr<IArrowResolver>;
    using TUnsupportedTypeCallback = std::function<void(std::variant<ETypeAnnotationKind, NUdf::EDataSlot>)>;

    enum EStatus {
        OK,
        NOT_FOUND,
        ERROR,
    };

    virtual ~IArrowResolver() = default;

    virtual EStatus LoadFunctionMetadata(const TPosition& pos, TStringBuf name, const TVector<const TTypeAnnotationNode*>& argTypes,
        const TTypeAnnotationNode* returnType, TExprContext& ctx) const = 0;

    virtual EStatus HasCast(const TPosition& pos, const TTypeAnnotationNode* from, const TTypeAnnotationNode* to, TExprContext& ctx) const = 0;

    virtual EStatus AreTypesSupported(const TPosition& pos, const TVector<const TTypeAnnotationNode*>& types, TExprContext& ctx,
        const TUnsupportedTypeCallback& onUnsupported = {}) const = 0;
};

}
