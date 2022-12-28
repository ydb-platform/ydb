#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

class IArrowResolver : public TThrRefBase {
public:
    using TPtr = TIntrusiveConstPtr<IArrowResolver>;

    enum EStatus {
        OK,
        NOT_FOUND,
        ERROR,
    };

    virtual ~IArrowResolver() = default;

    virtual EStatus LoadFunctionMetadata(const TPosition& pos, TStringBuf name, const TVector<const TTypeAnnotationNode*>& argTypes,
        const TTypeAnnotationNode* returnType, TExprContext& ctx) const = 0;

    virtual EStatus HasCast(const TPosition& pos, const TTypeAnnotationNode* from, const TTypeAnnotationNode* to, TExprContext& ctx) const = 0;

    virtual EStatus AreTypesSupported(const TPosition& pos, const TVector<const TTypeAnnotationNode*>& types, TExprContext& ctx) const = 0;
};

}
