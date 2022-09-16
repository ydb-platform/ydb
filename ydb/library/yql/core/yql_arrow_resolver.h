#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

class IArrowResolver : public TThrRefBase {
public:
    using TPtr = TIntrusiveConstPtr<IArrowResolver>;

    virtual ~IArrowResolver() = default;

    virtual bool LoadFunctionMetadata(const TPosition& pos, TStringBuf name, const TVector<const TTypeAnnotationNode*>& argTypes,
        const TTypeAnnotationNode*& returnType, TExprContext& ctx) const = 0;
};

}
