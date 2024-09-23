#pragma once

#include <ydb/library/yql/core/type_ann/type_ann_core.h>

#include <library/cpp/yson/writer.h>

#include <optional>

namespace NYql {

struct TPlanSettings {

    TPlanSettings& SetWithLimits(bool limits) {
        WithLimits = limits;
        return *this;
    }

    bool WithLimits = true;
};

class IPlanBuilder {
public:
    virtual ~IPlanBuilder() {};
    virtual void Clear() = 0;
    virtual void WritePlan(NYson::TYsonWriter& writer, const TExprNode::TPtr& root, const TPlanSettings& settings = {}) = 0;
};

TAutoPtr<IPlanBuilder> CreatePlanBuilder(TTypeAnnotationContext& types);

}
