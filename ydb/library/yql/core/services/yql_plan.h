#pragma once

#include <ydb/library/yql/core/type_ann/type_ann_core.h>

#include <library/cpp/yson/writer.h>

#include <optional>

namespace NYql {

struct TPlanSettings {

    TPlanSettings& SetLimitInputPins(std::optional<ui32> val) {
        LimitInputPins = std::move(val);
        return *this;
    }

    TPlanSettings& SetLimitOutputPins(std::optional<ui32> val) {
        LimitOutputPins = std::move(val);
        return *this;
    }

    std::optional<ui32> LimitInputPins = 10;
    std::optional<ui32> LimitOutputPins = 10;
};

class IPlanBuilder {
public:
    virtual ~IPlanBuilder() {};
    virtual void Clear() = 0;
    virtual void WritePlan(NYson::TYsonWriter& writer, const TExprNode::TPtr& root, const TPlanSettings& settings = {}) = 0;
};

TAutoPtr<IPlanBuilder> CreatePlanBuilder(TTypeAnnotationContext& types);

}
