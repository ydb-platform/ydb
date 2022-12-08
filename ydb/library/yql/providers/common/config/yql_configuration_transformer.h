#pragma once

#include "yql_dispatch.h"

#include <ydb/library/yql/core/yql_type_annotation.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>

namespace NYql {
namespace NCommon {

class TProviderConfigurationTransformer : public TSyncTransformerBase {
public:
    TProviderConfigurationTransformer(TSettingDispatcher::TPtr dispatcher,const TTypeAnnotationContext& types,
        const TString& provider, const THashSet<TStringBuf>& configureCallables = {});

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    void Rewind() final {
    }

protected:
    virtual bool HandleAttr(TPositionHandle pos, const TString& cluster, const TString& name,
        const TMaybe<TString>& value, TExprContext& ctx);
    virtual bool HandleAuth(TPositionHandle pos, const TString& cluster, const TString& alias, TExprContext& ctx);

protected:
    TSettingDispatcher::TPtr Dispatcher;
    const TTypeAnnotationContext& Types;
    TString Provider;
    THashSet<TStringBuf> ConfigureCallables;
};

THolder<IGraphTransformer> CreateProviderConfigurationTransformer(
    TSettingDispatcher::TPtr dispatcher,
    const TTypeAnnotationContext& types,
    const TString& provider
);

} // namespace NCommon
} // namespace NYql
