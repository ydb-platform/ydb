#pragma once

#include <yql/essentials/providers/common/config/yql_dispatch.h>

#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>

namespace NYql::NCommon {

class TProviderConfigurationTransformer: public TSyncTransformerBase {
public:
    TProviderConfigurationTransformer(ISettingDispatcher::TPtr dispatcher, const TTypeAnnotationContext& types,
                                      TString provider, const THashSet<TStringBuf>& configureCallables = {});

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    void Rewind() final {
    }

protected:
    virtual bool HandleAttr(TPositionHandle pos, const TString& cluster, const TString& name,
                            const TMaybe<TString>& value, TExprContext& ctx);
    virtual bool HandleAuth(TPositionHandle pos, const TString& cluster, const TString& alias, TExprContext& ctx);

    // FIXME switch to an accessor
    ISettingDispatcher::TPtr GetDispatcher() const;

    ISettingDispatcher::TPtr Dispatcher; // NOLINT(readability-identifier-naming)
    const TTypeAnnotationContext& Types_;
    TString Provider_;
    THashSet<TStringBuf> ConfigureCallables_;
};

THolder<IGraphTransformer> CreateProviderConfigurationTransformer(
    ISettingDispatcher::TPtr dispatcher,
    const TTypeAnnotationContext& types,
    const TString& provider);

} // namespace NYql::NCommon
