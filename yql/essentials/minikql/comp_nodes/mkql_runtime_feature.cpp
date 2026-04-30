#include "mkql_runtime_feature.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_configuration.h>

namespace NKikimr::NMiniKQL {

namespace {

TMaybe<TString> GetHostSetting(const NYql::TRuntimeSettings& runtimeSettings, TStringBuf name) {
    TMaybe<TString> result;
    NYql::TRuntimeSettingsConfiguration conf(runtimeSettings);
    conf.SerializeStaticSettings([&](const TString& settingName, const TString& value) {
        if (settingName == name) {
            result = value;
        }
    });
    return result;
}

class THostRuntimeSettingWrapper: public TMutableComputationNode<THostRuntimeSettingWrapper> {
    using TBaseComputation = TMutableComputationNode<THostRuntimeSettingWrapper>;

public:
    THostRuntimeSettingWrapper(TComputationMutables& mutables, IComputationNode* featureName)
        : TBaseComputation(mutables)
        , FeatureName_(featureName)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto nameValue = FeatureName_->GetValue(ctx);
        const TStringBuf name(nameValue.AsStringRef());
        const auto result = GetHostSetting(ctx.RuntimeSettings, name);
        if (!result.Defined()) {
            return NUdf::TUnboxedValuePod();
        }
        return MakeString(NUdf::TStringRef(*result)).MakeOptional();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(FeatureName_);
    }

    IComputationNode* const FeatureName_;
};

class TUdfRuntimeSettingWrapper: public TMutableComputationNode<TUdfRuntimeSettingWrapper> {
    using TBaseComputation = TMutableComputationNode<TUdfRuntimeSettingWrapper>;

public:
    TUdfRuntimeSettingWrapper(TComputationMutables& mutables, IComputationNode* module,
                              IComputationNode* featureName)
        : TBaseComputation(mutables)
        , Module_(module)
        , FeatureName_(featureName)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto moduleValue = Module_->GetValue(ctx);
        const auto featureNameValue = FeatureName_->GetValue(ctx);
        const TString result = ctx.RuntimeSettings.GetUdfSetting(
            TString(moduleValue.AsStringRef()),
            TString(featureNameValue.AsStringRef()));
        if (result.empty()) {
            return NUdf::TUnboxedValuePod();
        }
        return MakeString(NUdf::TStringRef(result)).MakeOptional();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Module_);
        DependsOn(FeatureName_);
    }

    IComputationNode* const Module_;
    IComputationNode* const FeatureName_;
};

} // namespace

IComputationNode* WrapHostRuntimeSetting(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto featureName = LocateNode(ctx.NodeLocator, callable, 0);
    return new THostRuntimeSettingWrapper(ctx.Mutables, featureName);
}

IComputationNode* WrapUdfRuntimeSetting(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    const auto module = LocateNode(ctx.NodeLocator, callable, 0);
    const auto featureName = LocateNode(ctx.NodeLocator, callable, 1);
    return new TUdfRuntimeSettingWrapper(ctx.Mutables, module, featureName);
}

} // namespace NKikimr::NMiniKQL
