#include "yql_kikimr_settings.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <util/generic/size_literals.h>
#include <util/string/split.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

namespace NYql {

using namespace NCommon;

namespace {

template <typename TType>
EOptionalFlag GetOptionalFlagValue(const TMaybe<TType>& flag) {
    if (!flag) {
        return EOptionalFlag::Auto;
    }

    if (flag.GetRef()) {
        return EOptionalFlag::Enabled;
    }

    return EOptionalFlag::Disabled;
}


ui64 ParseEnableSpillingNodes(const TString &v) {
    ui64 res = 0;
    TVector<TString> vec;
    StringSplitter(v).SplitBySet(",;| ").AddTo(&vec);
    for (auto& s: vec) {
        if (s.empty()) {
            throw yexception() << "Empty value item";
        }
        auto value = FromString<NYql::TDqSettings::EEnabledSpillingNodes>(s);
        res |= ui64(value);
    }
    return res;
}

static inline bool GetFlagValue(const TMaybe<bool>& flag) {
    return flag ? flag.GetRef() : false;
}

} // anonymous namespace end

TKikimrConfiguration::TKikimrConfiguration() {
    /* KQP */
    REGISTER_SETTING(*this, _KqpSessionIdleTimeoutSec);
    REGISTER_SETTING(*this, _KqpMaxActiveTxPerSession);
    REGISTER_SETTING(*this, _KqpTxIdleTimeoutSec);
    REGISTER_SETTING(*this, _KqpExprNodesAllocationLimit);
    REGISTER_SETTING(*this, _KqpExprStringsAllocationLimit);
    REGISTER_SETTING(*this, _KqpTablePathPrefix);
    REGISTER_SETTING(*this, _KqpSlowLogWarningThresholdMs);
    REGISTER_SETTING(*this, _KqpSlowLogNoticeThresholdMs);
    REGISTER_SETTING(*this, _KqpSlowLogTraceThresholdMs);
    REGISTER_SETTING(*this, _KqpYqlSyntaxVersion);
    REGISTER_SETTING(*this, _KqpAllowUnsafeCommit);
    REGISTER_SETTING(*this, _KqpMaxComputeActors);
    REGISTER_SETTING(*this, _KqpEnableSpilling);
    REGISTER_SETTING(*this, _KqpDisableLlvmForUdfStages);
    REGISTER_SETTING(*this, _KqpYqlCombinerMemoryLimit).Lower(0ULL).Upper(1_GB);

    REGISTER_SETTING(*this, KqpPushOlapProcess);

    /* Compile time */
    REGISTER_SETTING(*this, _CommitPerShardKeysSizeLimitBytes);
    REGISTER_SETTING(*this, _DefaultCluster);
    REGISTER_SETTING(*this, _ResultRowsLimit);
    REGISTER_SETTING(*this, EnableSystemColumns);
    REGISTER_SETTING(*this, UseLlvm);
    REGISTER_SETTING(*this, EnableLlvm);
    REGISTER_SETTING(*this, HashJoinMode).Parser([](const TString& v) { return FromString<NDq::EHashJoinMode>(v); });

    REGISTER_SETTING(*this, OptDisableTopSort);
    REGISTER_SETTING(*this, OptDisableSqlInToJoin);
    REGISTER_SETTING(*this, OptEnableInplaceUpdate);
    REGISTER_SETTING(*this, OptEnablePredicateExtract);
    REGISTER_SETTING(*this, OptEnableOlapPushdown);
    REGISTER_SETTING(*this, OptEnableOlapProvideComputeSharding);
    REGISTER_SETTING(*this, OverrideStatistics);
    REGISTER_SETTING(*this, OverridePlanner);
    REGISTER_SETTING(*this, UseGraceJoinCoreForMap);

    REGISTER_SETTING(*this, OptUseFinalizeByKey);
    REGISTER_SETTING(*this, CostBasedOptimizationLevel);
    REGISTER_SETTING(*this, EnableSpillingNodes)
        .Parser([](const TString& v) { return ParseEnableSpillingNodes(v); });

    REGISTER_SETTING(*this, MaxDPccpDPTableSize);

    REGISTER_SETTING(*this, MaxTasksPerStage);

    /* Runtime */
    REGISTER_SETTING(*this, ScanQuery);

    IndexAutoChooserMode = NKikimrConfig::TTableServiceConfig_EIndexAutoChooseMode_DISABLED;
    BlockChannelsMode = NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_SCALAR;
}

bool TKikimrSettings::HasAllowKqpUnsafeCommit() const {
    return GetFlagValue(_KqpAllowUnsafeCommit.Get());
}

bool TKikimrSettings::HasDefaultCluster() const {
    return _DefaultCluster.Get() && !_DefaultCluster.Get().GetRef().empty();
}

bool TKikimrSettings::SystemColumnsEnabled() const {
    return GetFlagValue(EnableSystemColumns.Get());
}

bool TKikimrSettings::SpillingEnabled() const {
    return GetFlagValue(_KqpEnableSpilling.Get());
}

bool TKikimrSettings::DisableLlvmForUdfStages() const {
    return GetFlagValue(_KqpDisableLlvmForUdfStages.Get());
}

bool TKikimrSettings::HasOptDisableTopSort() const {
    return GetFlagValue(OptDisableTopSort.Get());
}

bool TKikimrSettings::HasOptDisableSqlInToJoin() const {
    return GetFlagValue(OptDisableSqlInToJoin.Get());
}

bool TKikimrSettings::HasOptEnableInplaceUpdate() const {
    return GetFlagValue(OptEnableInplaceUpdate.Get());
}

bool TKikimrSettings::HasOptEnableOlapPushdown() const {
    return GetOptionalFlagValue(OptEnableOlapPushdown.Get()) != EOptionalFlag::Disabled;
}

bool TKikimrSettings::HasOptEnableOlapProvideComputeSharding() const {
    return GetOptionalFlagValue(OptEnableOlapProvideComputeSharding.Get()) == EOptionalFlag::Enabled;
}

bool TKikimrSettings::HasOptUseFinalizeByKey() const {
    return GetOptionalFlagValue(OptUseFinalizeByKey.Get()) != EOptionalFlag::Disabled;
}

EOptionalFlag TKikimrSettings::GetOptPredicateExtract() const {
    return GetOptionalFlagValue(OptEnablePredicateExtract.Get());
}

EOptionalFlag TKikimrSettings::GetUseLlvm() const {
    auto optionalFlag = GetOptionalFlagValue(UseLlvm.Get());
    if (optionalFlag == EOptionalFlag::Auto) {
        optionalFlag = GetOptionalFlagValue(EnableLlvm.Get());
    }
    return optionalFlag;
}

NDq::EHashJoinMode TKikimrSettings::GetHashJoinMode() const {
    auto maybeHashJoinMode = HashJoinMode.Get();
    return maybeHashJoinMode ? *maybeHashJoinMode : NDq::EHashJoinMode::Off;
}

TKikimrSettings::TConstPtr TKikimrConfiguration::Snapshot() const {
    return std::make_shared<const TKikimrSettings>(*this);
}

void TKikimrConfiguration::SetDefaultEnabledSpillingNodes(const TString& node) {
    DefaultEnableSpillingNodes = ParseEnableSpillingNodes(node);
}

ui64 TKikimrConfiguration::GetEnabledSpillingNodes() const {
    return EnableSpillingNodes.Get().GetOrElse(DefaultEnableSpillingNodes);
}

}
