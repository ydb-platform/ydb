#include "yql_kikimr_settings.h"

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

    REGISTER_SETTING(*this, KqpPushOlapProcess);

    /* Compile time */
    REGISTER_SETTING(*this, _CommitPerShardKeysSizeLimitBytes);
    REGISTER_SETTING(*this, _DefaultCluster);
    REGISTER_SETTING(*this, _ResultRowsLimit);
    REGISTER_SETTING(*this, EnableSystemColumns);
    REGISTER_SETTING(*this, UseLlvm);

    REGISTER_SETTING(*this, OptDisableJoinRewrite);
    REGISTER_SETTING(*this, OptDisableJoinTableLookup);
    REGISTER_SETTING(*this, OptDisableJoinReverseTableLookup);
    REGISTER_SETTING(*this, OptDisableJoinReverseTableLookupLeftSemi);
    REGISTER_SETTING(*this, OptDisableTopSort);
    REGISTER_SETTING(*this, OptDisableSqlInToJoin);
    REGISTER_SETTING(*this, OptEnableInplaceUpdate);
    REGISTER_SETTING(*this, OptEnablePredicateExtract);
    REGISTER_SETTING(*this, OptEnableOlapPushdown);
    REGISTER_SETTING(*this, OptUseFinalizeByKey);

    /* Runtime */
    REGISTER_SETTING(*this, ScanQuery);
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

bool TKikimrSettings::HasOptDisableJoinRewrite() const {
    return GetFlagValue(OptDisableJoinRewrite.Get());
}

bool TKikimrSettings::HasOptDisableJoinTableLookup() const {
    return GetFlagValue(OptDisableJoinTableLookup.Get());
}

bool TKikimrSettings::HasOptDisableJoinReverseTableLookup() const {
    return GetFlagValue(OptDisableJoinReverseTableLookup.Get());
}

bool TKikimrSettings::HasOptDisableJoinReverseTableLookupLeftSemi() const {
    return GetFlagValue(OptDisableJoinReverseTableLookupLeftSemi.Get());
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

bool TKikimrSettings::HasOptUseFinalizeByKey() const {
    return GetOptionalFlagValue(OptUseFinalizeByKey.Get()) == EOptionalFlag::Enabled;
}

EOptionalFlag TKikimrSettings::GetOptPredicateExtract() const {
    return GetOptionalFlagValue(OptEnablePredicateExtract.Get());
}

EOptionalFlag TKikimrSettings::GetUseLlvm() const {
    return GetOptionalFlagValue(UseLlvm.Get());
}

TKikimrSettings::TConstPtr TKikimrConfiguration::Snapshot() const {
    return std::make_shared<const TKikimrSettings>(*this);
}

}
