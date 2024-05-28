#pragma once

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/replication.pb.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <functional>

namespace NSchemeShardUT_Private {

using NKikimr::TOwnerId;
using NKikimr::TLocalPathId;
using NKikimr::TPathId;

struct TPathVersion {
    TPathId PathId = TPathId();
    ui64 Version = Max<ui64>();
};
using TApplyIf = TVector<TPathVersion>;

using TUserAttrs = TVector<std::pair<TString, TString>>;

namespace NLs {
    using TCheckFunc = std::function<void (const NKikimrScheme::TEvDescribeSchemeResult&)>;

    TCheckFunc ExtractVolumeConfig(NKikimrBlockStore::TVolumeConfig* config);
    TCheckFunc CheckMountToken(const TString& name, const TString& expectedOwner);

    TCheckFunc UserAttrsEqual(TUserAttrs attrs);
    TCheckFunc UserAttrsHas(TUserAttrs attrs);
    TCheckFunc PathVersionEqual(ui64 expectedVersion);
    TCheckFunc PathVersionOneOf(TSet<ui64> versions);

    TCheckFunc PathsInsideDomain(ui64 count);
    TCheckFunc PQPartitionsInsideDomain(ui64 count);
    TCheckFunc TopicReservedStorage(ui64 expected);
    TCheckFunc TopicAccountSize(ui64 expected);
    TCheckFunc TopicAccountSizeGE(ui64 expected);
    TCheckFunc TopicUsedReserveSize(ui64 expected);
    TCheckFunc PathsInsideDomainOneOf(TSet<ui64> variants);
    TCheckFunc ShardsInsideDomain(ui64 count);
    TCheckFunc ShardsInsideDomainOneOf(TSet<ui64> variants);
    TCheckFunc DomainLimitsIs(ui64 maxPaths, ui64 maxShards, ui64 maxPQPartitions = 0);

    TCheckFunc FreezeStateEqual(NKikimrSchemeOp::EFreezeState expectedState);


    void NotInSubdomain(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void InSubdomain(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void SubdomainWithNoEmptyStoragePools(const NKikimrScheme::TEvDescribeSchemeResult& record);

    TCheckFunc IsSubDomain(const TString& name);
    TCheckFunc SubDomainVersion(ui64 descrVersion);
    TCheckFunc StoragePoolsEqual(TSet<TString> poolNames);
    TCheckFunc DomainCoordinators(TVector<ui64> coordinators);
    TCheckFunc DomainMediators(TVector<ui64> mediators);
    TCheckFunc DomainSchemeshard(ui64 domainSchemeshardId);
    TCheckFunc DomainHive(ui64 domainHiveId);
    TCheckFunc DomainKey(ui64 pathId, ui64 schemeshardId);
    TCheckFunc DomainKey(TPathId pathId);
    TCheckFunc DomainSettings(ui32 planResolution, ui32 timeCastBucketsPerMediator);
    TCheckFunc DatabaseSizeIs(ui64 bytes);

    TCheckFunc IsExternalSubDomain(const TString& name);
    void InExternalSubdomain(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc ExtractTenantSchemeshard(ui64* tenantSchemeShardId);
    TCheckFunc ExtractTenantSysViewProcessor(ui64* tenantSVPId);
    TCheckFunc ExtractTenantStatisticsAggregator(ui64* tenantSAId);
    TCheckFunc ExtractDomainHive(ui64* domainHiveId);

    void NotFinished(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void Finished(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc CreatedAt(ui64 txId);


    TCheckFunc PathIdEqual(TPathId pathId);
    TCheckFunc PathIdEqual(ui64 pathId);
    TCheckFunc PathStringEqual(const TString& path);
    void NoChildren(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc ChildrenCount(ui32 count);
    TCheckFunc ChildrenCount(ui32 count, NKikimrSchemeOp::EPathState pathState);
    void PathNotExist(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void PathExist(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void PathRedirected(const NKikimrScheme::TEvDescribeSchemeResult& record);


    void IsTable(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void IsExternalTable(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void IsExternalDataSource(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void IsView(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc CheckColumns(const TString& name, const TSet<TString>& columns, const TSet<TString>& droppedColumns, const TSet<TString> keyColumns,
                            NKikimrSchemeOp::EPathState pathState = NKikimrSchemeOp::EPathState::EPathStateNoChanges);
    void CheckBoundaries(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc PartitionCount(ui32 count);
    TCheckFunc PartitionKeys(TVector<TString> lastShardKeys);
    TCheckFunc FollowerCount(ui32 count);
    TCheckFunc CrossDataCenterFollowerCount(ui32 count);
    TCheckFunc AllowFollowerPromotion(bool val);
    TCheckFunc FollowerGroups(const TVector<NKikimrHive::TFollowerGroup>& followerGroup = TVector<NKikimrHive::TFollowerGroup>{});
    TCheckFunc SizeToSplitEqual(ui32 size);
    TCheckFunc MinPartitionsCountEqual(ui32 count);
    void HasMinPartitionsCount(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void NoMinPartitionsCount(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc MaxPartitionsCountEqual(ui32 count);
    void HasMaxPartitionsCount(const NKikimrScheme::TEvDescribeSchemeResult& record);
    void NoMaxPartitionsCount(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc PartitioningByLoadStatus(bool status);
    TCheckFunc ColumnFamiliesCount(ui32 size);
    TCheckFunc ColumnFamiliesHas(ui32 familyId);
    TCheckFunc ColumnFamiliesHas(ui32 familyId, const TString& familyName);
    TCheckFunc KeyBloomFilterStatus(bool status);
    TCheckFunc HasTtlEnabled(const TString& columnName, const TDuration& expireAfter,
        NKikimrSchemeOp::TTTLSettings::EUnit columnUnit = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO);
    TCheckFunc HasTtlDisabled();
    TCheckFunc IsBackupTable(bool value);
    TCheckFunc ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode mode);
    TCheckFunc ReplicationState(NKikimrReplication::TReplicationState::StateCase state);

    TCheckFunc HasColumnTableSchemaPreset(const TString& presetName);
    TCheckFunc HasColumnTableSchemaVersion(ui64 schemaVersion);
    TCheckFunc HasColumnTableTtlSettingsVersion(ui64 ttlSettingsVersion);
    TCheckFunc HasColumnTableTtlSettingsEnabled(const TString& columnName, const TDuration& expireAfter);
    TCheckFunc HasColumnTableTtlSettingsDisabled();
    TCheckFunc HasColumnTableTtlSettingsTiering(const TString& tierName);

    TCheckFunc CheckPartCount(const TString& name, ui32 partCount, ui32 maxParts, ui32 tabletCount, ui32 groupCount,
                              NKikimrSchemeOp::EPathState pathState = NKikimrSchemeOp::EPathState::EPathStateNoChanges);
    TCheckFunc CheckPQAlterVersion (const TString& name, ui64 alterVersion);
    TCheckFunc IndexesCount(ui32 count);

    TCheckFunc IndexType(NKikimrSchemeOp::EIndexType type);
    TCheckFunc IndexState(NKikimrSchemeOp::EIndexState state);
    TCheckFunc IndexKeys(const TVector<TString>& keyNames);
    TCheckFunc IndexDataColumns(const TVector<TString>& dataColumnNames);

    TCheckFunc SequenceName(const TString& name);
    TCheckFunc SequenceIncrement(i64 increment);
    TCheckFunc SequenceMaxValue(i64 maxValue);
    TCheckFunc SequenceMinValue(i64 minValue);
    TCheckFunc SequenceCycle(bool cycle);
    TCheckFunc SequenceStartValue(i64 startValue);
    TCheckFunc SequenceCache(ui64 cache);

    TCheckFunc StreamMode(NKikimrSchemeOp::ECdcStreamMode mode);
    TCheckFunc StreamFormat(NKikimrSchemeOp::ECdcStreamFormat format);
    TCheckFunc StreamState(NKikimrSchemeOp::ECdcStreamState state);
    TCheckFunc StreamVirtualTimestamps(bool value);
    TCheckFunc StreamResolvedTimestamps(const TDuration& value);
    TCheckFunc StreamAwsRegion(const TString& value);
    TCheckFunc RetentionPeriod(const TDuration& value);

    TCheckFunc HasBackupInFly(ui64 txId);
    void NoBackupInFly(const NKikimrScheme::TEvDescribeSchemeResult& record);
    TCheckFunc BackupHistoryCount(ui64 count);

    TCheckFunc HasOwner(const TString& owner);
    TCheckFunc HasEffectiveRight(const TString& right);
    TCheckFunc HasNotEffectiveRight(const TString& right);

    TCheckFunc KesusConfigIs(ui64 self_check_period_millis, ui64 session_grace_period_millis);
    TCheckFunc DatabaseQuotas(ui64 dataStreamShards);
    TCheckFunc SharedHive(ui64 sharedHiveId);
    TCheckFunc ServerlessComputeResourcesMode(NKikimrSubDomains::EServerlessComputeResourcesMode serverlessComputeResourcesMode);

    struct TInverseTag {
        bool Value = false;
    };

    void HasOffloadConfigBase(const NKikimrScheme::TEvDescribeSchemeResult& record, TInverseTag inverse);
    inline void HasOffloadConfig(const NKikimrScheme::TEvDescribeSchemeResult& record) { return HasOffloadConfigBase(record, {}); };
    inline void HasNotOffloadConfig(const NKikimrScheme::TEvDescribeSchemeResult& record) { return HasOffloadConfigBase(record, {.Value = true}); };

    template<class TCheck>
    void PerformAllChecks(const NKikimrScheme::TEvDescribeSchemeResult& result, TCheck&& check) {
        check(result);
    }

    template<class TCheck, class... TArgs>
    void PerformAllChecks(const NKikimrScheme::TEvDescribeSchemeResult& result, TCheck&& check, TArgs&&... args) {
        check(result);
        PerformAllChecks(result, args...);
    }

    template<class... TArgs>
    TCheckFunc All(TArgs&&... args) {
        return [=] (const NKikimrScheme::TEvDescribeSchemeResult& result) {
            return PerformAllChecks(result, args...);
        };
    }

}
}
