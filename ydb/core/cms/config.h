#pragma once

#include "pdisk_state.h"
#include "pdisk_status.h"

#include <ydb/core/protos/cms.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

namespace NKikimr::NCms {

struct TCmsSentinelConfig {
    bool Enable = true;
    bool DryRun = false;

    TDuration UpdateConfigInterval;
    TDuration RetryUpdateConfig;

    TDuration UpdateStateInterval;
    TDuration UpdateStateTimeout;

    TDuration RetryChangeStatus;
    ui32 ChangeStatusRetries;

    ui32 DefaultStateLimit;
    ui32 GoodStateLimit;
    TMap<EPDiskState, ui32> StateLimits;

    ui32 DataCenterRatio;
    ui32 RoomRatio;
    ui32 RackRatio;
    ui32 FaultyPDisksThresholdPerNode;

    TMaybeFail<EPDiskStatus> EvictVDisksStatus;

    void Serialize(NKikimrCms::TCmsConfig::TSentinelConfig &config) const {
        config.SetEnable(Enable);
        config.SetDryRun(DryRun);
        config.SetUpdateConfigInterval(UpdateConfigInterval.GetValue());
        config.SetRetryUpdateConfig(RetryUpdateConfig.GetValue());
        config.SetUpdateStateInterval(UpdateStateInterval.GetValue());
        config.SetUpdateStateTimeout(UpdateStateTimeout.GetValue());
        config.SetRetryChangeStatus(RetryChangeStatus.GetValue());
        config.SetChangeStatusRetries(ChangeStatusRetries);
        config.SetDefaultStateLimit(DefaultStateLimit);
        config.SetGoodStateLimit(GoodStateLimit);
        config.SetDataCenterRatio(DataCenterRatio);
        config.SetRoomRatio(RoomRatio);
        config.SetRackRatio(RackRatio);
        config.SetFaultyPDisksThresholdPerNode(FaultyPDisksThresholdPerNode);

        SaveStateLimits(config);
        SaveEvictVDisksStatus(config);
    }

    void Deserialize(const NKikimrCms::TCmsConfig::TSentinelConfig &config) {
        Enable = config.GetEnable();
        DryRun = config.GetDryRun();
        UpdateConfigInterval = TDuration::MicroSeconds(config.GetUpdateConfigInterval());
        RetryUpdateConfig = TDuration::MicroSeconds(config.GetRetryUpdateConfig());
        UpdateStateInterval = TDuration::MicroSeconds(config.GetUpdateStateInterval());
        UpdateStateTimeout = TDuration::MicroSeconds(config.GetUpdateStateTimeout());
        RetryChangeStatus = TDuration::MicroSeconds(config.GetRetryChangeStatus());
        ChangeStatusRetries = config.GetChangeStatusRetries();
        DefaultStateLimit = config.GetDefaultStateLimit();
        GoodStateLimit = config.GetGoodStateLimit();
        DataCenterRatio = config.GetDataCenterRatio();
        RoomRatio = config.GetRoomRatio();
        RackRatio = config.GetRackRatio();
        FaultyPDisksThresholdPerNode = config.GetFaultyPDisksThresholdPerNode();

        auto newStateLimits = LoadStateLimits(config);
        StateLimits.swap(newStateLimits);

        EvictVDisksStatus = LoadEvictVDisksStatus(config);
    }

    void SaveStateLimits(NKikimrCms::TCmsConfig::TSentinelConfig &config) const {
        auto defaultStateLimits = DefaultStateLimits();
        bool differsFromDefault = false;

        if (defaultStateLimits.size() != StateLimits.size()) {
            differsFromDefault = true;
        } else {
            for (const auto& [state, limit] : defaultStateLimits) {
                auto it = StateLimits.find(state);
                if (it == StateLimits.end() || limit != it->second) {
                    differsFromDefault = true;
                    break;
                }
            }
        }

        if (!differsFromDefault) {
            return;
        }

        for (const auto& [state, limit] : StateLimits) {
            auto& stateLimit = *config.AddStateLimits();
            stateLimit.SetState(static_cast<ui32>(state));
            stateLimit.SetLimit(limit);
        }
    }

    static TMap<EPDiskState, ui32> LoadStateLimits(const NKikimrCms::TCmsConfig::TSentinelConfig &config) {
        TMap<EPDiskState, ui32> stateLimits = DefaultStateLimits();

        for (const auto &val : config.GetStateLimits()) {
            stateLimits[static_cast<EPDiskState>(val.GetState())] = val.GetLimit();
        }

        return stateLimits;
    }

    static TMap<EPDiskState, ui32> DefaultStateLimits() {
        TMap<EPDiskState, ui32> stateLimits;
        // error states
        stateLimits[NKikimrBlobStorage::TPDiskState::InitialFormatReadError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::InitialSysLogReadError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::InitialSysLogParseError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::CommonLoggerInitError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::OpenFileError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::ChunkQuotaError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::DeviceIoError] = 60;
        stateLimits[NKikimrBlobStorage::TPDiskState::Stopped] = 60;

        stateLimits[NKikimrBlobStorage::TPDiskState::Reserved15] = 0;
        stateLimits[NKikimrBlobStorage::TPDiskState::Reserved16] = 0;
        stateLimits[NKikimrBlobStorage::TPDiskState::Reserved17] = 0;
        // node online, pdisk missing
        stateLimits[NKikimrBlobStorage::TPDiskState::Missing] = 60;
        // node timeout
        stateLimits[NKikimrBlobStorage::TPDiskState::Timeout] = 60;
        // node offline
        stateLimits[NKikimrBlobStorage::TPDiskState::NodeDisconnected] = 60;
        // disable for unknown states
        stateLimits[NKikimrBlobStorage::TPDiskState::Unknown] = 0;

        return stateLimits;
    }

    static TMaybeFail<EPDiskStatus> LoadEvictVDisksStatus(const NKikimrCms::TCmsConfig::TSentinelConfig &config) {
        using EEvictVDisksStatus = NKikimrCms::TCmsConfig::TSentinelConfig;
        switch (config.GetEvictVDisksStatus()) {
            case EEvictVDisksStatus::UNKNOWN:
            case EEvictVDisksStatus::FAULTY:
                return EPDiskStatus::FAULTY;
            case EEvictVDisksStatus::DISABLED:
                return Nothing();
        }
        return EPDiskStatus::FAULTY;
    }

    void SaveEvictVDisksStatus(NKikimrCms::TCmsConfig::TSentinelConfig &config) const {
        using EEvictVDisksStatus = NKikimrCms::TCmsConfig::TSentinelConfig;

        if (EvictVDisksStatus.Empty()) {
            config.SetEvictVDisksStatus(EEvictVDisksStatus::DISABLED);
            return;
        }

        if (*EvictVDisksStatus == EPDiskStatus::FAULTY) {
            config.SetEvictVDisksStatus(EEvictVDisksStatus::FAULTY);
        }
    }
};

struct TCmsLogConfig {
    THashMap<ui32, bool> RecordLevels;
    bool EnabledByDefault = true;
    TDuration TTL;

    void Serialize(NKikimrCms::TCmsConfig::TLogConfig &config) const {
        config.SetDefaultLevel(EnabledByDefault
            ? NKikimrCms::TCmsConfig::TLogConfig::ENABLED
            : NKikimrCms::TCmsConfig::TLogConfig::DISABLED);
        for (auto pr : RecordLevels) {
            auto &entry = *config.AddComponentLevels();
            entry.SetRecordType(pr.first);
            entry.SetLevel(pr.second
                ? NKikimrCms::TCmsConfig::TLogConfig::ENABLED
                : NKikimrCms::TCmsConfig::TLogConfig::DISABLED);
        }
        config.SetTTL(TTL.GetValue());
    }

    void Deserialize(const NKikimrCms::TCmsConfig::TLogConfig &config) {
        RecordLevels.clear();

        EnabledByDefault = (config.GetDefaultLevel() == NKikimrCms::TCmsConfig::TLogConfig::ENABLED);
        for (auto &entry : config.GetComponentLevels()) {
            RecordLevels[entry.GetRecordType()]
                = entry.GetLevel() == NKikimrCms::TCmsConfig::TLogConfig::ENABLED;
        }
        TTL = TDuration::FromValue(config.GetTTL());
    }

    bool IsLogEnabled(ui32 recordType) const {
        if (RecordLevels.contains(recordType))
            return RecordLevels.at(recordType);
        return EnabledByDefault;
    }
};

struct TCmsConfig {
    TDuration DefaultRetryTime;
    TDuration DefaultPermissionDuration;
    TDuration DefaultWalleCleanupPeriod = TDuration::Minutes(1);
    TDuration InfoCollectionTimeout;
    NKikimrCms::TLimits TenantLimits;
    NKikimrCms::TLimits ClusterLimits;
    TCmsSentinelConfig SentinelConfig;
    TCmsLogConfig LogConfig;

    TCmsConfig() {
        Deserialize(NKikimrCms::TCmsConfig());
    }

    TCmsConfig(const NKikimrCms::TCmsConfig &config) {
        Deserialize(config);
    }

    void Serialize(NKikimrCms::TCmsConfig &config) const {
        config.SetDefaultRetryTime(DefaultRetryTime.GetValue());
        config.SetDefaultPermissionDuration(DefaultPermissionDuration.GetValue());
        config.SetInfoCollectionTimeout(InfoCollectionTimeout.GetValue());
        config.MutableTenantLimits()->CopyFrom(TenantLimits);
        config.MutableClusterLimits()->CopyFrom(ClusterLimits);
        SentinelConfig.Serialize(*config.MutableSentinelConfig());
        LogConfig.Serialize(*config.MutableLogConfig());
    }

    void Deserialize(const NKikimrCms::TCmsConfig &config) {
        DefaultRetryTime = TDuration::MicroSeconds(config.GetDefaultRetryTime());
        DefaultPermissionDuration = TDuration::MicroSeconds(config.GetDefaultPermissionDuration());
        InfoCollectionTimeout = TDuration::MicroSeconds(config.GetInfoCollectionTimeout());
        TenantLimits.CopyFrom(config.GetTenantLimits());
        ClusterLimits.CopyFrom(config.GetClusterLimits());
        SentinelConfig.Deserialize(config.GetSentinelConfig());
        LogConfig.Deserialize(config.GetLogConfig());
    }

    bool IsLogEnabled(ui32 recordType) const {
        return LogConfig.IsLogEnabled(recordType);
    }
};

} // namespace NKikimr::NCms
