#pragma once
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/tiering/tier_info.h>

namespace NKikimr::NOlap {
class TTiering;
}

namespace NKikimr::NOlap::NActualizer {

class TTieringActualizer: public IActualizer {
private:
    class TFullActualizationInfo {
    private:
        TRWAddress Address;
        YDB_ACCESSOR_DEF(TString, TargetTierName);
        YDB_ACCESSOR_DEF(ISnapshotSchema::TPtr, TargetScheme);
        i64 WaitDurationValue;
    public:
        TString DebugString() const {
            return TStringBuilder() << "{address=" << Address.DebugString() << ";target_tier=" << TargetTierName << ";wait_duration=" << TDuration::FromValue(WaitDurationValue) << "}";
        }

        const TRWAddress& GetAddress() const {
            return Address;
        }

        TFullActualizationInfo(TRWAddress&& address, const TString& targetTierName, const i64 waitDurationValue, const ISnapshotSchema::TPtr& targetScheme)
            : Address(std::move(address))
            , TargetTierName(targetTierName)
            , TargetScheme(targetScheme)
            , WaitDurationValue(waitDurationValue)
        {

        }

        TDuration GetWaitDuration() const {
            if (WaitDurationValue >= 0) {
                return TDuration::FromValue(WaitDurationValue);
            } else {
                return TDuration::Zero();
            }
        }

        TDuration GetLateness() const {
            if (WaitDurationValue >= 0) {
                return TDuration::Zero();
            } else {
                return TDuration::FromValue(-WaitDurationValue);
            }
        }
    };

    class TFindActualizationInfo {
    private:
        TRWAddress RWAddress;
        YDB_READONLY_DEF(TDuration, WaitDuration);
    public:
        const TRWAddress& GetRWAddress() const {
            return RWAddress;
        }

        TFindActualizationInfo(TRWAddress&& rwAddress, const TDuration waitDuration)
            : RWAddress(std::move(rwAddress))
            , WaitDuration(waitDuration) {

        }
    };

    std::optional<TTiering> Tiering;
    std::optional<ui32> TieringColumnId;

    std::shared_ptr<ISnapshotSchema> TargetCriticalSchema;
    const ui64 PathId;
    const TVersionedIndex& VersionedIndex;

    TInstant StartInstant = TInstant::Zero();
    THashMap<TRWAddress, std::map<TDuration, THashSet<ui64>>> PortionIdByWaitDuration;
    THashMap<ui64, TFindActualizationInfo> PortionsInfo;

    std::shared_ptr<ISnapshotSchema> GetTargetSchema(const std::shared_ptr<ISnapshotSchema>& portionSchema) const;

    std::optional<TFullActualizationInfo> BuildActualizationInfo(const TPortionInfo& portion, const TInstant now) const;

    virtual void DoAddPortion(const TPortionInfo& portion, const TAddExternalContext& addContext) override;
    virtual void DoRemovePortion(const TPortionInfo& info) override;
    virtual void DoBuildTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) const override;

public:
    void Refresh(const std::optional<TTiering>& info, const TAddExternalContext& externalContext);

    TTieringActualizer(const ui64 pathId, const TVersionedIndex& versionedIndex)
        : PathId(pathId)
        , VersionedIndex(versionedIndex)
    {
        Y_UNUSED(PathId);
    }
};

}