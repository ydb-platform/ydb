#pragma once
#include "counters.h"
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

namespace NKikimr::NOlap::NActualizer {

class TSchemeActualizer: public IActualizer {
private:
    const TSchemeCounters Counters;
    THashMap<TRWAddress, THashSet<ui64>> PortionsToActualizeScheme;
    std::shared_ptr<ISnapshotSchema> TargetSchema;
    const ui64 PathId;
    const TVersionedIndex& VersionedIndex;

    class TFindActualizationInfo {
    private:
        TRWAddress RWAddress;
    public:
        const TRWAddress& GetRWAddress() const {
            return RWAddress;
        }

        TFindActualizationInfo(TRWAddress&& rwAddress)
            : RWAddress(std::move(rwAddress)) {

        }
    };

    THashMap<ui64, TFindActualizationInfo> PortionsInfo;

    class TFullActualizationInfo {
    private:
        TRWAddress Address;
        YDB_ACCESSOR_DEF(std::shared_ptr<ISnapshotSchema>, TargetScheme);
    public:
        TFindActualizationInfo ExtractFindId() {
            return TFindActualizationInfo(std::move(Address));
        }

        TString DebugString() const {
            return TStringBuilder() << "{address=" << Address.DebugString() << ";target_scheme=" << TargetScheme->DebugString() << "}";
        }

        const TRWAddress& GetAddress() const {
            return Address;
        }

        TFullActualizationInfo(TRWAddress&& address, const std::shared_ptr<ISnapshotSchema>& targetScheme)
            : Address(std::move(address))
            , TargetScheme(targetScheme) {

        }
    };

    std::optional<TFullActualizationInfo> BuildActualizationInfo(const TPortionInfo& portion) const;

protected:
    virtual void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) override;
    virtual void DoRemovePortion(const ui64 portionId) override;
    virtual void DoExtractTasks(TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;
public:
    void Refresh(const TAddExternalContext& externalContext);

    TSchemeActualizer(const ui64 pathId, const TVersionedIndex& versionedIndex)
        : PathId(pathId)
        , VersionedIndex(versionedIndex) {
        Y_UNUSED(PathId);
    }
};

}