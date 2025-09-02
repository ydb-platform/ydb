#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap::NNormalizer::NSpecialColumns {

class TDeleteTrashImpl: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    class IAction {
    public:
        virtual TConclusionStatus ApplyOnExecute(NIceDb::TNiceDb& db) const = 0;
        virtual ~IAction() = default;
    };

private:
    bool PrechargeV0(NTabletFlatExecutor::TTransactionContext& txc);
    bool PrechargeV1(NTabletFlatExecutor::TTransactionContext& txc);
    bool PrechargeV2(NTabletFlatExecutor::TTransactionContext& txc);
    std::optional<std::vector<std::shared_ptr<IAction>>> LoadKeysV0(NTabletFlatExecutor::TTransactionContext& txc, const std::set<ui64>& columns);
    std::optional<std::vector<std::shared_ptr<IAction>>> LoadKeysV1(NTabletFlatExecutor::TTransactionContext& txc, const std::set<ui64>& columns);
    std::optional<std::vector<std::shared_ptr<IAction>>> LoadKeysV2(NTabletFlatExecutor::TTransactionContext& txc, const std::set<ui64>& columns);

    std::optional<std::vector<std::shared_ptr<IAction>>> KeysToDelete(NTabletFlatExecutor::TTransactionContext& txc);

    virtual std::set<ui64> GetColumnIdsToDelete() const = 0;

public:
    TDeleteTrashImpl(const TNormalizationController::TInitContext& context)
        : TBase(context) {
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

class TRemoveDeleteFlag: public TDeleteTrashImpl {
private:
    using TBase = TDeleteTrashImpl;

public:
    static TString GetClassNameStatic() {
        return "RemoveDeleteFlag";
    }

private:
    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TRemoveDeleteFlag>(GetClassNameStatic());

    virtual std::set<ui64> GetColumnIdsToDelete() const override {
        return { NPortion::TSpecialColumns::SPEC_COL_DELETE_FLAG_INDEX };
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return {};
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

public:
    TRemoveDeleteFlag(const TNormalizationController::TInitContext& context)
        : TBase(context) {
    }
};

class TRemoveWriteId: public TDeleteTrashImpl {
private:
    using TBase = TDeleteTrashImpl;

public:
    static TString GetClassNameStatic() {
        return "RemoveWriteId";
    }

private:
    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TRemoveWriteId>(GetClassNameStatic());

    virtual std::set<ui64> GetColumnIdsToDelete() const override {
        return { NPortion::TSpecialColumns::SPEC_COL_WRITE_ID_INDEX };
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return {};
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

public:
    TRemoveWriteId(const TNormalizationController::TInitContext& context)
        : TBase(context) {
    }
};

}   // namespace NKikimr::NOlap::NNormalizer::NSpecialColumns
