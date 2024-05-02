#pragma once
#include "context.h"
#include "evolution.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>

namespace NKikimrSchemeshardOlap {
class TEvolutions;
}

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ISSEntityUpdate;

class TEvolutions {
private:
    YDB_READONLY_DEF(NKikimrSchemeOp::TModifyScheme, AlterProtoOriginal);
    std::deque<TSSEntityEvolutionContainer> Evolutions;
public:
    TEvolutions() = default;
    TEvolutions(const NKikimrSchemeOp::TModifyScheme& alterProtoOriginal, std::deque<TSSEntityEvolutionContainer>&& evolutions)
        : AlterProtoOriginal(alterProtoOriginal)
        , Evolutions(std::move(evolutions))
    {

    }

    bool HasEvolutions() const {
        return Evolutions.size();
    }

    std::shared_ptr<ISSEntityEvolution> GetCurrentEvolution() const {
        if (Evolutions.size()) {
            return Evolutions.front().GetObjectPtr();
        } else {
            return nullptr;
        }
    }

    std::shared_ptr<ISSEntityEvolution> ExtractCurrentEvolution() {
        if (Evolutions.size()) {
            auto result = Evolutions.front().GetObjectPtr();
            Evolutions.pop_front();
            return result;
        } else {
            return nullptr;
        }
    }

    NKikimrSchemeshardOlap::TEvolutions SerializeToProto() const;
    TString SerializeToProtoString() const;

    TConclusionStatus DeserializeFromProto(const TString& protoStr);

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeshardOlap::TEvolutions& proto);

};

class ISSEntity {
private:
    YDB_READONLY_DEF(TPathId, PathId);
    bool Initialized = false;
protected:
    [[nodiscard]] virtual TConclusionStatus DoInitialize(const TEntityInitializationContext& context) = 0;
    virtual TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const = 0;
    virtual std::shared_ptr<ISSEntityEvolution> DoGetCurrentEvolution() const = 0;
    virtual std::shared_ptr<ISSEntityEvolution> DoExtractCurrentEvolution() = 0;
    [[nodiscard]] virtual TConclusionStatus DoStartUpdate(TEvolutions&& evolutions, const TStartUpdateContext& context) = 0;
    virtual void DoFinishUpdate() = 0;
    virtual void DoPersist(const TDBWriteContext& dbContext) const = 0;

    TConclusion<std::shared_ptr<ISSEntityUpdate>> CreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const {
        AFL_VERIFY(Initialized);
        return DoCreateUpdate(context, selfPtr);
    }

public:
    virtual ~ISSEntity() = default;
    virtual TString GetClassName() const = 0;
    virtual std::set<ui64> GetShardIds() const = 0;

    [[nodiscard]] TConclusionStatus Initialize(const TEntityInitializationContext& context) {
        AFL_VERIFY(!Initialized);
        Initialized = true;
        return DoInitialize(context);
    }

    ISSEntity(const TPathId& pathId)
        : PathId(pathId)
    {

    }

    TConclusionStatus StartUpdate(const TUpdateInitializationContext& uContext, const TStartUpdateContext& sContext, const std::shared_ptr<ISSEntity>& selfPtr);

    void FinishUpdate() {
        AFL_VERIFY(Initialized);
        return DoFinishUpdate();
    }

    void Persist(const TDBWriteContext& dbContext) const {
        AFL_VERIFY(Initialized);
        return DoPersist(dbContext);
    }

    std::shared_ptr<ISSEntityEvolution> GetCurrentEvolution() const {
        AFL_VERIFY(Initialized);
        return DoGetCurrentEvolution();
    }

    std::shared_ptr<ISSEntityEvolution> ExtractCurrentEvolution() {
        AFL_VERIFY(Initialized);
        return DoExtractCurrentEvolution();
    }

    static std::shared_ptr<ISSEntity> GetPathEntityVerified(TOperationContext& context, const TPath& path);

    TVector<ISubOperation::TPtr> BuildOperations(const TUpdateInitializationContext& context,
        const std::shared_ptr<ISSEntity>& selfPtr, const TOperationId& id) const;
};

}