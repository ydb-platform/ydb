#pragma once
#include "context.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimrSchemeshardOlap {
class TEntityEvolution;
}

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ISSEntityEvolution {
protected:
    std::set<ui64> ShardIds;

    virtual TConclusionStatus DoStartEvolution(const TEvolutionStartContext& context) = 0;
    virtual TConclusionStatus DoFinishEvolution(const TEvolutionStartContext& context) = 0;
    virtual TString DoGetShardTxBody(const TPathId& pathId, const ui64 tabletId, const TMessageSeqNo& seqNo) const = 0;
    virtual void DoSerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto) = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<ISSEntityEvolution, TString>;
    using TProto = NKikimrSchemeshardOlap::TEntityEvolution;
    virtual TString GetClassName() const = 0;

    virtual ~ISSEntityEvolution() = default;
    const std::set<ui64>& GetShardIds() const {
        return ShardIds;
    }

    TConclusionStatus StartEvolution(const TEvolutionStartContext& context) {
        return DoStartEvolution(context);
    }

    TConclusionStatus FinishEvolution(const TEvolutionFinishContext& context) {
        return DoFinishEvolution(context);
    }

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const = 0;
    TString GetShardTxBody(const TPathId& pathId, const ui64 tabletId, const TMessageSeqNo& seqNo) const {
        return DoGetShardTxBody(pathId, tabletId, seqNo);
    }

    void SerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const;

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto);
    ISSEntityEvolution() = default;
    ISSEntityEvolution(const std::set<ui64>& shardIds)
        : ShardIds(shardIds)
    {

    }

};

class TSSEntityEvolutionContainer: public NBackgroundTasks::TInterfaceProtoContainer<ISSEntityEvolution> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<ISSEntityEvolution>;
public:
    using TBase::TBase;
};

}