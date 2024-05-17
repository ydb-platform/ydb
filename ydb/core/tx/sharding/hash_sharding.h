#pragma once
#include "sharding.h"
#include <ydb/core/formats/arrow/hash/calcer.h>

namespace NKikimr::NSharding {

class THashShardingImpl: public TShardingBase {
private:
    using TBase = TShardingBase;
    ui64 Seed = 0;
    std::optional<NArrow::NHash::TXX64> HashCalcer;
    YDB_READONLY_DEF(std::vector<TString>, ShardingColumns);
protected:
    virtual void DoSerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const override {
        for (auto&& i : ShardingColumns) {
            proto.MutableHashSharding()->AddColumns(i);
        }
    }
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) override {
        if (!proto.HasHashSharding()) {
            return TConclusionStatus::Fail("no data about hash sharding");
        }
        if (!proto.GetHashSharding().GetColumns().size()) {
            return TConclusionStatus::Fail("no columns for hash sharding");
        }
        for (auto&& i : proto.GetHashSharding().GetColumns()) {
            ShardingColumns.emplace_back(i);
        }
        AFL_VERIFY(!HashCalcer);
        HashCalcer.emplace(ShardingColumns, NArrow::NHash::TXX64::ENoColumnPolicy::Verify, Seed);
        return TConclusionStatus::Success();
    }
public:
    THashShardingImpl() = default;

    THashShardingImpl(const std::vector<ui64>& shardIds, const std::vector<TString>& columnNames, ui64 seed = 0)
        : TBase(shardIds)
        , Seed(seed)
        , ShardingColumns(columnNames) {
        HashCalcer.emplace(columnNames, NArrow::NHash::TXX64::ENoColumnPolicy::Verify, Seed);
    }

    virtual TString DebugString() const override {
        return TBase::DebugString() + ";Columns: " + JoinSeq(", ", GetShardingColumns());
    }

    virtual std::vector<ui64> MakeHashes(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        AFL_VERIFY(!!HashCalcer);
        return HashCalcer->Execute(batch).value_or(Default<std::vector<ui64>>());
    }

    template <typename T>
    static ui64 CalcHash(const T value, const ui32 seed = 0) {
        static_assert(std::is_arithmetic<T>::value);
        return XXH64(&value, sizeof(value), seed);
    }

};

}
