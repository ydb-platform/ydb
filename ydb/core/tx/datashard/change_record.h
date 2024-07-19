#pragma once

#include "datashard_user_table.h"

#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/change_exchange/change_sender_resolver.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <util/generic/maybe.h>
#include <util/string/join.h>

namespace NKikimrChangeExchange {
    class TChangeRecord;
}

namespace NKikimr::NDataShard {

class TChangeRecordBuilder;

class TChangeRecord: public NChangeExchange::TChangeRecordBase {
    friend class TChangeRecordBuilder;

public:
    using TPtr = TIntrusivePtr<TChangeRecord>;

    ui64 GetGroup() const override { return Group; }
    ui64 GetStep() const override { return Step; }
    ui64 GetTxId() const override { return TxId; }
    EKind GetKind() const override { return Kind; }
    ui64 GetLockId() const { return LockId; }
    ui64 GetLockOffset() const { return LockOffset; }
    const TPathId& GetPathId() const { return PathId; }

    const TPathId& GetTableId() const { return TableId; }
    ui64 GetSchemaVersion() const { return SchemaVersion; }
    TUserTable::TCPtr GetSchema() const { return Schema; }

    void Serialize(NKikimrChangeExchange::TChangeRecord& record) const;

    TConstArrayRef<TCell> GetKey() const;
    TString GetPartitionKey() const;
    i64 GetSeqNo() const;
    TInstant GetApproximateCreationDateTime() const;
    bool IsBroadcast() const override;

    void Out(IOutputStream& out) const override;

    ui64 ResolvePartitionId(NChangeExchange::IChangeSenderResolver* const resolver) const override {
        const auto& partitions = resolver->GetPartitions();
        Y_ABORT_UNLESS(partitions);
        const auto& schema = resolver->GetSchema();
        const auto streamFormat = resolver->GetStreamFormat();

        switch (streamFormat) {
            case NKikimrSchemeOp::ECdcStreamFormatProto: {
                const auto range = TTableRange(GetKey());
                Y_ABORT_UNLESS(range.Point);

                const auto it = LowerBound(
                    partitions.cbegin(), partitions.cend(), true,
                    [&](const auto& partition, bool) {
                        Y_ABORT_UNLESS(partition.Range);
                        const int compares = CompareBorders<true, false>(
                            partition.Range->EndKeyPrefix.GetCells(), range.From,
                            partition.Range->IsInclusive || partition.Range->IsPoint,
                            range.InclusiveFrom || range.Point, schema
                        );

                        return (compares < 0);
                    }
                );

                Y_ABORT_UNLESS(it != partitions.cend());
                return it->ShardId;
            }

            case NKikimrSchemeOp::ECdcStreamFormatJson:
            case NKikimrSchemeOp::ECdcStreamFormatDynamoDBStreamsJson:
            case NKikimrSchemeOp::ECdcStreamFormatDebeziumJson: {
                using namespace NKikimr::NDataStreams::V1;
                const auto hashKey = HexBytesToDecimal(GetPartitionKey() /* MD5 */);
                return ShardFromDecimal(hashKey, partitions.size());
            }

            default: {
                Y_FAIL_S("Unknown format"
                    << ": format# " << static_cast<int>(streamFormat));
            }
        }
    }

private:
    ui64 Group = 0;
    ui64 Step = 0;
    ui64 TxId = 0;
    EKind Kind;
    ui64 LockId = 0;
    ui64 LockOffset = 0;
    TPathId PathId;

    ui64 SchemaVersion;
    TPathId TableId;
    TUserTable::TCPtr Schema;

    mutable TMaybe<TOwnedCellVec> Key;
    mutable TMaybe<TString> PartitionKey;

}; // TChangeRecord

class TChangeRecordBuilder: public NChangeExchange::TChangeRecordBuilder<TChangeRecord, TChangeRecordBuilder> {
public:
    using TBase::TBase;

    explicit TChangeRecordBuilder(EKind kind)
        : TBase()
    {
        GetRecord()->Kind = kind;
    }

    TSelf& WithGroup(ui64 group) {
        GetRecord()->Group = group;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithStep(ui64 step) {
        GetRecord()->Step = step;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithTxId(ui64 txId) {
        GetRecord()->TxId = txId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithLockId(ui64 lockId) {
        GetRecord()->LockId = lockId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithLockOffset(ui64 lockOffset) {
        GetRecord()->LockOffset = lockOffset;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithPathId(const TPathId& pathId) {
        GetRecord()->PathId = pathId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithTableId(const TPathId& tableId) {
        GetRecord()->TableId = tableId;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithSchemaVersion(ui64 version) {
        GetRecord()->SchemaVersion = version;
        return static_cast<TSelf&>(*this);
    }

    TSelf& WithSchema(TUserTable::TCPtr schema) {
        GetRecord()->Schema = schema;
        return static_cast<TSelf&>(*this);
    }

}; // TChangeRecordBuilder

}

namespace NKikimr {

template <>
struct TChangeRecordContainer<NDataShard::TChangeRecord>
    : public TBaseChangeRecordContainer
{
    TChangeRecordContainer() = default;

    explicit TChangeRecordContainer(TVector<NDataShard::TChangeRecord::TPtr>&& records)
        : Records(std::move(records))
    {}

    TVector<NDataShard::TChangeRecord::TPtr> Records;

    TString Out() override {
        return TStringBuilder() << "[" << JoinSeq(",", Records) << "]";
    }
};

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TChangeRecord, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TChangeRecord::TPtr, out, value) {
    return value->Out(out);
}
