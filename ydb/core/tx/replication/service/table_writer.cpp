#include "base_table_writer.h"
#include "json_change_record.h"
#include "table_writer.h"

#include <ydb/core/change_exchange/resolve_partition.h>
#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NReplication::NService {

class TParser: public IChangeRecordParser {
    TLightweightSchema::TCPtr Schema;

public:
    void SetSchema(TLightweightSchema::TCPtr schema) override {
        Schema = schema;
    }

    TChangeRecord::TPtr Parse(const TString& source, ui64 id, TString&& body) override {
        return TChangeRecordBuilder()
            .WithSourceId(source)
            .WithOrder(id)
            .WithBody(std::move(body))
            .WithSchema(Schema)
            .Build();
    }
};

class TSerializer: public IChangeRecordSerializer {
    class TVisitor: public NChangeExchange::TBaseVisitor {
        TMemoryPool& MemoryPool;
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& Record;

    public:
        explicit TVisitor(TMemoryPool& pool, NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record)
            : MemoryPool(pool)
            , Record(record)
        {
        }

        void Visit(const TChangeRecord& record) override {
            record.Serialize(Record, MemoryPool);
        }
    };

    TMemoryPool MemoryPool;

public:
    TSerializer()
        : MemoryPool(256)
    {
    }

    THolder<IChangeRecordSerializer> Clone() const override {
        return MakeHolder<TSerializer>();
    }

    void Serialize(TChangeRecord::TPtr in, NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& out) override {
        TVisitor visitor(MemoryPool, out);
        in->Accept(visitor);
    }
};

class TPartitionResolver final: public NChangeExchange::TBasePartitionResolver {
public:
    TPartitionResolver(const NKikimr::TKeyDesc& keyDesc)
        : KeyDesc(keyDesc)
    {
    }

    void Visit(const TChangeRecord& record) override {
        SetPartitionId(NChangeExchange::ResolveSchemaBoundaryPartitionId(KeyDesc, record.GetKey()));
    }

private:
    const NKikimr::TKeyDesc& KeyDesc;
};

IActor* CreateLocalTableWriter(const TPathId& tablePathId, EWriteMode mode) {
    auto createResolverFn = [](const NKikimr::TKeyDesc& keyDesc) {
        return new TPartitionResolver(keyDesc);
    };

    return CreateLocalTableWriter(tablePathId, MakeHolder<TParser>(), MakeHolder<TSerializer>(), createResolverFn, mode);
}

} // namespace NKikimr::NReplication::NService
