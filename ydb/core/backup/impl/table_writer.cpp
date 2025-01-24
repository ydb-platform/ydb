#include "change_record.h"
#include "table_writer.h"

#include <ydb/core/change_exchange/resolve_partition.h>
#include <ydb/core/tx/replication/service/base_table_writer.h>

Y_DECLARE_OUT_SPEC(inline, NKikimr::NBackup::NImpl::TChangeRecord, out, value) {
    return value.Out(out);
}

namespace NKikimr::NBackup::NImpl {

class TParser: public NReplication::NService::IChangeRecordParser {
    NReplication::NService::TLightweightSchema::TCPtr Schema;

public:
    void SetSchema(NReplication::NService::TLightweightSchema::TCPtr schema) override {
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

class TSerializer: public NReplication::NService::IChangeRecordSerializer {
    class TVisitor: public NChangeExchange::TBaseVisitor {
        const EWriterType Type;
        NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& Record;

    public:
        explicit TVisitor(EWriterType type, NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& record)
            : Type(type)
            , Record(record)
        {
        }

        void Visit(const TChangeRecord& record) override {
            record.Serialize(Record, Type);
        }
    };

    const EWriterType Type;

public:
    explicit TSerializer(EWriterType type)
        : Type(type)
    {
    }

    THolder<IChangeRecordSerializer> Clone() const override {
        return MakeHolder<TSerializer>(Type);
    }

    void Serialize(TChangeRecord::TPtr in, NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& out) override {
        TVisitor visitor(Type, out);
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

IActor* CreateLocalTableWriter(const TPathId& tablePathId, EWriterType type) {
    auto createResolverFn = [](const NKikimr::TKeyDesc& keyDesc) {
        return new TPartitionResolver(keyDesc);
    };

    return CreateLocalTableWriter(tablePathId, MakeHolder<TParser>(), MakeHolder<TSerializer>(type), createResolverFn);
}

}
