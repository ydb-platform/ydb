#include "base_table_writer.h"
#include "json_change_record.h"
#include "table_writer.h"

#include <ydb/core/change_exchange/resolve_partition.h>
#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NReplication::NService {

class TTransferParser: public IChangeRecordParser {
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


}
