#pragma once

#include "lightweight_schema.h"
#include "table_writer.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimrTxDataShard {
    class TEvApplyReplicationChanges_TChange;
}

namespace NKikimr::NReplication::NService {

class IChangeRecordParser {
public:
    virtual ~IChangeRecordParser() = default;
    virtual void SetSchema(TLightweightSchema::TCPtr schema) = 0;
    virtual NChangeExchange::IChangeRecord::TPtr Parse(const TString& source, ui64 id, TString&& body) = 0;
};

class IChangeRecordSerializer {
public:
    virtual ~IChangeRecordSerializer() = default;
    virtual THolder<IChangeRecordSerializer> Clone() const = 0;
    virtual void Serialize(NChangeExchange::IChangeRecord::TPtr in, NKikimrTxDataShard::TEvApplyReplicationChanges_TChange& out) = 0;
};

IActor* CreateLocalTableWriter(
    const TPathId& tablePathId,
    THolder<IChangeRecordParser>&& parser,
    THolder<IChangeRecordSerializer>&& serializer,
    std::function<NChangeExchange::IPartitionResolverVisitor*(const NKikimr::TKeyDesc&)>&& createResolverFn,
    EWriteMode mode = EWriteMode::Simple);

}
