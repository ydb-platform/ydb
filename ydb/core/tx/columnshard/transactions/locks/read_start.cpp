#include "read_start.h"

#include <ydb/core/tx/columnshard/transactions/protos/tx_event.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NTxInteractions {

std::shared_ptr<NKikimr::NOlap::NTxInteractions::ITxEvent> TEvReadStartWriter::DoBuildEvent() {
    return std::make_shared<TEvReadStart>(PathId, Schema, Filter);
}

bool TEvReadStart::DoDeserializeFromProto(const NKikimrColumnShardTxProto::TEvent& proto) {
    if (!proto.HasRead()) {
        YDB_LOG_ERROR("",
            {"error", "cannot_parse_TEvReadStart"},
            {"reason", "have not 'read' in proto"});
        return false;
    }
    Schema = NArrow::DeserializeSchema(proto.GetRead().GetSchema());
    if (!Schema) {
        YDB_LOG_ERROR("",
            {"error", "cannot_parse_TEvReadStart"},
            {"reason", "cannot_parse_schema"});
        return false;
    }
    Filter = TPKRangesFilter::BuildFromString(proto.GetRead().GetFilter(), Schema);
    if (!Filter) {
        YDB_LOG_ERROR("",
            {"error", "cannot_parse_TEvReadStart"},
            {"reason", "cannot_parse_filter"});
        return false;
    }
    return true;
}

void TEvReadStart::DoSerializeToProto(NKikimrColumnShardTxProto::TEvent& proto) const {
    AFL_VERIFY(!!Filter);
    AFL_VERIFY(!!Schema);
    *proto.MutableRead()->MutableFilter() = Filter->SerializeToString(Schema);
    *proto.MutableRead()->MutableSchema() = NArrow::SerializeSchema(*Schema);
}

void TEvReadStart::DoAddToInteraction(const ui64 lockId, TInteractionsContext& context) const {
    for (auto&& i : *Filter) {
        context.AddInterval(
            lockId, PathId.InternalPathId, TIntervalPoint::From(i.GetPredicateFrom(), Schema), TIntervalPoint::To(i.GetPredicateTo(), Schema));
    }
}

void TEvReadStart::DoRemoveFromInteraction(const ui64 lockId, TInteractionsContext& context) const {
    for (auto&& i : *Filter) {
        context.RemoveInterval(
            lockId, PathId.InternalPathId, TIntervalPoint::From(i.GetPredicateFrom(), Schema), TIntervalPoint::To(i.GetPredicateTo(), Schema));
    }
}

}   // namespace NKikimr::NOlap::NTxInteractions
