#include "abstract.h"

#include <ydb/core/tx/columnshard/transactions/protos/tx_event.pb.h>

namespace NKikimr::NOlap::NTxInteractions {

bool TTxEventContainer::DeserializeFromProto(
    const ITxEvent::TProto& proto, const std::vector<NScheme::TTypeInfo>& logicalTypes, const NArrow::TComparator& comparator) {
    if (proto.GetClassName() == "__UNDEFINED") {
        return true;
    }

    std::shared_ptr<ITxEvent> object(ITxEvent::TFactory::Construct(proto.GetClassName()));
    if (!object) {
        YDB_LOG_ERROR_COMP(NKikimrServices::BG_TASKS, "Incorrect class name",
            {"className", proto.GetClassName()},
            {"typeName", typeid(ITxEvent).name()});
        return false;
    }

    if (!object->DeserializeFromProto(proto, logicalTypes, comparator)) {
        YDB_LOG_ERROR_COMP(NKikimrServices::BG_TASKS, "Cannot parse class instance",
            {"className", proto.GetClassName()},
            {"typeName", typeid(ITxEvent).name()});
        return false;
    }

    Object = std::move(object);
    return true;
}

TConclusionStatus TTxEventContainer::DeserializeFromString(
    const TString& data, const std::vector<NScheme::TTypeInfo>& logicalTypes, const NArrow::TComparator& comparator) {
    ITxEvent::TProto proto;
    if (!proto.ParseFromArray(data.data(), data.size())) {
        return TConclusionStatus::Fail("cannot parse string as proto");
    }

    if (!DeserializeFromProto(proto, logicalTypes, comparator)) {
        return TConclusionStatus::Fail("cannot parse proto in container");
    }

    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap::NTxInteractions
