#include "misc.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {

void SetAppropriateStoregeIdAndInheritPortionStorageBasedOnType(google::protobuf::Message& index, std::string_view typeName) {
    if (NKikimrSchemeOp::TOlapIndexRequested* indexRequested = dynamic_cast<NKikimrSchemeOp::TOlapIndexRequested*>(&index)) {
        if (typeName == NKikimr::NScheme::TypeName(NKikimr::NScheme::NTypeIds::String) ||
            typeName == NKikimr::NScheme::TypeName(NKikimr::NScheme::NTypeIds::Utf8)) {
            indexRequested->SetInheritPortionStorage(true);
            indexRequested->SetStorageId("__DEFAULT");
        } else {
            indexRequested->SetInheritPortionStorage(false);
            indexRequested->SetStorageId("__LOCAL_METADATA");
        }
    } else {
        NKikimrSchemeOp::TOlapIndexDescription* indexDescritpion = dynamic_cast<NKikimrSchemeOp::TOlapIndexDescription*>(&index);
        if (typeName == NKikimr::NScheme::TypeName(NKikimr::NScheme::NTypeIds::String) ||
            typeName == NKikimr::NScheme::TypeName(NKikimr::NScheme::NTypeIds::Utf8)) {
            indexDescritpion->SetInheritPortionStorage(true);
            indexDescritpion->SetStorageId("__DEFAULT");
        } else {
            indexDescritpion->SetInheritPortionStorage(false);
            indexDescritpion->SetStorageId("__LOCAL_METADATA");
        }
    }
}
}   // namespace NKikimr::NOlap::NIndexes::NMinMax
