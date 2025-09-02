#include "abstract.h"
#include "string.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes {

TString IBitsStorage::DebugString() const {
    TStringBuilder sb;
    ui32 count1 = 0;
    ui32 count0 = 0;
    for (ui32 i = 0; i < GetBitsCount(); ++i) {
        //            if (i % 20 == 0 && i) {
        //                sb << i << " ";
        //            }
        if (Get(i)) {
            //                sb << 1 << " ";
            ++count1;
        } else {
            //                sb << 0 << " ";
            ++count0;
        }
    }
    sb << GetBitsCount() << "=" << count0 << "[0]+" << count1 << "[1]";
    return sb;
}

std::shared_ptr<IBitsStorageConstructor> IBitsStorageConstructor::GetDefault() {
    static std::shared_ptr<IBitsStorageConstructor> result = std::make_shared<TFixStringBitsStorageConstructor>();
    return result;
}

NKikimrSchemeOp::TSkipIndexBitSetStorage IBitsStorageConstructor::SerializeToProto() const {
    NKikimrSchemeOp::TSkipIndexBitSetStorage result;
    result.SetClassName(GetClassName());
    return result;
}

TConclusionStatus IBitsStorageConstructor::DeserializeFromProto(const NKikimrSchemeOp::TSkipIndexBitSetStorage& /*proto*/) {
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NOlap::NIndexes
