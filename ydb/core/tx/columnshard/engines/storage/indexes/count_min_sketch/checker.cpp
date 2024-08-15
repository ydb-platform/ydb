#include "checker.h"
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

void TCountMinSketchChecker::DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const {
    Y_UNUSED(proto);
    Y_ABORT("Unimplemented");  // unimplemented, should not be used
}

bool TCountMinSketchChecker::DoCheckImpl(const std::vector<TString>& blobs) const {
    Y_UNUSED(blobs);
    Y_ABORT("Unimplemented");  // unimplemented, should not be used
    return false;
}

bool TCountMinSketchChecker::DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) {
    Y_UNUSED(proto);
    Y_ABORT("Unimplemented");  // unimplemented, should not be used
    return false;
}

}   // namespace NKikimr::NOlap::NIndexes
