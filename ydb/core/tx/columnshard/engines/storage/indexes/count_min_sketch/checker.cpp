#include "checker.h"
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

void TCountMinSketchChecker::DoSerializeToProtoImpl(NKikimrSSA::TProgram::TOlapIndexChecker& proto) const {
    proto.MutableCountMinSketch();
}

bool TCountMinSketchChecker::DoCheckImpl(const std::vector<TString>& blobs) const {
    Y_UNUSED(blobs);
    return true;
}

bool TCountMinSketchChecker::DoDeserializeFromProtoImpl(const NKikimrSSA::TProgram::TOlapIndexChecker& proto) {
    return proto.HasCountMinSketch();
}

}   // namespace NKikimr::NOlap::NIndexes
