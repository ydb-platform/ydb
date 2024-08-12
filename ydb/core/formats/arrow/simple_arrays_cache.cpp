#include "simple_arrays_cache.h"
#include "common/validation.h"

namespace NKikimr::NArrow {

std::shared_ptr<arrow::Array> TThreadSimpleArraysCache::GetNullImpl(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount) {
    AFL_VERIFY(type);
    const TString key = type->ToString();
    const auto initializer = [type](const ui32 recordsCount) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayOfNull(type, recordsCount));
    };
    return InitializePosition(type->ToString(), recordsCount, initializer);
}

std::shared_ptr<arrow::Array> TThreadSimpleArraysCache::GetConstImpl(const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount) {
    AFL_VERIFY(type);
    AFL_VERIFY(scalar);
    AFL_VERIFY(scalar->type->id() == type->id())("scalar", scalar->type->ToString())("field", type->ToString());
    const auto initializer = [scalar](const ui32 recordsCount) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(*scalar, recordsCount));
    };
    return InitializePosition(type->ToString() + "::" + scalar->ToString(), recordsCount, initializer);
}

namespace {
static thread_local TThreadSimpleArraysCache SimpleArraysCache;
}

std::shared_ptr<arrow::Array> TThreadSimpleArraysCache::GetNull(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount) {
    return SimpleArraysCache.GetNullImpl(type, recordsCount);
}

std::shared_ptr<arrow::Array> TThreadSimpleArraysCache::GetConst(const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount) {
    return SimpleArraysCache.GetConstImpl(type, scalar, recordsCount);
}

std::shared_ptr<arrow::Array> TThreadSimpleArraysCache::Get(const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount) {
    if (scalar) {
        return GetConst(type, scalar, recordsCount);
    } else {
        return GetNull(type, recordsCount);
    }
}

}

