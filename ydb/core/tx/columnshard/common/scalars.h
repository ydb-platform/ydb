#pragma once

#include <ydb/library/formats/arrow/protos/ssa.pb.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>
#include <memory>

namespace NKikimr::NOlap {

void ScalarToConstant(const arrow20::Scalar& scalar, NKikimrSSA::TProgram_TConstant& value);
std::shared_ptr<arrow20::Scalar> ConstantToScalar(const NKikimrSSA::TProgram_TConstant& value,
                                                const std::shared_ptr<arrow20::DataType>& type);

TString SerializeKeyScalar(const std::shared_ptr<arrow20::Scalar>& key);
std::shared_ptr<arrow20::Scalar> DeserializeKeyScalar(const TString& key, const std::shared_ptr<arrow20::DataType>& type);

}
