#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <set>

namespace NKikimr {
namespace NGRpcService {

std::shared_ptr<arrow::RecordBatch> RowsToBatch(
    const TVector<std::pair<TSerializedCellVec, TString>>& rows,
    const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema,
    const std::set<std::string>& notNullColumns,
    bool enableValidation,
    TString& errorMessage);

} // namespace NGRpcService
} // namespace NKikimr
