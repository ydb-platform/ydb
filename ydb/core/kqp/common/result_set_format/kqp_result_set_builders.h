#pragma once

#include <ydb/core/kqp/common/result_set_format/kqp_result_set_format_settings.h>

#include <ydb/library/yql/dq/runtime/dq_transport.h>

namespace NKikimr::NKqp::NFormats {

/**
 * High-level helpers to build YDB result sets for any selected format
 */
void BuildResultSetFromRows(Ydb::ResultSet* ydbResult, const NFormats::TFormatsSettings& settings, bool fillSchema,
    NMiniKQL::TType* mkqlItemType, const NMiniKQL::TUnboxedValueBatch& rows, const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints, TMaybe<ui64> rowsLimitPerWrite);

void BuildResultSetFromBatches(Ydb::ResultSet* ydbResult, const NFormats::TFormatsSettings& settings, bool fillSchema,
    NMiniKQL::TType* mkqlItemType, const NYql::NDq::TDqDataSerializer& dataSerializer, TVector<NYql::NDq::TDqSerializedBatch>&& data,
    const TVector<ui32>* columnOrder, const TVector<TString>* columnHints);

} // namespace NKikimr::NKqp::NFormats
