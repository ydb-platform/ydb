#pragma once

#include "public.h"

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void CheckStackDepth();

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger MakeQueryLogger(TGuid queryId);
NLogging::TLogger MakeQueryLogger(TConstBaseQueryPtr query);

////////////////////////////////////////////////////////////////////////////////

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    TStringBuf source,
    TStringBuf lhsSource,
    TStringBuf rhsSource);

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(TRange<std::string> keyColumns, const std::string& columnName);

//! Derives type of reference expression based on table column type.
//!
//! For historical reasons reference expressions used to have `wire type` of column i.e.
//! if column had `Int16` type its reference would have `Int64` type.
//! `DeriveReferenceType` keeps this behaviour for V1 types, but for V3 types actual type is returned.
TLogicalTypePtr ToQLType(const TLogicalTypePtr& columnType);

////////////////////////////////////////////////////////////////////////////////

std::vector<EConstraintKind> GetExpressionConstraintSignature(
    const TConstExpressionPtr& expression,
    const NTableClient::TKeyColumns& keyColumns);

int GetConstraintSignatureScore(const std::vector<EConstraintKind>& signature);

////////////////////////////////////////////////////////////////////////////////

bool CanOmitOrderBy(int keyPrefix, TRange<TOrderItem> orderItems, TRange<std::string> keyColumns);

bool CanReverseScanForOrderBy(int fixedKeyPrefix, TRange<TOrderItem> orderItems, TRange<std::string> keyColumns, const TConstGroupClausePtr& groupClause);

////////////////////////////////////////////////////////////////////////////////

std::string ToLower(TStringBuf original);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
