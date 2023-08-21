#pragma once

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/assert/assert.h>

#include <array>

namespace NYT::NTableClient::NDetail {

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyValueCount(TLegacyKey key, int count);
int GetColumnIdOrThrow(std::optional<int> optionalId, TStringBuf name);
void ValidateRowValueCount(TUnversionedRow row, int id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient::NDetail
