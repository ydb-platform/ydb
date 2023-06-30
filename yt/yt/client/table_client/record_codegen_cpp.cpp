#include "record_codegen_cpp.h"

namespace NYT::NTableClient::NDetail {

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyValueCount(TLegacyKey key, int count)
{
    if (static_cast<int>(key.GetCount()) != count) {
        THROW_ERROR_EXCEPTION("Invalid number of key values: expected %v, got %v",
            count,
            key.GetCount());
    }
}

int GetColumnIdOrThrow(std::optional<int> optionalId, TStringBuf name)
{
    if (!optionalId) {
        THROW_ERROR_EXCEPTION("Column %Qv is not registered",
            name);
    }
    return *optionalId;
}

void ValidateRowValueCount(TUnversionedRow row, int id)
{
    if (static_cast<int>(row.GetCount()) < id) {
        THROW_ERROR_EXCEPTION("Too few values in row: expected > %v, actual %v",
            id,
            row.GetCount());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient::NDetail
