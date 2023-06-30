#include "record_helpers.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void ValidateRowNotNull(TUnversionedRow row)
{
    if (!row) {
        THROW_ERROR_EXCEPTION("Row must not be null");
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
