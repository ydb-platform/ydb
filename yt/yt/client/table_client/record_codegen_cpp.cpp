#include "record_codegen_cpp.h"

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient::NDetail {

using namespace NJson;
using namespace NYson;

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

TLogicalTypePtr FromRecordCodegenTypeV3(TStringBuf data)
{
    auto producer = TYsonProducer(BIND([&] (IYsonConsumer* consumer) {
        TMemoryInput input(data);

        ParseJson(&input, consumer);
    }));

    return ConvertTo<TLogicalTypePtr>(producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient::NDetail
