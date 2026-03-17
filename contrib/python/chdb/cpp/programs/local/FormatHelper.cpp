#include "FormatHelper.h"

#include <algorithm>

namespace CHDB {

static bool is_json_supported = true;

void SetCurrentFormat(const char * format)
{
    if (format)
    {
        String lowerFormat = format;
        std::transform(lowerFormat.begin(), lowerFormat.end(), lowerFormat.begin(), ::tolower);

        is_json_supported =  !(lowerFormat == "arrow" || lowerFormat == "parquet" || lowerFormat == "arrowstream"
            || lowerFormat == "protobuf" || lowerFormat == "protobuflist" || lowerFormat == "protobufsingle");

        return;
    }

    is_json_supported = true;
}

bool isJSONSupported()
{
    return is_json_supported;
}

} // namespace CHDB
