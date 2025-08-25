#include "json.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/str.h>

namespace NSQLHighlight {

    void Print(IOutputStream& out, const NJson::TJsonValue& json) {
        NJson::TJsonWriterConfig config = {
            .SortKeys = true,
        };

        TStringStream output;
        NJson::WriteJson(&output, &json, config);
        YQL_ENSURE(NJson::PrettifyJson(output.Str(), out));
    }

} // namespace NSQLHighlight
