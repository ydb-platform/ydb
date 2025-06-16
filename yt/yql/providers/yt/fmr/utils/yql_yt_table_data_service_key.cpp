#include "yql_yt_table_data_service_key.h"
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NYql::NFmr {

TString GetTableDataServiceKey(const TString& tableId, const TString& partId, ui64 chunk) {
    return TStringBuilder() << tableId << "_" << partId << ":" << chunk;
}

std::pair<TString, ui64> GetTableDataServiceGroupAndChunk(const TString& fullKey) {
    std::vector<TString> splittedKey;
    StringSplitter(fullKey).Split(':').AddTo(&splittedKey);
    YQL_ENSURE(splittedKey.size() == 2);
    ui64 chunkNum = FromString<ui64>(splittedKey[1]);
    return {splittedKey[0], chunkNum};
}

} // namespace NYql::NFmr
