#include "sql_format.h"

#include <yql/essentials/public/sql_format/abi/interface/sql_format.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>

namespace NSQLFormat {

bool SqlFormatSimple(const TString& query, TString& formattedQuery, TString& error) {
    auto* handle = YqlSqlFormatV1(query.data(), query.size());
    Y_DEFER {
        YqlSqlFormatFreeV1(handle);
    };
    const int status = YqlSqlFormatStatusV1(handle);
    size_t errorSize = 0;
    const char* errorData = YqlSqlFormatErrorV1(handle, &errorSize);
    if (status == YQL_SQL_FORMAT_EXCEPTION) {
        ythrow yexception() << TStringBuf(errorData, errorSize);
    }
    Y_ENSURE(status == YQL_SQL_FORMAT_OK || status == YQL_SQL_FORMAT_ERROR,
             "Unknown SQL formatter status: " << status);
    size_t outputSize = 0;
    const char* outputData = YqlSqlFormatOutputV1(handle, &outputSize);
    formattedQuery.assign(outputData, outputSize);
    if (status == YQL_SQL_FORMAT_ERROR) {
        error.assign(errorData, errorSize);
    }
    return status == YQL_SQL_FORMAT_OK;
}

} // namespace NSQLFormat
