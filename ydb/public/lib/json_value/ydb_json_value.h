#pragma once

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>

#include <library/cpp/json/writer/json.h>

namespace NYdb {

enum class EBinaryStringEncoding {
    /* For binary strings, encode every byte that is not a printable ascii symbol (codes 32-126) as utf-8.
     * Exceptions: \\ \" \b \t \f \r \n
     * Example: "Hello\x01" -> "Hello\\u0001"
    */
    Unicode = 1,

    /* Encode binary strings to base64
    */
    Base64
};

// ====== YDB to json ======
void FormatValueJson(const TValue& value, NJsonWriter::TBuf& writer, EBinaryStringEncoding encoding);

TString FormatValueJson(const TValue& value, EBinaryStringEncoding encoding);

void FormatResultRowJson(TResultSetParser& parser, const TVector<TColumn>& columns, NJsonWriter::TBuf& writer,
    EBinaryStringEncoding encoding);

TString FormatResultRowJson(TResultSetParser& parser, const TVector<TColumn>& columns,
    EBinaryStringEncoding encoding);

void FormatResultSetJson(const TResultSet& result, IOutputStream* out, EBinaryStringEncoding encoding);

TString FormatResultSetJson(const TResultSet& result, EBinaryStringEncoding encoding);

// ====== json to YDB ======
TValue JsonToYdbValue(const TString& jsonString, const TType& type, EBinaryStringEncoding encoding);
TValue JsonToYdbValue(const NJson::TJsonValue& jsonValue, const TType& type, EBinaryStringEncoding encoding);

} // namespace NYdb
