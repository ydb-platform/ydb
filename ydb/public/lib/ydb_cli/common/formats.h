#pragma once

namespace NYdb {
namespace NConsoleClient {

// EOutputFormat to be used in operations related to structured data
enum class EOutputFormat {
    Default /* "default" */,
    Pretty /* "pretty" */,
    PrettyTable /* "pretty-table" */,
    Json /* "json" */,
    JsonUnicode /* "json-unicode" */,
    JsonUnicodeArray /* "json-unicode-array" */,
    JsonBase64 /* "json-base64" */,
    JsonBase64Simplify /* "json-base64-simplify" */,
    JsonBase64Array /* "json-base64-array" */,
    JsonRawArray /* "json-raw-array" */,
    ProtoJsonBase64 /* "proto-json-base64" */,
    Csv /* "csv" */,
    Tsv /* "tsv" */,
    Parquet /* "parquet" */,
    NoFraming /* "no-framing" */,
    NewlineDelimited /* "newline-delimited" */,
    Raw /* "raw" */
};

// EMessagingFormat to be used in both input and output when working with files/pipes in operations related to messaging 
// This format defines rules for data transformation, framing, metadata envelope format
enum class EMessagingFormat {
    Pretty /* "pretty" */,
    SingleMessage /* "single-message" */,
    NewlineDelimited /* "newline-delimited" */,
    Concatenated /* "concatenated" */,

    JsonStreamConcat /* "json-stream-concat" */,
    JsonArray /* "json-array" */,
};

}
}
