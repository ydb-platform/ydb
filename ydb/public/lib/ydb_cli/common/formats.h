#pragma once

namespace NYdb {
namespace NConsoleClient {

// EOutputFormat to be used in operations related to structured data
enum class EOutputFormat {
    Default /* "default" */,
    Pretty /* "pretty" */,
    Json /* "json" */,
    JsonUnicode /* "json-unicode" */,
    JsonUnicodeArray /* "json-unicode-array" */,
    JsonBase64 /* "json-base64" */,
    JsonBase64Array /* "json-base64-array" */,
    JsonRawArray /* "json-raw-array" */,
    ProtoJsonBase64 /* "proto-json-base64" */,
    Csv /* "csv" */,
    Tsv /* "tsv" */,
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
