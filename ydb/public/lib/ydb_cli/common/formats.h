#pragma once

namespace NYdb {
namespace NConsoleClient {

enum class EOutputFormat {
    Default /* "default" */,
    Pretty /* "pretty" */,
    PrettyRaw /* "pretty-raw" */,
    PrettyUnicode /* "pretty-unicode" */,
    PrettyBase64 /* "pretty-base64" */,
    Json /* "json" */,
    JsonUnicode /* "json-unicode" */,
    JsonUnicodeArray /* "json-unicode-array" */,
    JsonBase64 /* "json-base64" */,
    JsonBase64Array /* "json-base64-array" */,
    JsonRawArray /* "json-raw-array" */,
    ProtoJsonBase64 /* "proto-json-base64" */,
    Csv /* "csv" */,
    Tsv /* "tsv" */,
    SingleMessage /* "single-message" */,
    NewlineDelimited /* "newline-delimited" */,
    NewlineBase64 /* "newline-base64" */,
    Concatenated /* "concatenated" */,

    JsonBase64StreamConcat /* "json-base64-stream-concat" */,
    JsonUnicodeStreamConcat /* "json-unicode-stream-concat" */,
    JsonRawStreamConcat /* "json-raw-stream-concat" */,
};

}
}
