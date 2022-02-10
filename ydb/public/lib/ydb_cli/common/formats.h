#pragma once

namespace NYdb {
namespace NConsoleClient {

enum class EOutputFormat {
    Default /* "default" */, 
    Pretty /* "pretty" */, 
    Json /* "json" (deprecated) */, 
    JsonUnicode /* "json-unicode" */, 
    JsonUnicodeArray /* "json-unicode-array" */, 
    JsonBase64 /* "json-base64" */, 
    JsonBase64Array /* "json-base64-array" */, 
    ProtoJsonBase64 /* "proto-json-base64" */, 
    Csv /* "csv" */,
    Tsv /* "tsv" */,
};

}
}
