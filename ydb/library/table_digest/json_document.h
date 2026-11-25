#pragma once

#include <library/cpp/json/json_writer.h>
#include <yql/essentials/types/binary_json/read.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

#include <cmath>

namespace NYql::NDigest {
namespace NDetail {

inline void WriteCanonicalJson(const NKikimr::NBinaryJson::TContainerCursor& cursor,
                                NJson::TJsonWriter& writer) {
    using NKikimr::NBinaryJson::EContainerType;
    using NKikimr::NBinaryJson::EEntryType;
    const auto writeEntry = [&writer](const NKikimr::NBinaryJson::TEntryCursor& entry) {
        switch (entry.GetType()) {
            case EEntryType::BoolFalse:
            case EEntryType::BoolTrue:
                writer.Write(entry.GetType() == EEntryType::BoolTrue);
                break;
            case EEntryType::Null:
                writer.WriteNull();
                break;
            case EEntryType::String:
                writer.Write(entry.GetString());
                break;
            case EEntryType::Number: {
                const double number = entry.GetNumber();
                if (!std::isfinite(number)) {
                    writer.UnsafeWrite(FloatToString(number));
                } else {
                    writer.Write(number == 0.0 ? 0.0 : number);
                }
                break;
            }
            case EEntryType::Container:
                WriteCanonicalJson(entry.GetContainer(), writer);
                break;
        }
    };

    switch (cursor.GetType()) {
        case EContainerType::TopLevelScalar:
            writeEntry(cursor.GetElement(0));
            break;
        case EContainerType::Array: {
            writer.OpenArray();
            auto it = cursor.GetArrayIterator();
            while (it.HasNext()) {
                writeEntry(it.Next());
            }
            writer.CloseArray();
            break;
        }
        case EContainerType::Object: {
            writer.OpenMap();
            auto it = cursor.GetObjectIterator();
            while (it.HasNext()) {
                const auto [key, value] = it.Next();
                writer.WriteKey(key.GetString());
                writeEntry(value);
            }
            writer.CloseMap();
            break;
        }
    }
}

} // namespace NDetail

// Decode the logical document instead of hashing its physical string/number
// indexes. Shortest-roundtrip formatting preserves finite double values.
// Unquoted nonfinite tokens distinguish numeric values from JSON strings.
inline TString CanonicalJsonDocument(TStringBuf document) {
    NJson::TJsonWriterConfig config;
    config.FloatToStringMode = PREC_AUTO;
    TStringStream output;
    NJson::TJsonWriter writer(&output, config);
    const auto reader = NKikimr::NBinaryJson::TBinaryJsonReader::Make(document);
    NDetail::WriteCanonicalJson(reader->GetRootCursor(), writer);
    writer.Flush();
    return output.Str();
}

} // namespace NYql::NDigest
