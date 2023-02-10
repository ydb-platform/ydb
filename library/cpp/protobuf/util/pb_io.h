#pragma once

#include <util/generic/fwd.h>
#include <util/generic/flags.h>

struct IBinSaver;

namespace google {
    namespace protobuf {
        class Message;
    }
}

namespace NProtoBuf {
    using Message = ::google::protobuf::Message;
}

class IInputStream;
class IOutputStream;

namespace NProtoBuf {
    /* Parse base64 URL encoded serialized message from string.
     */
    void ParseFromBase64String(const TStringBuf dataBase64, Message& m, bool allowUneven = false);
    bool TryParseFromBase64String(const TStringBuf dataBase64, Message& m, bool allowUneven = false);
    template <typename T>
    static T ParseFromBase64String(const TStringBuf& dataBase64, bool allowUneven = false) {
        T m;
        ParseFromBase64String(dataBase64, m, allowUneven);
        return m;
    }

    /* Serialize message into string and apply base64 URL encoding.
     */
    TString SerializeToBase64String(const Message& m);
    void SerializeToBase64String(const Message& m, TString& dataBase64);
    bool TrySerializeToBase64String(const Message& m, TString& dataBase64);

    const TString ShortUtf8DebugString(const Message& message);

    bool MergePartialFromString(NProtoBuf::Message& m, const TStringBuf serializedProtoMessage);
    bool MergeFromString(NProtoBuf::Message& m, const TStringBuf serializedProtoMessage);
}

int operator&(NProtoBuf::Message& m, IBinSaver& f);

// Write a textual representation of the given message to the given file.
void SerializeToTextFormat(const NProtoBuf::Message& m, const TString& fileName);
void SerializeToTextFormat(const NProtoBuf::Message& m, IOutputStream& out);

// Write a textual representation of the given message to the given output stream
// with flags UseShortRepeatedPrimitives and UseUtf8StringEscaping set to true.
void SerializeToTextFormatPretty(const NProtoBuf::Message& m, IOutputStream& out);

// Write a textual representation of the given message to the given output stream
// use enum id instead of enum name for all enum fields.
void SerializeToTextFormatWithEnumId(const NProtoBuf::Message& m, IOutputStream& out);

enum class EParseFromTextFormatOption : ui64 {
    // Unknown fields will be ignored by the parser
    AllowUnknownField = 1
};

Y_DECLARE_FLAGS(EParseFromTextFormatOptions, EParseFromTextFormatOption);

// Parse a text-format protocol message from the given file into message object.
void ParseFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options = {}, IOutputStream* warningStream = nullptr);
// NOTE: will read `in` till the end.
void ParseFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options = {}, IOutputStream* warningStream = nullptr);

/* @return              `true` if parsing was successfull and `false` otherwise.
 *
 * @see `ParseFromTextFormat`
 */
bool TryParseFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options = {}, IOutputStream* warningStream = nullptr);
// NOTE: will read `in` till the end.
bool TryParseFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options = {}, IOutputStream* warningStream = nullptr);

// @see `ParseFromTextFormat`
template <typename T>
static T ParseFromTextFormat(const TString& fileName,
                             const EParseFromTextFormatOptions options = {}, IOutputStream* warningStream = nullptr) {
    T message;
    ParseFromTextFormat(fileName, message, options, warningStream);
    return message;
}

// @see `ParseFromTextFormat`
// NOTE: will read `in` till the end.
template <typename T>
static T ParseFromTextFormat(IInputStream& in, const EParseFromTextFormatOptions options = {},
                             IOutputStream* warningStream = nullptr) {
    T message;
    ParseFromTextFormat(in, message, options, warningStream);
    return message;
}

// Merge a text-format protocol message from the given file into message object.
//
// NOTE: Even when parsing failed and exception was thrown `m` may be different from its original
// value. User must implement transactional logic around `MergeFromTextFormat` by himself.
void MergeFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options = {});
// NOTE: will read `in` till the end.
void MergeFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options = {});
/* @return              `true` if parsing was successfull and `false` otherwise.
 *
 * @see `MergeFromTextFormat`
 */
bool TryMergeFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options = {});
// NOTE: will read `in` till the end.
bool TryMergeFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options = {});

// @see `MergeFromTextFormat`
template <typename T>
static T MergeFromTextFormat(const TString& fileName,
                             const EParseFromTextFormatOptions options = {}) {
    T message;
    MergeFromTextFormat(fileName, message, options);
    return message;
}

// @see `MergeFromTextFormat`
// NOTE: will read `in` till the end.
template <typename T>
static T MergeFromTextFormat(IInputStream& in,
                             const EParseFromTextFormatOptions options = {}) {
    T message;
    MergeFromTextFormat(in, message, options);
    return message;
}
