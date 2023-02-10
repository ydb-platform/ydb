#include "pb_io.h"

#include <library/cpp/binsaver/bin_saver.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/message.h>
#include <google/protobuf/messagext.h>
#include <google/protobuf/text_format.h>

#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NProtoBuf {

    class TEnumIdValuePrinter : public google::protobuf::TextFormat::FastFieldValuePrinter {
    public:
        void PrintEnum(int32 val, const TString& /*name*/, google::protobuf::TextFormat::BaseTextGenerator* generator) const override {
            generator->PrintString(ToString(val));
        }
    };

    void ParseFromBase64String(const TStringBuf dataBase64, Message& m, bool allowUneven) {
        if (!m.ParseFromString(allowUneven ? Base64DecodeUneven(dataBase64) : Base64StrictDecode(dataBase64))) {
            ythrow yexception() << "can't parse " << m.GetTypeName() << " from base64-encoded string";
        }
    }

    bool TryParseFromBase64String(const TStringBuf dataBase64, Message& m, bool allowUneven) {
        try {
            ParseFromBase64String(dataBase64, m, allowUneven);
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    void SerializeToBase64String(const Message& m, TString& dataBase64) {
        TString rawData;
        if (!m.SerializeToString(&rawData)) {
            ythrow yexception() << "can't serialize " << m.GetTypeName();
        }

        Base64EncodeUrl(rawData, dataBase64);
    }

    TString SerializeToBase64String(const Message& m) {
        TString s;
        SerializeToBase64String(m, s);
        return s;
    }

    bool TrySerializeToBase64String(const Message& m, TString& dataBase64) {
        try {
            SerializeToBase64String(m, dataBase64);
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    const TString ShortUtf8DebugString(const Message& message) {
        TextFormat::Printer printer;
        printer.SetSingleLineMode(true);
        printer.SetUseUtf8StringEscaping(true);
        TString result;
        printer.PrintToString(message, &result);
        return result;
    }

    bool MergePartialFromString(NProtoBuf::Message& m, const TStringBuf serializedProtoMessage) {
        google::protobuf::io::CodedInputStream input(reinterpret_cast<const ui8*>(serializedProtoMessage.data()), serializedProtoMessage.size());
        bool ok = m.MergePartialFromCodedStream(&input);
        ok = ok && input.ConsumedEntireMessage();
        return ok;
    }

    bool MergeFromString(NProtoBuf::Message& m, const TStringBuf serializedProtoMessage) {
        return MergePartialFromString(m, serializedProtoMessage) && m.IsInitialized();
    }
}  // end of namespace NProtoBuf


namespace {
    class TErrorCollector: public NProtoBuf::io::ErrorCollector {
    public:
        TErrorCollector(const NProtoBuf::Message& m, IOutputStream* errorOut, IOutputStream* warningOut)
          : TypeName_(m.GetTypeName())
        {
            ErrorOut_ = errorOut ? errorOut : &Cerr;
            WarningOut_ = warningOut ? warningOut : &Cerr;
        }
        void AddError(int line, int column, const TProtoStringType& message) override {
            PrintErrorMessage(ErrorOut_, "Error", line, column, message);
        }
        void AddWarning(int line, int column, const TProtoStringType& message) override {
            PrintErrorMessage(WarningOut_, "Warning", line, column, message);
        }

    private:
        void PrintErrorMessage(IOutputStream* out, TStringBuf errorLevel, int line, int column, const TProtoStringType& message) {
            (*out) << errorLevel << " parsing text-format ";
            if (line >= 0) {
                (*out) << TypeName_ << ": " << (line + 1) << ":" << (column + 1) << ": " << message;
            } else {
                (*out) << TypeName_ << ": " << message;
            }
            out->Flush();
        }

    private:
        const TProtoStringType TypeName_;
        IOutputStream* ErrorOut_;
        IOutputStream* WarningOut_;
    };
}  // end of anonymous namespace


int operator&(NProtoBuf::Message& m, IBinSaver& f) {
    TStringStream ss;
    if (f.IsReading()) {
        f.Add(0, &ss.Str());
        m.ParseFromArcadiaStream(&ss);
    } else {
        m.SerializeToArcadiaStream(&ss);
        f.Add(0, &ss.Str());
    }
    return 0;
}

void SerializeToTextFormat(const NProtoBuf::Message& m, IOutputStream& out) {
    NProtoBuf::io::TCopyingOutputStreamAdaptor adaptor(&out);

    if (!NProtoBuf::TextFormat::Print(m, &adaptor)) {
        ythrow yexception() << "SerializeToTextFormat failed on Print";
    }
}

void SerializeToTextFormat(const NProtoBuf::Message& m, const TString& fileName) {
    /* TUnbufferedFileOutput is unbuffered, but TCopyingOutputStreamAdaptor adds
     * a buffer on top of it. */
    TUnbufferedFileOutput stream(fileName);
    SerializeToTextFormat(m, stream);
}

void SerializeToTextFormatWithEnumId(const NProtoBuf::Message& m, IOutputStream& out) {
    google::protobuf::TextFormat::Printer printer;
    printer.SetDefaultFieldValuePrinter(new NProtoBuf::TEnumIdValuePrinter());
    NProtoBuf::io::TCopyingOutputStreamAdaptor adaptor(&out);

    if (!printer.Print(m, &adaptor)) {
         ythrow yexception() << "SerializeToTextFormatWithEnumId failed on Print";
    }
}

void SerializeToTextFormatPretty(const NProtoBuf::Message& m, IOutputStream& out) {
    google::protobuf::TextFormat::Printer printer;
    printer.SetUseUtf8StringEscaping(true);
    printer.SetUseShortRepeatedPrimitives(true);

    NProtoBuf::io::TCopyingOutputStreamAdaptor adaptor(&out);

    if (!printer.Print(m, &adaptor)) {
         ythrow yexception() << "SerializeToTextFormatPretty failed on Print";
    }
}

static void ConfigureParser(const EParseFromTextFormatOptions options,
                            NProtoBuf::TextFormat::Parser& p) {
    if (options & EParseFromTextFormatOption::AllowUnknownField) {
        p.AllowUnknownField(true);
    }
}

void ParseFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options, IOutputStream* warningStream) {
    NProtoBuf::io::TCopyingInputStreamAdaptor adaptor(&in);
    NProtoBuf::TextFormat::Parser p;
    ConfigureParser(options, p);

    TStringStream errorLog;
    THolder<TErrorCollector> errorCollector;
    errorCollector = MakeHolder<TErrorCollector>(m, &errorLog, warningStream);
    p.RecordErrorsTo(errorCollector.Get());

    if (!p.Parse(&adaptor, &m)) {
        // remove everything that may have been read
        m.Clear();
        ythrow yexception() << errorLog.Str();
    }
}

void ParseFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options, IOutputStream* warningStream) {
    /* TUnbufferedFileInput is unbuffered, but TCopyingInputStreamAdaptor adds
    * a buffer on top of it. */
    TUnbufferedFileInput stream(fileName);
    ParseFromTextFormat(stream, m, options, warningStream);
}

bool TryParseFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options, IOutputStream* warningStream) {
    try {
        ParseFromTextFormat(fileName, m, options, warningStream);
    } catch (std::exception&) {
        return false;
    }

    return true;
}

bool TryParseFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options, IOutputStream* warningStream) {
    try {
        ParseFromTextFormat(in, m, options, warningStream);
    } catch (std::exception&) {
        return false;
    }

    return true;
}

void MergeFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options) {
    NProtoBuf::io::TCopyingInputStreamAdaptor adaptor(&in);
    NProtoBuf::TextFormat::Parser p;
    ConfigureParser(options, p);
    if (!p.Merge(&adaptor, &m)) {
        ythrow yexception() << "MergeFromTextFormat failed on Merge for " << m.GetTypeName();
    }
}

void MergeFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                         const EParseFromTextFormatOptions options) {
    /* TUnbufferedFileInput is unbuffered, but TCopyingInputStreamAdaptor adds
    * a buffer on top of it. */
    TUnbufferedFileInput stream(fileName);
    MergeFromTextFormat(stream, m, options);
}

bool TryMergeFromTextFormat(const TString& fileName, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options) {
    try {
        MergeFromTextFormat(fileName, m, options);
    } catch (std::exception&) {
        return false;
    }

    return true;
}

bool TryMergeFromTextFormat(IInputStream& in, NProtoBuf::Message& m,
                            const EParseFromTextFormatOptions options) {
    try {
        MergeFromTextFormat(in, m, options);
    } catch (std::exception&) {
        return false;
    }

    return true;
}
