#pragma once

#include <yt/yt/client/formats/parser.h>

#include "escape.h"

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

struct IYamrConsumer
    : public virtual TRefCounted
{
    virtual void ConsumeKey(TStringBuf key) = 0;
    virtual void ConsumeSubkey(TStringBuf subkey) = 0;
    virtual void ConsumeValue(TStringBuf value) = 0;
    virtual void SwitchTable(i64 tableIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYamrConsumer)

////////////////////////////////////////////////////////////////////////////////

class TYamrConsumerBase
    : public IYamrConsumer
{
public:
    explicit TYamrConsumerBase(NYson::IYsonConsumer* consumer);
    virtual void SwitchTable(i64 tableIndex) override;

protected:
    NYson::IYsonConsumer* Consumer;

};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYamrDelimitedBaseParserState,
    (InsideKey)
    (InsideSubkey)
    (InsideValue)
);

class TYamrDelimitedBaseParser
    : public IParser
{
public:
    TYamrDelimitedBaseParser(
        IYamrConsumerPtr consumer,
        const TYamrFormatConfigBasePtr& config,
        bool enableKeyEscaping,
        bool enableValueEscaping);

    virtual void Read(TStringBuf data) override;
    virtual void Finish() override;

private:
    using EState = EYamrDelimitedBaseParserState;

    const char* Consume(const char* begin, const char* end);

    void ProcessKey(TStringBuf key);
    void ProcessSubkey(TStringBuf subkey);
    void ProcessSubkeyBadFormat(TStringBuf subkey);
    void ProcessValue(TStringBuf value);
    void ProcessTableSwitch(TStringBuf tableIndex);

    const char* ProcessToken(
        void (TYamrDelimitedBaseParser::*processor)(TStringBuf value),
        const char* begin,
        const char* next);

    const char* FindNext(const char* begin, const char* end, const TEscapeTable& escapeTable);

    void ThrowIncorrectFormat() const;

    void OnRangeConsumed(const char* begin, const char* end);
    void AppendToContextBuffer(char symbol);

    TString GetContext() const;
    NYTree::IAttributeDictionaryPtr GetDebugInfo() const;

    IYamrConsumerPtr Consumer;

    TYamrFormatConfigBasePtr Config_;

    EState State;

    bool ExpectingEscapedChar;

    TString CurrentToken;

    // Diagnostic Info
    i64 Offset;
    i64 Record;
    i32 BufferPosition;

    static const int ContextBufferSize = 64;
    char ContextBuffer[ContextBufferSize];

    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYamrLenvalBaseParserState,
    (InsideTableSwitch)
    (InsideKey)
    (InsideSubkey)
    (InsideValue)
    (InsideEom)
);

class TYamrLenvalBaseParser
    : public IParser
{
public:
    TYamrLenvalBaseParser(
        IYamrConsumerPtr consumer,
        bool enableSubkey,
        bool enableEom);

    virtual void Read(TStringBuf data) override;
    virtual void Finish() override;

private:
    using EState = EYamrLenvalBaseParserState;

    const char* Consume(const char* begin, const char* end);
    const char* ConsumeInt(const char* begin, const char* end, int length);
    const char* ConsumeLength(const char* begin, const char* end);
    const char* ConsumeData(const char* begin, const char* end);

    IYamrConsumerPtr Consumer;

    bool EnableSubkey;

    TString CurrentToken;

    union {
        ui64 Value;
        char Bytes[8];
    } Union;

    bool ReadingLength = true;
    ui32 BytesToRead = 4;

    EState State = EState::InsideKey;

    bool EnableEom;
    bool MetEom = false;
    ui64 RowCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
