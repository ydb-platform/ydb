#include "dsv_parser.h"

#include "format.h"
#include "escape.h"

#include <yt/yt/client/formats/parser.h>

namespace NYT::NFormats {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDsvParserState,
    (InsidePrefix)
    (InsideKey)
    (InsideValue)
);

class TDsvParser
    : public IParser
{
public:
    TDsvParser(
        IYsonConsumer* consumer,
        TDsvFormatConfigPtr config,
        bool wrapWithmap);

    void Read(TStringBuf data) override;
    void Finish() override;

private:
    IYsonConsumer* Consumer;
    TDsvFormatConfigPtr Config;
    bool WrapWithMap; // This is actually used to represent "embedded" semantics.
    char LastCharacter; // This is used to verify record separator presence.

    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;

    bool NewRecordStarted;
    bool ExpectingEscapedChar;

    int RecordCount;
    int FieldCount;

    TString CurrentToken;

    const char* Consume(const char* begin, const char* end);

    void StartRecordIfNeeded();
    void FinishRecord();

    void ValidatePrefix(const TString& prefix) const;

    using EState = EDsvParserState;
    EState State;

    EState GetStartState() const;
};

////////////////////////////////////////////////////////////////////////////////

TDsvParser::TDsvParser(
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config,
    bool wrapWithMap)
    : Consumer(consumer)
    , Config(config)
    , WrapWithMap(wrapWithMap)
    , LastCharacter(Config->RecordSeparator)
    , NewRecordStarted(!wrapWithMap)
    , ExpectingEscapedChar(false)
    , RecordCount(1)
    , FieldCount(1)
    , State(GetStartState())
{
    ConfigureEscapeTables(config, false /* addCarriageReturn */, &KeyEscapeTable_, &ValueEscapeTable_);
}

void TDsvParser::Read(TStringBuf data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TDsvParser::Finish()
{
    if (ExpectingEscapedChar) {
        THROW_ERROR_EXCEPTION("Incomplete escape sequence in DSV");
    }
    if (WrapWithMap && LastCharacter != Config->RecordSeparator) {
        THROW_ERROR_EXCEPTION("Expected record to be terminated with record separator");
    }
    if (!CurrentToken.empty()) {
        StartRecordIfNeeded();
    }
    if (State == EState::InsideValue) {
        Consumer->OnStringScalar(CurrentToken);
    }
    if (State == EState::InsidePrefix && !CurrentToken.empty()) {
        ValidatePrefix(CurrentToken);
    }
    CurrentToken.clear();
    FinishRecord();
}

const char* TDsvParser::Consume(const char* begin, const char* end)
{
    if (end - begin > 0) {
        LastCharacter = *(end - 1);
    }
    // Process escaping symbols.
    if (Config->EnableEscaping && !ExpectingEscapedChar && *begin == Config->EscapingSymbol) {
        ExpectingEscapedChar = true;
        return begin + 1;
    }
    if (ExpectingEscapedChar) {
        CurrentToken.append(EscapeBackward[static_cast<ui8>(*begin)]);
        ExpectingEscapedChar = false;
        return begin + 1;
    }

    // Read until first stop symbol.
    auto next = State == EState::InsideKey
        ? KeyEscapeTable_.FindNext(begin, end)
        : ValueEscapeTable_.FindNext(begin, end);
    CurrentToken.append(begin, next);
    if (next == end || (Config->EnableEscaping && *next == Config->EscapingSymbol)) {
        return next;
    }

    if (*next == '\0') {
        THROW_ERROR_EXCEPTION("Unescaped \\0 symbol in DSV")
            << TErrorAttribute("record_index", RecordCount)
            << TErrorAttribute("field_index", FieldCount);
    }

    // Here, we have finished reading prefix, key or value
    if (State == EState::InsidePrefix) {
        StartRecordIfNeeded();
        ValidatePrefix(CurrentToken);
        State = EState::InsideKey;
    } else if (State == EState::InsideKey) {
        StartRecordIfNeeded();
        if (*next == Config->KeyValueSeparator) {
            Consumer->OnKeyedItem(CurrentToken);
            State = EState::InsideValue;
        }
    } else if (State == EState::InsideValue) {
        Consumer->OnStringScalar(CurrentToken);
        State = EState::InsideKey;
        FieldCount += 1;
    } else {
        YT_ABORT();
    }

    CurrentToken.clear();
    if (*next == Config->RecordSeparator) {
        FinishRecord();
    }
    return next + 1;
}

TDsvParser::EState TDsvParser::GetStartState() const
{
    return Config->LinePrefix ? EState::InsidePrefix : EState::InsideKey;
}

void TDsvParser::StartRecordIfNeeded()
{
    if (!NewRecordStarted) {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        NewRecordStarted = true;
    }
}

void TDsvParser::FinishRecord()
{
    if (WrapWithMap && NewRecordStarted) {
        Consumer->OnEndMap();
        NewRecordStarted = false;
    }
    State = GetStartState();

    RecordCount += 1;
    FieldCount = 1;
}

void TDsvParser::ValidatePrefix(const TString& prefix) const
{
    if (prefix != *Config->LinePrefix) {
        // TODO(babenko): provide position
        THROW_ERROR_EXCEPTION("Malformed line prefix in DSV: expected %Qv, found %Qv",
            *Config->LinePrefix,
            prefix)
            << TErrorAttribute("record_index", RecordCount)
            << TErrorAttribute("field_index", FieldCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ParseDsv(
    IInputStream* input,
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config)
{
    auto parser = CreateParserForDsv(consumer, config);
    Parse(input, parser.get());
}

void ParseDsv(
    TStringBuf data,
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config)
{
    auto parser = CreateParserForDsv(consumer, config);
    parser->Read(data);
    parser->Finish();
}

std::unique_ptr<IParser> CreateParserForDsv(
    IYsonConsumer* consumer,
    TDsvFormatConfigPtr config,
    bool wrapWithMap)
{
    return std::make_unique<TDsvParser>(consumer, config, wrapWithMap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
