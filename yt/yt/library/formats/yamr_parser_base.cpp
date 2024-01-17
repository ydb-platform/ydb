#include "yamr_parser_base.h"

#include "format.h"

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/helpers.h>

#include <util/string/escape.h>

namespace NYT::NFormats {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TYamrConsumerBase::TYamrConsumerBase(NYson::IYsonConsumer* consumer)
    : Consumer(consumer)
{ }

void TYamrConsumerBase::SwitchTable(i64 tableIndex)
{
    static const TString Key = FormatEnum(EControlAttribute::TableIndex);
    Consumer->OnListItem();
    Consumer->OnBeginAttributes();
    Consumer->OnKeyedItem(Key);
    Consumer->OnInt64Scalar(tableIndex);
    Consumer->OnEndAttributes();
    Consumer->OnEntity();
}

////////////////////////////////////////////////////////////////////////////////

TYamrDelimitedBaseParser::TYamrDelimitedBaseParser(
    IYamrConsumerPtr consumer,
    const TYamrFormatConfigBasePtr& config,
    bool enableKeyEscaping,
    bool enableValueEscaping)
    : Consumer(consumer)
    , Config_(config)
    , State(EState::InsideKey)
    , ExpectingEscapedChar(false)
    , Offset(0)
    , Record(1)
    , BufferPosition(0)
{
    ConfigureEscapeTables(
        config,
        enableKeyEscaping,
        enableValueEscaping,
        false /* escapingForWriter */,
        &KeyEscapeTable_,
        &ValueEscapeTable_);
}

void TYamrDelimitedBaseParser::Read(TStringBuf data)
{
    auto current = data.begin();
    auto end = data.end();
    while (current != end) {
        current = Consume(current, end);
    }
}

void TYamrDelimitedBaseParser::Finish()
{
    if (ExpectingEscapedChar) {
        ThrowIncorrectFormat();
    }
    if (State == EState::InsideKey && !CurrentToken.empty()) {
        ThrowIncorrectFormat();
    }
    if (State == EState::InsideSubkey) {
        ProcessSubkey(CurrentToken);
        ProcessValue("");
    }
    if (State == EState::InsideValue) {
        ProcessValue(CurrentToken);
    }
}

TString TYamrDelimitedBaseParser::GetContext() const
{
    TString result;
    const char* last = ContextBuffer + BufferPosition;
    if (Offset >= ContextBufferSize) {
        result.append(last, ContextBuffer + ContextBufferSize);
    }
    result.append(ContextBuffer, last);
    return result;
}

IAttributeDictionaryPtr TYamrDelimitedBaseParser::GetDebugInfo() const
{
    auto result = CreateEphemeralAttributes();
    result->Set("context", GetContext());
    result->Set("offset", Offset);
    result->Set("record", Record);
    result->Set("state", State);
    return result;
}

void TYamrDelimitedBaseParser::ProcessTableSwitch(TStringBuf tableIndex)
{
    YT_ASSERT(!ExpectingEscapedChar);
    YT_ASSERT(State == EState::InsideKey);
    i64 value;
    try {
        value = FromString<i64>(tableIndex);
    } catch (const std::exception& ex) {
        TString tableIndexString(tableIndex);
        if (tableIndex.size() > ContextBufferSize) {
            tableIndexString = TString(tableIndex.SubStr(0, ContextBufferSize)) + "...truncated...";
        }
        THROW_ERROR_EXCEPTION("YAMR line %Qv cannot be parsed as a table switch; did you forget a record separator?",
            tableIndexString)
            << *GetDebugInfo();
    }
    Consumer->SwitchTable(value);
}

void TYamrDelimitedBaseParser::ProcessKey(TStringBuf key)
{
    YT_ASSERT(!ExpectingEscapedChar);
    YT_ASSERT(State == EState::InsideKey);
    Consumer->ConsumeKey(key);
    State = Config_->HasSubkey ? EState::InsideSubkey : EState::InsideValue;
}

void TYamrDelimitedBaseParser::ProcessSubkey(TStringBuf subkey)
{
    YT_ASSERT(!ExpectingEscapedChar);
    YT_ASSERT(State == EState::InsideSubkey);
    Consumer->ConsumeSubkey(subkey);
    State = EState::InsideValue;
}

void TYamrDelimitedBaseParser::ProcessSubkeyBadFormat(TStringBuf subkey)
{
    YT_ASSERT(!ExpectingEscapedChar);
    YT_ASSERT(State == EState::InsideSubkey);
    Consumer->ConsumeSubkey(subkey);
    Consumer->ConsumeValue("");
    State = EState::InsideKey;
}

void TYamrDelimitedBaseParser::ProcessValue(TStringBuf value)
{
    YT_ASSERT(!ExpectingEscapedChar);
    YT_ASSERT(State == EState::InsideValue);
    Consumer->ConsumeValue(value);
    State = EState::InsideKey;
    Record += 1;
}

const char* TYamrDelimitedBaseParser::ProcessToken(
    void (TYamrDelimitedBaseParser::*processor)(TStringBuf value),
    const char* begin,
    const char* next)
{
    if (CurrentToken.empty()) {
        (this->*processor)(TStringBuf(begin, next));
    } else {
        CurrentToken.append(begin, next);
        (this->*processor)(CurrentToken);
        CurrentToken.clear();
    }

    OnRangeConsumed(next, next + 1);
    return next + 1;
}

const char* TYamrDelimitedBaseParser::FindNext(const char* begin, const char* end, const TEscapeTable& escapeTable)
{
    const char* next = escapeTable.FindNext(begin, end);
    OnRangeConsumed(begin, next);
    return next;
}

const char* TYamrDelimitedBaseParser::Consume(const char* begin, const char* end)
{
    if (ExpectingEscapedChar) {
        // Read and unescape.
        CurrentToken.append(EscapeBackward[static_cast<ui8>(*begin)]);
        ExpectingEscapedChar = false;
        OnRangeConsumed(begin, begin + 1);
        return begin + 1;
    }

    YT_ASSERT(!ExpectingEscapedChar);

    const char* next = FindNext(begin, end, State == EState::InsideValue ? ValueEscapeTable_ : KeyEscapeTable_);
    if (next == end) {
        CurrentToken.append(begin, next);
        if (CurrentToken.length() > MaxRowWeightLimit) {
            THROW_ERROR_EXCEPTION(
                "YAMR token length limit exceeded: %v > %v",
                CurrentToken.length(),
                MaxRowWeightLimit)
                << *GetDebugInfo();
        }
        return end;
    }

    if (*next == Config_->EscapingSymbol) {
        CurrentToken.append(begin, next);
        OnRangeConsumed(next, next + 1);
        ExpectingEscapedChar = true;
        return next + 1;
    }

    switch (State) {
        case EState::InsideKey:
            if (*next == Config_->RecordSeparator) {
                return ProcessToken(&TYamrDelimitedBaseParser::ProcessTableSwitch, begin, next);
            }

            if (*next == Config_->FieldSeparator) {
                return ProcessToken(&TYamrDelimitedBaseParser::ProcessKey, begin, next);
            }
            break;

        case EState::InsideSubkey:
            if (*next == Config_->FieldSeparator) {
                return ProcessToken(&TYamrDelimitedBaseParser::ProcessSubkey, begin, next);
            }

            if (*next == Config_->RecordSeparator) {
                // See yamr_parser_ut.cpp: IncompleteRows() for details.
                return ProcessToken(&TYamrDelimitedBaseParser::ProcessSubkeyBadFormat, begin, next);
            }
            break;

        case EState::InsideValue:
            if (*next == Config_->RecordSeparator) {
                return ProcessToken(&TYamrDelimitedBaseParser::ProcessValue, begin, next);
            }
            break;
    }

    ThrowIncorrectFormat();

    // To suppress warnings.
    YT_ABORT();
}

void TYamrDelimitedBaseParser::ThrowIncorrectFormat() const
{
    THROW_ERROR_EXCEPTION("Unexpected symbol in YAMR row: expected %Qv, found %Qv",
        EscapeC(Config_->FieldSeparator),
        EscapeC(Config_->RecordSeparator))
        << *GetDebugInfo();
}

void TYamrDelimitedBaseParser::OnRangeConsumed(const char* begin, const char* end)
{
    Offset += end - begin;
    auto current = std::max(begin, end - ContextBufferSize);
    for ( ; current < end; ++current) {
        AppendToContextBuffer(*current);
    }
}

void TYamrDelimitedBaseParser::AppendToContextBuffer(char symbol)
{
    ContextBuffer[BufferPosition] = symbol;
    ++BufferPosition;
    if (BufferPosition >= ContextBufferSize) {
        BufferPosition -= ContextBufferSize;
    }
}

////////////////////////////////////////////////////////////////////////////////

TYamrLenvalBaseParser::TYamrLenvalBaseParser(
    IYamrConsumerPtr consumer,
    bool enableSubkey,
    bool enableEom)
    : Consumer(consumer)
    , EnableSubkey(enableSubkey)
    , EnableEom(enableEom)
{ }

void TYamrLenvalBaseParser::Read(TStringBuf data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TYamrLenvalBaseParser::Finish()
{
    if (EnableEom && !MetEom) {
        THROW_ERROR_EXCEPTION("Missing EOM marker in the stream");
    }

    if (State == EState::InsideValue && !ReadingLength && BytesToRead == 0) {
        Consumer->ConsumeValue(CurrentToken);
        return;
    }

    if (!(State == EState::InsideKey && ReadingLength && BytesToRead == 4)) {
        THROW_ERROR_EXCEPTION("Premature end of stream");
    }
}

const char* TYamrLenvalBaseParser::Consume(const char* begin, const char* end)
{
    if (MetEom) {
        THROW_ERROR_EXCEPTION("Garbage after EOM marker");
    }
    if (ReadingLength) {
        return ConsumeLength(begin, end);
    } else {
        return ConsumeData(begin, end);
    }
}

const char* TYamrLenvalBaseParser::ConsumeInt(const char* begin, const char* end, int length)
{
    const char* current = begin;
    while (BytesToRead != 0 && current != end) {
        Union.Bytes[length - BytesToRead] = *current;
        ++current;
        --BytesToRead;
    }
    return current;
}

const char* TYamrLenvalBaseParser::ConsumeLength(const char* begin, const char* end)
{
    YT_ASSERT(ReadingLength);
    const char* next = ConsumeInt(begin, end, 4);

    if (BytesToRead == 0) {
        ReadingLength = false;
        BytesToRead = static_cast<ui32>(Union.Value);
    }

    if (BytesToRead == static_cast<ui32>(-1)) {
        if (State == EState::InsideKey) {
            BytesToRead = 4;
            State = EState::InsideTableSwitch;
        } else {
            THROW_ERROR_EXCEPTION("Unexpected table switch instruction");
        }
    } else if (BytesToRead == static_cast<ui32>(-5)) {
        if (!EnableEom) {
            THROW_ERROR_EXCEPTION("Unexpected EOM marker");
        }
        if (State == EState::InsideKey) {
            BytesToRead = 8;
            State = EState::InsideEom;
        } else {
            THROW_ERROR_EXCEPTION("Unexpected EOM marker");
        }
    }

    if (BytesToRead > MaxRowWeightLimit) {
        THROW_ERROR_EXCEPTION(
            "YAMR lenval length limit exceeded: %v > %v",
            BytesToRead,
            MaxRowWeightLimit);
    }

    return next;
}

const char* TYamrLenvalBaseParser::ConsumeData(const char* begin, const char* end)
{
    if (State == EState::InsideTableSwitch) {
        YT_ASSERT(CurrentToken.empty());
        const char* next = ConsumeInt(begin, end, 4);

        if (BytesToRead == 0) {
            Consumer->SwitchTable(static_cast<ui32>(Union.Value));
            State = EState::InsideKey;
            ReadingLength = true;
            BytesToRead = 4;
        }

        return next;
    } else if (State == EState::InsideEom) {
        const char* next = ConsumeInt(begin, end, 8);

        if (BytesToRead == 0) {
            MetEom = true;
            if (Union.Value != RowCount) {
                THROW_ERROR_EXCEPTION("Row count mismatch")
                    << TErrorAttribute("eom_marker_row_count", Union.Value)
                    << TErrorAttribute("actual_row_count", RowCount);
            }
            State = EState::InsideKey;
            ReadingLength = true;
            BytesToRead = 4;
        }

        return next;
    }

    // Consume ordinary string tokens.
    TStringBuf data;
    const char* current = begin + BytesToRead;

    if (current > end) {
        CurrentToken.append(begin, end);
        BytesToRead -= (end - begin);
        YT_ASSERT(BytesToRead > 0);
        return end;
    }

    if (CurrentToken.empty()) {
        data = TStringBuf(begin, current);
    } else {
        CurrentToken.append(begin, current);
        data = CurrentToken;
    }

    switch (State) {
        case EState::InsideKey:
            ++RowCount;
            Consumer->ConsumeKey(data);
            State = EnableSubkey ? EState::InsideSubkey : EState::InsideValue;
            break;
        case EState::InsideSubkey:
            Consumer->ConsumeSubkey(data);
            State = EState::InsideValue;
            break;
        case EState::InsideValue:
            Consumer->ConsumeValue(data);
            State = EState::InsideKey;
            break;
        default:
            YT_ABORT();
    }

    CurrentToken.clear();
    ReadingLength = true;
    BytesToRead = 4;

    return current;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
