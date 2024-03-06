#ifndef PULL_PARSER_INL_H_
#error "Direct inclusion of this file is not allowed, include pull_parser.h"
// For the sake of sane code completion.
#include "pull_parser.h"
#endif

#include "detail.h"

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/parser_helpers.h>

#include <optional>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonItem::TYsonItem(const TYsonItem& other)
{
    memcpy(this, &other, sizeof(*this));
}

TYsonItem& TYsonItem::operator =(const TYsonItem& other)
{
    memcpy(this, &other, sizeof(*this));
    return *this;
}

TYsonItem TYsonItem::Simple(EYsonItemType type)
{
    TYsonItem result;
    result.Type_ = type;
    return result;
}

TYsonItem TYsonItem::Boolean(bool data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::BooleanValue;
    result.Data_.Boolean = data;
    return result;
}

TYsonItem TYsonItem::Int64(i64 data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::Int64Value;
    result.Data_.Int64 = data;
    return result;
}

TYsonItem TYsonItem::Uint64(ui64 data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::Uint64Value;
    result.Data_.Uint64 = data;
    return result;
}

TYsonItem TYsonItem::Double(double data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::DoubleValue;
    result.Data_.Double = data;
    return result;
}

TYsonItem TYsonItem::String(TStringBuf data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::StringValue;
    result.Data_.String.Ptr = data.data();
    result.Data_.String.Size = data.length();
    return result;
}

EYsonItemType TYsonItem::GetType() const
{
    return Type_;
}

bool TYsonItem::UncheckedAsBoolean() const
{
    YT_ASSERT(GetType() == EYsonItemType::BooleanValue);
    return Data_.Boolean;
}

i64 TYsonItem::UncheckedAsInt64() const
{
    YT_ASSERT(GetType() == EYsonItemType::Int64Value);
    return Data_.Int64;
}

ui64 TYsonItem::UncheckedAsUint64() const
{
    YT_ASSERT(GetType() == EYsonItemType::Uint64Value);
    return Data_.Uint64;
}

double TYsonItem::UncheckedAsDouble() const
{
    YT_ASSERT(GetType() == EYsonItemType::DoubleValue);
    return Data_.Double;
}

TStringBuf TYsonItem::UncheckedAsString() const
{
    YT_ASSERT(GetType() == EYsonItemType::StringValue);
    return TStringBuf(Data_.String.Ptr, Data_.String.Size);
}

template <typename T>
T TYsonItem::UncheckedAs() const
{
    if constexpr (std::is_same_v<T, i64>) {
        return UncheckedAsInt64();
    } else if constexpr (std::is_same_v<T, ui64>) {
        return UncheckedAsUint64();
    } else if constexpr (std::is_same_v<T, double>) {
        return UncheckedAsDouble();
    } else if constexpr (std::is_same_v<T, TStringBuf>) {
        return UncheckedAsString();
    } else if constexpr (std::is_same_v<T, bool>) {
        return UncheckedAsBoolean();
    } else {
        static_assert(TDependentFalse<T>);
    }
}

bool TYsonItem::IsEndOfStream() const
{
    return GetType() == EYsonItemType::EndOfStream;
}

////////////////////////////////////////////////////////////////////////////////

void NDetail::TZeroCopyInputStreamReader::RefreshBlock()
{
    if (RecordingFrom_) {
        RecordOutput_->Write(RecordingFrom_, Current_ - RecordingFrom_);
    }
    TotalReadBlocksSize_ += (Current_ - Begin_);
    size_t size = Reader_->Next(&Begin_);
    Current_ = Begin_;
    End_ = Begin_ + size;
    if (RecordOutput_) {
        RecordingFrom_ = Begin_;
    }
    if (size == 0) {
        Finished_ = true;
    }
}

const char* NDetail::TZeroCopyInputStreamReader::Begin() const
{
    return Begin_;
}

const char* NDetail::TZeroCopyInputStreamReader::Current() const
{
    return Current_;
}

const char* NDetail::TZeroCopyInputStreamReader::End() const
{
    return End_;
}

void NDetail::TZeroCopyInputStreamReader::Advance(size_t bytes)
{
    Current_ += bytes;
}

bool NDetail::TZeroCopyInputStreamReader::IsFinished() const
{
    return Finished_;
}

////////////////////////////////////////////////////////////////////////////////

constexpr char ItemTypeToMarker(EYsonItemType itemType)
{
    switch (itemType) {
        case EYsonItemType::Uint64Value:
            return NDetail::Uint64Marker;
        case EYsonItemType::Int64Value:
            return NDetail::Int64Marker;
        case EYsonItemType::DoubleValue:
            return NDetail::DoubleMarker;
        case EYsonItemType::StringValue:
            return NDetail::StringMarker;
        case EYsonItemType::BeginList:
            return NDetail::BeginListSymbol;
        case EYsonItemType::EndList:
            return NDetail::EndListSymbol;
        case EYsonItemType::EntityValue:
            return NDetail::EntitySymbol;
        default:
            THROW_ERROR_EXCEPTION("Cannot convert item type %Qlv to marker",
                itemType);
    }
}

void TYsonPullParser::MaybeSkipSemicolon()
{
    auto c = Lexer_.GetChar<true>();
    if (c == ';') {
        Lexer_.Advance(1);
        SyntaxChecker_.OnSeparator();
    }
}

template <EYsonItemType ItemType, bool IsOptional>
auto TYsonPullParser::ParseItem() -> std::conditional_t<IsOptional, bool, void>
{
    static constexpr auto Marker = ItemTypeToMarker(ItemType);

    auto parse = [this] {
        MaybeSkipSemicolon();

        auto c = Lexer_.GetChar<false>();
        if (c == Marker) {
            Lexer_.Advance(1);
            if constexpr (ItemType == EYsonItemType::BeginList) {
                SyntaxChecker_.OnBeginList();
            } else if constexpr (ItemType == EYsonItemType::EndList) {
                SyntaxChecker_.OnEndList();
            } else if constexpr (ItemType == EYsonItemType::EntityValue) {
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::EntityValue);
            } else {
                static_assert(ItemType == EYsonItemType::BeginList);
            }
            return true;
        }

        if constexpr (IsOptional) {
            if (c == NDetail::EntitySymbol) {
                Lexer_.Advance(1);
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::EntityValue);
                return false;
            }
        }

        // Slow path.
        auto item = Next();
        if (item.GetType() == ItemType) {
            return true;
        }

        if constexpr (IsOptional) {
            if (item.GetType() == EYsonItemType::EntityValue) {
                return false;
            }
        }

        ThrowUnexpectedTokenException("item", *this, item, ItemType, /*isOptional*/ true);
    };

    auto result = parse();
    if constexpr (IsOptional) {
        return result;
    }
}

void TYsonPullParser::ParseBeginList()
{
    ParseItem<EYsonItemType::BeginList, false>();
}

bool TYsonPullParser::ParseOptionalBeginList()
{
    return ParseItem<EYsonItemType::BeginList, true>();
}

bool TYsonPullParser::IsMarker(char marker)
{
    MaybeSkipSemicolon();

    auto c = Lexer_.GetChar<false>();
    if (c == marker) {
        return true;
    } else {
        while (c == ';' || IsSpace(c)) {
            Lexer_.Advance(1);
            if (c == ';') {
                SyntaxChecker_.OnSeparator();
            }
            c = Lexer_.GetChar<false>();
        }
        return c == marker;
    }
}

bool TYsonPullParser::IsEndList()
{
    return IsMarker(NDetail::EndListSymbol);
}

bool TYsonPullParser::IsEntity()
{
    return IsMarker(NDetail::EntitySymbol);
}

void TYsonPullParser::ParseEndList()
{
    ParseItem<EYsonItemType::EndList, false>();
}

void TYsonPullParser::ParseEntity()
{
    ParseItem<EYsonItemType::EntityValue, false>();
}

template <typename TValue, EYsonItemType ItemType>
TValue TYsonPullParser::ParseTypedValueFallback()
{
    using TNonOptionalValue = typename TOptionalTraits<TValue>::TValue;
    constexpr auto IsOptional = !std::is_same_v<typename TOptionalTraits<TValue>::TValue, TValue>;

    auto item = Next();
    if (item.GetType() == ItemType) {
        return item.UncheckedAs<TNonOptionalValue>();
    }

    if constexpr (IsOptional) {
        if (item.GetType() == EYsonItemType::EntityValue) {
            return std::nullopt;
        }
    }

    ThrowUnexpectedTokenException("value", *this, item, ItemType, IsOptional);
}

template <typename TValue, EYsonItemType ItemType>
TValue TYsonPullParser::ParseTypedValue()
{
    static constexpr auto Marker = ItemTypeToMarker(ItemType);
    using TNonOptionalValue = typename TOptionalTraits<TValue>::TValue;
    constexpr auto IsOptional = !std::is_same_v<typename TOptionalTraits<TValue>::TValue, TValue>;

    auto readBinaryValue = [&] (TLexer& lexer) {
        if constexpr (std::is_same_v<TNonOptionalValue, i64>) {
            SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Int64Value);
            return lexer.ReadBinaryInt64();
        } else if constexpr (std::is_same_v<TNonOptionalValue, ui64>) {
            SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Uint64Value);
            return lexer.ReadBinaryUint64();
        } else if constexpr (std::is_same_v<TNonOptionalValue, double>) {
            SyntaxChecker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
            return lexer.ReadBinaryDouble();
        } else if constexpr (std::is_same_v<TNonOptionalValue, TStringBuf>) {
            SyntaxChecker_.OnString();
            return lexer.ReadBinaryString();
        } else {
            static_assert(TDependentFalse<TNonOptionalValue>);
        }
    };

    MaybeSkipSemicolon();

    auto c = Lexer_.GetChar<false>();
    if (c == Marker) {
        Lexer_.Advance(1);
        auto value = readBinaryValue(Lexer_);
        return value;
    }

    if constexpr (IsOptional) {
        if (c == NDetail::EntitySymbol) {
            Lexer_.Advance(1);
            SyntaxChecker_.OnSimpleNonstring(EYsonItemType::EntityValue);
            return std::nullopt;
        }
    }

    // Slow path.
    return ParseTypedValueFallback<TValue, ItemType>();
}

template <typename TValue, EYsonItemType ItemType>
int TYsonPullParser::ParseVarintToArray(char* out)
{
    static constexpr auto Marker = ItemTypeToMarker(ItemType);
    using TNonOptionalValue = typename TOptionalTraits<TValue>::TValue;
    constexpr auto IsOptional = !std::is_same_v<typename TOptionalTraits<TValue>::TValue, TValue>;

    MaybeSkipSemicolon();

    auto c = Lexer_.GetChar<false>();
    if (c == Marker) {
        Lexer_.Advance(1);
        auto bytesWritten = Lexer_.ReadVarint64ToArray(out);
        SyntaxChecker_.OnSimpleNonstring(ItemType);
        return bytesWritten;
    }

    if constexpr (IsOptional) {
        if (c == NDetail::EntitySymbol) {
            Lexer_.Advance(1);
            SyntaxChecker_.OnSimpleNonstring(EYsonItemType::EntityValue);
            return 0;
        }
    }

    // Slow path.
    auto value = ParseTypedValueFallback<TValue, ItemType>();
    TNonOptionalValue nonOptionalValue;
    if constexpr (IsOptional) {
        if (!value) {
            return 0;
        }
        nonOptionalValue = *value;
    } else {
        nonOptionalValue = value;
    }
    if constexpr (std::is_same_v<TNonOptionalValue, i64>) {
        return WriteVarInt64(out, nonOptionalValue);
    } else {
        static_assert(std::is_same_v<TNonOptionalValue, ui64>);
        return WriteVarUint64(out, nonOptionalValue);
    }
}

ui64 TYsonPullParser::ParseUint64()
{
    return ParseTypedValue<ui64, EYsonItemType::Uint64Value>();
}

std::optional<ui64> TYsonPullParser::ParseOptionalUint64()
{
    return ParseTypedValue<std::optional<ui64>, EYsonItemType::Uint64Value>();
}

int TYsonPullParser::ParseUint64AsVarint(char* out)
{
    return ParseVarintToArray<ui64, EYsonItemType::Uint64Value>(out);
}

int TYsonPullParser::ParseOptionalUint64AsVarint(char* out)
{
    return ParseVarintToArray<std::optional<ui64>, EYsonItemType::Uint64Value>(out);
}

i64 TYsonPullParser::ParseInt64()
{
    return ParseTypedValue<i64, EYsonItemType::Int64Value>();
}

std::optional<i64> TYsonPullParser::ParseOptionalInt64()
{
    return ParseTypedValue<std::optional<i64>, EYsonItemType::Int64Value>();
}

int TYsonPullParser::ParseInt64AsZigzagVarint(char* out)
{
    return ParseVarintToArray<i64, EYsonItemType::Int64Value>(out);
}

int TYsonPullParser::ParseOptionalInt64AsZigzagVarint(char* out)
{
    return ParseVarintToArray<std::optional<i64>, EYsonItemType::Int64Value>(out);
}

double TYsonPullParser::ParseDouble()
{
    return ParseTypedValue<double, EYsonItemType::DoubleValue>();
}

std::optional<double> TYsonPullParser::ParseOptionalDouble()
{
    return ParseTypedValue<std::optional<double>, EYsonItemType::DoubleValue>();
}

TStringBuf TYsonPullParser::ParseString()
{
    return ParseTypedValue<TStringBuf, EYsonItemType::StringValue>();
}

std::optional<TStringBuf> TYsonPullParser::ParseOptionalString()
{
    return ParseTypedValue<std::optional<TStringBuf>, EYsonItemType::StringValue>();
}

bool TYsonPullParser::ParseBoolean()
{
    MaybeSkipSemicolon();

    auto c = Lexer_.GetChar<false>();
    if (c == NDetail::TrueMarker || c == NDetail::FalseMarker) {
        Lexer_.Advance(1);
        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
        return c == NDetail::TrueMarker;
    }
    return ParseTypedValueFallback<bool, EYsonItemType::BooleanValue>();
}

std::optional<bool> TYsonPullParser::ParseOptionalBoolean()
{
    MaybeSkipSemicolon();

    auto c = Lexer_.GetChar<false>();
    if (c == NDetail::TrueMarker || c == NDetail::FalseMarker) {
        Lexer_.Advance(1);
        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
        return c == NDetail::TrueMarker;
    } else if (c == NDetail::EntitySymbol) {
        Lexer_.Advance(1);
        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::EntityValue);
        return std::nullopt;
    }
    return ParseTypedValueFallback<std::optional<bool>, EYsonItemType::BooleanValue>();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TVisitor>
typename TVisitor::TResult TYsonPullParser::NextImpl(TVisitor* visitor)
{
    using namespace NDetail;

    while (true) {
        char ch = Lexer_.GetChar<true>();
        switch (ch) {
            case BeginAttributesSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnAttributesBegin();
                return visitor->OnBeginAttributes();
            case EndAttributesSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnAttributesEnd();
                return visitor->OnEndAttributes();
            case BeginMapSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnBeginMap();
                return visitor->OnBeginMap();
            case EndMapSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnEndMap();
                return visitor->OnEndMap();
            case BeginListSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnBeginList();
                return visitor->OnBeginList();
            case EndListSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnEndList();
                return visitor->OnEndList();
            case '"': {
                Lexer_.Advance(1);
                TStringBuf value = Lexer_.ReadQuotedString();
                SyntaxChecker_.OnString();
                return visitor->OnString(value);
            }
            case StringMarker: {
                Lexer_.Advance(1);
                SyntaxChecker_.OnString();
                TStringBuf value = Lexer_.ReadBinaryString();
                return visitor->OnString(value);
            }
            case Int64Marker: {
                Lexer_.Advance(1);
                i64 value = Lexer_.ReadBinaryInt64();
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Int64Value);
                return visitor->OnInt64(value);
            }
            case Uint64Marker: {
                Lexer_.Advance(1);
                ui64 value = Lexer_.ReadBinaryUint64();
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Uint64Value);
                return visitor->OnUint64(value);
            }
            case DoubleMarker: {
                Lexer_.Advance(1);
                double value = Lexer_.ReadBinaryDouble();
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
                return visitor->OnDouble(value);
            }
            case FalseMarker: {
                Lexer_.Advance(1);
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
                return visitor->OnBoolean(false);
            }
            case TrueMarker: {
                Lexer_.Advance(1);
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
                return visitor->OnBoolean(true);
            }
            case EntitySymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::EntityValue);
                return visitor->OnEntity();
            case EndSymbol:
                SyntaxChecker_.OnFinish();
                return visitor->OnEndOfStream();
            case '%': {
                Lexer_.Advance(1);
                ch = Lexer_.template GetChar<false>();
                if (ch == 't' || ch == 'f') {
                    SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
                    return visitor->OnBoolean(Lexer_.template ReadBoolean<false>());
                } else {
                    SyntaxChecker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
                    return visitor->OnDouble(Lexer_.template ReadNanOrInf<false>());
                }
            }
            case '=':
                SyntaxChecker_.OnEquality();
                Lexer_.Advance(1);
                visitor->OnEquality();
                continue;
            case ';':
                SyntaxChecker_.OnSeparator();
                Lexer_.Advance(1);
                visitor->OnSeparator();
                continue;
            default:
                if (isspace(ch)) {
                    Lexer_.SkipSpaceAndGetChar<true>();
                    continue;
                } else if (isdigit(ch) || ch == '-' || ch == '+') { // case of '+' is handled in AfterPlus state
                    TStringBuf valueBuffer;

                    ENumericResult numericResult = Lexer_.template ReadNumeric<true>(&valueBuffer);
                    if (numericResult == ENumericResult::Double) {
                        double value;
                        try {
                            value = FromString<double>(valueBuffer);
                        } catch (const std::exception& ex) {
                            THROW_ERROR CreateLiteralError(ETokenType::Double, valueBuffer.begin(), valueBuffer.size())
                                << ex;
                        }
                        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
                        return visitor->OnDouble(value);
                    } else if (numericResult == ENumericResult::Int64) {
                        i64 value;
                        try {
                            value = FromString<i64>(valueBuffer);
                        } catch (const std::exception& ex) {
                            THROW_ERROR CreateLiteralError(ETokenType::Int64, valueBuffer.begin(), valueBuffer.size())
                                << ex;
                        }
                        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Int64Value);
                        return visitor->OnInt64(value);
                    } else if (numericResult == ENumericResult::Uint64) {
                        ui64 value;
                        try {
                            value = FromString<ui64>(valueBuffer.SubStr(0, valueBuffer.size() - 1));
                        } catch (const std::exception& ex) {
                            THROW_ERROR CreateLiteralError(ETokenType::Uint64, valueBuffer.begin(), valueBuffer.size())
                                << ex;
                        }
                        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Uint64Value);
                        return visitor->OnUint64(value);
                    }
                } else if (isalpha(ch) || ch == '_') {
                    TStringBuf value = Lexer_.template ReadUnquotedString<true>();
                    SyntaxChecker_.OnString();
                    return visitor->OnString(value);
                } else {
                    THROW_ERROR_EXCEPTION("Unexpected %Qv while parsing node", ch);
                }
        }
    }
}

template <typename TVisitor>
void TYsonPullParser::TraverseComplexValueOrAttributes(TVisitor* visitor, bool stopAfterAttributes)
{
    class TForwardingVisitor
    {
    public:
        using TResult = typename TVisitor::TResult;

    public:
        TForwardingVisitor(TVisitor* visitor)
            : Underlying(visitor)
        { }

        TResult OnBeginAttributes()
        {
            IsAttributes = true;
            IsComposite = true;
            return Underlying->OnBeginAttributes();
        }

        TResult OnEndAttributes()
        {
            return Underlying->OnEndAttributes();
        }

        TResult OnBeginMap()
        {
            IsComposite = true;
            return Underlying->OnBeginMap();
        }

        TResult OnEndMap()
        {
            return Underlying->OnEndMap();
        }

        TResult OnBeginList()
        {
            IsComposite = true;
            return Underlying->OnBeginList();
        }

        TResult OnEndList()
        {
            return Underlying->OnEndList();
        }

        TResult OnString(TStringBuf value)
        {
            return Underlying->OnString(value);
        }

        TResult OnInt64(i64 value)
        {
            return Underlying->OnInt64(value);
        }

        TResult OnUint64(ui64 value)
        {
            return Underlying->OnUint64(value);
        }

        TResult OnDouble(double value)
        {
            return Underlying->OnDouble(value);
        }

        TResult OnBoolean(bool value)
        {
            return Underlying->OnBoolean(value);
        }

        TResult OnEntity()
        {
            return Underlying->OnEntity();
        }

        TResult OnEndOfStream()
        {
            return Underlying->OnEndOfStream();
        }

        TResult OnEquality()
        {
            return Underlying->OnEquality();
        }

        TResult OnSeparator()
        {
            return Underlying->OnSeparator();
        }

    public:
        TVisitor* const Underlying;
        bool IsAttributes = false;
        bool IsComposite = false;
    };

    TForwardingVisitor forwardingVisitor(visitor);
    NextImpl(&forwardingVisitor);

    if (!forwardingVisitor.IsComposite) {
        return;
    }

    const auto nestingLevel = GetNestingLevel();
    while (GetNestingLevel() >= nestingLevel) {
        NextImpl(visitor);
    }

    if (!stopAfterAttributes && forwardingVisitor.IsAttributes) {
        TraverseComplexValueOrAttributes(visitor, stopAfterAttributes);
    }
}

template <typename TVisitor>
void TYsonPullParser::TraverseComplexValueOrAttributes(
    TVisitor* visitor,
    const TYsonItem& previousItem,
    bool stopAfterAttributes)
{
    auto traverse = [&] {
        const auto nestingLevel = GetNestingLevel();
        while (GetNestingLevel() >= nestingLevel) {
            NextImpl(visitor);
        }
    };

    switch (previousItem.GetType()) {
        case EYsonItemType::BeginAttributes:
            visitor->OnBeginAttributes();
            traverse();
            if (!stopAfterAttributes) {
                TraverseComplexValueOrAttributes(visitor, stopAfterAttributes);
            }
            return;
        case EYsonItemType::BeginList:
            visitor->OnBeginList();
            traverse();
            return;
        case EYsonItemType::BeginMap:
            visitor->OnBeginMap();
            traverse();
            return;

        case EYsonItemType::EntityValue:
            visitor->OnEntity();
            return;
        case EYsonItemType::BooleanValue:
            visitor->OnBoolean(previousItem.UncheckedAsBoolean());
            return;
        case EYsonItemType::Int64Value:
            visitor->OnInt64(previousItem.UncheckedAsInt64());
            return;
        case EYsonItemType::Uint64Value:
            visitor->OnUint64(previousItem.UncheckedAsUint64());
            return;
        case EYsonItemType::DoubleValue:
            visitor->OnDouble(previousItem.UncheckedAsDouble());
            return;
        case EYsonItemType::StringValue:
            visitor->OnString(previousItem.UncheckedAsString());
            return;

        case EYsonItemType::EndOfStream:
        case EYsonItemType::EndAttributes:
        case EYsonItemType::EndMap:
        case EYsonItemType::EndList:
            YT_ABORT();
    }
    YT_ABORT();
}

size_t TYsonPullParser::GetNestingLevel() const
{
    return SyntaxChecker_.GetNestingLevel();
}

bool TYsonPullParser::IsOnValueBoundary(size_t nestingLevel) const
{
    return SyntaxChecker_.IsOnValueBoundary(nestingLevel);
}

////////////////////////////////////////////////////////////////////////////////

TYsonPullParserCursor::TYsonPullParserCursor(TYsonPullParser* parser)
    : IsOnFragmentStart_(
        parser->GetYsonType() != EYsonType::Node &&
        parser->GetTotalReadSize() == 0)
    , Current_(parser->Next())
    , Parser_(parser)
{ }

const TYsonItem& TYsonPullParserCursor::GetCurrent() const
{
    return Current_;
}

const TYsonItem* TYsonPullParserCursor::operator->() const
{
    return &GetCurrent();
}

const TYsonItem& TYsonPullParserCursor::operator*() const
{
    return GetCurrent();
}

void TYsonPullParserCursor::Next()
{
    // NB(levysotsky): Hand-crafted YT_ASSERT to avoid
    // stack frame bloating due to Y_FORCE_INLINE.
#ifndef NDEBUG
    if (IsOnFragmentStart_) {
        FailAsTryConsumeFragmentStartNotCalled();
    }
#endif
    Current_ = Parser_->Next();
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename TFunction, EYsonItemType BeginItemType, EYsonItemType EndItemType>
void ParseComposite(TYsonPullParserCursor* cursor, TFunction function)
{
    if constexpr (BeginItemType == EYsonItemType::BeginAttributes) {
        EnsureYsonToken("attributes", *cursor, BeginItemType);
    } else if constexpr (BeginItemType == EYsonItemType::BeginList) {
        EnsureYsonToken("list", *cursor, BeginItemType);
    } else if constexpr (BeginItemType == EYsonItemType::BeginMap) {
        EnsureYsonToken("map", *cursor, BeginItemType);
    } else {
        static_assert(BeginItemType == EYsonItemType::BeginAttributes, "unexpected item type");
    }

    cursor->Next();
    while ((*cursor)->GetType() != EndItemType) {
        function(cursor);
    }
    cursor->Next();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
void TYsonPullParserCursor::ParseMap(TFunction function)
{
    NDetail::ParseComposite<TFunction, EYsonItemType::BeginMap, EYsonItemType::EndMap>(this, function);
}

template <typename TFunction>
void TYsonPullParserCursor::ParseList(TFunction function)
{
    NDetail::ParseComposite<TFunction, EYsonItemType::BeginList, EYsonItemType::EndList>(this, function);
}

template <typename TFunction>
void TYsonPullParserCursor::ParseAttributes(TFunction function)
{
    NDetail::ParseComposite<TFunction, EYsonItemType::BeginAttributes, EYsonItemType::EndAttributes>(this, function);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void EnsureYsonToken(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    EYsonItemType expected)
{
    if (expected != cursor->GetType()) {
        ThrowUnexpectedYsonTokenException(description, cursor, {expected});
    }
}

Y_FORCE_INLINE void EnsureYsonToken(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    EYsonItemType expected)
{
    if (expected != item.GetType()) {
        ThrowUnexpectedYsonTokenException(description, parser, item, {expected});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
