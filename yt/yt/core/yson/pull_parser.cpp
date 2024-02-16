#include "pull_parser.h"
#include "consumer.h"
#include "token_writer.h"

#include <util/stream/output.h>

namespace NYT::NYson {

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TZeroCopyInputStreamReader::TZeroCopyInputStreamReader(IZeroCopyInput* reader)
    : Reader_(reader)
{ }

size_t TZeroCopyInputStreamReader::GetTotalReadSize() const
{
    return TotalReadBlocksSize_ + (Current_ - Begin_);
}

void NDetail::TZeroCopyInputStreamReader::StartRecording(IOutputStream* out)
{
    YT_VERIFY(!RecordOutput_);
    RecordOutput_ = out;
    RecordingFrom_ = Current_;
}

void NDetail::TZeroCopyInputStreamReader::CancelRecording()
{
    YT_VERIFY(RecordOutput_);
    RecordOutput_ = nullptr;
    RecordingFrom_ = nullptr;
}

void NDetail::TZeroCopyInputStreamReader::FinishRecording()
{
    YT_VERIFY(RecordOutput_);
    if (RecordingFrom_) {
        RecordOutput_->Write(RecordingFrom_, Current_ - RecordingFrom_);
    }

    RecordOutput_ = nullptr;
    RecordingFrom_ = nullptr;
}

////////////////////////////////////////////////////////////////////////////////

class TYsonItemCreatingVisitor
{
public:
    using TResult = TYsonItem;

public:
    TYsonItem OnBeginAttributes()
    {
        return TYsonItem::Simple(EYsonItemType::BeginAttributes);
    }

    TYsonItem OnEndAttributes()
    {
        return TYsonItem::Simple(EYsonItemType::EndAttributes);
    }

    TYsonItem OnBeginMap()
    {
        return TYsonItem::Simple(EYsonItemType::BeginMap);
    }

    TYsonItem OnEndMap()
    {
        return TYsonItem::Simple(EYsonItemType::EndMap);
    }

    TYsonItem OnBeginList()
    {
        return TYsonItem::Simple(EYsonItemType::BeginList);
    }

    TYsonItem OnEndList()
    {
        return TYsonItem::Simple(EYsonItemType::EndList);
    }

    TYsonItem OnString(TStringBuf value)
    {
        return TYsonItem::String(value);
    }

    TYsonItem OnInt64(i64 value)
    {
        return TYsonItem::Int64(value);
    }

    TYsonItem OnUint64(ui64 value)
    {
        return TYsonItem::Uint64(value);
    }

    TYsonItem OnDouble(double value)
    {
        return TYsonItem::Double(value);
    }

    TYsonItem OnBoolean(bool value)
    {
        return TYsonItem::Boolean(value);
    }

    TYsonItem OnEntity()
    {
        return TYsonItem::Simple(EYsonItemType::EntityValue);
    }

    TYsonItem OnEndOfStream()
    {
        return TYsonItem::Simple(EYsonItemType::EndOfStream);
    }

    void OnEquality()
    { }

    void OnSeparator()
    { }
};

class TNullVisitor
{
public:
    using TResult = void;

public:
    void OnBeginAttributes()
    { }

    void OnEndAttributes()
    { }

    void OnBeginMap()
    { }

    void OnEndMap()
    { }

    void OnBeginList()
    { }

    void OnEndList()
    { }

    void OnString(TStringBuf /*value*/)
    { }

    void OnInt64(i64 /*value*/)
    { }

    void OnUint64(ui64 /*value*/)
    { }

    void OnDouble(double /*value*/)
    { }

    void OnBoolean(bool /*value*/)
    { }

    void OnEntity()
    { }

    void OnEndOfStream()
    { }

    void OnEquality()
    { }

    void OnSeparator()
    { }
};

class TYsonTokenWritingVisitor
{
public:
    using TResult = void;

public:
    TYsonTokenWritingVisitor(TCheckedInDebugYsonTokenWriter* writer)
        : Writer_(writer)
    { }

    void OnBeginAttributes()
    {
        Writer_->WriteBeginAttributes();
    }

    void OnEndAttributes()
    {
        Writer_->WriteEndAttributes();
    }

    void OnBeginMap()
    {
        Writer_->WriteBeginMap();
    }

    void OnEndMap()
    {
        Writer_->WriteEndMap();
    }

    void OnBeginList()
    {
        Writer_->WriteBeginList();
    }

    void OnEndList()
    {
        Writer_->WriteEndList();
    }

    void OnString(TStringBuf value)
    {
        Writer_->WriteBinaryString(value);
    }

    void OnInt64(i64 value)
    {
        Writer_->WriteBinaryInt64(value);
    }

    void OnUint64(ui64 value)
    {
        Writer_->WriteBinaryUint64(value);
    }

    void OnDouble(double value)
    {
        Writer_->WriteBinaryDouble(value);
    }

    void OnBoolean(bool value)
    {
        Writer_->WriteBinaryBoolean(value);
    }

    void OnEntity()
    {
        Writer_->WriteEntity();
    }

    void OnEndOfStream()
    { }

    void OnEquality()
    {
        Writer_->WriteKeyValueSeparator();
    }

    void OnSeparator()
    {
        Writer_->WriteItemSeparator();
    }

private:
    TCheckedInDebugYsonTokenWriter* const Writer_;
};

class TYsonConsumerVisitor
{
public:
    using TResult = void;

public:
    TYsonConsumerVisitor(const TYsonSyntaxChecker& syntaxChecker, IYsonConsumer* consumer)
        : SyntaxChecker_(syntaxChecker)
        , Consumer_(consumer)
    { }

    void OnBeginAttributes()
    {
        MaybeEmitOnListItem(-1);
        Consumer_->OnBeginAttributes();
    }

    void OnEndAttributes()
    {
        Consumer_->OnEndAttributes();
    }

    void OnBeginMap()
    {
        MaybeEmitOnListItem(-1);
        Consumer_->OnBeginMap();
    }

    void OnEndMap()
    {
        Consumer_->OnEndMap();
    }

    void OnBeginList()
    {
        MaybeEmitOnListItem(-1);
        SetWaitingForListItem(true, 0);
        Consumer_->OnBeginList();
    }

    void OnEndList()
    {
        SetWaitingForListItem(false, +1);
        Consumer_->OnEndList();
    }

    void OnString(TStringBuf value)
    {
        if (SyntaxChecker_.IsOnKey()) {
            Consumer_->OnKeyedItem(value);
        } else {
            MaybeEmitOnListItem(0);
            Consumer_->OnStringScalar(value);
        }
    }

    void OnInt64(i64 value)
    {
        MaybeEmitOnListItem(0);
        Consumer_->OnInt64Scalar(value);
    }

    void OnUint64(ui64 value)
    {
        MaybeEmitOnListItem(0);
        Consumer_->OnUint64Scalar(value);
    }

    void OnDouble(double value)
    {
        MaybeEmitOnListItem(0);
        Consumer_->OnDoubleScalar(value);
    }

    void OnBoolean(bool value)
    {
        MaybeEmitOnListItem(0);
        Consumer_->OnBooleanScalar(value);
    }

    void OnEntity()
    {
        MaybeEmitOnListItem(0);
        Consumer_->OnEntity();
    }

    void OnEndOfStream()
    { }

    void OnEquality()
    { }

    void OnSeparator()
    {
        if (SyntaxChecker_.IsListSeparator()) {
            SetWaitingForListItem(true, 0);
        }
    }

private:
    void MaybeEmitOnListItem(int nestingLevelDelta)
    {
        auto wasWaiting = SetWaitingForListItem(false, nestingLevelDelta);
        if (wasWaiting) {
            Consumer_->OnListItem();
        }
    }

    bool SetWaitingForListItem(bool value, int nestingLevelDelta)
    {
        auto nestingLevel = static_cast<int>(SyntaxChecker_.GetNestingLevel()) + nestingLevelDelta;
        if (nestingLevel >= std::ssize(WaitingForListItem_)) {
            WaitingForListItem_.resize(std::max<int>(
                nestingLevel + 1,
                2 * std::ssize(WaitingForListItem_)));
        }
        return std::exchange(WaitingForListItem_[nestingLevel], value);
    }

private:
    const TYsonSyntaxChecker& SyntaxChecker_;
    IYsonConsumer* const Consumer_;
    TCompactVector<bool, 16> WaitingForListItem_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

bool operator==(TYsonItem lhs, TYsonItem rhs)
{
    if (lhs.GetType() != rhs.GetType()) {
        return false;
    }
    switch (lhs.GetType()) {
        case EYsonItemType::BooleanValue:
            return lhs.UncheckedAsBoolean() == rhs.UncheckedAsBoolean();
        case EYsonItemType::Int64Value:
            return lhs.UncheckedAsInt64() == rhs.UncheckedAsInt64();
        case EYsonItemType::Uint64Value:
            return lhs.UncheckedAsUint64() == rhs.UncheckedAsUint64();
        case EYsonItemType::DoubleValue:
            return lhs.UncheckedAsDouble() == rhs.UncheckedAsDouble();
        case EYsonItemType::StringValue:
            return lhs.UncheckedAsString() == rhs.UncheckedAsString();
        default:
            return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonPullParser::TYsonPullParser(IZeroCopyInput* input, EYsonType ysonType, int nestingLevelLimit)
    : StreamReader_(input)
    , Lexer_(StreamReader_)
    , SyntaxChecker_(ysonType, nestingLevelLimit)
    , YsonType_(ysonType)
{ }

size_t TYsonPullParser::GetTotalReadSize() const
{
    return Lexer_.GetTotalReadSize();
}

TYsonItem TYsonPullParser::Next()
{
    try {
        Lexer_.CheckpointContext();
        auto visitor = NDetail::TYsonItemCreatingVisitor();
        return NextImpl(&visitor);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error occurred while parsing YSON")
            << GetErrorAttributes()
            << ex;
    }
}

void TYsonPullParser::StartRecording(IOutputStream* out)
{
    Lexer_.StartRecording(out);
}

void TYsonPullParser::CancelRecording()
{
    Lexer_.CancelRecording();
}

void TYsonPullParser::FinishRecording()
{
    Lexer_.FinishRecording();
}

EYsonType TYsonPullParser::GetYsonType() const
{
    return YsonType_;
}

void TYsonPullParser::SkipComplexValue()
{
    auto visitor = NDetail::TNullVisitor();
    TraverseComplexValueOrAttributes(&visitor, /*stopAfterAttributes*/ false);
}

void TYsonPullParser::TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer)
{
    auto visitor = NDetail::TYsonTokenWritingVisitor(writer);
    TraverseComplexValueOrAttributes(&visitor, /*stopAfterAttributes*/ false);
}

void TYsonPullParser::SkipComplexValue(const TYsonItem& previousItem)
{
    auto visitor = NDetail::TNullVisitor();
    TraverseComplexValueOrAttributes(&visitor, previousItem, /*stopAfterAttributes*/ false);
}

void TYsonPullParser::TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer, const TYsonItem& previousItem)
{
    auto visitor = NDetail::TYsonTokenWritingVisitor(writer);
    TraverseComplexValueOrAttributes(
        &visitor,
        previousItem,
        /*stopAfterAttributes*/ false);
}

void TYsonPullParser::TransferComplexValue(IYsonConsumer* consumer, const TYsonItem& previousItem)
{
    auto visitor = NDetail::TYsonConsumerVisitor(SyntaxChecker_, consumer);
    TraverseComplexValueOrAttributes(
        &visitor,
        previousItem,
        /*stopAfterAttributes*/ false);
}


void TYsonPullParser::SkipAttributes(const TYsonItem& previousItem)
{
    EnsureYsonToken("attributes", *this, previousItem, EYsonItemType::BeginAttributes);
    auto visitor = NDetail::TNullVisitor();
    TraverseComplexValueOrAttributes(&visitor, previousItem, /*stopAfterAttributes*/ true);
}

void TYsonPullParser::SkipComplexValueOrAttributes(const TYsonItem& previousItem)
{
    auto visitor = NDetail::TNullVisitor();
    TraverseComplexValueOrAttributes(&visitor, previousItem, /*stopAfterAttributes*/ true);
}

void TYsonPullParser::TransferAttributes(TCheckedInDebugYsonTokenWriter* writer, const TYsonItem& previousItem)
{
    EnsureYsonToken("attributes", *this, previousItem, EYsonItemType::BeginAttributes);
    auto visitor = NDetail::TYsonTokenWritingVisitor(writer);
    TraverseComplexValueOrAttributes(
        &visitor,
        previousItem,
        /*stopAfterAttributes*/ true);
}

void TYsonPullParser::TransferAttributes(IYsonConsumer* consumer, const TYsonItem& previousItem)
{
    EnsureYsonToken("attributes", *this, previousItem, EYsonItemType::BeginAttributes);
    auto visitor = NDetail::TYsonConsumerVisitor(SyntaxChecker_, consumer);
    TraverseComplexValueOrAttributes(
        &visitor,
        previousItem,
        /*stopAfterAttributes*/ true);
}

std::vector<TErrorAttribute> TYsonPullParser::GetErrorAttributes() const
{
    auto result = Lexer_.GetErrorAttributes();
    auto [context, contextPosition] = Lexer_.GetContextFromCheckpoint();

    TStringStream markedContext;
    markedContext
        << EscapeC(context.substr(0, contextPosition))
        << "  ERROR>>>  "
        << EscapeC(context.substr(contextPosition));

    result.emplace_back("context", EscapeC(context));
    result.emplace_back("context_pos", contextPosition);
    result.emplace_back("marked_context", markedContext.Str());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TYsonPullParserCursor::StartRecording(IOutputStream* out)
{
    Parser_->StartRecording(out);
}

void TYsonPullParserCursor::CancelRecording()
{
    Parser_->CancelRecording();
}

void TYsonPullParserCursor::SkipComplexValueAndFinishRecording()
{
    Parser_->SkipComplexValue(Current_);
    Parser_->FinishRecording();
    Current_ = Parser_->Next();
}

std::vector<TErrorAttribute> TYsonPullParserCursor::GetErrorAttributes() const
{
    return Parser_->GetErrorAttributes();
}

void TYsonPullParserCursor::SkipComplexValue()
{
    Parser_->SkipComplexValue(Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::TransferComplexValue(NYT::NYson::IYsonConsumer* consumer)
{
    Parser_->TransferComplexValue(consumer, Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::TransferComplexValue(NYT::NYson::TCheckedInDebugYsonTokenWriter* writer)
{
    Parser_->TransferComplexValue(writer, Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::SkipAttributes()
{
    Parser_->SkipAttributes(Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::TransferAttributes(NYT::NYson::IYsonConsumer* consumer)
{
    Parser_->TransferAttributes(consumer, Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::TransferAttributes(NYT::NYson::TCheckedInDebugYsonTokenWriter* writer)
{
    Parser_->TransferAttributes(writer, Current_);
    Current_ = Parser_->Next();
}

bool TYsonPullParserCursor::TryConsumeFragmentStart()
{
    auto result = IsOnFragmentStart_;
    IsOnFragmentStart_ = false;
    return result;
}

[[noreturn]] void TYsonPullParserCursor::FailAsTryConsumeFragmentStartNotCalled()
{
    Y_ABORT("TryConsumeFragmentStart must be called before anything else when parsing list or map fragment");
}

////////////////////////////////////////////////////////////////////////////////

TString CreateExpectedItemTypesString(const std::vector<EYsonItemType>& expected)
{
    YT_VERIFY(!expected.empty());
    if (expected.size() > 1) {
        TStringStream out;
        out << "one of the tokens {";
        for (const auto& token : expected) {
            out << Format("%Qlv, ", token);
        }
        out << "}";
        return out.Str();
    } else {
        return Format("%Qlv", expected[0]);
    }
}

void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    const std::vector<EYsonItemType>& expected)
{
    THROW_ERROR_EXCEPTION("Cannot parse %Qv: expected %v, actual %Qlv",
        description,
        CreateExpectedItemTypesString(expected),
        item.GetType())
        << parser.GetErrorAttributes();
}

void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    const std::vector<EYsonItemType>& expected)
{
    THROW_ERROR_EXCEPTION("Cannot parse %Qv; expected %v, actual %Qlv",
        description,
        CreateExpectedItemTypesString(expected),
        cursor->GetType())
        << cursor.GetErrorAttributes();
}

void ThrowUnexpectedTokenException(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    EYsonItemType expected,
    bool optional)
{
    std::vector<EYsonItemType> allExpected = {expected};
    if (optional) {
        allExpected.push_back(EYsonItemType::EntityValue);
    }

    auto fullDescription = TString(optional ? "optional " : "") + description;
    ThrowUnexpectedYsonTokenException(
        fullDescription,
        parser,
        item,
        allExpected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
