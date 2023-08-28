#pragma once

#include "detail.h"

// For the sake of sane code completion.
#include "parser.h"

#include <yt/yt/core/misc/finally.h>

#include <util/string/escape.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

template <class TConsumer, class TBlockStream, size_t MaxContextSize, bool EnableLinePositionInfo>
class TParser
    : public TLexerBase<TReaderWithContext<TBlockStream, MaxContextSize>, EnableLinePositionInfo>
{
private:
    using TBase = TLexerBase<TReaderWithContext<TBlockStream, MaxContextSize>, EnableLinePositionInfo>;

    TConsumer* const Consumer_;
    const int NestingLevelLimit_;

    int NestingLevel_ = 0;
    bool Stopped_ = false;

public:
    TParser(
        const TBlockStream& blockStream,
        TConsumer* consumer,
        i64 memoryLimit,
        int nestingLevelLimit)
        : TBase(blockStream, memoryLimit)
        , Consumer_(consumer)
        , NestingLevelLimit_(nestingLevelLimit)
    { }

    void DoParse(EYsonType parsingMode)
    {
        Stopped_ = false;
        try {
            switch (parsingMode) {
                case EYsonType::Node:
                    ParseNode<true>();
                    break;

                case EYsonType::ListFragment:
                    ParseListFragment<true>(EndSymbol);
                    break;

                case EYsonType::MapFragment:
                    ParseMapFragment<true>(EndSymbol);
                    break;

                default:
                    YT_ABORT();
            }

            if (Stopped_) {
                return;
            }

            while (!(TBase::IsFinished() && TBase::IsEmpty())) {
                if (TBase::template SkipSpaceAndGetChar<true>() != EndSymbol) {
                    auto character = *TBase::Current();
                    if (character == ItemSeparatorSymbol) {
                        THROW_ERROR_EXCEPTION("Stray %Qv found; maybe you should use yson_type = %Qlv",
                            character,
                            EYsonType::ListFragment)
                            << *this;
                    }
                    THROW_ERROR_EXCEPTION("Stray %Qv found",
                        character)
                        << *this;
                } else if (!TBase::IsEmpty()) {
                    TBase::Advance(1);
                }
            }
        } catch (const std::exception& ex) {
            auto [context, contextPosition] = TBase::GetContextFromCheckpoint();
            THROW_ERROR_EXCEPTION("Error occurred while parsing YSON")
                << TErrorAttribute("context", EscapeC(context))
                << TErrorAttribute("context_pos", contextPosition)
                << ex;
        }
    }

    void Stop()
    {
        Stopped_ = true;
    }

private:

    /// Parse routines
    void ParseAttributes()
    {
        TBase::CheckpointContext();
        Consumer_->OnBeginAttributes();

        if (Stopped_) {
            return;
        }

        ParseMapFragment(EndAttributesSymbol);

        if (Stopped_) {
            return;
        }

        TBase::CheckpointContext();
        TBase::SkipCharToken(EndAttributesSymbol);
        Consumer_->OnEndAttributes();
    }

    void ParseMap()
    {
        TBase::CheckpointContext();
        Consumer_->OnBeginMap();

        if (Stopped_) {
            return;
        }

        ParseMapFragment(EndMapSymbol);

        if (Stopped_) {
            return;
        }

        TBase::CheckpointContext();
        TBase::SkipCharToken(EndMapSymbol);
        Consumer_->OnEndMap();
    }

    void ParseList()
    {
        TBase::CheckpointContext();
        Consumer_->OnBeginList();

        if (Stopped_) {
            return;
        }

        ParseListFragment(EndListSymbol);

        if (Stopped_) {
            return;
        }

        TBase::CheckpointContext();
        TBase::SkipCharToken(EndListSymbol);
        Consumer_->OnEndList();
    }

    template <bool AllowFinish>
    void ParseNode()
    {
        ParseNode<AllowFinish>(TBase::SkipSpaceAndGetChar());
    }

    template <bool AllowFinish>
    void ParseNode(char ch)
    {
        if (NestingLevel_ >= NestingLevelLimit_) {
            auto nestingLevelLimit = NestingLevelLimit_;
            THROW_ERROR_EXCEPTION("Depth limit exceeded while parsing YSON")
                << TErrorAttribute("limit", nestingLevelLimit);
        }

        ++NestingLevel_;

        auto guard = Finally([this] {
            --NestingLevel_;
        });

        TBase::CheckpointContext();
        if (ch == BeginAttributesSymbol) {
            TBase::Advance(1);
            ParseAttributes();

            if (Stopped_) {
                return;
            }

            ch = TBase::SkipSpaceAndGetChar();
            TBase::CheckpointContext();
        }

        switch (ch) {
            case BeginMapSymbol:
                TBase::Advance(1);
                ParseMap();
                break;

            case BeginListSymbol:
                TBase::Advance(1);
                ParseList();
                break;

            case '"': {
                TBase::Advance(1);
                TStringBuf value = TBase::ReadQuotedString();
                Consumer_->OnStringScalar(value);
                break;
            }
            case StringMarker: {
                TBase::Advance(1);
                TStringBuf value = TBase::ReadBinaryString();
                Consumer_->OnStringScalar(value);
                break;
            }
            case Int64Marker:{
                TBase::Advance(1);
                i64 value = TBase::ReadBinaryInt64();
                Consumer_->OnInt64Scalar(value);
                break;
            }
            case Uint64Marker:{
                TBase::Advance(1);
                ui64 value = TBase::ReadBinaryUint64();
                Consumer_->OnUint64Scalar(value);
                break;
            }
            case DoubleMarker: {
                TBase::Advance(1);
                double value = TBase::ReadBinaryDouble();
                Consumer_->OnDoubleScalar(value);
                break;
            }
            case FalseMarker: {
                TBase::Advance(1);
                Consumer_->OnBooleanScalar(false);
                break;
            }
            case TrueMarker: {
                TBase::Advance(1);
                Consumer_->OnBooleanScalar(true);
                break;
            }
            case EntitySymbol:
                TBase::Advance(1);
                Consumer_->OnEntity();
                break;

            default: {
                if (isdigit(ch) || ch == '-' || ch == '+') { // case of '+' is handled in AfterPlus state
                    ReadNumeric<AllowFinish>();
                } else if (isalpha(ch) || ch == '_') {
                    TStringBuf value = TBase::template ReadUnquotedString<AllowFinish>();
                    Consumer_->OnStringScalar(value);
                } else if (ch == '%') {
                    TBase::Advance(1);
                    ch = TBase::template GetChar<AllowFinish>();
                    if (ch == 't' || ch == 'f') {
                        Consumer_->OnBooleanScalar(TBase::template ReadBoolean<AllowFinish>());
                    } else {
                        Consumer_->OnDoubleScalar(TBase::template ReadNanOrInf<AllowFinish>());
                    }
                } else if (ch == EndSymbol) {
                    THROW_ERROR_EXCEPTION("Unexpected end of stream while parsing node")
                        << *this;
                } else {
                    THROW_ERROR_EXCEPTION("Unexpected %Qv while parsing node", ch)
                        << *this;
                }
                break;
            }
        }
    }

    void ParseKey()
    {
        return ParseKey(TBase::SkipSpaceAndGetChar());
    }

    void ParseKey(char ch)
    {
        TBase::CheckpointContext();
        switch (ch) {
            case '"': {
                TBase::Advance(1);
                TStringBuf value = TBase::ReadQuotedString();
                Consumer_->OnKeyedItem(value);
                break;
            }
            case StringMarker: {
                TBase::Advance(1);
                TStringBuf value = TBase::ReadBinaryString();
                Consumer_->OnKeyedItem(value);
                break;
            }
            default: {
                if (isalpha(ch) || ch == '_') {
                    TStringBuf value = TBase::ReadUnquotedString();
                    Consumer_->OnKeyedItem(value);
                } else {
                    THROW_ERROR_EXCEPTION("Unexpected %Qv while parsing key",
                        ch)
                        << *this;
                }
            }
        }
    }

    template <bool AllowFinish>
    void ParseMapFragment(char endSymbol)
    {
        char ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
        while (ch != endSymbol) {
            ParseKey(ch);

            if (Stopped_) {
                return;
            }

            ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            TBase::CheckpointContext();
            if (ch == KeyValueSeparatorSymbol) {
                TBase::Advance(1);
            } else {
                THROW_ERROR_EXCEPTION("Expected %Qv but %Qv found",
                    KeyValueSeparatorSymbol,
                    ch)
                    << *this;
            }
            ParseNode<AllowFinish>();

            if (Stopped_) {
                return;
            }

            ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            TBase::CheckpointContext();
            if (ch == ItemSeparatorSymbol) {
                TBase::Advance(1);
                ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            } else if (ch != endSymbol) {
                THROW_ERROR_EXCEPTION("Expected %Qv or %Qv but %Qv found",
                    ItemSeparatorSymbol,
                    endSymbol,
                    ch)
                    << *this;
            }
        }
    }

    void ParseMapFragment(char endSymbol)
    {
        ParseMapFragment<false>(endSymbol);
    }

    template <bool AllowFinish>
    void ParseListFragment(char endSymbol)
    {
        char ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
        while (ch != endSymbol) {
            TBase::CheckpointContext();
            Consumer_->OnListItem();

            if (Stopped_) {
                return;
            }

            ParseNode<AllowFinish>(ch);

            if (Stopped_) {
                return;
            }

            ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            TBase::CheckpointContext();
            if (ch == ItemSeparatorSymbol) {
                TBase::Advance(1);
                ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            } else if (ch != endSymbol) {
                THROW_ERROR_EXCEPTION("Expected %Qv or %Qv but %Qv found",
                    ItemSeparatorSymbol,
                    endSymbol,
                    ch)
                    << *this;
            }
        }
    }

    void ParseListFragment(char endSymbol)
    {
        ParseListFragment<false>(endSymbol);
    }

    template <bool AllowFinish>
    void ReadNumeric()
    {
        TStringBuf valueBuffer;
        ENumericResult numericResult = TBase::template ReadNumeric<AllowFinish>(&valueBuffer);

        if (numericResult == ENumericResult::Double) {
            double value;
            try {
                value = FromString<double>(valueBuffer);
            } catch (const std::exception& ex) {
                // This exception is wrapped in parser.
                THROW_ERROR CreateLiteralError(ETokenType::Double, valueBuffer.begin(), valueBuffer.size())
                    << *this
                    << ex;
            }
            Consumer_->OnDoubleScalar(value);
        } else if (numericResult == ENumericResult::Int64) {
            i64 value;
            try {
                value = FromString<i64>(valueBuffer);
            } catch (const std::exception& ex) {
                // This exception is wrapped in parser.
                THROW_ERROR CreateLiteralError(ETokenType::Int64, valueBuffer.begin(), valueBuffer.size())
                    << *this
                    << ex;
            }
            Consumer_->OnInt64Scalar(value);
        } else if (numericResult == ENumericResult::Uint64) {
            ui64 value;
            try {
                value = FromString<ui64>(valueBuffer.SubStr(0, valueBuffer.size() - 1));
            } catch (const std::exception& ex) {
                // This exception is wrapped in parser.
                THROW_ERROR CreateLiteralError(ETokenType::Uint64, valueBuffer.begin(), valueBuffer.size())
                    << *this
                    << ex;
            }
            Consumer_->OnUint64Scalar(value);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail

template <class TConsumer, class TBlockStream>
class TParserYsonStreamImpl
{
public:
    TParserYsonStreamImpl()
    { }

    void DoParse(
        const TBlockStream& blockStream,
        IYsonConsumer* consumer,
        EYsonType parsingMode,
        const TYsonParserConfig& config)
    {
        if (config.EnableLinePositionInfo && config.EnableContext) {
            using TImpl = NDetail::TParser<TConsumer, TBlockStream, 64, true>;
            TImpl impl(blockStream, consumer, config.MemoryLimit, config.NestingLevelLimit);
            BlockStream_ = static_cast<TBlockStream*>(&impl);
            impl.DoParse(parsingMode);
        } else if (config.EnableLinePositionInfo && !config.EnableContext) {
            using TImpl = NDetail::TParser<TConsumer, TBlockStream, 0, true>;
            TImpl impl(blockStream, consumer, config.MemoryLimit, config.NestingLevelLimit);
            BlockStream_ = static_cast<TBlockStream*>(&impl);
            impl.DoParse(parsingMode);
        } else if (!config.EnableLinePositionInfo && config.EnableContext) {
            using TImpl = NDetail::TParser<TConsumer, TBlockStream, 64, false>;
            TImpl impl(blockStream, consumer, config.MemoryLimit, config.NestingLevelLimit);
            BlockStream_ = static_cast<TBlockStream*>(&impl);
            impl.DoParse(parsingMode);
        } else {
            using TImpl = NDetail::TParser<TConsumer, TBlockStream, 0, false>;
            TImpl impl(blockStream, consumer, config.MemoryLimit, config.NestingLevelLimit);
            BlockStream_ = static_cast<TBlockStream*>(&impl);
            impl.DoParse(parsingMode);
        }
    }

    const char* GetCurrentPositionInBlock()
    {
        return BlockStream_->Current();
    }

private:
    TBlockStream* BlockStream_;
};

class TStatelessYsonParserImplBase
{
public:
    virtual void Parse(TStringBuf data, EYsonType type = EYsonType::Node) = 0;
    virtual void Stop() = 0;

    virtual ~TStatelessYsonParserImplBase()
    { }
};

template <class TConsumer, size_t MaxContextSize, bool EnableLinePositionInfo>
class TStatelessYsonParserImpl
    : public TStatelessYsonParserImplBase
{
private:
    using TParser = NDetail::TParser<TConsumer, TStringReader, MaxContextSize, EnableLinePositionInfo>;
    TParser Parser;

public:
    TStatelessYsonParserImpl(
        TConsumer* consumer,
        i64 memoryLimit,
        int nestingLevelLimit)
        : Parser(TStringReader(), consumer, memoryLimit, nestingLevelLimit)
    { }

    void Parse(TStringBuf data, EYsonType type = EYsonType::Node) override
    {
        Parser.SetBuffer(data.begin(), data.end());
        Parser.DoParse(type);
    }

    void Stop() override
    {
        Parser.Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
