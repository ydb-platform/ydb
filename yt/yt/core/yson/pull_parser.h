#pragma once

#include "public.h"
#include "detail.h"

#include "syntax_checker.h"

#include <util/generic/strbuf.h>
#include <util/stream/output.h>
#include <util/stream/zerocopy.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// TYsonItem represents single meaningful yson item.
// We don't use std::variant for performance reasons.
class TYsonItem
{
public:
    Y_FORCE_INLINE TYsonItem(const TYsonItem& other);
    Y_FORCE_INLINE TYsonItem& operator =(const TYsonItem& other);

    static Y_FORCE_INLINE TYsonItem Simple(EYsonItemType type);
    static Y_FORCE_INLINE TYsonItem Boolean(bool data);
    static Y_FORCE_INLINE TYsonItem Int64(i64 data);
    static Y_FORCE_INLINE TYsonItem Uint64(ui64 data);
    static Y_FORCE_INLINE TYsonItem Double(double data);
    static Y_FORCE_INLINE TYsonItem String(TStringBuf data);
    Y_FORCE_INLINE EYsonItemType GetType() const;
    Y_FORCE_INLINE bool UncheckedAsBoolean() const;
    Y_FORCE_INLINE i64 UncheckedAsInt64() const;
    Y_FORCE_INLINE ui64 UncheckedAsUint64() const;
    Y_FORCE_INLINE double UncheckedAsDouble() const;
    Y_FORCE_INLINE TStringBuf UncheckedAsString() const;
    template <typename T>
    Y_FORCE_INLINE T UncheckedAs() const;
    Y_FORCE_INLINE bool IsEndOfStream() const;

private:
    TYsonItem() = default;

private:
    #pragma pack(push, 1)
    struct TSmallStringBuf
    {
        const char* Ptr;
        ui32 Size;
    };
    union TData
    {
        TData() {
            Zero(*this);
        }

        bool Boolean;
        i64 Int64;
        ui64 Uint64;
        double Double;
        TSmallStringBuf String;
    };
    TData Data_;
    EYsonItemType Type_;
    #pragma pack(pop)
};
// NB TYsonItem is 16 bytes long so it fits in a processor register.
static_assert(sizeof(TYsonItem) <= 16);

bool operator==(TYsonItem lhs, TYsonItem rhs);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TZeroCopyInputStreamReader
{
public:
    TZeroCopyInputStreamReader(IZeroCopyInput* reader);

    Y_FORCE_INLINE void RefreshBlock();
    Y_FORCE_INLINE const char* Begin() const;
    Y_FORCE_INLINE const char* Current() const;
    Y_FORCE_INLINE const char* End() const;
    Y_FORCE_INLINE void Advance(size_t bytes);
    Y_FORCE_INLINE bool IsFinished() const;
    size_t GetTotalReadSize() const;

    void StartRecording(IOutputStream* out);
    void CancelRecording();
    void FinishRecording();

private:
    IZeroCopyInput* Reader_;
    const char* Begin_ = nullptr;
    const char* End_ = nullptr;
    const char* Current_ = nullptr;
    ui64 TotalReadBlocksSize_ = 0;
    bool Finished_ = false;

    const char* RecordingFrom_ = nullptr;
    IOutputStream* RecordOutput_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TYsonPullParser
{
public:
    TYsonPullParser(
        IZeroCopyInput* input,
        EYsonType ysonType,
        int nestingLevelLimit = DefaultYsonParserNestingLevelLimit);

    TYsonItem Next();

    // See comments for corresponding |TYsonPullParserCursor| methods.
    void SkipComplexValue();
    void TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer);

    // These methods are analogous to the ones without the |previousItem| argument,
    // but process the complex value that starts from |previousItem|.
    void SkipComplexValue(const TYsonItem& previousItem);
    void TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer, const TYsonItem& previousItem);
    void TransferComplexValue(IYsonConsumer* consumer, const TYsonItem& previousItem);

    void SkipComplexValueOrAttributes(const TYsonItem& previousItem);
    void SkipAttributes(const TYsonItem& previousItem);
    void TransferAttributes(TCheckedInDebugYsonTokenWriter* writer, const TYsonItem& previousItem);
    void TransferAttributes(IYsonConsumer* consumer, const TYsonItem& previousItem);

    Y_FORCE_INLINE size_t GetNestingLevel() const;
    Y_FORCE_INLINE bool IsOnValueBoundary(size_t nestingLevel) const;

    size_t GetTotalReadSize() const;

    // Return error attributes about yson context that is being parsed.
    std::vector<TErrorAttribute> GetErrorAttributes() const;

    // All the |Parse*| methods expect the next item to have corresponding type,
    // throwing exception if it does not.
    Y_FORCE_INLINE ui64 ParseUint64();
    Y_FORCE_INLINE i64 ParseInt64();
    Y_FORCE_INLINE TStringBuf ParseString();
    Y_FORCE_INLINE bool ParseBoolean();
    Y_FORCE_INLINE double ParseDouble();
    Y_FORCE_INLINE void ParseBeginList();
    Y_FORCE_INLINE void ParseEndList();
    Y_FORCE_INLINE void ParseEntity();

    // |ParseOptional*| allow the next item to be entity
    // and return |std::nullopt| in this case.
    Y_FORCE_INLINE std::optional<ui64> ParseOptionalUint64();
    Y_FORCE_INLINE std::optional<i64> ParseOptionalInt64();
    Y_FORCE_INLINE std::optional<TStringBuf> ParseOptionalString();
    Y_FORCE_INLINE std::optional<bool> ParseOptionalBoolean();
    Y_FORCE_INLINE std::optional<double> ParseOptionalDouble();

    // Returns |true| iff the item was '['.
    Y_FORCE_INLINE bool ParseOptionalBeginList();

    // |Parse*AsVarint| expect the the next item to have corresponding type,
    // throwing exception if it does not.
    // Optional version returns 0 iff parsed item is entity.
    // Non-optional version never returns 0.
    //
    // |Parse*Uint64AsVarint| writes the integer as varint
    // to the memory location pointed by |out|.
    // Returns the number of written bytes.
    Y_FORCE_INLINE int ParseUint64AsVarint(char* out);
    Y_FORCE_INLINE int ParseOptionalUint64AsVarint(char* out);

    // |Parse*Int64AsZigzagVarint| zigzag encodes the integer and
    // writes it as varint to the memory location pointed by |out|.
    // Returns the number of written bytes.
    Y_FORCE_INLINE int ParseInt64AsZigzagVarint(char* out);
    Y_FORCE_INLINE int ParseOptionalInt64AsZigzagVarint(char* out);

    // Returns |true| iff the next item is ']'.
    // NOTE: it does NOT move the cursor.
    Y_FORCE_INLINE bool IsEndList();

    // Returns |true| iff the next item is '#'.
    // NOTE: it does NOT move the cursor.
    Y_FORCE_INLINE bool IsEntity();

    void StartRecording(IOutputStream* out);
    void CancelRecording();
    void FinishRecording();

    EYsonType GetYsonType() const;

private:
    template <typename TVisitor>
    Y_FORCE_INLINE typename TVisitor::TResult NextImpl(TVisitor* visitor);

    template <typename TVisitor>
    void TraverseComplexValueOrAttributes(TVisitor* visitor, bool stopAfterAttributes);
    template <typename TVisitor>
    void TraverseComplexValueOrAttributes(TVisitor* visitor, const TYsonItem& previousItem, bool stopAfterAttributes);

    Y_FORCE_INLINE void MaybeSkipSemicolon();

    template <EYsonItemType ItemType, bool IsOptional>
    Y_FORCE_INLINE auto ParseItem() -> std::conditional_t<IsOptional, bool, void>;

    template <typename TValue, EYsonItemType ItemType>
    Y_FORCE_INLINE TValue ParseTypedValue();

    template <typename TValue, EYsonItemType ItemType>
    Y_FORCE_INLINE TValue ParseTypedValueFallback();

    template <typename TValue, EYsonItemType ItemType>
    Y_FORCE_INLINE int ParseVarintToArray(char* out);

    Y_FORCE_INLINE bool IsMarker(char marker);

private:
    using TLexer = NDetail::TLexerBase<NDetail::TReaderWithContext<NDetail::TZeroCopyInputStreamReader, 64>, false>;

    NDetail::TZeroCopyInputStreamReader StreamReader_;
    TLexer Lexer_;
    NDetail::TYsonSyntaxChecker SyntaxChecker_;
    EYsonType YsonType_;
};

////////////////////////////////////////////////////////////////////////////////

// Cursor based iteration using pull parser.
// To check if yson stream is exhausted check if current item is of type EYsonItemType::EndOfStream.
class TYsonPullParserCursor
{
public:
    // This constructor extracts next element from parser immediately.
    Y_FORCE_INLINE TYsonPullParserCursor(TYsonPullParser* parser);

    Y_FORCE_INLINE const TYsonItem& GetCurrent() const;
    Y_FORCE_INLINE const TYsonItem* operator->() const;
    Y_FORCE_INLINE const TYsonItem& operator*() const;

    Y_FORCE_INLINE void Next();

    // Return error attributes about current yson context.
    std::vector<TErrorAttribute> GetErrorAttributes() const;

    // If cursor is positioned over simple value  (i.e. just integer) cursor is moved one element further.
    // If cursor is positioned over start of list/map cursor will be moved to the first item after
    // current list/map.
    // If cursor is positioned over start of attributes all attributes will be skipped and then value which
    // owns these attributes will be skipped as well.
    void SkipComplexValue();

    // Transfer complex value is similar to SkipComplexValue
    // except that it feeds passed consumer with skipped value.
    void TransferComplexValue(IYsonConsumer* consumer);
    void TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer);

    // |Parse...| methods call |function(this)| for each item of corresponding composite object
    // and expect |function| to consume it (e.g. call |cursor->Next()|).
    // For map and attributes cursor will point to the key and |function| must consume both key and value.
    template <typename TFunction>
    void ParseMap(TFunction function);
    template <typename TFunction>
    void ParseList(TFunction function);
    template <typename TFunction>
    void ParseAttributes(TFunction function);

    // Transfer or skip attributes (if cursor is not positioned over attributes, throws an error).
    void SkipAttributes();
    void TransferAttributes(IYsonConsumer* consumer);
    void TransferAttributes(TCheckedInDebugYsonTokenWriter* writer);

    // Start recording the bytes from parsed stream to |out|.
    void StartRecording(IOutputStream* out);

    // Stop the recording.
    void CancelRecording();

    // Record the current complex value and stop recording.
    void SkipComplexValueAndFinishRecording();

    // Returns |true| iff cursor is positioned over the beginning of the first
    // item of top-level map or list fragment (for corresponding EYsonType).
    // Consequent calls will return |false|.
    bool TryConsumeFragmentStart();

private:
    bool IsOnFragmentStart_;
    TYsonItem Current_;
    TYsonPullParser* Parser_;

private:
    void SkipComplexValueOrAttributes();
    [[noreturn]] static void FailAsTryConsumeFragmentStartNotCalled();
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void EnsureYsonToken(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    EYsonItemType expected);

Y_FORCE_INLINE void EnsureYsonToken(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    EYsonItemType expected);


////////////////////////////////////////////////////////////////////////////////

[[noreturn]] void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    const std::vector<EYsonItemType>& expected);

[[noreturn]] void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    const std::vector<EYsonItemType>& expected);

[[noreturn]] void ThrowUnexpectedTokenException(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    EYsonItemType expected,
    bool isOptional);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PULL_PARSER_INL_H_
#include "pull_parser-inl.h"
#undef PULL_PARSER_INL_H_
