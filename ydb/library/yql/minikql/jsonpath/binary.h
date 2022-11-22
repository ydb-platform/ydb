#pragma once

#include "ast_nodes.h"

#include <ydb/library/rewrapper/re.h>

#include <util/system/unaligned_mem.h>
#include <util/generic/buffer.h>
#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/hash.h>

#include <variant>
#include <type_traits>

namespace NYql::NJsonPath {

class TJsonPath : public TSimpleRefCount<TJsonPath>, public TBuffer {
};

using TJsonPathPtr = TIntrusivePtr<TJsonPath>;
using TUint = ui64;

enum class EJsonPathItemType {
    MemberAccess = 0,
    WildcardMemberAccess = 1,
    ArrayAccess = 2,
    WildcardArrayAccess = 3,
    ContextObject = 4,
    NumberLiteral = 5,
    LastArrayIndex = 6,
    UnaryPlus = 7,
    UnaryMinus = 8,
    BinaryAdd = 9,
    BinarySubstract = 10,
    BinaryMultiply = 11,
    BinaryDivide = 12,
    BinaryModulo = 13,
    Variable = 14,
    BinaryLess = 15,
    BinaryLessEqual = 16,
    BinaryGreater = 17,
    BinaryGreaterEqual = 18,
    BinaryEqual = 19,
    BinaryNotEqual = 20,
    BinaryAnd = 21,
    BinaryOr = 22,
    UnaryNot = 23,
    BooleanLiteral = 24,
    NullLiteral = 25,
    StringLiteral = 26,
    FilterObject = 27,
    FilterPredicate = 28,
    AbsMethod = 29,
    FloorMethod = 30,
    CeilingMethod = 31,
    DoubleMethod = 32,
    TypeMethod = 33,
    SizeMethod = 34,
    KeyValueMethod = 35,
    StartsWithPredicate = 36,
    ExistsPredicate = 37,
    IsUnknownPredicate = 38,
    LikeRegexPredicate = 39,
};

struct TArraySubscriptOffsets {
    TUint FromOffset = 0;
    TUint ToOffset = 0;

    bool IsRange() const;
};

struct TBinaryOpArgumentsOffset {
    TUint LeftOffset = 0;
    TUint RightOffset = 0;
};

struct TFilterPredicateOffset {
    TUint Offset = 0;
};

struct TStartsWithPrefixOffset {
    TUint Offset = 0;
};

struct TJsonPathItem {
    // Position in the source jsonpath
    TPosition Pos;

    // Type of item
    EJsonPathItemType Type;

    // Offset in buffer pointing to the input item
    TMaybe<TUint> InputItemOffset;

    // Data associated with this item. To determine which variant
    // type was filled callee must examine Type field.
    // WARNING: Some item types do not fill Data field at all! You must
    //          check item type before accesing this field.
    std::variant<
        TStringBuf,
        TVector<TArraySubscriptOffsets>,
        TBinaryOpArgumentsOffset,
        TFilterPredicateOffset,
        TStartsWithPrefixOffset,
        NReWrapper::IRePtr,
        double,
        bool
    > Data;

    const TStringBuf GetString() const;
    const TVector<TArraySubscriptOffsets>& GetSubscripts() const;
    const TBinaryOpArgumentsOffset& GetBinaryOpArguments() const;
    const NReWrapper::IRePtr& GetRegex() const;
    double GetNumber() const;
    bool GetBoolean() const;
    TFilterPredicateOffset GetFilterPredicateOffset() const;
    TStartsWithPrefixOffset GetStartsWithPrefixOffset() const;

    // Pointer to the binary representation of jsonpath.
    // We do not use this directly but Data field can reference to it.
    // For example if this item is a string then Data contains TStringBuf
    // pointing to some part inside buffer. We must ensure that it is not
    // destructed while this item is alive so we keep shared pointer to it.
    const TJsonPathPtr JsonPath;
};

class TJsonPathBuilder : public IAstNodeVisitor {
public:
    TJsonPathBuilder()
        : Result(new TJsonPath())
    {
    }

    void VisitRoot(const TRootNode& node) override;

    void VisitContextObject(const TContextObjectNode& node) override;

    void VisitVariable(const TVariableNode& node) override;

    void VisitLastArrayIndex(const TLastArrayIndexNode& node) override;

    void VisitNumberLiteral(const TNumberLiteralNode& node) override;

    void VisitMemberAccess(const TMemberAccessNode& node) override;

    void VisitWildcardMemberAccess(const TWildcardMemberAccessNode& node) override;

    void VisitArrayAccess(const TArrayAccessNode& node) override;

    void VisitWildcardArrayAccess(const TWildcardArrayAccessNode& node) override;

    void VisitUnaryOperation(const TUnaryOperationNode& node) override;

    void VisitBinaryOperation(const TBinaryOperationNode& node) override;

    void VisitBooleanLiteral(const TBooleanLiteralNode& node) override;

    void VisitNullLiteral(const TNullLiteralNode& node) override;

    void VisitStringLiteral(const TStringLiteralNode& node) override;

    void VisitFilterObject(const TFilterObjectNode& node) override;

    void VisitFilterPredicate(const TFilterPredicateNode& node) override;

    void VisitMethodCall(const TMethodCallNode& node) override;

    void VisitStartsWithPredicate(const TStartsWithPredicateNode& node) override;

    void VisitExistsPredicate(const TExistsPredicateNode& node) override;

    void VisitIsUnknownPredicate(const TIsUnknownPredicateNode& node) override;

    void VisitLikeRegexPredicate(const TLikeRegexPredicateNode& node) override;

    TJsonPathPtr ShrinkAndGetResult();

private:
    void WriteZeroInputItem(EJsonPathItemType type, const TAstNode& node);

    void WriteSingleInputItem(EJsonPathItemType type, const TAstNode& node, const TAstNodePtr input);

    void WriteTwoInputsItem(EJsonPathItemType type, const TAstNode& node, const TAstNodePtr firstInput, const TAstNodePtr secondInput);

    void WritePos(const TAstNode& node);

    void WriteType(EJsonPathItemType type);

    void WriteMode(EJsonPathMode mode);

    void WriteNextPosition();

    void WriteFinishPosition();

    void WriteString(TStringBuf value);

    void RewriteUintSequence(const TVector<TUint>& sequence, TUint offset);

    void WriteUintSequence(const TVector<TUint>& sequence);

    void RewriteUint(TUint value, TUint offset);

    void WriteUint(TUint value);

    void WriteDouble(double value);

    void WriteBool(bool value);

    template <typename T>
    void WritePOD(const T& value) {
        static_assert(std::is_pod_v<T>, "Type must be POD");
        Result->Append(reinterpret_cast<const char*>(&value), sizeof(T));
    }

    TUint CurrentEndPos() const;

    TJsonPathPtr Result;
};

class TJsonPathReader {
public:
    TJsonPathReader(const TJsonPathPtr path);

    const TJsonPathItem& ReadFirst();

    const TJsonPathItem& ReadInput(const TJsonPathItem& node);

    const TJsonPathItem& ReadFromSubscript(const TArraySubscriptOffsets& subscript);

    const TJsonPathItem& ReadToSubscript(const TArraySubscriptOffsets& subscript);

    const TJsonPathItem& ReadLeftOperand(const TJsonPathItem& node);

    const TJsonPathItem& ReadRightOperand(const TJsonPathItem& node);

    const TJsonPathItem& ReadFilterPredicate(const TJsonPathItem& node);

    const TJsonPathItem& ReadPrefix(const TJsonPathItem& node);

    EJsonPathMode GetMode() const;

private:
    const TJsonPathItem& ReadFromPos(TUint pos);

    TUint ReadUint(TUint& pos);

    double ReadDouble(TUint& pos);

    bool ReadBool(TUint& pos);

    EJsonPathItemType ReadType(TUint& pos);

    EJsonPathMode ReadMode(TUint& pos);

    const TStringBuf ReadString(TUint& pos);

    TVector<TArraySubscriptOffsets> ReadSubscripts(TUint& pos);

    template <typename T>
    T ReadPOD(TUint& pos) {
        static_assert(std::is_pod_v<T>, "Type must be POD");
        T value = ReadUnaligned<T>(Path->Begin() + pos);
        pos += sizeof(T);
        return std::move(value);
    }

    const TJsonPathPtr Path;
    TUint InitialPos;
    EJsonPathMode Mode;
    THashMap<TUint, TJsonPathItem> ItemCache;
};

}
