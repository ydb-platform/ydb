#include "binary.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql::NJsonPath {

bool TArraySubscriptOffsets::IsRange() const {
    return ToOffset > 0;
}

const TStringBuf TJsonPathItem::GetString() const {
    return std::get<TStringBuf>(Data);
}

const TVector<TArraySubscriptOffsets>& TJsonPathItem::GetSubscripts() const {
    return std::get<TVector<TArraySubscriptOffsets>>(Data);
}

const TBinaryOpArgumentsOffset& TJsonPathItem::GetBinaryOpArguments() const {
    return std::get<TBinaryOpArgumentsOffset>(Data);
}

double TJsonPathItem::GetNumber() const {
    return std::get<double>(Data);
}

bool TJsonPathItem::GetBoolean() const {
    return std::get<bool>(Data);
}

TFilterPredicateOffset TJsonPathItem::GetFilterPredicateOffset() const {
    return std::get<TFilterPredicateOffset>(Data);
}

TStartsWithPrefixOffset TJsonPathItem::GetStartsWithPrefixOffset() const {
    return std::get<TStartsWithPrefixOffset>(Data);
}

const NReWrapper::IRePtr& TJsonPathItem::GetRegex() const {
    return std::get<NReWrapper::IRePtr>(Data);
}

TJsonPathReader::TJsonPathReader(const TJsonPathPtr path)
    : Path(path)
    , InitialPos(0)
    , Mode(ReadMode(InitialPos))
{
}

const TJsonPathItem& TJsonPathReader::ReadFirst() {
    return ReadFromPos(InitialPos);
}

const TJsonPathItem& TJsonPathReader::ReadInput(const TJsonPathItem& item) {
    YQL_ENSURE(item.InputItemOffset.Defined());
    return ReadFromPos(*item.InputItemOffset);
}

const TJsonPathItem& TJsonPathReader::ReadFromSubscript(const TArraySubscriptOffsets& subscript) {
    return ReadFromPos(subscript.FromOffset);
}

const TJsonPathItem& TJsonPathReader::ReadToSubscript(const TArraySubscriptOffsets& subscript) {
    YQL_ENSURE(subscript.IsRange());
    return ReadFromPos(subscript.ToOffset);
}

const TJsonPathItem& TJsonPathReader::ReadLeftOperand(const TJsonPathItem& node) {
    return ReadFromPos(node.GetBinaryOpArguments().LeftOffset);
}

const TJsonPathItem& TJsonPathReader::ReadRightOperand(const TJsonPathItem& node) {
    return ReadFromPos(node.GetBinaryOpArguments().RightOffset);
}

const TJsonPathItem& TJsonPathReader::ReadFilterPredicate(const TJsonPathItem& node) {
    return ReadFromPos(node.GetFilterPredicateOffset().Offset);
}

const TJsonPathItem& TJsonPathReader::ReadPrefix(const TJsonPathItem& node) {
    return ReadFromPos(node.GetStartsWithPrefixOffset().Offset);
}

EJsonPathMode TJsonPathReader::GetMode() const {
    return Mode;
}

const TJsonPathItem& TJsonPathReader::ReadFromPos(TUint pos) {
    YQL_ENSURE(pos < Path->Size());

    const auto it = ItemCache.find(pos);
    if (it != ItemCache.end()) {
        return it->second;
    }

    TJsonPathItem& result = ItemCache[pos];
    result.Type = ReadType(pos);

    const auto row = ReadUint(pos);
    const auto column = ReadUint(pos);
    result.Pos = TPosition(column, row, "jsonpath");

    switch (result.Type) {
        // Items without input
        case EJsonPathItemType::FilterObject:
        case EJsonPathItemType::NullLiteral:
        case EJsonPathItemType::ContextObject:
        case EJsonPathItemType::LastArrayIndex:
            break;

        case EJsonPathItemType::Variable:
        case EJsonPathItemType::StringLiteral:
            result.Data = ReadString(pos);
            break;

        case EJsonPathItemType::NumberLiteral:
            result.Data = ReadDouble(pos);
            break;

        case EJsonPathItemType::BooleanLiteral:
            result.Data = ReadBool(pos);
            break;

        // Items with single input
        case EJsonPathItemType::TypeMethod:
        case EJsonPathItemType::SizeMethod:
        case EJsonPathItemType::KeyValueMethod:
        case EJsonPathItemType::AbsMethod:
        case EJsonPathItemType::FloorMethod:
        case EJsonPathItemType::CeilingMethod:
        case EJsonPathItemType::DoubleMethod:
        case EJsonPathItemType::WildcardArrayAccess:
        case EJsonPathItemType::WildcardMemberAccess:
        case EJsonPathItemType::UnaryMinus:
        case EJsonPathItemType::UnaryPlus:
        case EJsonPathItemType::UnaryNot:
        case EJsonPathItemType::IsUnknownPredicate:
        case EJsonPathItemType::ExistsPredicate:
            result.InputItemOffset = ReadUint(pos);
            break;

        case EJsonPathItemType::MemberAccess:
            result.Data = ReadString(pos);
            result.InputItemOffset = ReadUint(pos);
            break;

        case EJsonPathItemType::ArrayAccess:
            result.Data = ReadSubscripts(pos);
            result.InputItemOffset = ReadUint(pos);
            break;

        case EJsonPathItemType::FilterPredicate:
            result.Data = TFilterPredicateOffset{ReadUint(pos)};
            result.InputItemOffset = ReadUint(pos);
            break;

        case EJsonPathItemType::StartsWithPredicate:
            result.Data = TStartsWithPrefixOffset{ReadUint(pos)};
            result.InputItemOffset = ReadUint(pos);
            break;

        case EJsonPathItemType::LikeRegexPredicate: {
            const auto serializedRegex = ReadString(pos);

            auto regex = NReWrapper::NDispatcher::Deserialize(serializedRegex);
            result.Data = std::move(regex);
            result.InputItemOffset = ReadUint(pos);
            break;
        }

        // Items with 2 inputs
        case EJsonPathItemType::BinaryAdd:
        case EJsonPathItemType::BinarySubstract:
        case EJsonPathItemType::BinaryMultiply:
        case EJsonPathItemType::BinaryDivide:
        case EJsonPathItemType::BinaryModulo:
        case EJsonPathItemType::BinaryLess:
        case EJsonPathItemType::BinaryLessEqual:
        case EJsonPathItemType::BinaryGreater:
        case EJsonPathItemType::BinaryGreaterEqual:
        case EJsonPathItemType::BinaryEqual:
        case EJsonPathItemType::BinaryNotEqual:
        case EJsonPathItemType::BinaryAnd:
        case EJsonPathItemType::BinaryOr:
            TBinaryOpArgumentsOffset data;
            data.LeftOffset = ReadUint(pos);
            data.RightOffset = ReadUint(pos);
            result.Data = data;
            break;
    }

    return result;
}

TUint TJsonPathReader::ReadUint(TUint& pos) {
    return ReadPOD<TUint>(pos);
}

double TJsonPathReader::ReadDouble(TUint& pos) {
    return ReadPOD<double>(pos);
}

bool TJsonPathReader::ReadBool(TUint& pos) {
    return ReadPOD<bool>(pos);
}

EJsonPathItemType TJsonPathReader::ReadType(TUint& pos) {
    return static_cast<EJsonPathItemType>(ReadUint(pos));
}

EJsonPathMode TJsonPathReader::ReadMode(TUint& pos) {
    return static_cast<EJsonPathMode>(ReadUint(pos));
}

const TStringBuf TJsonPathReader::ReadString(TUint& pos) {
    TUint length = ReadUint(pos);
    TStringBuf result(Path->Begin() + pos, length);
    pos += length;
    return result;
}

TVector<TArraySubscriptOffsets> TJsonPathReader::ReadSubscripts(TUint& pos) {
    const auto count = ReadUint(pos);
    TVector<TArraySubscriptOffsets> result(count);

    for (size_t i = 0; i < count; i++) {
        result[i].FromOffset = ReadUint(pos);
        result[i].ToOffset = ReadUint(pos);
    }
    return result;
}

void TJsonPathBuilder::VisitRoot(const TRootNode& node) {
    // Block structure:
    // <(1) TUint>
    // Components:
    // (1) Must be casted to EJsonPathMode. Jsonpath execution mode
    WriteMode(node.GetMode());
    node.GetExpr()->Accept(*this);
}

void TJsonPathBuilder::VisitContextObject(const TContextObjectNode& node) {
    WriteZeroInputItem(EJsonPathItemType::ContextObject, node);
}

void TJsonPathBuilder::VisitVariable(const TVariableNode& node) {
    WriteZeroInputItem(EJsonPathItemType::Variable, node);
    WriteString(node.GetName());
}

void TJsonPathBuilder::VisitLastArrayIndex(const TLastArrayIndexNode& node) {
    WriteZeroInputItem(EJsonPathItemType::LastArrayIndex, node);
}

void TJsonPathBuilder::VisitNumberLiteral(const TNumberLiteralNode& node) {
    WriteZeroInputItem(EJsonPathItemType::NumberLiteral, node);
    WriteDouble(node.GetValue());
}

void TJsonPathBuilder::VisitMemberAccess(const TMemberAccessNode& node) {
    // Block structure:
    // <(1) TUint> <(2) TUint> <(3) TUint> <(4) TUint> <(5) char[]> <(6) TUint>
    // Components:
    // (1) Must be casted to EJsonPathItemType. Member access item type
    // (2) Row of the position in the source jsonpath
    // (3) Column of the position in the source jsonpath
    // (4) Length of member name string
    // (5) Member name string
    // (6) Offset of the input item
    WriteType(EJsonPathItemType::MemberAccess);
    WritePos(node);
    WriteString(node.GetMember());

    WriteNextPosition();
    node.GetInput()->Accept(*this);
}

void TJsonPathBuilder::VisitWildcardMemberAccess(const TWildcardMemberAccessNode& node) {
    WriteSingleInputItem(EJsonPathItemType::WildcardMemberAccess, node, node.GetInput());
}

void TJsonPathBuilder::VisitArrayAccess(const TArrayAccessNode& node) {
    // Block structure:
    // <(1) TUint> <(2) TUint> <(3) TUint> <(4) TUint> <(5) pair<TUint, TUint>[]> <(6) TUint> <(7) items>
    // Components:
    // (1) Must be casted to EJsonPathItemType. Array access item type
    // (2) Row of the position in the source jsonpath
    // (3) Column of the position in the source jsonpath
    // (4) Count of subscripts stored
    // (5) Array of pairs with offsets to subscript items. If subscript is a single index, only first element
    //     is set to it's offset and second is zero. If subscript is a range, both pair elements are valid offsets
    //     to the elements of range (lower and upper bound).
    // (6) Offset of the input item
    // (7) Array of subcsripts. For details about encoding see VisitArraySubscript
    WriteType(EJsonPathItemType::ArrayAccess);
    WritePos(node);

    // (4) Write count of subscripts stored
    const auto& subscripts = node.GetSubscripts();
    const auto count = subscripts.size();
    WriteUint(count);

    // (5) We do not know sizes of each subscript. Write array of zeros for offsets
    const auto indexStart = CurrentEndPos();
    TVector<TUint> offsets(2 * count);
    WriteUintSequence(offsets);

    // (6) Reserve space for input offset to rewrite it later
    const auto inputStart = CurrentEndPos();
    WriteFinishPosition();

    // (7) Write all subscripts and record offset for each of them
    for (size_t i = 0; i < count; i++) {
        offsets[2 * i] = CurrentEndPos();
        subscripts[i].From->Accept(*this);

        if (subscripts[i].To) {
            offsets[2 * i + 1] = CurrentEndPos();
            subscripts[i].To->Accept(*this);
        }
    }

    // (5) Rewrite offsets with correct values
    RewriteUintSequence(offsets, indexStart);

    // (6) Rewrite input offset
    RewriteUint(CurrentEndPos(), inputStart);
    node.GetInput()->Accept(*this);
}

void TJsonPathBuilder::VisitWildcardArrayAccess(const TWildcardArrayAccessNode& node) {
    WriteSingleInputItem(EJsonPathItemType::WildcardArrayAccess, node, node.GetInput());
}

void TJsonPathBuilder::VisitUnaryOperation(const TUnaryOperationNode& node) {
    EJsonPathItemType type;
    switch (node.GetOp()) {
        case EUnaryOperation::Plus:
            type = EJsonPathItemType::UnaryPlus;
            break;
        case EUnaryOperation::Minus:
            type = EJsonPathItemType::UnaryMinus;
            break;
        case EUnaryOperation::Not:
            type = EJsonPathItemType::UnaryNot;
            break;
    }

    WriteSingleInputItem(type, node, node.GetExpr());
}

void TJsonPathBuilder::VisitBinaryOperation(const TBinaryOperationNode& node) {
    EJsonPathItemType type;
    switch (node.GetOp()) {
        case EBinaryOperation::Add:
            type = EJsonPathItemType::BinaryAdd;
            break;
        case EBinaryOperation::Substract:
            type = EJsonPathItemType::BinarySubstract;
            break;
        case EBinaryOperation::Multiply:
            type = EJsonPathItemType::BinaryMultiply;
            break;
        case EBinaryOperation::Divide:
            type = EJsonPathItemType::BinaryDivide;
            break;
        case EBinaryOperation::Modulo:
            type = EJsonPathItemType::BinaryModulo;
            break;
        case EBinaryOperation::Less:
            type = EJsonPathItemType::BinaryLess;
            break;
        case EBinaryOperation::LessEqual:
            type = EJsonPathItemType::BinaryLessEqual;
            break;
        case EBinaryOperation::Greater:
            type = EJsonPathItemType::BinaryGreater;
            break;
        case EBinaryOperation::GreaterEqual:
            type = EJsonPathItemType::BinaryGreaterEqual;
            break;
        case EBinaryOperation::Equal:
            type = EJsonPathItemType::BinaryEqual;
            break;
        case EBinaryOperation::NotEqual:
            type = EJsonPathItemType::BinaryNotEqual;
            break;
        case EBinaryOperation::And:
            type = EJsonPathItemType::BinaryAnd;
            break;
        case EBinaryOperation::Or:
            type = EJsonPathItemType::BinaryOr;
            break;
    }

    WriteTwoInputsItem(type, node, node.GetLeftExpr(), node.GetRightExpr());
}

void TJsonPathBuilder::VisitBooleanLiteral(const TBooleanLiteralNode& node) {
    WriteZeroInputItem(EJsonPathItemType::BooleanLiteral, node);
    WriteBool(node.GetValue());
}

void TJsonPathBuilder::VisitNullLiteral(const TNullLiteralNode& node) {
    WriteZeroInputItem(EJsonPathItemType::NullLiteral, node);
}

void TJsonPathBuilder::VisitStringLiteral(const TStringLiteralNode& node) {
    WriteZeroInputItem(EJsonPathItemType::StringLiteral, node);
    WriteString(node.GetValue());
}

void TJsonPathBuilder::VisitFilterObject(const TFilterObjectNode& node) {
    WriteZeroInputItem(EJsonPathItemType::FilterObject, node);
}

void TJsonPathBuilder::VisitFilterPredicate(const TFilterPredicateNode& node) {
    WriteTwoInputsItem(EJsonPathItemType::FilterPredicate, node, node.GetPredicate(), node.GetInput());
}

void TJsonPathBuilder::VisitMethodCall(const TMethodCallNode& node) {
    EJsonPathItemType type;
    switch (node.GetType()) {
        case EMethodType::Abs:
            type = EJsonPathItemType::AbsMethod;
            break;
        case EMethodType::Floor:
            type = EJsonPathItemType::FloorMethod;
            break;
        case EMethodType::Ceiling:
            type = EJsonPathItemType::CeilingMethod;
            break;
        case EMethodType::Double:
            type = EJsonPathItemType::DoubleMethod;
            break;
        case EMethodType::Type:
            type = EJsonPathItemType::TypeMethod;
            break;
        case EMethodType::Size:
            type = EJsonPathItemType::SizeMethod;
            break;
        case EMethodType::KeyValue:
            type = EJsonPathItemType::KeyValueMethod;
            break;
    }

    WriteSingleInputItem(type, node, node.GetInput());
}

TJsonPathPtr TJsonPathBuilder::ShrinkAndGetResult() {
    Result->ShrinkToFit();
    return Result;
}

void TJsonPathBuilder::VisitStartsWithPredicate(const TStartsWithPredicateNode& node) {
    WriteTwoInputsItem(EJsonPathItemType::StartsWithPredicate, node, node.GetPrefix(), node.GetInput());
}

void TJsonPathBuilder::VisitExistsPredicate(const TExistsPredicateNode& node) {
    WriteSingleInputItem(EJsonPathItemType::ExistsPredicate, node, node.GetInput());
}

void TJsonPathBuilder::VisitIsUnknownPredicate(const TIsUnknownPredicateNode& node) {
    WriteSingleInputItem(EJsonPathItemType::IsUnknownPredicate, node, node.GetInput());
}

void TJsonPathBuilder::VisitLikeRegexPredicate(const TLikeRegexPredicateNode& node) {
    // Block structure:
    // <(1) TUint> <(2) TUint> <(3) TUint> <(4) TUint> <(5) char[]> <(6) TUint>
    // Components:
    // (1) Must be casted to EJsonPathItemType. Member access item type
    // (2) Row of the position in the source jsonpath
    // (3) Column of the position in the source jsonpath
    // (4) Length of serialized Hyperscan database
    // (5) Serialized Hyperscan database
    // (6) Offset of the input item
    WriteType(EJsonPathItemType::LikeRegexPredicate);
    WritePos(node);

    const TString serializedRegex = node.GetRegex()->Serialize();
    WriteString(serializedRegex);

    WriteNextPosition();
    node.GetInput()->Accept(*this);
}

void TJsonPathBuilder::WriteZeroInputItem(EJsonPathItemType type, const TAstNode& node) {
    // Block structure:
    // <(1) TUint> <(2) TUint> <(3) TUint>
    // Components:
    // (1) Item type
    // (2) Row of the position in the source jsonpath
    // (3) Column of the position in the source jsonpath
    WriteType(type);
    WritePos(node);
}

void TJsonPathBuilder::WriteSingleInputItem(EJsonPathItemType type, const TAstNode& node, const TAstNodePtr input) {
    // Block structure:
    // <(1) TUint> <(2) TUint> <(3) TUint> <(4) TUint> <(5) item>
    // Components:
    // (1) Item type
    // (2) Row of the position in the source jsonpath
    // (3) Column of the position in the source jsonpath
    // (4) Offset of the input item
    // (5) Input item
    WriteZeroInputItem(type, node);

    WriteNextPosition();
    input->Accept(*this);
}

void TJsonPathBuilder::WriteTwoInputsItem(EJsonPathItemType type, const TAstNode& node, const TAstNodePtr firstInput, const TAstNodePtr secondInput) {
    // Block structure:
    // <(1) TUint> <(2) TUint> <(3) TUint> <(4) TUint> <(5) TUint> <(6) item> <(7) item>
    // Components:
    // (1) Item type
    // (2) Row of the position in the source jsonpath
    // (3) Column of the position in the source jsonpath
    // (4) Offset of the first input
    // (5) Offset of the second input
    // (6) JsonPath item representing first input
    // (7) JsonPath item representing right input
    WriteZeroInputItem(type, node);

    // (4) and (5) Fill offsets with zeros
    const auto indexStart = CurrentEndPos();
    WriteUint(0);
    WriteUint(0);

    // (6) Write first input and record it's offset
    const auto firstInputStart = CurrentEndPos();
    firstInput->Accept(*this);

    // (7) Write second input and record it's offset
    const auto secondInputStart = CurrentEndPos();
    secondInput->Accept(*this);

    // (4) and (5) Rewrite offsets with correct values
    RewriteUintSequence({firstInputStart, secondInputStart}, indexStart);
}

void TJsonPathBuilder::WritePos(const TAstNode& node) {
    WriteUint(node.GetPos().Row);
    WriteUint(node.GetPos().Column);
}

void TJsonPathBuilder::WriteType(EJsonPathItemType type) {
    WriteUint(static_cast<TUint>(type));
}

void TJsonPathBuilder::WriteMode(EJsonPathMode mode) {
    WriteUint(static_cast<TUint>(mode));
}

void TJsonPathBuilder::WriteNextPosition() {
    WriteUint(CurrentEndPos() + sizeof(TUint));
}

void TJsonPathBuilder::WriteFinishPosition() {
    WriteUint(0);
}

void TJsonPathBuilder::WriteString(TStringBuf value) {
    WriteUint(value.size());
    Result->Append(value.Data(), value.size());
}

void TJsonPathBuilder::RewriteUintSequence(const TVector<TUint>& sequence, TUint offset) {
    const auto length = sequence.size() * sizeof(TUint);
    Y_ASSERT(offset + length < CurrentEndPos());

    MemCopy(Result->Data() + offset, reinterpret_cast<const char*>(sequence.data()), length);
}

void TJsonPathBuilder::WriteUintSequence(const TVector<TUint>& sequence) {
    const auto length = sequence.size() * sizeof(TUint);
    Result->Append(reinterpret_cast<const char*>(sequence.data()), length);
}

void TJsonPathBuilder::RewriteUint(TUint value, TUint offset) {
    Y_ASSERT(offset + sizeof(TUint) < CurrentEndPos());

    MemCopy(Result->Data() + offset, reinterpret_cast<const char*>(&value), sizeof(TUint));
}

void TJsonPathBuilder::WriteUint(TUint value) {
    WritePOD(value);
}

void TJsonPathBuilder::WriteDouble(double value) {
    WritePOD(value);
}

void TJsonPathBuilder::WriteBool(bool value) {
    WritePOD(value);
}

TUint TJsonPathBuilder::CurrentEndPos() const {
    return Result->Size();
}


}
