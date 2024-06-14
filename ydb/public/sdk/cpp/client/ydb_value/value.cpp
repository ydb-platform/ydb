#include "value.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/value_helpers/helpers.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/uuid/uuid.h>

#include <util/generic/bitmap.h>
#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NYdb {

static void CheckKind(TTypeParser::ETypeKind actual, TTypeParser::ETypeKind expected, const TString& method)
{
    if (expected != actual) {
        ThrowFatalError(TStringBuilder() << method << "(): invalid state, expected type: "
            << expected << ", actual: " << actual);
    }
}

static TTypeParser::ETypeKind GetKind(const Ydb::Type& type) {
    using ETypeKind = TTypeParser::ETypeKind;

    switch (type.type_case()) {
        case Ydb::Type::kTypeId:
            return ETypeKind::Primitive;
        case Ydb::Type::kDecimalType:
            return ETypeKind::Decimal;
        case Ydb::Type::kPgType:
            return ETypeKind::Pg;
        case Ydb::Type::kOptionalType:
            return ETypeKind::Optional;
        case Ydb::Type::kListType:
            return ETypeKind::List;
        case Ydb::Type::kTupleType:
            return ETypeKind::Tuple;
        case Ydb::Type::kStructType:
            return ETypeKind::Struct;
        case Ydb::Type::kDictType:
            return ETypeKind::Dict;
        case Ydb::Type::kVariantType:
            return ETypeKind::Variant;
        case Ydb::Type::kVoidType:
            return ETypeKind::Void;
        case Ydb::Type::kNullType:
            return ETypeKind::Null;
        case Ydb::Type::kEmptyListType:
            return ETypeKind::EmptyList;
        case Ydb::Type::kEmptyDictType:
            return ETypeKind::EmptyDict;
        case Ydb::Type::kTaggedType:
            return ETypeKind::Tagged;
        default:
            break;
    }

    ThrowFatalError(TStringBuilder() << "Unexpected proto type kind: " << (ui32) type.type_case());
    return ETypeKind::Void;
}

bool TypesEqual(const TType& t1, const TType& t2) {
    return TypesEqual(t1.GetProto(), t2.GetProto());
}

////////////////////////////////////////////////////////////////////////////////

class TType::TImpl {
public:
    TImpl(const Ydb::Type& typeProto)
        : ProtoType_(typeProto) {}

    TImpl(Ydb::Type&& typeProto)
        : ProtoType_(std::move(typeProto)) {}

    Ydb::Type ProtoType_;
};

////////////////////////////////////////////////////////////////////////////////

TType::TType(const Ydb::Type& typeProto)
    : Impl_(new TImpl(typeProto)) {}

TType::TType(Ydb::Type&& typeProto)
    : Impl_(new TImpl(std::move(typeProto))) {}

TString TType::ToString() const {
    return FormatType(*this);
}

void TType::Out(IOutputStream& o) const {
    o << FormatType(*this);
}

const Ydb::Type& TType::GetProto() const {
    return Impl_->ProtoType_;
}

Ydb::Type& TType::GetProto()
{
    return Impl_->ProtoType_;
}

////////////////////////////////////////////////////////////////////////////////

class TTypeParser::TImpl {
public:
    TImpl(const TType& type)
        : Type_(type)
    {
        Reset();
    }

    void Reset() {
        Path_.clear();
        Path_.emplace_back(TProtoPosition{&Type_.GetProto(), -1});
    }

    ETypeKind GetKind(ui32 offset = 0) const {
        return NYdb::GetKind(GetProto(offset));
    }

    EPrimitiveType GetPrimitive() const {
        CheckKind(ETypeKind::Primitive, "GetPrimitive");
        return EPrimitiveType(GetProto().type_id());
    }

    TDecimalType GetDecimal() const {
        CheckKind(ETypeKind::Decimal, "GetDecimal");
        return TDecimalType(
            GetProto().decimal_type().precision(),
            GetProto().decimal_type().scale());
    }

    TPgType GetPg() const {
        CheckKind(ETypeKind::Pg, "GetPg");
        const auto& pg = GetProto().pg_type();
        TPgType type(pg.type_name(), pg.type_modifier());
        type.Oid = pg.oid();
        type.Typlen = pg.typlen();
        type.Typmod = pg.typmod();
        return type;
    }

    template<ETypeKind kind>
    void Open() {
        CheckKind(kind, "Open");
        ForwardStep();
    }

    void OpenVariant(int index) {
        CheckKind(ETypeKind::Variant, "Open");
        const Ydb::VariantType& variantType = GetProto().variant_type();
        const google::protobuf::Message* nextPtr = nullptr;
        switch (variantType.type_case()) {
            case Ydb::VariantType::kTupleItems: {
                auto& tupleType = variantType.tuple_items();
                if (index >= tupleType.elements_size()) {
                    return FatalError("variant index is out of range");
                }
                nextPtr = &tupleType.elements(index);
                break;
            }
            case Ydb::VariantType::kStructItems: {
                auto& structType = variantType.struct_items();
                if (index >= structType.members_size()) {
                    return FatalError("variant index is out of range");
                }
                nextPtr = &structType.members(index).type();
                break;
            }
            default: {
                return FatalError("unknown variant type case");
            }
        }
        Path_.emplace_back(TProtoPosition{nextPtr, -1});
    }

    template<ETypeKind kind>
    void Close() {
        CheckPreviousKind(kind, "Close");
        BackwardStep();
        Path_.back().Idx = -1;
    }

    const TString& GetMemberName() {
        CheckPreviousKind(ETypeKind::Struct, "GetMemberName");
        return GetProto(1).struct_type().members(Path_[Path_.size() - 2].Idx).name();
    }

    template<ETypeKind kind>
    bool TryNext() {
        CheckPreviousKind(kind, "TryNext");

        BackwardStep();
        Path_.back().Idx++;
        return ForwardStep();
    }

    void DictKey() {
        CheckPreviousKind(ETypeKind::Dict, "DictKey");
        BackwardStep();
        Path_.back().Idx = 0;
        ForwardStep();
    }

    void DictPayload() {
        CheckPreviousKind(ETypeKind::Dict, "DictPayload");
        BackwardStep();
        Path_.back().Idx = 1;
        ForwardStep();
    }

    const TString& GetTag() {
        CheckPreviousKind(ETypeKind::Tagged, "GetTag");
        return GetProto(1).tagged_type().tag();
    }

    bool ForwardStep() {
        auto& idx = Path_.back().Idx;
        const google::protobuf::Message* nextPtr = nullptr;
        bool hasIdx = true;

        switch (GetKind()) {
            case ETypeKind::Optional:
                nextPtr = &GetProto().optional_type().item();
                break;

            case ETypeKind::List:
                nextPtr = &GetProto().list_type().item();
                break;

            case ETypeKind::Struct: {
                if (idx >= 0) {
                    auto& structType = GetProto().struct_type();
                    if (idx >= structType.members_size()) {
                        idx = structType.members_size() - 1;
                        hasIdx = false;
                    }
                    if (idx >= 0) {
                        nextPtr = &structType.members(idx).type();
                    }
                } else {
                    nextPtr = &GetProto();
                }
                break;
            }

            case ETypeKind::Tuple: {
                if (idx >= 0) {
                    auto& tupleType = GetProto().tuple_type();
                    if (idx >= tupleType.elements_size()) {
                        idx = tupleType.elements_size() - 1;
                        hasIdx = false;
                    }
                    if (idx >= 0) {
                        nextPtr = &tupleType.elements(idx);
                    }
                } else {
                    nextPtr = &GetProto();
                }
                break;
            }

            case ETypeKind::Dict: {
                if (idx == 0) {
                    nextPtr = &GetProto().dict_type().key();
                } else if (idx == 1) {
                    nextPtr = &GetProto().dict_type().payload();
                } else {
                    nextPtr = &GetProto();
                }
                break;
            }

            case ETypeKind::Variant: {
                const Ydb::VariantType& variantType = GetProto().variant_type();
                auto wrappedVariant = std::make_unique<Ydb::Type>();
                switch (variantType.type_case()) {
                    case Ydb::VariantType::kTupleItems: {
                        *wrappedVariant->mutable_tuple_type() = variantType.tuple_items();
                        break;
                    }
                    case Ydb::VariantType::kStructItems: {
                        *wrappedVariant->mutable_struct_type() = variantType.struct_items();
                        break;
                    }
                    default: {
                        FatalError(TStringBuilder() << "Unexpected variant type kind: " << variantType);
                        break;
                    }
                }
                WrappedVariants_.emplace_back(std::move(wrappedVariant));
                nextPtr = WrappedVariants_.back().get();
                break;
            }

            case ETypeKind::Tagged: {
                nextPtr = &GetProto().tagged_type().type();
                break;
            }

            default:
                FatalError(TStringBuilder() << "Unexpected type kind: " << GetKind());
                break;
        }

        Path_.emplace_back(TProtoPosition{nextPtr, -1});
        return hasIdx;
    }

private:
    void CheckKind(ETypeKind kind, const TString& method) const {
        NYdb::CheckKind(GetKind(), kind, method);
    }

    void CheckPreviousKind(ETypeKind kind, const TString method) const {
        if (Path_.size() < 2) {
            FatalError("Expected container type.");
            return;
        }

        NYdb::CheckKind(GetKind(1), kind, method);
    }

    const Ydb::Type& GetProto(ui32 offset = 0) const {
        return *static_cast<const Ydb::Type*>(Path_[Path_.size() - (offset + 1)].Ptr);
    }

    void BackwardStep() {
        Path_.pop_back();
    }

    void FatalError(const TString& msg) const {
        ThrowFatalError(TStringBuilder() << "TTypeParser: " << msg);
    }

private:
    struct TProtoPosition {
        const google::protobuf::Message* Ptr;
        i32 Idx;
    };

private:
    TType Type_;
    TStackVec<TProtoPosition, 8> Path_;
    TStackVec<std::unique_ptr<Ydb::Type>, 8> WrappedVariants_;
};

////////////////////////////////////////////////////////////////////////////////

TTypeParser::TTypeParser(TTypeParser&&) = default;
TTypeParser::~TTypeParser() = default;

TTypeParser::TTypeParser(const TType& type)
    : Impl_(new TImpl(type)) {}

TTypeParser::ETypeKind TTypeParser::GetKind() const {
    return Impl_->GetKind();
}

EPrimitiveType TTypeParser::GetPrimitive() const {
    return Impl_->GetPrimitive();
}

TDecimalType TTypeParser::GetDecimal() const {
    return Impl_->GetDecimal();
}

TPgType TTypeParser::GetPg() const {
    return Impl_->GetPg();
}

void TTypeParser::OpenOptional() {
    Impl_->Open<ETypeKind::Optional>();
}

void TTypeParser::CloseOptional() {
    Impl_->Close<ETypeKind::Optional>();
}

void TTypeParser::OpenList() {
    Impl_->Open<ETypeKind::List>();
}

void TTypeParser::CloseList() {
    Impl_->Close<ETypeKind::List>();
}

void TTypeParser::OpenStruct() {
    Impl_->Open<ETypeKind::Struct>();
}

void TTypeParser::CloseStruct() {
    Impl_->Close<ETypeKind::Struct>();
}

const TString& TTypeParser::GetMemberName() {
    return Impl_->GetMemberName();
}

bool TTypeParser::TryNextMember() {
    return Impl_->TryNext<ETypeKind::Struct>();
}

void TTypeParser::OpenTuple() {
    Impl_->Open<ETypeKind::Tuple>();
}

void TTypeParser::CloseTuple() {
    Impl_->Close<ETypeKind::Tuple>();
}

bool TTypeParser::TryNextElement() {
    return Impl_->TryNext<ETypeKind::Tuple>();
}

void TTypeParser::OpenDict() {
    Impl_->Open<ETypeKind::Dict>();
}

void TTypeParser::CloseDict() {
    Impl_->Close<ETypeKind::Dict>();
}

void TTypeParser::DictKey() {
    Impl_->DictKey();
}

void TTypeParser::DictPayload() {
    Impl_->DictPayload();
}

void TTypeParser::OpenVariant(size_t index) {
    Impl_->OpenVariant(index);
}

void TTypeParser::OpenVariant() {
    Impl_->Open<ETypeKind::Variant>();
}

void TTypeParser::CloseVariant() {
    Impl_->Close<ETypeKind::Variant>();
}

void TTypeParser::OpenTagged() {
    Impl_->Open<ETypeKind::Tagged>();
}

const TString& TTypeParser::GetTag() {
    return Impl_->GetTag();
}

void TTypeParser::CloseTagged() {
    Impl_->Close<ETypeKind::Tagged>();
}

////////////////////////////////////////////////////////////////////////////////

void FormatTypeInternal(TTypeParser& parser, IOutputStream& out) {
    switch (parser.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            out << parser.GetPrimitive();
            break;

        case TTypeParser::ETypeKind::Decimal: {
            auto decimal = parser.GetDecimal();
            out << "Decimal(" << (ui32)decimal.Precision << ',' << (ui32)decimal.Scale << ")";
            break;
        }

        case TTypeParser::ETypeKind::Pg: {
            auto pg = parser.GetPg();
            out << "Pg('"sv << pg.TypeName << "\',\'" << pg.TypeModifier << "\',"
                << pg.Oid << "," << pg.Typlen << ',' << pg.Typmod << ')';
            break;
        }

        case TTypeParser::ETypeKind::Optional:
            parser.OpenOptional();
            FormatTypeInternal(parser, out);
            parser.CloseOptional();
            out << '?';
            break;

        case TTypeParser::ETypeKind::Tagged:
            parser.OpenTagged();
            out << "Tagged<";
            FormatTypeInternal(parser, out);
            out << ",\'" << parser.GetTag() << "\'>";
            parser.CloseTagged();
            break;

        case TTypeParser::ETypeKind::List:
            out << "List<";
            parser.OpenList();
            FormatTypeInternal(parser, out);
            parser.CloseList();
            out << '>';
            break;

        case TTypeParser::ETypeKind::Tuple: {
            out << "Tuple<";
            parser.OpenTuple();
            bool needsComma = false;
            while (parser.TryNextElement()) {
                if (needsComma) {
                    out << ',';
                }
                FormatTypeInternal(parser, out);
                needsComma = true;
            }
            parser.CloseTuple();
            out << '>';
            break;
        }

        case TTypeParser::ETypeKind::Struct: {
            out << "Struct<";
            parser.OpenStruct();
            bool needsComma = false;
            while (parser.TryNextMember()) {
                if (needsComma) {
                    out << ',';
                }
                out << '\'' << parser.GetMemberName() << "\':";
                FormatTypeInternal(parser, out);
                needsComma = true;
            }
            parser.CloseStruct();
            out << '>';
            break;
        }

        case TTypeParser::ETypeKind::Dict:
            out << "Dict<";
            parser.OpenDict();
            parser.DictKey();
            FormatTypeInternal(parser, out);
            out << ',';
            parser.DictPayload();
            FormatTypeInternal(parser, out);
            parser.CloseDict();
            out << '>';
            break;

        case TTypeParser::ETypeKind::Variant:
            // TODO: Variant
            break;

        case TTypeParser::ETypeKind::Void:
            out << "Void"sv;
            break;

        case TTypeParser::ETypeKind::Null:
            out << "Null"sv;
            break;

        default:
            ThrowFatalError(TStringBuilder()
                << "Unexpected type kind: " << parser.GetKind());
    }
}

TString FormatType(const TType& type) {
    TTypeParser parser(type);
    TStringStream out;
    FormatTypeInternal(parser, out);
    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TTypeBuilder::TImpl {
    using ETypeKind = TTypeParser::ETypeKind;

public:
    TImpl()
    {
        Path_.emplace_back(TProtoPosition{&ProtoType_});
    }

    TImpl(Ydb::Type& type)
    {
        Path_.emplace_back(TProtoPosition{&type});
    }

    TType Build() {
        if (Path_.size() > 1) {
            FatalError("Invalid Build() call, type is incomplete.");
        }

        Ydb::Type type;
        type.Swap(&ProtoType_);
        return TType(std::move(type));
    }

    void Primitive(const EPrimitiveType& primitiveType) {
        GetProto().set_type_id(Ydb::Type::PrimitiveTypeId(primitiveType));
    }

    void Decimal(const TDecimalType& decimalType) {
        auto& decimal = *GetProto().mutable_decimal_type();
        decimal.set_precision(decimalType.Precision);
        decimal.set_scale(decimalType.Scale);
    }

    void Pg(const TPgType& pgType) {
        auto& pg = *GetProto().mutable_pg_type();
        pg.set_type_name(pgType.TypeName);
        pg.set_type_modifier(pgType.TypeModifier);
    }

    void BeginOptional() {
        AddPosition(GetProto().mutable_optional_type()->mutable_item());
    }

    void EndOptional() {
        CloseContainer<ETypeKind::Optional>();
    }

    void Optional(const TType& itemType) {
        GetProto().mutable_optional_type()->mutable_item()->CopyFrom(itemType.GetProto());
    }

    void BeginList() {
        AddPosition(GetProto().mutable_list_type()->mutable_item());
    }

    void EndList() {
        CloseContainer<ETypeKind::List>();
    }

    void List(const TType& itemType) {
        GetProto().mutable_list_type()->mutable_item()->CopyFrom(itemType.GetProto());
    }

    void BeginStruct() {
        GetProto().mutable_struct_type();
        AddPosition(&GetProto());
    }

    void EndStruct() {
        CloseContainer<ETypeKind::Struct>();
    }

    void AddMember(const TString& memberName) {
        CheckPreviousKind(ETypeKind::Struct, "AddMember");
        PopPosition();
        auto member = GetProto().mutable_struct_type()->add_members();
        member->set_name(memberName);
        AddPosition(member->mutable_type());
    }

    void AddMember(const TString& memberName, const TType& memberType) {
        AddMember(memberName);
        GetProto().CopyFrom(memberType.GetProto());
    }

    void SelectMember(size_t index) {
        CheckPreviousKind(ETypeKind::Struct, "SelectMember");
        PopPosition();
        auto member = GetProto().mutable_struct_type()->mutable_members(index);
        AddPosition(member->mutable_type());
    }

    void BeginTuple() {
        GetProto().mutable_tuple_type();
        AddPosition(&GetProto());
    }

    void EndTuple() {
        CloseContainer<ETypeKind::Tuple>();
    }

    void AddElement() {
        CheckPreviousKind(ETypeKind::Tuple, "AddElement");
        PopPosition();
        AddPosition(GetProto().mutable_tuple_type()->add_elements());
    }

    void AddElement(const TType& elementType) {
        AddElement();
        GetProto().CopyFrom(elementType.GetProto());
    }

    void SelectElement(size_t index) {
        CheckPreviousKind(ETypeKind::Tuple, "SelectElement");
        PopPosition();
        AddPosition(GetProto().mutable_tuple_type()->mutable_elements(index));
    }

    void BeginDict() {
        GetProto().mutable_dict_type();
        AddPosition(&GetProto());
    }

    void EndDict() {
        CloseContainer<ETypeKind::Dict>();
    }

    void DictKey() {
        CheckPreviousKind(ETypeKind::Dict, "DictKey");
        PopPosition();
        AddPosition(GetProto().mutable_dict_type()->mutable_key());
    }

    void DictKey(const TType& keyType) {
        DictKey();
        GetProto().CopyFrom(keyType.GetProto());
    }

    void DictPayload() {
        CheckPreviousKind(ETypeKind::Dict, "DictPayload");
        PopPosition();
        AddPosition(GetProto().mutable_dict_type()->mutable_payload());
    }

    void DictPayload(const TType& payloadType) {
        DictPayload();
        GetProto().CopyFrom(payloadType.GetProto());
    }

    void BeginTagged(const TString& tag) {
        GetProto().mutable_tagged_type()->set_tag(tag);
        AddPosition(GetProto().mutable_tagged_type()->mutable_type());
    }

    void EndTagged() {
        CloseContainer<ETypeKind::Tagged>();
    }

    void Tagged(const TString& tag, const TType& itemType) {
        auto taggedType = GetProto().mutable_tagged_type();
        taggedType->set_tag(tag);
        taggedType->mutable_type()->CopyFrom(itemType.GetProto());
    }

    Ydb::Type& GetProto(ui32 offset = 0) {
        return *static_cast<Ydb::Type*>(Path_[Path_.size() - (offset + 1)].Ptr);
    }

    void SetType(const TType& type) {
        GetProto().CopyFrom(type.GetProto());
    }

    void SetType(TType&& type) {
        GetProto() = std::move(type.GetProto());
    }

private:
    void AddPosition(Ydb::Type* type) {
        Path_.emplace_back(TProtoPosition{type});
    }

    void PopPosition() {
        Path_.pop_back();
    }

    void FatalError(const TString& msg) const {
        ThrowFatalError(TStringBuilder() << "TTypeBuilder: " << msg);
    }

    void CheckKind(ETypeKind kind, const TString& method) {
        NYdb::CheckKind(GetKind(), kind, method);
    }

    void CheckPreviousKind(ETypeKind kind, const TString& method) {
        NYdb::CheckKind(GetKind(1), kind, method);
    }

    ETypeKind GetKind(ui32 offset = 0) {
        return NYdb::GetKind(GetProto(offset));
    }

    template<ETypeKind kind>
    void CloseContainer() {
        auto sz = Path_.size();
        if (sz < 2) {
            FatalError("No opened container to close");
            return;
        }
        CheckPreviousKind(kind, "End");
        PopPosition();
    }

private:
    struct TProtoPosition {
        google::protobuf::Message* Ptr;
    };

private:
    Ydb::Type ProtoType_;
    TStackVec<TProtoPosition, 8> Path_;
};

////////////////////////////////////////////////////////////////////////////////

TTypeBuilder::TTypeBuilder(TTypeBuilder&&) = default;
TTypeBuilder::~TTypeBuilder() = default;

TTypeBuilder::TTypeBuilder()
    : Impl_(new TImpl()) {}

TType TTypeBuilder::Build() {
    return Impl_->Build();
}

TTypeBuilder& TTypeBuilder::Primitive(const EPrimitiveType& primitiveType) {
    Impl_->Primitive(primitiveType);
    return *this;
}

TTypeBuilder& TTypeBuilder::Decimal(const TDecimalType& decimalType) {
    Impl_->Decimal(decimalType);
    return *this;
}

TTypeBuilder& TTypeBuilder::Pg(const TPgType& pgType) {
    Impl_->Pg(pgType);
    return *this;
}

TTypeBuilder& TTypeBuilder::BeginOptional() {
    Impl_->BeginOptional();
    return *this;
}

TTypeBuilder& TTypeBuilder::EndOptional() {
    Impl_->EndOptional();
    return *this;
}

TTypeBuilder& TTypeBuilder::Optional(const TType& itemType) {
    Impl_->Optional(itemType);
    return *this;
}

TTypeBuilder& TTypeBuilder::BeginList() {
    Impl_->BeginList();
    return *this;
}

TTypeBuilder& TTypeBuilder::EndList() {
    Impl_->EndList();
    return *this;
}

TTypeBuilder& TTypeBuilder::List(const TType& itemType) {
    Impl_->List(itemType);
    return *this;
}

TTypeBuilder& TTypeBuilder::BeginStruct() {
    Impl_->BeginStruct();
    return *this;
}

TTypeBuilder& TTypeBuilder::EndStruct() {
    Impl_->EndStruct();
    return *this;
}

TTypeBuilder& TTypeBuilder::AddMember(const TString& memberName) {
    Impl_->AddMember(memberName);
    return *this;
}

TTypeBuilder& TTypeBuilder::AddMember(const TString& memberName, const TType& memberType) {
    Impl_->AddMember(memberName, memberType);
    return *this;
}

TTypeBuilder& TTypeBuilder::BeginTuple() {
    Impl_->BeginTuple();
    return *this;
}

TTypeBuilder& TTypeBuilder::EndTuple() {
    Impl_->EndTuple();
    return *this;
}

TTypeBuilder& TTypeBuilder::AddElement() {
    Impl_->AddElement();
    return *this;
}

TTypeBuilder& TTypeBuilder::AddElement(const TType& elementType) {
    Impl_->AddElement(elementType);
    return *this;
}

TTypeBuilder& TTypeBuilder::BeginDict() {
    Impl_->BeginDict();
    return *this;
}

TTypeBuilder& TTypeBuilder::DictKey() {
    Impl_->DictKey();
    return *this;
}

TTypeBuilder& TTypeBuilder::DictKey(const TType& keyType) {
    Impl_->DictKey(keyType);
    return *this;
}

TTypeBuilder& TTypeBuilder::DictPayload() {
    Impl_->DictPayload();
    return *this;
}

TTypeBuilder& TTypeBuilder::DictPayload(const TType& payloadType) {
    Impl_->DictPayload(payloadType);
    return *this;
}

TTypeBuilder& TTypeBuilder::EndDict() {
    Impl_->EndDict();
    return *this;
}

TTypeBuilder& TTypeBuilder::BeginTagged(const TString& tag) {
    Impl_->BeginTagged(tag);
    return *this;
}

TTypeBuilder& TTypeBuilder::EndTagged() {
    Impl_->EndTagged();
    return *this;
}

TTypeBuilder& TTypeBuilder::Tagged(const TString& tag, const TType& itemType) {
    Impl_->Tagged(tag, itemType);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TDecimalValue::TDecimalValue(const Ydb::Value& valueProto, const TDecimalType& decimalType)
    : DecimalType_(decimalType)
    , Low_(valueProto.low_128())
    , Hi_(valueProto.high_128())
{}

TDecimalValue::TDecimalValue(const TString& decimalString, ui8 precision, ui8 scale)
    : DecimalType_(precision, scale)
{
    NYql::NDecimal::TInt128 val = NYql::NDecimal::FromString(decimalString, precision, scale);
    static_assert(sizeof(val) == 16, "wrong TInt128 size");
    char* buf = reinterpret_cast<char*>(&val);
    Low_ = *(ui64*)buf;
    Hi_ = *(i64*)(buf + 8);
}

TString TDecimalValue::ToString() const {
    NYql::NDecimal::TInt128 val = NYql::NDecimal::FromHalfs(Low_, Hi_);
    return NYql::NDecimal::ToString(val, DecimalType_.Precision, DecimalType_.Scale);
}

////////////////////////////////////////////////////////////////////////////////

TPgValue::TPgValue(const Ydb::Value& pgValueProto, const TPgType& pgType)
    : PgType_(pgType)
{
    if (pgValueProto.has_text_value()) {
        Kind_ = VK_TEXT;
        Content_ = pgValueProto.text_value();
        return;
    }

    if (pgValueProto.has_bytes_value()) {
        Kind_ = VK_BINARY;
        Content_ = pgValueProto.bytes_value();
        return;
    }

    Kind_ = VK_NULL;
}

TPgValue::TPgValue(EPgValueKind kind, const TString& content, const TPgType& pgType)
    : PgType_(pgType)
    , Kind_(kind)
    , Content_(content)
{
}

bool TPgValue::IsNull() const {
    return Kind_ == VK_NULL;
}

bool TPgValue::IsText() const {
    return Kind_ == VK_TEXT;
}

////////////////////////////////////////////////////////////////////////////////

TUuidValue::TUuidValue(ui64 low_128, ui64 high_128) {
    Buf_.Halfs[0] = low_128;
    Buf_.Halfs[1] = high_128;
}

TUuidValue::TUuidValue(const Ydb::Value& valueProto) {
    Buf_.Halfs[0] = valueProto.low_128();
    Buf_.Halfs[1] = valueProto.high_128();
}

TUuidValue::TUuidValue(const TString& uuidString) {
    ui16 dw[8];
    if (!NKikimr::NUuid::ParseUuidToArray(uuidString, dw, false)) {
        ThrowFatalError(TStringBuilder() << "Unable to parse string as uuid");
    }
    static_assert(sizeof(dw) == sizeof(Buf_.Bytes));
    // TODO: check output on big-endian machines here and everywhere.
    std::memcpy(Buf_.Bytes, dw, sizeof(dw));
}

TString TUuidValue::ToString() const {
    TStringStream s;
    ui16 dw[8];
    static_assert(sizeof(dw) == sizeof(Buf_.Bytes));
    std::memcpy(dw, Buf_.Bytes, sizeof(dw));
    NKikimr::NUuid::UuidToString(dw, s);
    return s.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TValue::TImpl {
public:
    TImpl(const TType& type, const Ydb::Value& valueProto)
        : Type_(type)
        , ProtoValue_(valueProto) {}

    TImpl(const TType& type, Ydb::Value&& valueProto)
        : Type_(type)
        , ProtoValue_(std::move(valueProto)) {}

    TType Type_;
    Ydb::Value ProtoValue_;
};

////////////////////////////////////////////////////////////////////////////////

TValue::TValue(const TType& type, const Ydb::Value& valueProto)
    : Impl_(new TImpl(type, valueProto)) {}

TValue::TValue(const TType& type, Ydb::Value&& valueProto)
    : Impl_(new TImpl(type, std::move(valueProto))) {}

const TType& TValue::GetType() const {
    return Impl_->Type_;
}

TType & TValue::GetType() {
    return Impl_->Type_;
}

const Ydb::Value& TValue::GetProto() const {
    return Impl_->ProtoValue_;
}

Ydb::Value& TValue::GetProto() {
    return Impl_->ProtoValue_;
}

////////////////////////////////////////////////////////////////////////////////

class TValueParser::TImpl {
    using ETypeKind = TTypeParser::ETypeKind;

    enum class EParseKind {
        Value,
        Null,
        Items,
        Pairs,
        Pair
    };

    struct TProtoPosition {
        EParseKind Kind;
        const google::protobuf::Message* Ptr;
        i32 Idx;
    };

public:
    TImpl(const TValue& value)
        : Value_(value.Impl_)
        , TypeParser_(value.GetType())
    {
        Reset(Value_->ProtoValue_);
    }

    TImpl(const TType& type)
        : TypeParser_(type) {}

    void Reset(const Ydb::Value& value) {
        TypeParser_.Impl_->Reset();
        Path_.clear();
        AddPath(EParseKind::Value, &value);
    }

    ETypeKind GetKind() const {
        return TypeParser_.GetKind();
    }

    EPrimitiveType GetPrimitiveType() const {
        CheckKind(ETypeKind::Primitive, "GetPrimitiveType");
        return TypeParser_.GetPrimitive();
    }

    bool GetBool() const {
        CheckPrimitive(NYdb::EPrimitiveType::Bool);
        return GetProto().bool_value();
    }

    i8 GetInt8() const {
        CheckPrimitive(NYdb::EPrimitiveType::Int8);
        return GetProto().int32_value();
    }

    ui8 GetUint8() const {
        CheckPrimitive(NYdb::EPrimitiveType::Uint8);
        return GetProto().uint32_value();
    }

    i16 GetInt16() const {
        CheckPrimitive(NYdb::EPrimitiveType::Int16);
        return GetProto().int32_value();
    }

    ui16 GetUint16() const {
        CheckPrimitive(NYdb::EPrimitiveType::Uint16);
        return GetProto().uint32_value();
    }

    i32 GetInt32() const {
        CheckPrimitive(NYdb::EPrimitiveType::Int32);
        return GetProto().int32_value();
    }

    ui32 GetUint32() const {
        CheckPrimitive(NYdb::EPrimitiveType::Uint32);
        return GetProto().uint32_value();
    }

    i64 GetInt64() const {
        CheckPrimitive(NYdb::EPrimitiveType::Int64);
        return GetProto().int64_value();
    }

    ui64 GetUint64() const {
        CheckPrimitive(NYdb::EPrimitiveType::Uint64);
        return GetProto().uint64_value();
    }

    float GetFloat() const {
        CheckPrimitive(NYdb::EPrimitiveType::Float);
        return GetProto().float_value();
    }

    double GetDouble() const {
        CheckPrimitive(NYdb::EPrimitiveType::Double);
        return GetProto().double_value();
    }

    TInstant GetDate() const {
        CheckPrimitive(NYdb::EPrimitiveType::Date);
        return TInstant::Days(GetProto().uint32_value());
    }

    TInstant GetDatetime() const {
        CheckPrimitive(NYdb::EPrimitiveType::Datetime);
        return TInstant::Seconds(GetProto().uint32_value());
    }

    TInstant GetTimestamp() const {
        CheckPrimitive(NYdb::EPrimitiveType::Timestamp);
        return TInstant::MicroSeconds(GetProto().uint64_value());
    }

    i64 GetInterval() const {
        CheckPrimitive(NYdb::EPrimitiveType::Interval);
        return GetProto().int64_value();
    }

    i32 GetDate32() const {
        CheckPrimitive(NYdb::EPrimitiveType::Date32);
        return GetProto().int32_value();
    }

    i64 GetDatetime64() const {
        CheckPrimitive(NYdb::EPrimitiveType::Datetime64);
        return GetProto().int64_value();
    }

    i64 GetTimestamp64() const {
        CheckPrimitive(NYdb::EPrimitiveType::Timestamp64);
        return GetProto().int64_value();
    }

    i64 GetInterval64() const {
        CheckPrimitive(NYdb::EPrimitiveType::Interval64);
        return GetProto().int64_value();
    }

    const TString& GetTzDate() const {
        CheckPrimitive(NYdb::EPrimitiveType::TzDate);
        return GetProto().text_value();
    }

    const TString& GetTzDatetime() const {
        CheckPrimitive(NYdb::EPrimitiveType::TzDatetime);
        return GetProto().text_value();
    }

    const TString& GetTzTimestamp() const {
        CheckPrimitive(NYdb::EPrimitiveType::TzTimestamp);
        return GetProto().text_value();
    }

    const TString& GetString() const {
        CheckPrimitive(NYdb::EPrimitiveType::String);
        return GetProto().bytes_value();
    }

    const TString& GetUtf8() const {
        CheckPrimitive(NYdb::EPrimitiveType::Utf8);
        return GetProto().text_value();
    }

    const TString& GetYson() const {
        CheckPrimitive(NYdb::EPrimitiveType::Yson);
        return GetProto().bytes_value();
    }

    const TString& GetJson() const {
        CheckPrimitive(NYdb::EPrimitiveType::Json);
        return GetProto().text_value();
    }

    TUuidValue GetUuid() const {
        CheckPrimitive(NYdb::EPrimitiveType::Uuid);
        return TUuidValue(GetProto());
    }

    const TString& GetJsonDocument() const {
        CheckPrimitive(NYdb::EPrimitiveType::JsonDocument);
        return GetProto().text_value();
    }

    const TString& GetDyNumber() const {
        CheckPrimitive(NYdb::EPrimitiveType::DyNumber);
        return GetProto().text_value();
    }

    TDecimalValue GetDecimal() const {
        CheckDecimal();
        return TDecimalValue(GetProto(), TypeParser_.GetDecimal());
    }

    TPgValue GetPg() const {
        CheckPg();
        return TPgValue(GetProto(), TypeParser_.GetPg());
    }

    void OpenOptional() {
        TypeParser_.OpenOptional();

        if (GetProto().value_case() == Ydb::Value::kNestedValue) {
            AddPath(EParseKind::Value, &GetProto().nested_value());
        } else if (GetProto().value_case() == Ydb::Value::kNullFlagValue) {
            AddPath(EParseKind::Null, &GetProto());
        } else {
            AddPath(EParseKind::Value, &GetProto());
        }
    }

    bool IsNull() const {
        return GetPathBack().Kind == EParseKind::Null;
    }

    void CloseOptional() {
        PopPath();
        TypeParser_.CloseOptional();
    }

    void OpenList() {
        TypeParser_.OpenList();
        OpenItems();
    }

    bool TryNextListItem() {
        return NextItems();
    }

    void CloseList() {
        CloseItems();
        TypeParser_.CloseList();
    }

    void OpenStruct() {
        TypeParser_.OpenStruct();
        OpenItems();
    }

    bool TryNextMember() {
        if (TypeParser_.TryNextMember()) {
            if (NextItems()) {
                return true;
            }

            FatalError(TStringBuilder() << "Missing struct 'items' value at index: " << GetPathBack().Idx);
        }

        return false;
    }

    const TString& GetMemberName() {
        return TypeParser_.GetMemberName();
    }

    void CloseStruct() {
        CloseItems();
        TypeParser_.CloseStruct();
    }

    void OpenTuple() {
        TypeParser_.OpenTuple();
        OpenItems();
    }

    bool TryNextElement() {
        if (TypeParser_.TryNextElement()) {
            if (NextItems()) {
                return true;
            }

            FatalError(TStringBuilder() << "Missing tuple 'items' value at index: " << GetPathBack().Idx);
        }

        return false;
    }

    void CloseTuple() {
        CloseItems();
        TypeParser_.CloseTuple();
    }

    void OpenDict() {
        TypeParser_.OpenDict();
        OpenPairs();
    }

    bool TryNextDictItem() {
        return NextPairs();
    }

    void DictKey() {
        TypeParser_.DictKey();

        if (GetPathBack().Kind != EParseKind::Pair) {
            PopPath();
        }

        AddPath(EParseKind::Value, &GetProtoPair().key());
    }

    void DictPayload() {
        TypeParser_.DictPayload();

        if (GetPathBack().Kind != EParseKind::Pair) {
            PopPath();
        }

        AddPath(EParseKind::Value, &GetProtoPair().payload());
    }

    void CloseDict() {
        ClosePairs();
        TypeParser_.CloseDict();
    }

    void OpenVariant() {
        auto variantIndex = GetProto().variant_index();
        TypeParser_.OpenVariant(variantIndex);
        if (GetProto().value_case() == Ydb::Value::kNestedValue) {
            AddPath(EParseKind::Value, &GetProto().nested_value());
        } else {
            FatalError(TStringBuilder() << "No nested value for variant type.");
        }
    }

    void CloseVariant() {
        PopPath();
        TypeParser_.CloseVariant();
    }

    void OpenTagged() {
        TypeParser_.OpenTagged();
    }

    const TString& GetTag() {
        return TypeParser_.GetTag();
    }

    void CloseTagged() {
        TypeParser_.CloseTagged();
    }

private:
    const TProtoPosition& GetPathBack() const {
        if (Path_.empty()) {
            FatalError(TStringBuilder() << "Bad parser state, no open value.");
        }

        return Path_.back();
    }

    void PopPath() {
        if (Path_.empty()) {
            FatalError(TStringBuilder() << "Bad parser state, no open value.");
        }

        Path_.pop_back();
    }

    void AddPath(EParseKind kind, const google::protobuf::Message* message, i32 idx = -1) {
        if (!Path_.empty()) {
            if (Path_.back().Kind == EParseKind::Null) {
                FatalError(TStringBuilder() << "Can't parse inside NULL value");
                return;
            }

            switch (Path_.back().Kind) {
                case EParseKind::Value:
                    break;

                case EParseKind::Pair:
                    if (kind != EParseKind::Value) {
                        FatalError(TStringBuilder() << "Bad parser state, expected dict pair.");
                        return;
                    }
                    break;

                default:
                    FatalError(TStringBuilder() << "Bad parser state, no value to parse.");
                    return;
            }
        }

        Path_.emplace_back(TProtoPosition{kind, message, idx});
    }

    void OpenItems(i32 idx = -1) {
        if (idx < 0) {
            AddPath(EParseKind::Items, &GetProto(), idx);
        } else {
            AddPath(EParseKind::Value, &GetProto().items(idx), idx);
        }
    }

    void CloseItems() {
        PopPath();
    }

    bool NextItems() {
        auto idx = GetPathBack().Idx;
        idx++;

        PopPath();

        bool isEnd = (idx == static_cast<i32>(GetProto().itemsSize()));
        if (isEnd) {
            idx--;
        }

        OpenItems(idx);

        return !isEnd;
    }

    void OpenPairs(i32 idx = -1) {
        if (idx < 0) {
            AddPath(EParseKind::Pairs, &GetProto(), idx);
        } else {
            AddPath(EParseKind::Pair, &GetProto().pairs(idx), idx);
        }
    }

    bool NextPairs() {
        if (GetPathBack().Kind != EParseKind::Pairs && GetPathBack().Kind != EParseKind::Pair) {
            PopPath();
        }

        auto idx = GetPathBack().Idx;
        idx++;

        PopPath();

        bool isEnd = (idx == static_cast<i32>(GetProto().pairsSize()));
        if (isEnd) {
            idx--;
        }

        OpenPairs(idx);

        return !isEnd;
    }

    void ClosePairs() {
        if (GetPathBack().Kind != EParseKind::Pairs && GetPathBack().Kind != EParseKind::Pair) {
            PopPath();
        }

        PopPath();
    }

    void CheckKind(ETypeKind kind, const TString& method) const {
        NYdb::CheckKind(TypeParser_.GetKind(), kind, method);
    }

    void CheckTransportKind(Ydb::Value::ValueCase expectedCase) const {
        if (expectedCase != GetProto().value_case()) {
            FatalError(TStringBuilder() << "Transport value case mismatch, requested: " << (ui32)expectedCase
                << ", actual: " << (ui32)GetProto().value_case());
        }
    }

    void CheckPrimitive(EPrimitiveType primitiveType) const {
        CheckKind(ETypeKind::Primitive, "Get");

        if (primitiveType != TypeParser_.GetPrimitive()) {
            FatalError(TStringBuilder() << "Type mismatch, requested: " << primitiveType
                << ", actual: " << TypeParser_.GetPrimitive());
        }

        CheckTransportKind(GetPrimitiveValueCase(primitiveType));
    }

    void CheckDecimal() const {
        CheckKind(ETypeKind::Decimal, "Get");
        CheckTransportKind(Ydb::Value::kLow128);
    }

    void CheckPg() const {
        CheckKind(ETypeKind::Pg, "Get");
    }

    const Ydb::Value& GetProto() const {
        return *static_cast<const Ydb::Value*>(GetPathBack().Ptr);
    }

    const Ydb::ValuePair& GetProtoPair() const {
        if (GetPathBack().Kind != EParseKind::Pair) {
            FatalError(TStringBuilder() << "Bad parser state, expected dict pair");
        }

        return *static_cast<const Ydb::ValuePair*>(GetPathBack().Ptr);
    }

    Ydb::Value::ValueCase GetPrimitiveValueCase(NYdb::EPrimitiveType primitiveTypeId) const {
        switch (primitiveTypeId) {
            case NYdb::EPrimitiveType::Bool:
                return Ydb::Value::kBoolValue;
            case NYdb::EPrimitiveType::Int8:
                return Ydb::Value::kInt32Value;
            case NYdb::EPrimitiveType::Uint8:
                return Ydb::Value::kUint32Value;
            case NYdb::EPrimitiveType::Int16:
                return Ydb::Value::kInt32Value;
            case NYdb::EPrimitiveType::Uint16:
                return Ydb::Value::kUint32Value;
            case NYdb::EPrimitiveType::Int32:
                return Ydb::Value::kInt32Value;
            case NYdb::EPrimitiveType::Uint32:
                return Ydb::Value::kUint32Value;
            case NYdb::EPrimitiveType::Int64:
                return Ydb::Value::kInt64Value;
            case NYdb::EPrimitiveType::Uint64:
                return Ydb::Value::kUint64Value;
            case NYdb::EPrimitiveType::Float:
                return Ydb::Value::kFloatValue;
            case NYdb::EPrimitiveType::Double:
                return Ydb::Value::kDoubleValue;
            case NYdb::EPrimitiveType::Date:
            case NYdb::EPrimitiveType::Datetime:
                return Ydb::Value::kUint32Value;
            case NYdb::EPrimitiveType::Date32:
                return Ydb::Value::kInt32Value;
            case NYdb::EPrimitiveType::Timestamp:
                return Ydb::Value::kUint64Value;
            case NYdb::EPrimitiveType::Interval:
            case NYdb::EPrimitiveType::Interval64:
            case NYdb::EPrimitiveType::Timestamp64:
            case NYdb::EPrimitiveType::Datetime64:
                return Ydb::Value::kInt64Value;
            case NYdb::EPrimitiveType::TzDate:
            case NYdb::EPrimitiveType::TzDatetime:
            case NYdb::EPrimitiveType::TzTimestamp:
                return Ydb::Value::kTextValue;
            case NYdb::EPrimitiveType::String:
                return Ydb::Value::kBytesValue;
            case NYdb::EPrimitiveType::Utf8:
                return Ydb::Value::kTextValue;
            case NYdb::EPrimitiveType::Yson:
                return Ydb::Value::kBytesValue;
            case NYdb::EPrimitiveType::Json:
            case NYdb::EPrimitiveType::JsonDocument:
            case NYdb::EPrimitiveType::DyNumber:
                return Ydb::Value::kTextValue;
            case NYdb::EPrimitiveType::Uuid:
                return Ydb::Value::kLow128;
            default:
                FatalError(TStringBuilder() << "Unexpected primitive type: " << primitiveTypeId);
                return Ydb::Value::kBytesValue;
        }
    }

    void FatalError(const TString& msg) const {
        ThrowFatalError(TStringBuilder() << "TValueParser: " << msg);
    }

private:
    std::shared_ptr<TValue::TImpl> Value_;
    TTypeParser TypeParser_;
    TStackVec<TProtoPosition, 8> Path_;
};

////////////////////////////////////////////////////////////////////////////////

TValueParser::TValueParser(TValueParser&&) = default;
TValueParser::~TValueParser() = default;

TValueParser::TValueParser(const TValue& value)
    : Impl_(new TImpl(value)) {}

TValueParser::TValueParser(const TType& type)
    : Impl_(new TImpl(type)) {}

void TValueParser::Reset(const Ydb::Value& value) {
    Impl_->Reset(value);
}

TTypeParser::ETypeKind TValueParser::GetKind() const {
    return Impl_->GetKind();
}

EPrimitiveType TValueParser::GetPrimitiveType() const {
    return Impl_->GetPrimitiveType();
}

////////////////////////////////////////////////////////////////////////////////

bool TValueParser::GetBool() const {
    return Impl_->GetBool();
}

i8 TValueParser::GetInt8() const {
    return Impl_->GetInt8();
}

ui8 TValueParser::GetUint8() const {
    return Impl_->GetUint8();
}

i16 TValueParser::GetInt16() const {
    return Impl_->GetInt16();
}

ui16 TValueParser::GetUint16() const {
    return Impl_->GetUint16();
}

i32 TValueParser::GetInt32() const {
    return Impl_->GetInt32();
}

ui32 TValueParser::GetUint32() const {
    return Impl_->GetUint32();
}

i64 TValueParser::GetInt64() const {
    return Impl_->GetInt64();
}

ui64 TValueParser::GetUint64() const {
    return Impl_->GetUint64();
}

float TValueParser::GetFloat() const {
    return Impl_->GetFloat();
}

double TValueParser::GetDouble() const {
    return Impl_->GetDouble();
}

TInstant TValueParser::GetDate() const {
    return Impl_->GetDate();
}

TInstant TValueParser::GetDatetime() const {
    return Impl_->GetDatetime();
}

TInstant TValueParser::GetTimestamp() const {
    return Impl_->GetTimestamp();
}

i64 TValueParser::GetInterval() const {
    return Impl_->GetInterval();
}

i32 TValueParser::GetDate32() const {
    return Impl_->GetDate32();
}

i64 TValueParser::GetDatetime64() const {
    return Impl_->GetDatetime64();
}

i64 TValueParser::GetTimestamp64() const {
    return Impl_->GetTimestamp64();
}

i64 TValueParser::GetInterval64() const {
    return Impl_->GetInterval64();
}

const TString& TValueParser::GetTzDate() const {
    return Impl_->GetTzDate();
}

const TString& TValueParser::GetTzDatetime() const {
    return Impl_->GetTzDatetime();
}

const TString& TValueParser::GetTzTimestamp() const {
    return Impl_->GetTzTimestamp();
}

const TString& TValueParser::GetString() const {
    return Impl_->GetString();
}

const TString& TValueParser::GetUtf8() const {
    return Impl_->GetUtf8();
}

const TString& TValueParser::GetYson() const {
    return Impl_->GetYson();
}

const TString& TValueParser::GetJson() const {
    return Impl_->GetJson();
}

TUuidValue TValueParser::GetUuid() const {
    return Impl_->GetUuid();
}

const TString& TValueParser::GetJsonDocument() const {
    return Impl_->GetJsonDocument();
}

const TString& TValueParser::GetDyNumber() const {
    return Impl_->GetDyNumber();
}

TDecimalValue TValueParser::GetDecimal() const {
    return Impl_->GetDecimal();
}

TPgValue TValueParser::GetPg() const {
    return Impl_->GetPg();
}

////////////////////////////////////////////////////////////////////////////////

#define RET_OPT_VALUE(Type, Name) \
    Impl_->OpenOptional(); \
    auto value = Impl_->IsNull() ? TMaybe<Type>() : Impl_->Get##Name(); \
    Impl_->CloseOptional(); \
    return value;

TMaybe<bool> TValueParser::GetOptionalBool() const {
    RET_OPT_VALUE(bool, Bool);
}

TMaybe<i8> TValueParser::GetOptionalInt8() const {
    RET_OPT_VALUE(i8, Int8);
}

TMaybe<ui8> TValueParser::GetOptionalUint8() const {
    RET_OPT_VALUE(ui8, Uint8);
}

TMaybe<i16> TValueParser::GetOptionalInt16() const {
    RET_OPT_VALUE(i16, Int16);
}

TMaybe<ui16> TValueParser::GetOptionalUint16() const {
    RET_OPT_VALUE(ui16, Uint16);
}

TMaybe<i32> TValueParser::GetOptionalInt32() const {
    RET_OPT_VALUE(i32, Int32);
}

TMaybe<ui32> TValueParser::GetOptionalUint32() const {
    RET_OPT_VALUE(ui32, Uint32);
}

TMaybe<i64> TValueParser::GetOptionalInt64() const {
    RET_OPT_VALUE(i64, Int64);
}

TMaybe<ui64> TValueParser::GetOptionalUint64() const {
    RET_OPT_VALUE(ui64, Uint64);
}

TMaybe<float> TValueParser::GetOptionalFloat() const {
    RET_OPT_VALUE(float, Float);
}

TMaybe<double> TValueParser::GetOptionalDouble() const {
    RET_OPT_VALUE(double, Double);
}

TMaybe<TInstant> TValueParser::GetOptionalDate() const {
    RET_OPT_VALUE(TInstant, Date);
}

TMaybe<TInstant> TValueParser::GetOptionalDatetime() const {
    RET_OPT_VALUE(TInstant, Datetime);
}

TMaybe<TInstant> TValueParser::GetOptionalTimestamp() const {
    RET_OPT_VALUE(TInstant, Timestamp);
}

TMaybe<i64> TValueParser::GetOptionalInterval() const {
    RET_OPT_VALUE(i64, Interval);
}

TMaybe<i32> TValueParser::GetOptionalDate32() const {
    RET_OPT_VALUE(i64, Date32);
}

TMaybe<i64> TValueParser::GetOptionalDatetime64() const {
    RET_OPT_VALUE(i64, Datetime64);
}

TMaybe<i64> TValueParser::GetOptionalTimestamp64() const {
    RET_OPT_VALUE(i64, Timestamp64);
}

TMaybe<i64> TValueParser::GetOptionalInterval64() const {
    RET_OPT_VALUE(i64, Interval64);
}

TMaybe<TString> TValueParser::GetOptionalTzDate() const {
    RET_OPT_VALUE(TString, TzDate);
}

TMaybe<TString> TValueParser::GetOptionalTzDatetime() const {
    RET_OPT_VALUE(TString, TzDatetime);
}

TMaybe<TString> TValueParser::GetOptionalTzTimestamp() const {
    RET_OPT_VALUE(TString, TzTimestamp);
}

TMaybe<TString> TValueParser::GetOptionalString() const {
    RET_OPT_VALUE(TString, String);
}

TMaybe<TString> TValueParser::GetOptionalUtf8() const {
    RET_OPT_VALUE(TString, Utf8);
}

TMaybe<TString> TValueParser::GetOptionalYson() const {
    RET_OPT_VALUE(TString, Yson);
}

TMaybe<TString> TValueParser::GetOptionalJson() const {
    RET_OPT_VALUE(TString, Json);
}

TMaybe<TUuidValue> TValueParser::GetOptionalUuid() const {
    RET_OPT_VALUE(TUuidValue, Uuid);
}

TMaybe<TString> TValueParser::GetOptionalJsonDocument() const {
    RET_OPT_VALUE(TString, JsonDocument);
}

TMaybe<TString> TValueParser::GetOptionalDyNumber() const {
    RET_OPT_VALUE(TString, DyNumber);
}

TMaybe<TDecimalValue> TValueParser::GetOptionalDecimal() const {
    RET_OPT_VALUE(TDecimalValue, Decimal);
}

////////////////////////////////////////////////////////////////////////////////

void TValueParser::OpenOptional() {
    Impl_->OpenOptional();
}

bool TValueParser::IsNull() const {
    return Impl_->IsNull();
}

void TValueParser::CloseOptional() {
    Impl_->CloseOptional();
}

void TValueParser::OpenList() {
    Impl_->OpenList();
}

bool TValueParser::TryNextListItem() {
    return Impl_->TryNextListItem();
}

void TValueParser::CloseList() {
    Impl_->CloseList();
}

void TValueParser::OpenStruct() {
    Impl_->OpenStruct();
}

bool TValueParser::TryNextMember() {
    return Impl_->TryNextMember();
}

const TString& TValueParser::GetMemberName() const {
    return Impl_->GetMemberName();
}

void TValueParser::CloseStruct() {
    Impl_->CloseStruct();
}

void TValueParser::OpenTuple() {
    Impl_->OpenTuple();
}

bool TValueParser::TryNextElement() {
    return Impl_->TryNextElement();
}

void TValueParser::CloseTuple() {
    Impl_->CloseTuple();
}

void TValueParser::OpenDict() {
    Impl_->OpenDict();
}

bool TValueParser::TryNextDictItem() {
    return Impl_->TryNextDictItem();
}

void TValueParser::DictKey() {
    Impl_->DictKey();
}

void TValueParser::DictPayload() {
    Impl_->DictPayload();
}

void TValueParser::CloseDict() {
    Impl_->CloseDict();
}

void TValueParser::OpenVariant() {
    Impl_->OpenVariant();
}

void TValueParser::CloseVariant() {
    Impl_->CloseVariant();
}

void TValueParser::OpenTagged() {
    Impl_->OpenTagged();
}

const TString& TValueParser::GetTag() const {
    return Impl_->GetTag();
}

void TValueParser::CloseTagged() {
    Impl_->CloseTagged();
}

////////////////////////////////////////////////////////////////////////////////

class TValueBuilderImpl {
    using ETypeKind = TTypeParser::ETypeKind;
    using TMembersMap = TMap<TString, size_t>;

    struct TProtoPosition {
        Ydb::Value& Value;
        bool BuildType = false;
        ui32 OptLevel = 0;

       TProtoPosition(Ydb::Value& value)
            : Value(value) {}
    };

    struct TStructPosition {
        const TMembersMap* MembersMap = nullptr;
        TDynBitMap FilledMembers;

        TStructPosition(const TMembersMap* membersMap)
            : MembersMap(membersMap)
            , FilledMembers() {}
    };

public:
    TValueBuilderImpl()
        : TypeBuilder_()
    {
        PushPath(ProtoValue_);
    }

    TValueBuilderImpl(const TType& type)
        : TypeBuilder_()
    {
        PushPath(ProtoValue_);
        GetType().CopyFrom(type.GetProto());
    }

    TValueBuilderImpl(Ydb::Type& type, Ydb::Value& value)
        : TypeBuilder_(type)
    {
        PushPath(value);
    }

    void CheckValue() {
        if (Path_.size() > 1) {
            FatalError("Invalid Build() call, value is incomplete.");
            return;
        }
    }

    TValue BuildValue() {
        CheckValue();

        Ydb::Value value;
        value.Swap(&ProtoValue_);

        return TValue(TypeBuilder_.Build(), std::move(value));
    }

    void Bool(bool value) {
        FillPrimitiveType(EPrimitiveType::Bool);
        GetValue().set_bool_value(value);
    }

    void Int8(i8 value) {
        FillPrimitiveType(EPrimitiveType::Int8);
        GetValue().set_int32_value(value);
    }

    void Uint8(ui8 value) {
        FillPrimitiveType(EPrimitiveType::Uint8);
        GetValue().set_uint32_value(value);
    }

    void Int16(i16 value) {
        FillPrimitiveType(EPrimitiveType::Int16);
        GetValue().set_int32_value(value);
    }

    void Uint16(ui16 value) {
        FillPrimitiveType(EPrimitiveType::Uint16);
        GetValue().set_uint32_value(value);
    }

    void Int32(i32 value) {
        FillPrimitiveType(EPrimitiveType::Int32);
        GetValue().set_int32_value(value);
    }

    void Uint32(ui32 value) {
        FillPrimitiveType(EPrimitiveType::Uint32);
        GetValue().set_uint32_value(value);
    }

    void Int64(i64 value) {
        FillPrimitiveType(EPrimitiveType::Int64);
        GetValue().set_int64_value(value);
    }

    void Uint64(ui64 value) {
        FillPrimitiveType(EPrimitiveType::Uint64);
        GetValue().set_uint64_value(value);
    }

    void Float(float value) {
        FillPrimitiveType(EPrimitiveType::Float);
        GetValue().set_float_value(value);
    }

    void Double(double value) {
        FillPrimitiveType(EPrimitiveType::Double);
        GetValue().set_double_value(value);
    }

    void Date(const TInstant& value) {
        FillPrimitiveType(EPrimitiveType::Date);
        GetValue().set_uint32_value(value.Days());
    }

    void Datetime(const TInstant& value) {
        FillPrimitiveType(EPrimitiveType::Datetime);
        GetValue().set_uint32_value(value.Seconds());
    }

    void Timestamp(const TInstant& value) {
        FillPrimitiveType(EPrimitiveType::Timestamp);
        GetValue().set_uint64_value(value.MicroSeconds());
    }

    void Interval(i64 value) {
        FillPrimitiveType(EPrimitiveType::Interval);
        GetValue().set_int64_value(value);
    }

    void Date32(const i32 value) {
        FillPrimitiveType(EPrimitiveType::Date32);
        GetValue().set_int32_value(value);
    }

    void Datetime64(const i64 value) {
        FillPrimitiveType(EPrimitiveType::Datetime64);
        GetValue().set_int64_value(value);
    }

    void Timestamp64(const i64 value) {
        FillPrimitiveType(EPrimitiveType::Timestamp64);
        GetValue().set_int64_value(value);
    }

    void Interval64(const i64 value) {
        FillPrimitiveType(EPrimitiveType::Interval64);
        GetValue().set_int64_value(value);
    }

    void TzDate(const TString& value) {
        FillPrimitiveType(EPrimitiveType::TzDate);
        GetValue().set_text_value(value);
    }

    void TzDatetime(const TString& value) {
        FillPrimitiveType(EPrimitiveType::TzDatetime);
        GetValue().set_text_value(value);
    }

    void TzTimestamp(const TString& value) {
        FillPrimitiveType(EPrimitiveType::TzTimestamp);
        GetValue().set_text_value(value);
    }

    void String(const TString& value) {
        FillPrimitiveType(EPrimitiveType::String);
        GetValue().set_bytes_value(value);
    }

    void Utf8(const TString& value) {
        FillPrimitiveType(EPrimitiveType::Utf8);
        GetValue().set_text_value(value);
    }

    void Yson(const TString& value) {
        FillPrimitiveType(EPrimitiveType::Yson);
        GetValue().set_bytes_value(value);
    }

    void Json(const TString& value) {
        FillPrimitiveType(EPrimitiveType::Json);
        GetValue().set_text_value(value);
    }

    void Uuid(const TUuidValue& value) {
        FillPrimitiveType(EPrimitiveType::Uuid);
        GetValue().set_low_128(value.Buf_.Halfs[0]);
        GetValue().set_high_128(value.Buf_.Halfs[1]);
    }

    void JsonDocument(const TString& value) {
        FillPrimitiveType(EPrimitiveType::JsonDocument);
        GetValue().set_text_value(value);
    }

    void DyNumber(const TString& value) {
        FillPrimitiveType(EPrimitiveType::DyNumber);
        GetValue().set_text_value(value);
    }

    void Decimal(const TDecimalValue& value) {
        FillDecimalType(value.DecimalType_);
        GetValue().set_low_128(value.Low_);
        GetValue().set_high_128(value.Hi_);
    }

    void Pg(const TPgValue& value) {
        FillPgType(value.PgType_);
        if (value.IsNull()) {
            GetValue().set_null_flag_value(::google::protobuf::NULL_VALUE);
        } else if (value.IsText()) {
            GetValue().set_text_value(value.Content_);
        } else {
            GetValue().set_bytes_value(value.Content_);
        }
    }

    void BeginOptional() {
        SetBuildType(!CheckType(ETypeKind::Optional));

        auto optLevel = PathTop().OptLevel;
        if (optLevel == 0) {
            PushPath(GetValue());
        }

        TypeBuilder_.BeginOptional();

        ++PathTop().OptLevel;
    }

    void EndOptional() {
        CheckContainerKind(ETypeKind::Optional);

        TypeBuilder_.EndOptional();

        if (!PathTop().OptLevel) {
            FatalError(TStringBuilder() << "No opened optional");
        }

        --PathTop().OptLevel;

        if (!PathTop().OptLevel) {
            PopPath();
        }
    }

    void BeginOptional(const TType& itemType) {
        BeginOptional();
        if (!CheckType(itemType)) {
            TypeBuilder_.SetType(itemType);
        }
    }

    void EmptyOptional(const TType& itemType) {
        BeginOptional(itemType);
        NestEmptyOptional();
        EndOptional();
    }

    void EmptyOptional(EPrimitiveType itemType) {
        BeginOptional();
        FillPrimitiveType(itemType);
        NestEmptyOptional();
        EndOptional();
    }

    void EmptyOptional() {
        BeginOptional();
        if (!CheckType()) {
            FatalError(TStringBuilder() << "EmptyOptional: unknown item type");
            return;
        }
        NestEmptyOptional();
        EndOptional();
    }

    void BeginList(const TType& itemType) {
        BeginList();

        if (!CheckType(itemType)) {
            TypeBuilder_.SetType(itemType);
        }
    }

    void BeginList() {
        SetBuildType(!CheckType(ETypeKind::List));

        TypeBuilder_.BeginList();
        PushPath(GetValue());
    }

    void EndList() {
        CheckContainerKind(ETypeKind::List);

        TypeBuilder_.EndList();
        PopPath();
    }

    void AddListItem() {
        CheckContainerKind(ETypeKind::List);
        PopPath();
        PushPath(*GetValue().add_items());
    }

    void AddListItem(const TValue& itemValue) {
        CheckContainerKind(ETypeKind::List);
        PopPath();
        PushPath(*GetValue().add_items());

        if (!CheckType(itemValue.GetType())) {
            TypeBuilder_.SetType(itemValue.GetType());
        }
        SetProtoValue(itemValue);
    }

    void AddListItem(TValue&& itemValue) {
        CheckContainerKind(ETypeKind::List);
        PopPath();
        PushPath(*GetValue().add_items());

        if (!CheckType(itemValue.GetType())) {
            TypeBuilder_.SetType(std::move(itemValue.GetType()));
        }
        SetProtoValue(std::move(itemValue));
    }

    void EmptyList(const TType& itemType) {
        BeginList(itemType);
        EndList();
    }

    void EmptyList() {
        BeginList();
        if (!CheckType()) {
            FatalError(TStringBuilder() << "EmptyList: unknown item type");
            return;
        }
        EndList();
    }

    void BeginStruct() {
        SetBuildType(!CheckType(ETypeKind::Struct));
        if (!GetBuildType()) {
            auto& membersMap = GetMembersMap(GetType().mutable_struct_type());
            GetValue().mutable_items()->Reserve(membersMap.size());
            for (ui32 i = 0; i < membersMap.size(); ++i) {
                GetValue().mutable_items()->Add();
            }

            PushStructsPath(&membersMap);
        }

        TypeBuilder_.BeginStruct();
        PushPath(GetValue());
    }

    void AddMember(const TString& memberName) {
        CheckContainerKind(ETypeKind::Struct);

        PopPath();

        if (GetBuildType()) {
            TypeBuilder_.AddMember(memberName);
            PushPath(*GetValue().add_items());
        } else {
            auto membersMap = StructsPathTop().MembersMap;
            if (!membersMap) {
                FatalError(TStringBuilder() << "Missing struct members info.");
                return;
            }

            auto memberIndex = membersMap->FindPtr(memberName);
            if (!memberIndex) {
                FatalError(TStringBuilder() << "Struct member not found: " << memberName);
                return;
            }

            TypeBuilder_.SelectMember(*memberIndex);
            PushPath(*GetValue().mutable_items(*memberIndex));

            StructsPathTop().FilledMembers.Set(*memberIndex);
        }
    }

    void AddMember(const TString& memberName, const TValue& memberValue) {
        AddMember(memberName);

        if (!CheckType(memberValue.GetType())) {
            TypeBuilder_.SetType(memberValue.GetType());
        }

        SetProtoValue(memberValue);
    }

    void AddMember(const TString& memberName, TValue&& memberValue) {
        AddMember(memberName);

        if (!CheckType(memberValue.GetType())) {
            TypeBuilder_.SetType(std::move(memberValue.GetType()));
        }

        SetProtoValue(std::move(memberValue));
    }

    void EndStruct() {
        CheckContainerKind(ETypeKind::Struct);

        PopPath();
        TypeBuilder_.EndStruct();

        if (!GetBuildType()) {
            auto& membersMap = *StructsPathTop().MembersMap;
            auto& filledMembers = StructsPathTop().FilledMembers;

            if (filledMembers.Count() < membersMap.size()) {
                filledMembers.Flip();
                auto index = filledMembers.FirstNonZeroBit();
                auto it = std::find_if(membersMap.begin(), membersMap.end(),
                    [index](const auto& pair) { return pair.second == index; });

                FatalError(TStringBuilder() << "No value given for struct member: " << it->first);
            }

            PopStructsPath();
        }
    }

    void BeginTuple() {
        SetBuildType(!CheckType(ETypeKind::Tuple));
        TypeBuilder_.BeginTuple();
        PushPath(GetValue());
    }

    void AddElement() {
        CheckContainerKind(ETypeKind::Tuple);

        PopPath();

        if (GetBuildType()) {
            TypeBuilder_.AddElement();
        } else {
            auto index = GetValue().items_size();
            auto tuple_size = GetType(1).tuple_type().elements_size();
            if (index >= tuple_size) {
                FatalError(TStringBuilder() << "Tuple elements count mismatch, expected: "
                    << tuple_size << ", actual: " << index + 1);
                return;
            }
            TypeBuilder_.SelectElement(index);
        }

        PushPath(*GetValue().add_items());
    }

    void AddElement(const TValue& elementValue) {
        AddElement();

        if (!CheckType(elementValue.GetType())) {
            TypeBuilder_.SetType(elementValue.GetType());
        }

        SetProtoValue(elementValue);
    }

    void EndTuple() {
        CheckContainerKind(ETypeKind::Tuple);

        PopPath();
        TypeBuilder_.EndTuple();

        if (!GetBuildType()) {
            auto expectedElements = GetType().tuple_type().elements_size();
            auto actualIElements = GetValue().items_size();
            if (expectedElements != actualIElements) {
                FatalError(TStringBuilder() << "Tuple elements count mismatch, expected: " << expectedElements
                    << ", actual: " << actualIElements);
            }
        }
    }

    void BeginDict() {
        SetBuildType(!CheckType(ETypeKind::Dict));
        TypeBuilder_.BeginDict();
        PushPath(GetValue());
    }

    void BeginDict(const TType& keyType, const TType& payloadType) {
        BeginDict();

        TypeBuilder_.DictKey();
        if (!CheckType(keyType)) {
            TypeBuilder_.SetType(keyType);
        }

        TypeBuilder_.DictPayload();
        if (!CheckType(payloadType)) {
            TypeBuilder_.SetType(payloadType);
        }

        TypeBuilder_.EndDict();
        TypeBuilder_.BeginDict();
    }

    void AddDictItem() {
        CheckContainerKind(ETypeKind::Dict);
        PopPath();

        GetValue().add_pairs();
        PushPath(GetValue());
    }

    void DictKey() {
        CheckContainerKind(ETypeKind::Dict);
        PopPath();

        TypeBuilder_.DictKey();
        PushPath(*GetValue().mutable_pairs()->rbegin()->mutable_key());
    }

    void DictKey(const TValue& keyValue) {
        DictKey();

        if (!CheckType(keyValue.GetType())) {
            TypeBuilder_.SetType(keyValue.GetType());
        }

        SetProtoValue(keyValue);
    }

    void DictPayload() {
        CheckContainerKind(ETypeKind::Dict);
        PopPath();

        TypeBuilder_.DictPayload();
        PushPath(*GetValue().mutable_pairs()->rbegin()->mutable_payload());
    }

    void DictPayload(const TValue& payloadValue) {
        DictPayload();

        if (!CheckType(payloadValue.GetType())) {
            TypeBuilder_.SetType(payloadValue.GetType());
        }

        SetProtoValue(payloadValue);
    }

    void EndDict() {
        CheckContainerKind(ETypeKind::Dict);

        PopPath();
        TypeBuilder_.EndDict();
    }

    void EmptyDict(const TType& keyType, const TType& payloadType) {
        BeginDict(keyType, payloadType);
        EndDict();
    }

    void EmptyDict() {
        BeginDict();

        TypeBuilder_.DictKey();
        if (!CheckType()) {
            FatalError(TStringBuilder() << "EmptyDict: unknown key type");
            return;
        }

        TypeBuilder_.DictPayload();
        if (!CheckType()) {
            FatalError(TStringBuilder() << "EmptyDict: unknown payload type");
            return;
        }

        EndDict();
    }

    void BeginTagged(const TString& tag) {
        SetBuildType(!CheckType(ETypeKind::Tagged));
        TypeBuilder_.BeginTagged(tag);
        PushPath(GetValue());
    }

    void EndTagged() {
        CheckContainerKind(ETypeKind::Tagged);

        PopPath();
        TypeBuilder_.EndTagged();
    }

private:
    Ydb::Type& GetType(size_t offset = 0) {
        return TypeBuilder_.GetProto(offset);
    }

    Ydb::Value& GetValue() {
        return PathTop().Value;
    }

    void SetProtoValue(const TValue& value) {
        GetValue().CopyFrom(value.GetProto());
    }

    void SetProtoValue(TValue&& value) {
        GetValue() = std::move(value.GetProto());
    }

    bool GetBuildType() {
        return PathTop().BuildType;
    }

    void SetBuildType(bool value) {
        PathTop().BuildType = value;
    }

    void FillPrimitiveType(EPrimitiveType type) {
        if (!CheckPrimitiveType(type)) {
            TypeBuilder_.Primitive(type);
        }
    }

    void FillDecimalType(const TDecimalType& type) {
        if (!CheckDecimalType()) {
            TypeBuilder_.Decimal(type);
        }
    }

    void FillPgType(const TPgType& type) {
        if (!CheckPgType()) {
            TypeBuilder_.Pg(type);
        }
    }

    bool CheckType() {
        if (!GetType().type_case()) {
            return false;
        }

        return true;
    }

    bool CheckType(ETypeKind kind) {
        if (!CheckType()) {
            return false;
        }

        auto expectedKind = GetKind(GetType());
        if (expectedKind != kind) {
            FatalError(TStringBuilder() << "Type mismatch, expected: " << expectedKind
                << ", actual: " << kind);
            return false;
        }

        return true;
    }

    bool CheckType(const TType& type) {
        if (!CheckType()) {
            return false;
        }

        if (!TypesEqual(GetType(), type.GetProto())) {
            FatalError(TStringBuilder() << "Type mismatch, expected: " << FormatType(GetType())
                << ", actual: " << FormatType(type));
            return false;
        }

        return true;
    }

    bool CheckPrimitiveType(EPrimitiveType type) {
        if (!CheckType(ETypeKind::Primitive)) {
            return false;
        }

        auto expectedType = EPrimitiveType(GetType().type_id());
        if (expectedType != type) {
            FatalError(TStringBuilder() << "Primitive type mismatch, expected: " << expectedType
                << ", actual: " << type);
            return false;
        }

        return true;
    }

    bool CheckDecimalType() {
        return CheckType(ETypeKind::Decimal);
    }

    bool CheckPgType() {
        return CheckType(ETypeKind::Pg);
    }

    void CheckContainerKind(ETypeKind kind) {
        if (Path_.size() < 2) {
            FatalError(TStringBuilder() << "No opened container");
        }

        auto actualKind = GetKind(GetType(1));
        if (actualKind != kind) {
            FatalError(TStringBuilder() << "Container type mismatch, expected: " << kind
                << ", actual: " << actualKind);
        }
    }

    void NestEmptyOptional() {
        ui32 optLevel = PathTop().OptLevel - 1;
        for (ui32 i = 0; i < optLevel; ++i) {
            PushPath(*GetValue().mutable_nested_value());
        }

        GetValue().set_null_flag_value(::google::protobuf::NULL_VALUE);

        for (ui32 i = 0; i < optLevel; ++i) {
            PopPath();
        }
    }

    TProtoPosition& PathTop() {
        return Path_.back();
    }

    void PushPath(Ydb::Value& value) {
        Path_.emplace_back(value);
    }

    void PopPath() {
        Path_.pop_back();
    }

    void PushStructsPath(const TMembersMap* membersMap) {
        StructsPath_.emplace_back(membersMap);
    }

    void PopStructsPath() {
        StructsPath_.pop_back();
    }

    TStructPosition& StructsPathTop() {
        return StructsPath_.back();
    }

    TMembersMap& GetMembersMap(const Ydb::StructType* structType) {
        auto it = StructsMap_.find(structType);
        if (it == StructsMap_.end()) {
            TMembersMap membersMap;
            for (size_t i = 0; i < (size_t)structType->members_size(); ++i) {
                membersMap.emplace(std::make_pair(structType->members(i).name(), i));
            }
            auto result = StructsMap_.emplace(std::make_pair(structType, std::move(membersMap)));
            it = result.first;
        }

        return it->second;
    }

    void FatalError(const TString& msg) const {
        ThrowFatalError(TStringBuilder() << "TValueBuilder: " << msg);
    }

private:

    //TTypeBuilder TypeBuilder_;
    TTypeBuilder::TImpl TypeBuilder_;
    Ydb::Value ProtoValue_;
    TMap<const Ydb::StructType*, TMembersMap> StructsMap_;

    TStackVec<TProtoPosition, 8> Path_;
    TStackVec<TStructPosition, 8> StructsPath_;
};

////////////////////////////////////////////////////////////////////////////////

template<typename TDerived>
TValueBuilderBase<TDerived>::TValueBuilderBase(TValueBuilderBase&&) = default;

template<typename TDerived>
TValueBuilderBase<TDerived>::~TValueBuilderBase() = default;

template<typename TDerived>
TValueBuilderBase<TDerived>::TValueBuilderBase()
    : Impl_(new TValueBuilderImpl()) {}

template<typename TDerived>
TValueBuilderBase<TDerived>::TValueBuilderBase(const TType& type)
    : Impl_(new TValueBuilderImpl(type)) {}

template<typename TDerived>
TValueBuilderBase<TDerived>::TValueBuilderBase(Ydb::Type& type, Ydb::Value& value)
    : Impl_(new TValueBuilderImpl(type, value)) {}

template<typename TDerived>
void TValueBuilderBase<TDerived>::CheckValue() {
    return Impl_->CheckValue();
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Bool(bool value) {
    Impl_->Bool(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Int8(i8 value) {
    Impl_->Int8(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Uint8(ui8 value) {
    Impl_->Uint8(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Int16(i16 value) {
    Impl_->Int16(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Uint16(ui16 value) {
    Impl_->Uint16(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Int32(i32 value) {
    Impl_->Int32(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Uint32(ui32 value) {
    Impl_->Uint32(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Int64(i64 value) {
    Impl_->Int64(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Uint64(ui64 value) {
    Impl_->Uint64(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Float(float value) {
    Impl_->Float(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Double(double value) {
    Impl_->Double(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Date(const TInstant& value) {
    Impl_->Date(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Datetime(const TInstant& value) {
    Impl_->Datetime(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Timestamp(const TInstant& value) {
    Impl_->Timestamp(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Interval(i64 value) {
    Impl_->Interval(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Date32(const i32 value) {
    Impl_->Date32(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Datetime64(const i64 value) {
    Impl_->Datetime64(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Timestamp64(const i64 value) {
    Impl_->Timestamp64(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Interval64(const i64 value) {
    Impl_->Interval64(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::TzDate(const TString& value) {
    Impl_->TzDate(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::TzDatetime(const TString& value) {
    Impl_->TzDatetime(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::TzTimestamp(const TString& value) {
    Impl_->TzTimestamp(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::String(const TString& value) {
    Impl_->String(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Utf8(const TString& value) {
    Impl_->Utf8(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Yson(const TString& value) {
    Impl_->Yson(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Json(const TString& value) {
    Impl_->Json(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Uuid(const TUuidValue& value) {
    Impl_->Uuid(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::JsonDocument(const TString& value) {
    Impl_->JsonDocument(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::DyNumber(const TString& value) {
    Impl_->DyNumber(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Decimal(const TDecimalValue& value) {
    Impl_->Decimal(value);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::Pg(const TPgValue& value) {
    Impl_->Pg(value);
    return static_cast<TDerived&>(*this);
}

#define SET_OPT_VALUE_MAYBE(Name) \
    if (value) { \
        Impl_->BeginOptional(); \
        Impl_->Name(*value); \
        Impl_->EndOptional(); \
    } else { \
        Impl_->EmptyOptional(EPrimitiveType::Name); \
    } \
    return static_cast<TDerived&>(*this);

#define SET_OPT_VALUE(Name) \
    Impl_->BeginOptional(); \
    Impl_->Name(value); \
    Impl_->EndOptional(); \
    return static_cast<TDerived&>(*this);

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalBool(const TMaybe<bool>& value) {
    SET_OPT_VALUE_MAYBE(Bool);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalInt8(const TMaybe<i8>& value) {
    SET_OPT_VALUE_MAYBE(Int8);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalUint8(const TMaybe<ui8>& value) {
    SET_OPT_VALUE_MAYBE(Uint8);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalInt16(const TMaybe<i16>& value) {
    SET_OPT_VALUE_MAYBE(Int16);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalUint16(const TMaybe<ui16>& value) {
    SET_OPT_VALUE_MAYBE(Uint16);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalInt32(const TMaybe<i32>& value) {
    SET_OPT_VALUE_MAYBE(Int32);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalUint32(const TMaybe<ui32>& value) {
    SET_OPT_VALUE_MAYBE(Uint32);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalInt64(const TMaybe<i64>& value) {
    SET_OPT_VALUE_MAYBE(Int64);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalUint64(const TMaybe<ui64>& value) {
    SET_OPT_VALUE_MAYBE(Uint64);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalFloat(const TMaybe<float>& value) {
    SET_OPT_VALUE_MAYBE(Float);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalDouble(const TMaybe<double>& value) {
    SET_OPT_VALUE_MAYBE(Double);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalDate(const TMaybe<TInstant>& value) {
    SET_OPT_VALUE_MAYBE(Date);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalDatetime(const TMaybe<TInstant>& value) {
    SET_OPT_VALUE_MAYBE(Datetime);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalTimestamp(const TMaybe<TInstant>& value) {
    SET_OPT_VALUE_MAYBE(Timestamp);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalInterval(const TMaybe<i64>& value) {
    SET_OPT_VALUE_MAYBE(Interval);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalDate32(const TMaybe<i32>& value) {
    SET_OPT_VALUE_MAYBE(Date32);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalDatetime64(const TMaybe<i64>& value) {
    SET_OPT_VALUE_MAYBE(Datetime64);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalTimestamp64(const TMaybe<i64>& value) {
    SET_OPT_VALUE_MAYBE(Timestamp64);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalInterval64(const TMaybe<i64>& value) {
    SET_OPT_VALUE_MAYBE(Interval64);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalTzDate(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(TzDate);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalTzDatetime(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(TzDatetime);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalTzTimestamp(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(TzTimestamp);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalString(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(String);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalUtf8(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(Utf8);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalYson(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(Yson);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalJson(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(Json);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalUuid(const TMaybe<TUuidValue>& value) {
    SET_OPT_VALUE_MAYBE(Uuid);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalJsonDocument(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(JsonDocument);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::OptionalDyNumber(const TMaybe<TString>& value) {
    SET_OPT_VALUE_MAYBE(DyNumber);
}


template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::BeginOptional() {
    Impl_->BeginOptional();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EndOptional() {
    Impl_->EndOptional();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EmptyOptional(const TType& itemType) {
    Impl_->EmptyOptional(itemType);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EmptyOptional(EPrimitiveType itemType) {
    Impl_->EmptyOptional(itemType);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EmptyOptional() {
    Impl_->EmptyOptional();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::BeginList() {
    Impl_->BeginList();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EndList() {
    Impl_->EndList();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddListItem() {
    Impl_->AddListItem();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddListItem(const TValue& itemValue) {
    Impl_->AddListItem(itemValue);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddListItem(TValue&& itemValue) {
    Impl_->AddListItem(std::move(itemValue));
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EmptyList(const TType& itemType) {
    Impl_->EmptyList(itemType);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EmptyList() {
    Impl_->EmptyList();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::BeginStruct() {
    Impl_->BeginStruct();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddMember(const TString& memberName) {
    Impl_->AddMember(memberName);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddMember(const TString& memberName, const TValue& memberValue) {
    Impl_->AddMember(memberName, memberValue);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddMember(const TString& memberName, TValue&& memberValue) {
    Impl_->AddMember(memberName, std::move(memberValue));
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EndStruct() {
    Impl_->EndStruct();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::BeginTuple() {
    Impl_->BeginTuple();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddElement() {
    Impl_->AddElement();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddElement(const TValue& elementValue) {
    Impl_->AddElement(elementValue);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EndTuple() {
    Impl_->EndTuple();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::BeginDict() {
    Impl_->BeginDict();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::AddDictItem() {
    Impl_->AddDictItem();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::DictKey() {
    Impl_->DictKey();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::DictKey(const TValue& keyValue) {
    Impl_->DictKey(keyValue);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::DictPayload() {
    Impl_->DictPayload();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::DictPayload(const TValue& payloadValue) {
    Impl_->DictPayload(payloadValue);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EndDict() {
    Impl_->EndDict();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EmptyDict(const TType& keyType, const TType& payloadType) {
    Impl_->EmptyDict(keyType, payloadType);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EmptyDict() {
    Impl_->EmptyDict();
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::BeginTagged(const TString& tag) {
    Impl_->BeginTagged(tag);
    return static_cast<TDerived&>(*this);
}

template<typename TDerived>
TDerived& TValueBuilderBase<TDerived>::EndTagged() {
    Impl_->EndTagged();
    return static_cast<TDerived&>(*this);
}

////////////////////////////////////////////////////////////////////////////////

template class TValueBuilderBase<TValueBuilder>;
template class TValueBuilderBase<TParamValueBuilder>;

////////////////////////////////////////////////////////////////////////////////

TValueBuilder::TValueBuilder()
    : TValueBuilderBase() {}

TValueBuilder::TValueBuilder(const TType& type)
    : TValueBuilderBase(type) {}

TValue TValueBuilder::Build() {
    return Impl_->BuildValue();
}

} // namespace NYdb
