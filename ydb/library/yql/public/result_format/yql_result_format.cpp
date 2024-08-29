#include "yql_result_format.h"

namespace NYql::NResult {

namespace {
    void Check(bool value, const TSourceLocation& location) {
        if (!value) {
            throw location + TUnsupportedException();
        }
    }

    NYT::TNode MakeDataType(const TString& name) {
        return NYT::TNode().Add("DataType").Add(name);
    }
}

#define CHECK(value) Check(value, __LOCATION__)
#define UNEXPECTED ythrow TUnsupportedException() << "Unhandled case"

void ParseType(const NYT::TNode& typeNode, ITypeVisitor& visitor) {
    CHECK(typeNode.IsList());
    CHECK(typeNode.AsList().size() >= 1);
    CHECK(typeNode.AsList()[0].IsString());
    const auto& name = typeNode.AsList()[0].AsString();
    if (name == "VoidType") {
        CHECK(typeNode.AsList().size() == 1);
        visitor.OnVoid();
    } else if (name == "NullType") {
        CHECK(typeNode.AsList().size() == 1);
        visitor.OnNull();
    } else if (name == "EmptyListType") {
        CHECK(typeNode.AsList().size() == 1);
        visitor.OnEmptyList();
    } else if (name == "EmptyDictType") {
        CHECK(typeNode.AsList().size() == 1);
        visitor.OnEmptyDict();
    } else if (name == "DataType") {
        CHECK(typeNode.AsList().size() >= 2);
        CHECK(typeNode.AsList()[1].IsString());
        const auto& dataName = typeNode.AsList()[1].AsString();
        if (dataName == "Bool") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnBool();
        } else if (dataName == "Int8") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnInt8();
        } else if (dataName == "Uint8") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnUint8();
        } else if (dataName == "Int16") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnInt16();
        } else if (dataName == "Uint16") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnUint16();
        } else if (dataName == "Int32") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnInt32();
        } else if (dataName == "Uint32") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnUint32();
        } else if (dataName == "Int64") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnInt64();
        } else if (dataName == "Uint64") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnUint64();
        } else if (dataName == "Float") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnFloat();
        } else if (dataName == "Double") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnDouble();
        } else if (dataName == "String") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnString();
        } else if (dataName == "Utf8") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnUtf8();
        } else if (dataName == "Yson") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnYson();
        } else if (dataName == "Json") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnJson();
        } else if (dataName == "JsonDocument") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnJsonDocument();
        } else if (dataName == "Uuid") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnUuid();
        } else if (dataName == "DyNumber") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnDyNumber();
        } else if (dataName == "Date") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnDate();
        } else if (dataName == "Datetime") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnDatetime();
        } else if (dataName == "Timestamp") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTimestamp();
        } else if (dataName == "TzDate") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTzDate();
        } else if (dataName == "TzDatetime") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTzDatetime();
        } else if (dataName == "TzTimestamp") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTzTimestamp();
        } else if (dataName == "Interval") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnInterval();
        } else if (dataName == "Date32") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnDate32();
        } else if (dataName == "Datetime64") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnDatetime64();
        } else if (dataName == "Timestamp64") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTimestamp64();
        } else if (dataName == "TzDate32") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTzDate32();
        } else if (dataName == "TzDatetime64") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTzDatetime64();
        } else if (dataName == "TzTimestamp64") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnTzTimestamp64();
        } else if (dataName == "Interval64") {
            CHECK(typeNode.AsList().size() == 2);
            visitor.OnInterval64();
        } else if (dataName == "Decimal") {
            CHECK(typeNode.AsList().size() == 4);
            CHECK(typeNode.AsList()[2].IsString());
            CHECK(typeNode.AsList()[3].IsString());
            ui32 precision;
            ui32 scale;
            CHECK(TryFromString(typeNode.AsList()[2].AsString(), precision));
            CHECK(TryFromString(typeNode.AsList()[3].AsString(), scale));
            visitor.OnDecimal(precision, scale);
        } else {
            ythrow TUnsupportedException() << "Unexpected data type name: " << dataName;
        }
    } else if (name == "OptionalType") {
        CHECK(typeNode.AsList().size() == 2);
        visitor.OnBeginOptional();
        ParseType(typeNode.AsList()[1], visitor);
        visitor.OnEndOptional();
    } else if (name == "ListType") {
        CHECK(typeNode.AsList().size() == 2);
        visitor.OnBeginList();
        ParseType(typeNode.AsList()[1], visitor);
        visitor.OnEndList();
    } else if (name == "TupleType") {
        CHECK(typeNode.AsList().size() == 2);
        visitor.OnBeginTuple();
        CHECK(typeNode.AsList()[1].IsList());
        for (const auto& v : typeNode.AsList()[1].AsList()) {
            visitor.OnTupleItem();
            ParseType(v, visitor);
        }

        visitor.OnEndTuple();
    } else if (name == "StructType") {
        CHECK(typeNode.AsList().size() == 2);
        visitor.OnBeginStruct();
        CHECK(typeNode.AsList()[1].IsList());
        for (const auto& v : typeNode.AsList()[1].AsList()) {
            CHECK(v.IsList());
            CHECK(v.AsList().size() == 2);
            CHECK(v.AsList()[0].IsString());
            const auto& member = v.AsList()[0].AsString();
            visitor.OnStructItem(member);
            ParseType(v.AsList()[1], visitor);
        }

        visitor.OnEndStruct();
    } else if (name == "DictType") {
        CHECK(typeNode.AsList().size() == 3);
        visitor.OnBeginDict();
        visitor.OnDictKey();
        ParseType(typeNode.AsList()[1], visitor);
        visitor.OnDictPayload();
        ParseType(typeNode.AsList()[2], visitor);
        visitor.OnEndDict();
    } else if (name == "VariantType") {
        CHECK(typeNode.AsList().size() == 2);
        visitor.OnBeginVariant();
        ParseType(typeNode.AsList()[1], visitor);
        visitor.OnEndVariant();
    } else if (name == "TaggedType") {
        CHECK(typeNode.AsList().size() == 3);
        CHECK(typeNode.AsList()[1].IsString());
        visitor.OnBeginTagged(typeNode.AsList()[1].AsString());
        ParseType(typeNode.AsList()[2], visitor);
        visitor.OnEndTagged();
    } else if (name == "PgType") {
        CHECK(typeNode.AsList().size() == 3);
        CHECK(typeNode.AsList()[1].IsString());
        CHECK(typeNode.AsList()[2].IsString());
        visitor.OnPgType(typeNode.AsList()[1].AsString(), typeNode.AsList()[2].AsString());
     } else {
        ythrow TUnsupportedException() << "Unexpected type name: " << name;
    }
}

TTypeBuilder::TTypeBuilder() {
    Stack.push_back(&Root);
}

const NYT::TNode& TTypeBuilder::GetResult() const {
    CHECK(Stack.size() == 1);
    return Root;
}

void TTypeBuilder::OnVoid() {
    Top() = NYT::TNode().Add("VoidType");
}

void TTypeBuilder::OnNull() {
    Top() = NYT::TNode().Add("NullType");
}

void TTypeBuilder::OnEmptyList() {
    Top() = NYT::TNode().Add("EmptyListType");
}

void TTypeBuilder::OnEmptyDict() {
    Top() = NYT::TNode().Add("EmptyDictType");
}

void TTypeBuilder::OnBool() {
    Top() = MakeDataType("Bool");
}

void TTypeBuilder::OnInt8() {
    Top() = MakeDataType("Int8");
}

void TTypeBuilder::OnUint8() {
    Top() = MakeDataType("Uint8");
}

void TTypeBuilder::OnInt16() {
    Top() = MakeDataType("Int16");
}

void TTypeBuilder::OnUint16() {
    Top() = MakeDataType("Uint16");
}

void TTypeBuilder::OnInt32() {
    Top() = MakeDataType("Int32");
}

void TTypeBuilder::OnUint32() {
    Top() = MakeDataType("Uint32");
}

void TTypeBuilder::OnInt64() {
    Top() = MakeDataType("Int64");
}

void TTypeBuilder::OnUint64() {
    Top() = MakeDataType("Uint64");
}

void TTypeBuilder::OnFloat() {
    Top() = MakeDataType("Float");
}

void TTypeBuilder::OnDouble() {
    Top() = MakeDataType("Double");
}

void TTypeBuilder::OnString() {
    Top() = MakeDataType("String");
}

void TTypeBuilder::OnUtf8() {
    Top() = MakeDataType("Utf8");
}

void TTypeBuilder::OnYson() {
    Top() = MakeDataType("Yson");
}

void TTypeBuilder::OnJson() {
    Top() = MakeDataType("Json");
}

void TTypeBuilder::OnJsonDocument() {
    Top() = MakeDataType("JsonDocument");
}

void TTypeBuilder::OnUuid() {
    Top() = MakeDataType("Uuid");
}

void TTypeBuilder::OnDyNumber() {
    Top() = MakeDataType("DyNumber");
}

void TTypeBuilder::OnDate() {
    Top() = MakeDataType("Date");
}

void TTypeBuilder::OnDatetime() {
    Top() = MakeDataType("Datetime");
}

void TTypeBuilder::OnTimestamp() {
    Top() = MakeDataType("Timestamp");
}

void TTypeBuilder::OnTzDate() {
    Top() = MakeDataType("TzDate");
}

void TTypeBuilder::OnTzDatetime() {
    Top() = MakeDataType("TzDatetime");
}

void TTypeBuilder::OnTzTimestamp() {
    Top() = MakeDataType("TzTimestamp");
}

void TTypeBuilder::OnInterval() {
    Top() = MakeDataType("Interval");
}

void TTypeBuilder::OnDate32() {
    Top() = MakeDataType("Date32");
}

void TTypeBuilder::OnDatetime64() {
    Top() = MakeDataType("Datetime64");
}

void TTypeBuilder::OnTimestamp64() {
    Top() = MakeDataType("Timestamp64");
}

void TTypeBuilder::OnTzDate32() {
    Top() = MakeDataType("TzDate32");
}

void TTypeBuilder::OnTzDatetime64() {
    Top() = MakeDataType("TzDatetime64");
}

void TTypeBuilder::OnTzTimestamp64() {
    Top() = MakeDataType("TzTimestamp64");
}

void TTypeBuilder::OnInterval64() {
    Top() = MakeDataType("Interval64");
}

void TTypeBuilder::OnDecimal(ui32 precision, ui32 scale) {
    Top() = NYT::TNode().Add("DataType").Add("Decimal")
        .Add(ToString(precision)).Add(ToString(scale));
}

NYT::TNode& TTypeBuilder::Top() {
    return *Stack.back();
}

void TTypeBuilder::OnBeginOptional() {
    Top() = NYT::TNode().Add("OptionalType");
    Push();
}

void TTypeBuilder::OnEndOptional() {
    Pop();
}

void TTypeBuilder::OnBeginList() {
    Top() = NYT::TNode().Add("ListType");
    Push();
}

void TTypeBuilder::OnEndList() {
    Pop();
}

void TTypeBuilder::OnBeginTuple() {
    Top() = NYT::TNode().Add("TupleType").Add(NYT::TNode::CreateList());
    Stack.push_back(&Top().AsList()[1]);
}

void TTypeBuilder::OnTupleItem() {
    if (!Top().AsList().empty()) {
        Pop();
    }
    
    Push();
}

void TTypeBuilder::OnEndTuple() {
    if (!Top().AsList().empty()) {
        Pop();
    }

    Pop();
}

void TTypeBuilder::OnBeginStruct() {
    Top() = NYT::TNode().Add("StructType").Add(NYT::TNode::CreateList());
    Stack.push_back(&Top().AsList()[1]);
}

void TTypeBuilder::OnStructItem(TStringBuf member) {
    if (!Top().AsList().empty()) {
        Pop();
    }

    Top().Add(NYT::TNode::CreateList());
    auto& pair = Top().AsList().back();
    pair.Add(member);
    auto ptr = &pair.Add();
    Stack.push_back(ptr);
}

void TTypeBuilder::OnEndStruct() {
    if (!Top().AsList().empty()) {
        Pop();
    }

    Pop();
}

void TTypeBuilder::OnBeginDict() {
    Top() = NYT::TNode().Add("DictType");
    Stack.push_back(&Top());
}


void TTypeBuilder::OnDictKey() {
    Push();
}

void TTypeBuilder::OnDictPayload() {
    Pop();
    Push();
}

void TTypeBuilder::OnEndDict() {
    Pop();
    Pop();
}

void TTypeBuilder::OnBeginVariant() {
    Top() = NYT::TNode().Add("VariantType");
    Push();
}

void TTypeBuilder::OnEndVariant() {
    Pop();
}

void TTypeBuilder::OnBeginTagged(TStringBuf tag) {
    Top() = NYT::TNode().Add("TaggedType").Add(tag);
    Push();
}

void TTypeBuilder::OnEndTagged() {
    Pop();
}

void TTypeBuilder::OnPgType(TStringBuf name, TStringBuf category) {
    Top() = NYT::TNode().Add("PgType").Add(name).Add(category);
}

void TTypeBuilder::Push() {
    auto ptr = &Top().Add();
    Stack.push_back(ptr);
}

void TTypeBuilder::Pop() {
    Stack.pop_back();
}

void TThrowingTypeVisitor::OnVoid() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnNull() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEmptyList() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEmptyDict() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBool() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnInt8() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnUint8() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnInt16() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnUint16() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnInt32() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnUint32() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnInt64() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnUint64() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnFloat() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDouble() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnString() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnUtf8() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnYson() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnJson() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnJsonDocument() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnUuid() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDyNumber() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDate() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDatetime() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTimestamp() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTzDate() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTzDatetime() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTzTimestamp() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnInterval() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDate32() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDatetime64() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTimestamp64() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTzDate32() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTzDatetime64() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTzTimestamp64() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnInterval64() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDecimal(ui32 precision, ui32 scale) {
    Y_UNUSED(precision);
    Y_UNUSED(scale);
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBeginOptional() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEndOptional() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBeginList() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEndList() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBeginTuple() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnTupleItem() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEndTuple() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBeginStruct() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnStructItem(TStringBuf member) {
    Y_UNUSED(member);
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEndStruct() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBeginDict() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDictKey() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnDictPayload() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEndDict() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBeginVariant() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEndVariant() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnBeginTagged(TStringBuf tag) {
    Y_UNUSED(tag);
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnEndTagged() {
    UNEXPECTED;
}

void TThrowingTypeVisitor::OnPgType(TStringBuf name, TStringBuf category) {
    Y_UNUSED(name);
    Y_UNUSED(category);
    UNEXPECTED;
}

}
