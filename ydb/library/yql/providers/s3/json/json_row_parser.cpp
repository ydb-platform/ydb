#include "json_row_parser.h"

#include <library/cpp/json/fast_sax/parser.h>

#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.h>

#include <util/string/builder.h>

namespace NYql::NJson {

struct TDefaultIgnoreWrapper : public TArrowBuilderWrapper {
    TDefaultIgnoreWrapper(TString& errorDetails) : TArrowBuilderWrapper(errorDetails) {}
    bool AddBoolean(bool) override { return true; }
    bool AddInteger(long long) override { return true; }
    bool AddUInteger(unsigned long long) override { return true; }
    bool AddDouble(double) override { return true; }
    bool AddString(const TStringBuf&) override { return true; }
    void AddNull() override { }
    std::shared_ptr<arrow::Array> Build() override { return nullptr; };
};

template <typename T, bool isOptional>
struct TArrowUintBuildWrapper : public TArrowBuilderWrapper {

    NYql::NUdf::TFixedSizeArrayBuilder<T, isOptional> builder;

    TArrowUintBuildWrapper(TString& errorDetails, const std::shared_ptr<arrow::DataType>& type, ui32 length) 
        : TArrowBuilderWrapper(errorDetails)
        , builder(NKikimr::NMiniKQL::TTypeInfoHelper(), type, *arrow::system_memory_pool(), length) {
    }

    bool AddBoolean(bool value) override {
        builder.Add(NUdf::TBlockItem(static_cast<T>(value)));
        return true;   
    }

    bool AddInteger(long long) override {
        ErrorDetails = "out of range";
        return true;
    }

    bool AddUInteger(unsigned long long value) override {
        builder.Add(NUdf::TBlockItem(static_cast<T>(value)));
        return true;
    }

    bool AddDouble(double) override {
        ErrorDetails = "conversion floating point to integer is not supported";
        return false;
    }

    bool AddString(const TStringBuf&) override {
        ErrorDetails = "conversion string to integer is not supported";
        return false;
    }

    void AddNull() override {
        builder.Add(NUdf::TBlockItem{});
    }

    std::shared_ptr<arrow::Array> Build() override { 
        return builder.Build(false).make_array();   
    }
};

template <typename T, bool isOptional>
struct TArrowIntBuildWrapper : public TArrowBuilderWrapper {

    NYql::NUdf::TFixedSizeArrayBuilder<T, isOptional> builder;

    TArrowIntBuildWrapper(TString& errorDetails, const std::shared_ptr<arrow::DataType>& type, ui32 length) 
        : TArrowBuilderWrapper(errorDetails)
        , builder(NKikimr::NMiniKQL::TTypeInfoHelper(), type, *arrow::system_memory_pool(), length) {
    }

    bool AddBoolean(bool value) override {
        builder.Add(NUdf::TBlockItem(static_cast<T>(value)));
        return true;   
    }

    bool AddInteger(long long value) override {
        builder.Add(NUdf::TBlockItem(static_cast<T>(value)));
        return true;
    }

    bool AddUInteger(unsigned long long value) override {
        // std::numeric_limits<i32>::max();
        builder.Add(NUdf::TBlockItem(static_cast<T>(value)));
        return true;
    }

    bool AddDouble(double) override {
        ErrorDetails = "conversion floating point to integer is not supported";
        return false;
    }

    bool AddString(const TStringBuf&) override {
        ErrorDetails = "conversion string to integer is not supported";
        return false;
    }

    void AddNull() override {
        builder.Add(NUdf::TBlockItem{});
    }

    std::shared_ptr<arrow::Array> Build() override { 
        return builder.Build(false).make_array();   
    }
};

template <typename T, bool isOptional>
struct TArrowStringBuildWrapper : public TArrowBuilderWrapper {

    NYql::NUdf::TStringArrayBuilder<T, isOptional> builder;

    TArrowStringBuildWrapper(TString& errorDetails, const std::shared_ptr<arrow::DataType>& type, ui32 length) 
        : TArrowBuilderWrapper(errorDetails)
        , builder(NKikimr::NMiniKQL::TTypeInfoHelper(), type, *arrow::system_memory_pool(), length) {
    }

    bool AddBoolean(bool) override {
        ErrorDetails = "conversion boolean to string is not supported";
        return false;
    }

    bool AddInteger(long long) override {
        ErrorDetails = "conversion integer to string is not supported";
        return false;
    }

    bool AddUInteger(unsigned long long) override {
        ErrorDetails = "conversion unsigned integer to string is not supported";
        return false;
    }

    bool AddDouble(double) override {
        ErrorDetails = "conversion floating point to string is not supported";
        return false;
    }

    bool AddString(const TStringBuf& value) override {
        builder.Add(NUdf::TBlockItem{value});
        return true;
    }

    void AddNull() override {
        builder.Add(NUdf::TBlockItem{});
    }

    std::shared_ptr<arrow::Array> Build() override { 
        return builder.Build(false).make_array();   
    }
};

std::shared_ptr<TArrowBuilderWrapper> CreateBuilderWrapper(TString& errorDetails, const std::shared_ptr<arrow::DataType>& type, bool nullable, ui32 length) {
    switch (type->id()) {
        case arrow::Type::UINT8:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui8, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui8, false>>(errorDetails, type, length));
        case arrow::Type::INT8:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<i8, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<i8, false>>(errorDetails, type, length));
        case arrow::Type::UINT16:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui16, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui16, false>>(errorDetails, type, length));
        case arrow::Type::INT16:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<i16, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<i16, false>>(errorDetails, type, length));
        case arrow::Type::UINT32:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui32, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui32, false>>(errorDetails, type, length));
        case arrow::Type::INT32:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowIntBuildWrapper<i32, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowIntBuildWrapper<i32, false>>(errorDetails, type, length));
        case arrow::Type::UINT64:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui64, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<ui64, false>>(errorDetails, type, length));
        case arrow::Type::INT64:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<i64, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowUintBuildWrapper<i64, false>>(errorDetails, type, length));
        case arrow::Type::STRING:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowStringBuildWrapper<arrow::StringType, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowStringBuildWrapper<arrow::StringType, false>>(errorDetails, type, length));
        case arrow::Type::BINARY:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowStringBuildWrapper<arrow::BinaryType, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowStringBuildWrapper<arrow::BinaryType, false>>(errorDetails, type, length));
        case arrow::Type::TIMESTAMP:
            return nullable ?
                std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowIntBuildWrapper<i64, true>>(errorDetails, type, length))
            :   std::static_pointer_cast<TArrowBuilderWrapper>(std::make_shared<TArrowIntBuildWrapper<i64, false>>(errorDetails, type, length));
        case arrow::Type::BOOL:
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
        case arrow::Type::TIME32:
        case arrow::Type::TIME64:
        default:
// Cerr << "DEFAULT BUILDER" << Endl;
            return std::make_shared<TDefaultIgnoreWrapper>(errorDetails);
    }
}

TJsonRowParser::TJsonRowParser(std::shared_ptr<arrow::Schema> outputSchema, ui32 maxRowCount) 
    : OutputSchema(outputSchema)
    , Fields(outputSchema->fields())
    , MaxRowCount(maxRowCount) {

    InputLayout.reserve(32);
    OutputColumnCount = Fields.size();
    for (ui32 i = 0; i < OutputColumnCount; i++) {
// Cerr << "FIELD " << Fields[i]->name() << Endl;
        Builders.push_back(CreateBuilderWrapper(ErrorDetails, Fields[i]->type(), Fields[i]->nullable(), MaxRowCount));
        OutputLayout.emplace(Fields[i]->name(), i);
    }
    AssignedFields.resize(OutputColumnCount);
}

void TJsonRowParser::ParseNextRow(TStringBuf rowBuffer) {
// Cerr << "PARSE ROW: " << rowBuffer << Endl;
    Stack.resize(0);
    std::fill(AssignedFields.begin(), AssignedFields.end(), false);
    TopLevelMap = false;
    ObjectFound = false;
    NextIndex = 0;
    ErrorFound = false;
    ErrorDetails = "";
    FieldIndex.reset();
    Finished = false;

    ::NJson::ReadJsonFast(rowBuffer, this);

    if (!ErrorFound) {
        for (ui32 i = 0; i < OutputColumnCount; i++) {
            if (!AssignedFields[i]) {
                if (!Fields[i]->nullable()) {
                    ErrorFound = true;
                    ErrorMessage = TStringBuilder() << "missed mandatory field \"" << Fields[i]->name() << "\"";
// Cerr << "ERROR: " << ErrorMessage << Endl;
                    return;
                } else {
                    Builders[i]->AddNull();
                }
            }
        }
        CurrentRowCount++;
    }
}

std::shared_ptr<arrow::RecordBatch> TJsonRowParser::TakeBatch() {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto& builder : Builders) {
        columns.push_back(builder->Build());
    }
    auto batch = arrow::RecordBatch::Make(OutputSchema, CurrentRowCount, columns);
    CurrentRowCount = 0;
    return batch;
}

bool TJsonRowParser::OnMapKey(const TStringBuf& key, bool) {
    if (!TopLevelMap) {
        if (Stack.empty()) {
            ErrorDetails = "top level json object is expected, starting with '{'";
            return false;
        }
        FieldIndex.reset();
    } else if (auto it = OutputLayout.find(key); it != OutputLayout.end()) {
        FieldIndex = it->second;
        AssignedFields[it->second] = true;
    } else {
        FieldIndex.reset();
    }
    return true;
}

    // virtual bool OnNull();

bool TJsonRowParser::OnValue() {
    if (Stack.empty()) {
        ErrorDetails = "top level json object is expected, starting with '{'";
        return false;
    }
    return true;
}

bool TJsonRowParser::OnBoolean(bool value) {
    if (!OnValue()) {
        return false;
    }
    return FieldIndex ? Builders[*FieldIndex]->AddBoolean(value) : true;
}

bool TJsonRowParser::OnInteger(long long value) {
    if (!OnValue()) {
        return false;
    }
    return FieldIndex ? Builders[*FieldIndex]->AddInteger(value) : true;
}

bool TJsonRowParser::OnUInteger(unsigned long long value) {
    if (!OnValue()) {
        return false;
    }
    return FieldIndex ? Builders[*FieldIndex]->AddUInteger(value) : true;
}

bool TJsonRowParser::OnDouble(double value) {
    if (!OnValue()) {
        return false;
    }
    return FieldIndex ? Builders[*FieldIndex]->AddDouble(value) : true;
}

bool TJsonRowParser::OnString(const TStringBuf& value) {
    if (!OnValue()) {
        return false;
    }
    return FieldIndex ? Builders[*FieldIndex]->AddString(value) : true;
}

bool TJsonRowParser::OnOpenMap() {
    Stack.push_back(ComplextType::TypeMap);
    TopLevelMap = Stack.size() == 1;
    ObjectFound = true;
    return true;
}

bool TJsonRowParser::OnMapKey(const TStringBuf& key) {
    return OnMapKey(key, true);
}

bool TJsonRowParser::OnCloseMap() {
    if (Stack.empty()) {
        ErrorDetails = "unbalanced '}''";
        return false;
    }

    if (Stack.back() != ComplextType::TypeMap) {
        ErrorDetails = "expected ']'";
        return false;
    }

    Stack.pop_back();

    if (Stack.empty()) {
        Finished = true;
        return false;
    }

    TopLevelMap = Stack.size() == 1 && Stack.back() == ComplextType::TypeMap;

    return true;
}

bool TJsonRowParser::OnOpenArray() {
    if (Stack.empty()) {
        ErrorDetails = "top level json object is expected, starting with '{'";
        return false;
    }

    Stack.push_back(ComplextType::TypeArray);
    TopLevelMap = false;
    return true;
}

bool TJsonRowParser::OnCloseArray() {
    if (Stack.empty()) {
        ErrorDetails = "unbalanced ']'";
        return false;
    }

    if (Stack.back() != ComplextType::TypeArray) {
        ErrorDetails = "expected '}'";
        return false;
    }

    Stack.pop_back();
    TopLevelMap = Stack.size() == 1 && Stack.back() == ComplextType::TypeMap;

    return true;
}

bool TJsonRowParser::OnMapKeyNoCopy(const TStringBuf& key) {
    return OnMapKey(key, false);
}

bool TJsonRowParser::OnEnd() {
    return ObjectFound;
}

void TJsonRowParser::OnError(size_t off, TStringBuf reason) {
    if (!Finished) {
        ErrorFound = true;
        if (ErrorDetails) {
            ErrorMessage = TStringBuilder() << reason << ", " << ErrorDetails;
        } else {
            ErrorMessage = reason;
        }
        ErrorOffset = off;
// Cerr << "ERROR at " << off << ": " << ErrorMessage << Endl;
    }
}

} // namespace NYql::NJson
