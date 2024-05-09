#pragma once

#include <vector>
#include <map>

#include <arrow/api.h>

#include <library/cpp/json/common/defs.h>

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw yexception() << _s.ToString(); \
    } while (false)

namespace NYql::NJson {

struct TArrowBuilderWrapper {

    TArrowBuilderWrapper(TString& errorDetails) : ErrorDetails(errorDetails) {}
    virtual ~TArrowBuilderWrapper() {}

    TString& ErrorDetails;

    virtual bool AddBoolean(bool value) = 0;
    virtual bool AddInteger(long long value) = 0;
    virtual bool AddUInteger(unsigned long long value) = 0;
    virtual bool AddDouble(double value) = 0;
    virtual bool AddString(const TStringBuf& value) = 0;
    virtual void AddNull() = 0;

    virtual std::shared_ptr<arrow::Array> Build() = 0;
};

struct TJsonRowParser : public ::NJson::TJsonCallbacks {

    enum ComplextType {
        TypeMap,
        TypeArray
    };

    std::vector<ComplextType> Stack;
    std::vector<std::pair<TStringBuf, ui32>> InputLayout;
    std::map<TStringBuf, ui32> OutputLayout;
    std::vector<bool> AssignedFields;
    bool TopLevelMap = false;
    bool ObjectFound = false;
    ui32 NextIndex = 0;
    ui32 OutputColumnCount = 0;
    std::optional<ui32> FieldIndex;
    TString ErrorDetails;
    bool ErrorFound = false;
    bool Finished = false;
    TString ErrorMessage;
    ui32 ErrorOffset;
    ui32 CurrentRowCount = 0;

    std::shared_ptr<arrow::Schema> OutputSchema;
    const std::vector<std::shared_ptr<arrow::Field>>& Fields;
    const ui32 MaxRowCount;
    std::vector<std::shared_ptr<TArrowBuilderWrapper>> Builders;

    TJsonRowParser(std::shared_ptr<arrow::Schema> outputSchema, ui32 maxRowCount = 4096);

    void ParseNextRow(TStringBuf rowBuffer);
    std::shared_ptr<arrow::RecordBatch> TakeBatch();

    // virtual bool OnNull();
    bool OnBoolean(bool value) override;
    bool OnInteger(long long value) override;
    bool OnUInteger(unsigned long long value) override;
    bool OnDouble(double value) override;
    bool OnString(const TStringBuf& value) override;
    bool OnOpenMap() override;
    bool OnMapKey(const TStringBuf& key) override;
    bool OnCloseMap() override;
    bool OnOpenArray() override;
    bool OnCloseArray() override;
    // bool OnStringNoCopy(const TStringBuf& value) override;
    bool OnMapKeyNoCopy(const TStringBuf& key) override;
    bool OnEnd() override;
    void OnError(size_t off, TStringBuf reason) override;

    bool OnValue();
    bool OnMapKey(const TStringBuf& key, bool copyValue);
};

} // namespace NYql::NJson
