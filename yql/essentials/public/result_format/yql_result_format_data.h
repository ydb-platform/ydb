#pragma once

#include "yql_result_format_common.h"

#include <library/cpp/yson/node/node.h>

namespace NYql::NResult {

class IDataVisitor {
public:
    virtual ~IDataVisitor() = default;

    virtual void OnVoid() = 0;
    virtual void OnNull() = 0;
    virtual void OnEmptyList() = 0;
    virtual void OnEmptyDict() = 0;
    virtual void OnBool(bool value) = 0;
    virtual void OnInt8(i8 value) = 0;
    virtual void OnUint8(ui8 value) = 0;
    virtual void OnInt16(i16 value) = 0;
    virtual void OnUint16(ui16 value) = 0;
    virtual void OnInt32(i32 value) = 0;
    virtual void OnUint32(ui32 value) = 0;
    virtual void OnInt64(i64 value) = 0;
    virtual void OnUint64(ui64 value) = 0;
    virtual void OnFloat(float value) = 0;
    virtual void OnDouble(double value) = 0;
    virtual void OnString(TStringBuf value, bool isUtf8) = 0;
    virtual void OnUtf8(TStringBuf value) = 0;
    virtual void OnYson(TStringBuf value, bool isUtf8) = 0;
    virtual void OnJson(TStringBuf value) = 0;
    virtual void OnJsonDocument(TStringBuf value) = 0;
    virtual void OnUuid(TStringBuf value, bool isUtf8) = 0;
    virtual void OnDyNumber(TStringBuf value, bool isUtf8) = 0;
    virtual void OnDate(ui16 value) = 0;
    virtual void OnDatetime(ui32 value) = 0;
    virtual void OnTimestamp(ui64 value) = 0;
    virtual void OnTzDate(TStringBuf value) = 0;
    virtual void OnTzDatetime(TStringBuf value) = 0;
    virtual void OnTzTimestamp(TStringBuf value) = 0;
    virtual void OnInterval(i64 value) = 0;
    virtual void OnDate32(i32 value) = 0;
    virtual void OnDatetime64(i64 value) = 0;
    virtual void OnTimestamp64(i64 value) = 0;
    virtual void OnTzDate32(TStringBuf value) = 0;
    virtual void OnTzDatetime64(TStringBuf value) = 0;
    virtual void OnTzTimestamp64(TStringBuf value) = 0;
    virtual void OnInterval64(i64 value) = 0;
    virtual void OnDecimal(TStringBuf value) = 0;
    virtual void OnBeginOptional() = 0;
    virtual void OnEmptyOptional() = 0;
    virtual void OnBeforeOptionalItem() = 0;
    virtual void OnAfterOptionalItem() = 0;
    virtual void OnEndOptional() = 0;
    virtual void OnBeginList() = 0;
    virtual void OnBeforeListItem() = 0;
    virtual void OnAfterListItem() = 0;
    virtual void OnEndList() = 0;
    virtual void OnBeginTuple() = 0;
    virtual void OnBeforeTupleItem() = 0;
    virtual void OnAfterTupleItem() = 0;
    virtual void OnEndTuple() = 0;
    virtual void OnBeginStruct() = 0;
    virtual void OnBeforeStructItem() = 0;
    virtual void OnAfterStructItem() = 0;
    virtual void OnEndStruct() = 0;
    virtual void OnBeginDict() = 0;
    virtual void OnBeforeDictItem() = 0;
    virtual void OnBeforeDictKey() = 0;
    virtual void OnAfterDictKey() = 0;
    virtual void OnBeforeDictPayload() = 0;
    virtual void OnAfterDictPayload() = 0;
    virtual void OnAfterDictItem() = 0;
    virtual void OnEndDict() = 0;
    virtual void OnBeginVariant(ui32 index) = 0;
    virtual void OnEndVariant() = 0;
    virtual void OnPg(TMaybe<TStringBuf> value, bool isUtf8) = 0;
};

class TSameActionDataVisitor : public IDataVisitor {
public:
    void OnVoid() override;
    void OnNull() override;
    void OnEmptyList() override;
    void OnEmptyDict() override;
    void OnBool(bool value) override;
    void OnInt8(i8 value) override;
    void OnUint8(ui8 value) override;
    void OnInt16(i16 value) override;
    void OnUint16(ui16 value) override;
    void OnInt32(i32 value) override;
    void OnUint32(ui32 value) override;
    void OnInt64(i64 value) override;
    void OnUint64(ui64 value) override;
    void OnFloat(float value) override;
    void OnDouble(double value) override;
    void OnString(TStringBuf value, bool isUtf8) override;
    void OnUtf8(TStringBuf value) override;
    void OnYson(TStringBuf value, bool isUtf8) override;
    void OnJson(TStringBuf value) override;
    void OnJsonDocument(TStringBuf value) override;
    void OnUuid(TStringBuf value, bool isUtf8) override;
    void OnDyNumber(TStringBuf value, bool isUtf8) override;
    void OnDate(ui16 value) override;
    void OnDatetime(ui32 value) override;
    void OnTimestamp(ui64 value) override;
    void OnTzDate(TStringBuf value) override;
    void OnTzDatetime(TStringBuf value) override;
    void OnTzTimestamp(TStringBuf value) override;
    void OnInterval(i64 value) override;
    void OnDate32(i32 value) override;
    void OnDatetime64(i64 value) override;
    void OnTimestamp64(i64 value) override;
    void OnTzDate32(TStringBuf value) override;
    void OnTzDatetime64(TStringBuf value) override;
    void OnTzTimestamp64(TStringBuf value) override;
    void OnInterval64(i64 value) override;
    void OnDecimal(TStringBuf value) override;
    void OnBeginOptional() override;
    void OnBeforeOptionalItem() override;
    void OnAfterOptionalItem() override;
    void OnEmptyOptional() override;
    void OnEndOptional() override;
    void OnBeginList() override;
    void OnBeforeListItem() override;
    void OnAfterListItem() override;
    void OnEndList() override;
    void OnBeginTuple() override;
    void OnBeforeTupleItem() override;
    void OnAfterTupleItem() override;
    void OnEndTuple() override;
    void OnBeginStruct() override;
    void OnBeforeStructItem() override;
    void OnAfterStructItem() override;
    void OnEndStruct() override;
    void OnBeginDict() override;
    void OnBeforeDictItem() override;
    void OnBeforeDictKey() override;
    void OnAfterDictKey() override;
    void OnBeforeDictPayload() override;
    void OnAfterDictPayload() override;
    void OnAfterDictItem() override;
    void OnEndDict() override;
    void OnBeginVariant(ui32 index) override;
    void OnEndVariant() override;
    void OnPg(TMaybe<TStringBuf> value, bool isUtf8) override;

public:
    virtual void Do() = 0;
};

class TThrowingDataVisitor : public TSameActionDataVisitor {
public:
    void Do() final;
};

class TEmptyDataVisitor : public TSameActionDataVisitor {
public:
    void Do() final;
};

void ParseData(const NYT::TNode& typeNode, const NYT::TNode& dataNode, IDataVisitor& visitor);

class TDataBuilder : public IDataVisitor {
public:
    TDataBuilder();
    const NYT::TNode& GetResult() const;

    void OnVoid() final;
    void OnNull() final;
    void OnEmptyList() final;
    void OnEmptyDict() final;
    void OnBool(bool value) final;
    void OnInt8(i8 value) final;
    void OnUint8(ui8 value) final;
    void OnInt16(i16 value) final;
    void OnUint16(ui16 value) final;
    void OnInt32(i32 value) final;
    void OnUint32(ui32 value) final;
    void OnInt64(i64 value) final;
    void OnUint64(ui64 value) final;
    void OnFloat(float value) final;
    void OnDouble(double value) final;
    void OnString(TStringBuf value, bool isUtf8) final;
    void OnUtf8(TStringBuf value) final;
    void OnYson(TStringBuf value, bool isUtf8) final;
    void OnJson(TStringBuf value) final;
    void OnJsonDocument(TStringBuf value) final;
    void OnUuid(TStringBuf value, bool isUtf8) final;
    void OnDyNumber(TStringBuf value, bool isUtf8) final;
    void OnDate(ui16 value) final;
    void OnDatetime(ui32 value) final;
    void OnTimestamp(ui64 value) final;
    void OnTzDate(TStringBuf value) final;
    void OnTzDatetime(TStringBuf value) final;
    void OnTzTimestamp(TStringBuf value) final;
    void OnInterval(i64 value) final;
    void OnDate32(i32 value) final;
    void OnDatetime64(i64 value) final;
    void OnTimestamp64(i64 value) final;
    void OnTzDate32(TStringBuf value) final;
    void OnTzDatetime64(TStringBuf value) final;
    void OnTzTimestamp64(TStringBuf value) final;
    void OnInterval64(i64 value) final;
    void OnDecimal(TStringBuf value) final;
    void OnBeginOptional() final;
    void OnBeforeOptionalItem() final;
    void OnAfterOptionalItem() final;
    void OnEmptyOptional() final;
    void OnEndOptional() final;
    void OnBeginList() final;
    void OnBeforeListItem() final;
    void OnAfterListItem() final;
    void OnEndList() final;
    void OnBeginTuple() final;
    void OnBeforeTupleItem() final;
    void OnAfterTupleItem() final;
    void OnEndTuple() final;
    void OnBeginStruct() final;
    void OnBeforeStructItem() final;
    void OnAfterStructItem() final;
    void OnEndStruct() final;
    void OnBeginDict() final;
    void OnBeforeDictItem() final;
    void OnBeforeDictKey() final;
    void OnAfterDictKey() final;
    void OnBeforeDictPayload() final;
    void OnAfterDictPayload() final;
    void OnAfterDictItem() final;
    void OnEndDict() final;
    void OnBeginVariant(ui32 index) final;
    void OnEndVariant() final;
    void OnPg(TMaybe<TStringBuf> value, bool isUtf8) final;

private:
    NYT::TNode& Top();
    void Push(NYT::TNode* value);
    void Pop();

private:
    NYT::TNode Root_;
    TVector<NYT::TNode*> Stack_;
};

}
