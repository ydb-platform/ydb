#pragma once

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node.h>

namespace NYql::NResult {

class TUnsupportedException : public yexception {};

class ITypeVisitor {
public:
    virtual ~ITypeVisitor() = default;

    virtual void OnVoid() = 0;
    virtual void OnNull() = 0;
    virtual void OnEmptyList() = 0;
    virtual void OnEmptyDict() = 0;
    virtual void OnBool() = 0;
    virtual void OnInt8() = 0;
    virtual void OnUint8() = 0;
    virtual void OnInt16() = 0;
    virtual void OnUint16() = 0;
    virtual void OnInt32() = 0;
    virtual void OnUint32() = 0;
    virtual void OnInt64() = 0;
    virtual void OnUint64() = 0;
    virtual void OnFloat() = 0;
    virtual void OnDouble() = 0;
    virtual void OnString() = 0;
    virtual void OnUtf8() = 0;
    virtual void OnYson() = 0;
    virtual void OnJson() = 0;
    virtual void OnJsonDocument() = 0;
    virtual void OnUuid() = 0;
    virtual void OnDyNumber() = 0;
    virtual void OnDate() = 0;
    virtual void OnDatetime() = 0;
    virtual void OnTimestamp() = 0;
    virtual void OnTzDate() = 0;
    virtual void OnTzDatetime() = 0;
    virtual void OnTzTimestamp() = 0;
    virtual void OnInterval() = 0;
    virtual void OnDate32() = 0;
    virtual void OnDatetime64() = 0;
    virtual void OnTimestamp64() = 0;
    virtual void OnTzDate32() = 0;
    virtual void OnTzDatetime64() = 0;
    virtual void OnTzTimestamp64() = 0;
    virtual void OnInterval64() = 0;
    virtual void OnDecimal(ui32 precision, ui32 scale) = 0;
    virtual void OnBeginOptional() = 0;
    virtual void OnEndOptional() = 0;
    virtual void OnBeginList() = 0;
    virtual void OnEndList() = 0;
    virtual void OnBeginTuple() = 0;
    virtual void OnTupleItem() = 0;
    virtual void OnEndTuple() = 0;
    virtual void OnBeginStruct() = 0;
    virtual void OnStructItem(TStringBuf member) = 0;
    virtual void OnEndStruct() = 0;
    virtual void OnBeginDict() = 0;
    virtual void OnDictKey() = 0;
    virtual void OnDictPayload() = 0;
    virtual void OnEndDict() = 0;
    virtual void OnBeginVariant() = 0;
    virtual void OnEndVariant() = 0;
    virtual void OnBeginTagged(TStringBuf tag) = 0;
    virtual void OnEndTagged() = 0;
    virtual void OnPgType(TStringBuf name, TStringBuf category) = 0;
};

class TThrowingTypeVisitor : public ITypeVisitor {
public:
    void OnVoid() override;
    void OnNull() override;
    void OnEmptyList() override;
    void OnEmptyDict() override;
    void OnBool() override;
    void OnInt8() override;
    void OnUint8() override;
    void OnInt16() override;
    void OnUint16() override;
    void OnInt32() override;
    void OnUint32() override;
    void OnInt64() override;
    void OnUint64() override;
    void OnFloat() override;
    void OnDouble() override;
    void OnString() override;
    void OnUtf8() override;
    void OnYson() override;
    void OnJson() override;
    void OnJsonDocument() override;
    void OnUuid() override;
    void OnDyNumber() override;
    void OnDate() override;
    void OnDatetime() override;
    void OnTimestamp() override;
    void OnTzDate() override;
    void OnTzDatetime() override;
    void OnTzTimestamp() override;
    void OnInterval() override;
    void OnDate32() override;
    void OnDatetime64() override;
    void OnTimestamp64() override;
    void OnTzDate32() override;
    void OnTzDatetime64() override;
    void OnTzTimestamp64() override;
    void OnInterval64() override;
    void OnDecimal(ui32 precision, ui32 scale) override;
    void OnBeginOptional() override;
    void OnEndOptional() override;
    void OnBeginList() override;
    void OnEndList() override;
    void OnBeginTuple() override;
    void OnTupleItem() override;
    void OnEndTuple() override;
    void OnBeginStruct() override;
    void OnStructItem(TStringBuf member) override;
    void OnEndStruct() override;
    void OnBeginDict() override;
    void OnDictKey() override;
    void OnDictPayload() override;
    void OnEndDict() override;
    void OnBeginVariant() override;
    void OnEndVariant() override;
    void OnBeginTagged(TStringBuf tag) override;
    void OnEndTagged() override;
    void OnPgType(TStringBuf name, TStringBuf category) override;
};

void ParseType(const NYT::TNode& typeNode, ITypeVisitor& visitor);

class TTypeBuilder : public ITypeVisitor {
public:
    TTypeBuilder();
    const NYT::TNode& GetResult() const;

private:
    void OnVoid() final;
    void OnNull() final;
    void OnEmptyList() final;
    void OnEmptyDict() final;
    void OnBool() final;
    void OnInt8() final;
    void OnUint8() final;
    void OnInt16() final;
    void OnUint16() final;
    void OnInt32() final;
    void OnUint32() final;
    void OnInt64() final;
    void OnUint64() final;
    void OnFloat() final;
    void OnDouble() final;
    void OnString() final;
    void OnUtf8() final;
    void OnYson() final;
    void OnJson() final;
    void OnJsonDocument() final;
    void OnUuid() final;
    void OnDyNumber() final;
    void OnDate() final;
    void OnDatetime() final;
    void OnTimestamp() final;
    void OnTzDate() final;
    void OnTzDatetime() final;
    void OnTzTimestamp() final;
    void OnInterval() final;
    void OnDate32() final;
    void OnDatetime64() final;
    void OnTimestamp64() final;
    void OnTzDate32() final;
    void OnTzDatetime64() final;
    void OnTzTimestamp64() final;
    void OnInterval64() final;
    void OnDecimal(ui32 precision, ui32 scale) final;
    void OnBeginOptional() final;
    void OnEndOptional() final;
    void OnBeginList() final;
    void OnEndList() final;
    void OnBeginTuple() final;
    void OnTupleItem() final;
    void OnEndTuple() final;
    void OnBeginStruct() final;
    void OnStructItem(TStringBuf member) final;
    void OnEndStruct() final;
    void OnBeginDict() final;
    void OnDictKey() final;
    void OnDictPayload() final;
    void OnEndDict() final;
    void OnBeginVariant() final;
    void OnEndVariant() final;
    void OnBeginTagged(TStringBuf tag) final;
    void OnEndTagged() final;
    void OnPgType(TStringBuf name, TStringBuf category) final;

private:
    NYT::TNode& Top();
    void Push();
    void Pop();

private:
    NYT::TNode Root;
    TVector<NYT::TNode*> Stack;
};

}
