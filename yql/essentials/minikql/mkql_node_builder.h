#pragma once
#include "defs.h"
#include "mkql_node.h"

namespace NKikimr::NMiniKQL {

TDataLiteral* BuildDataLiteral(const NUdf::TStringRef& data, NUdf::TDataTypeId type, const TTypeEnvironment& env);
inline TDataLiteral* BuildDataLiteral(const NUdf::TStringRef& data, NUdf::EDataSlot slot, const TTypeEnvironment& env) {
    return BuildDataLiteral(data, NUdf::GetDataTypeInfo(slot).TypeId, env);
}

TDataLiteral* BuildDataLiteral(const NUdf::TUnboxedValuePod& value, NUdf::TDataTypeId type, const TTypeEnvironment& env);
inline TDataLiteral* BuildDataLiteral(const NUdf::TUnboxedValuePod& value, NUdf::EDataSlot slot, const TTypeEnvironment& env) {
    return BuildDataLiteral(value, NUdf::GetDataTypeInfo(slot).TypeId, env);
}

TOptionalLiteral* BuildOptionalLiteral(TRuntimeNode value, const TTypeEnvironment& env);
TOptionalLiteral* BuildEmptyOptionalLiteral(TType* itemType, const TTypeEnvironment& env);
TOptionalLiteral* BuildEmptyOptionalDataLiteral(NUdf::TDataTypeId schemeType, const TTypeEnvironment& env);
TType* UnpackOptional(TRuntimeNode data, bool& isOptional);
TType* UnpackOptional(TType* type, bool& isOptional);
TDataType* UnpackOptionalData(TRuntimeNode data, bool& isOptional);
TDataType* UnpackOptionalData(TType* type, bool& isOptional);
TType* UnpackOptionalBlockItemType(TBlockType* type, const TTypeEnvironment& env);

TBlockType::EShape GetResultShape(const TVector<TType*>& types);

class TTupleLiteralBuilder {
public:
    explicit TTupleLiteralBuilder(const TTypeEnvironment& env);
    TTupleLiteralBuilder(const TTupleLiteralBuilder&) = default;
    TTupleLiteralBuilder& operator=(const TTupleLiteralBuilder&) = default;
    void Reserve(ui32 size);
    TTupleLiteralBuilder& Add(TRuntimeNode value);
    TTupleLiteral* Build();
    void Clear();

private:
    const TTypeEnvironment& Env_;
    TVector<TType*> Types_;
    TVector<TRuntimeNode> Values_;
};

class TStructTypeBuilder {
public:
    explicit TStructTypeBuilder(const TTypeEnvironment& env);
    TStructTypeBuilder(const TStructTypeBuilder&) = default;
    TStructTypeBuilder& operator=(const TStructTypeBuilder&) = default;
    void Reserve(ui32 size);
    TStructTypeBuilder& Add(const TStringBuf& name, TType* type, ui32* index = nullptr);
    TStructType* Build();
    void FillIndexes();
    void Clear();

private:
    const TTypeEnvironment* Env_;
    TVector<TStructMember> Members_;
};

class TListLiteralBuilder {
public:
    TListLiteralBuilder(const TTypeEnvironment& env, TType* type);
    TListLiteralBuilder(const TListLiteralBuilder&) = default;
    TListLiteralBuilder& operator=(const TListLiteralBuilder&) = default;
    TListLiteralBuilder& Add(TRuntimeNode item);
    TListLiteral* Build();
    void Clear();

private:
    const TTypeEnvironment* Env_;
    TType* Type_;
    TVector<TRuntimeNode> Items_;
};

class TStructLiteralBuilder {
public:
    explicit TStructLiteralBuilder(const TTypeEnvironment& env);
    TStructLiteralBuilder(const TStructLiteralBuilder&) = default;
    TStructLiteralBuilder& operator=(const TStructLiteralBuilder&) = default;
    void Reserve(ui32 size);
    TStructLiteralBuilder& Add(const TStringBuf& name, TRuntimeNode value);
    TStructLiteral* Build();
    void Clear();

private:
    const TTypeEnvironment* Env_;
    TVector<TStructMember> Members_;
    TVector<TRuntimeNode> Values_;
};

class TDictLiteralBuilder {
public:
    TDictLiteralBuilder(const TTypeEnvironment& env, TType* keyType, TType* payloadType);
    TDictLiteralBuilder(const TDictLiteralBuilder&) = default;
    TDictLiteralBuilder& operator=(const TDictLiteralBuilder&) = default;
    void Reserve(ui32 size);
    TDictLiteralBuilder& Add(TRuntimeNode key, TRuntimeNode payload);
    TDictLiteral* Build();
    void Clear();

private:
    const TTypeEnvironment* Env_;
    TType* KeyType_;
    TType* PayloadType_;
    TVector<std::pair<TRuntimeNode, TRuntimeNode>> Items_;
};

class TCallableTypeBuilder {
public:
    TCallableTypeBuilder(const TTypeEnvironment& env, const TStringBuf& name, TType* returnType);
    TCallableTypeBuilder(const TCallableTypeBuilder&) = default;
    TCallableTypeBuilder& operator=(const TCallableTypeBuilder&) = default;
    void Reserve(ui32 size);
    TCallableTypeBuilder& Add(TType* time);
    TCallableTypeBuilder& SetArgumentName(const TStringBuf& name);
    TCallableTypeBuilder& SetArgumentFlags(ui64 flags);
    TCallableTypeBuilder& SetOptionalArgs(ui32 count);
    TCallableTypeBuilder& SetPayload(const TStringBuf& data);
    TCallableType* Build();
    void Clear();

private:
    const TTypeEnvironment* Env_;
    TInternName Name_;
    TType* ReturnType_;
    TVector<TType*> Arguments_;
    ui32 OptionalArgsCount_;
    TVector<TStringBuf> ArgNames_;
    TVector<ui64> ArgFlags_;
    TStringBuf FuncPayload_;
    bool HasPayload_;
};

class TCallableBuilder {
public:
    TCallableBuilder(const TTypeEnvironment& env, const TStringBuf& name, TType* returnType,
                     bool disableMerge = false);
    TCallableBuilder(const TCallableBuilder&) = default;
    TCallableBuilder& operator=(const TCallableBuilder&) = default;
    void Reserve(ui32 size);
    TCallableBuilder& Add(TRuntimeNode input);
    TCallableBuilder& SetArgumentName(const TStringBuf& name);
    TCallableBuilder& SetArgumentFlags(ui64 flags);
    TCallableBuilder& SetOptionalArgs(ui32 count);
    TCallableBuilder& SetTypePayload(const TStringBuf& data);
    TCallable* Build();
    void Clear();

private:
    const TTypeEnvironment* Env_;
    TInternName Name_;
    TType* ReturnType_;
    bool DisableMerge_;
    TVector<TType*> Arguments_;
    TVector<TRuntimeNode> Inputs_;
    ui32 OptionalArgsCount_;
    TVector<TStringBuf> ArgNames_;
    TVector<ui64> ArgFlags_;
    TStringBuf FuncPayload_;
    bool HasPayload_;
};

} // namespace NKikimr::NMiniKQL
