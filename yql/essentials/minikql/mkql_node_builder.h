#pragma once
#include "defs.h"
#include "mkql_node.h"

namespace NKikimr {
namespace NMiniKQL {

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

TBlockType::EShape GetResultShape(const TVector<TType*>& types);

class TTupleLiteralBuilder {
public:
    TTupleLiteralBuilder(const TTypeEnvironment& env);
    TTupleLiteralBuilder(const TTupleLiteralBuilder&) = default;
    TTupleLiteralBuilder& operator=(const TTupleLiteralBuilder&) = default;
    void Reserve(ui32 size);
    TTupleLiteralBuilder& Add(TRuntimeNode value);
    TTupleLiteral* Build();
    void Clear();
private:
    const TTypeEnvironment& Env;
    TVector<TType*> Types;
    TVector<TRuntimeNode> Values;
};

class TStructTypeBuilder {
public:
    TStructTypeBuilder(const TTypeEnvironment& env);
    TStructTypeBuilder(const TStructTypeBuilder&) = default;
    TStructTypeBuilder& operator=(const TStructTypeBuilder&) = default;
    void Reserve(ui32 size);
    TStructTypeBuilder& Add(const TStringBuf& name, TType* type, ui32* index = nullptr);
    TStructType* Build();
    void FillIndexes();
    void Clear();

private:
    const TTypeEnvironment* Env;
    TVector<TStructMember> Members;
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
    const TTypeEnvironment* Env;
    TType* Type;
    TVector<TRuntimeNode> Items;
};

class TStructLiteralBuilder {
public:
    TStructLiteralBuilder(const TTypeEnvironment& env);
    TStructLiteralBuilder(const TStructLiteralBuilder&) = default;
    TStructLiteralBuilder& operator=(const TStructLiteralBuilder&) = default;
    void Reserve(ui32 size);
    TStructLiteralBuilder& Add(const TStringBuf& name, TRuntimeNode value);
    TStructLiteral* Build();
    void Clear();

private:
    const TTypeEnvironment* Env;
    TVector<TStructMember> Members;
    TVector<TRuntimeNode> Values;
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
    const TTypeEnvironment* Env;
    TType* KeyType;
    TType* PayloadType;
    TVector<std::pair<TRuntimeNode, TRuntimeNode>> Items;
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
    const TTypeEnvironment* Env;
    TInternName Name;
    TType* ReturnType;
    TVector<TType*> Arguments;
    ui32 OptionalArgsCount;
    TVector<TStringBuf> ArgNames;
    TVector<ui64> ArgFlags;
    TStringBuf FuncPayload;
    bool HasPayload;
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
    const TTypeEnvironment* Env;
    TInternName Name;
    TType* ReturnType;
    bool DisableMerge;
    TVector<TType*> Arguments;
    TVector<TRuntimeNode> Inputs;
    ui32 OptionalArgsCount;
    TVector<TStringBuf> ArgNames;
    TVector<ui64> ArgFlags;
    TStringBuf FuncPayload;
    bool HasPayload;
};

}
}
