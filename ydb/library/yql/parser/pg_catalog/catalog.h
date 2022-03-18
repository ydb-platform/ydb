#pragma once
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql::NPg {

enum class EOperKind {
    Binary,
    LeftUnary,
    RightUnary
};

struct TOperDesc {
    ui32 OperId = 0;
    TString Name;
    EOperKind Kind = EOperKind::Binary;
    ui32 LeftType = 0;
    ui32 RightType = 0;
    ui32 ResultType = 0;
    ui32 ProcId = 0;
};

struct TProcDesc {
    ui32 ProcId = 0;
    TString Name;
    TString Src;
    TVector<ui32> ArgTypes;
    ui32 ResultType = 0;
    bool IsStrict = true;
};

struct TTypeDesc {
    ui32 TypeId = 0;
    ui32 ArrayTypeId = 0;
    TString Name;
    ui32 ElementTypeId = 0;
    bool PassByValue = false;
    char Category = '\0';
    ui32 InFuncId = 0;
    ui32 OutFuncId = 0;
    ui32 SendFuncId = 0;
    ui32 ReceiveFuncId = 0;
    i32 TypeLen = 0;
};

enum class ECastMethod {
    Function,
    InOut,
    Binary
};

struct TCastDesc {
    ui32 SourceId = 0;
    ui32 TargetId = 0;
    ECastMethod Method = ECastMethod::Function;
    ui32 FunctionId = 0;
};

enum class EAggKind {
    Normal,
    OrderedSet,
    Hypothetical
};

struct TAggregateDesc {
    TString Name;
    TVector<ui32> ArgTypes;
    EAggKind Kind = EAggKind::Normal;
    ui32 TransTypeId = 0;
    ui32 TransFuncId = 0;
    ui32 FinalFuncId = 0;
    ui32 CombineFuncId = 0;
    ui32 SerializeFuncId = 0;
    ui32 DeserializeFuncId = 0;
    TString InitValue;
};

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId);

bool HasType(const TStringBuf& name);
const TTypeDesc& LookupType(const TString& name);
const TTypeDesc& LookupType(ui32 typeId);

bool HasCast(ui32 sourceId, ui32 targetId);
const TCastDesc& LookupCast(ui32 sourceId, ui32 targetId);

const TOperDesc& LookupOper(const TString& name, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId, const TVector<ui32>& argTypeIds);
const TOperDesc& LookupOper(ui32 operId);

bool HasAggregation(const TStringBuf& name);
const TAggregateDesc& LookupAggregation(const TStringBuf& name, const TVector<ui32>& argTypeIds);

bool IsCompatibleTo(ui32 actualType, ui32 expectedType);

}
