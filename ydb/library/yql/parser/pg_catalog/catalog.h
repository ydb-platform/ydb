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
    TString Code;
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
    TString ElementType;
    bool PassByValue = false;
};

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds);
const TProcDesc& LookupProc(ui32 procId);

const TTypeDesc& LookupType(const TString& name);
const TTypeDesc& LookupType(ui32 typeId);

}
