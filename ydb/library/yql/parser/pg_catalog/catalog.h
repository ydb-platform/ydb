#pragma once
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

enum class EOperKind {
    Binary,
    LeftUnary,
    RightUnary
};

struct TOperDesc {
    ui32 OperId = 0;
    EOperKind Kind = EOperKind::Binary;
    TString LeftType;
    TString RightType;
    TString ResultType;
    TString Code;
};

struct TProcDesc {
    ui32 ProcId = 0;
    TString Name;
    TString Src;
    TVector<TString> ArgTypes;
    TString ResultType;
    bool IsStrict = true;
};

const TProcDesc& LookupProc(const TString& name, const TVector<TString>& argTypes);

}
