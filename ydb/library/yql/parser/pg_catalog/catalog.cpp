#include "catalog.h"
#include <util/generic/utility.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <library/cpp/resource/resource.h>

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

using TOperators = THashMap<ui32, TOperDesc>;

struct TProcDesc {
    ui32 ProcId = 0;
    TString Name;
    TString Src;
    TVector<TString> ArgTypes;
    TString ResultType;
    bool IsStrict = true;
    bool IsVariadic = false;
};

using TProcs = THashMap<ui32, TProcDesc>;

class TParser {
public:
    void Do(const TString& dat) {
        enum class EState {
            WaitBracket,
            InsideBrackets,
            WaitForEndOfKey,
            WaitForValue,
            WaitForEndOfValue
        };

        EState state = EState::WaitBracket;
        TStringBuilder key;
        TStringBuilder value;
        for (char c : dat) {
            switch (state) {
                case EState::WaitBracket: {
                    if (c == '{') {
                        state = EState::InsideBrackets;
                    }

                    break;
                }
                case EState::InsideBrackets: {
                    if (c == '}') {
                        state = EState::WaitBracket;
                        OnFinish();
                        continue;
                    }

                    if (c == ' ' || c == ',' || c == '\n') {
                        continue;
                    }

                    key.clear();
                    key << c;
                    state = EState::WaitForEndOfKey;
                    break;
                }
                case EState::WaitForEndOfKey: {
                    if (c != ' ') {
                        key << c;
                        continue;
                    }

                    state = EState::WaitForValue;
                    break;
                }
                case EState::WaitForValue: {
                    if (c != '\'') {
                        continue;
                    }

                    state = EState::WaitForEndOfValue;
                    value.clear();
                    break;
                }
                case EState::WaitForEndOfValue: {
                    if (c != '\'') {
                        value << c;
                        continue;
                    }

                    state = EState::InsideBrackets;
                    OnKey(key, value);
                    break;
                }
            }
        }
    }

    virtual void OnFinish() = 0;
    virtual void OnKey(const TString& key, const TString& value) = 0;
};

class TOperatorsParser : public TParser {
public:
    TOperatorsParser(TOperators& operators)
        : Operators(operators)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastOperator.OperId = FromString<ui32>(value);
        } else if (key == "oprkind") {
            if (value == "r") {
                LastOperator.Kind = EOperKind::RightUnary;
            } else if (value == "l") {
                LastOperator.Kind = EOperKind::LeftUnary;
            }
        } else if (key == "oprleft") {
            if (value != "0") {
                LastOperator.LeftType = value;
            }
        } else if (key == "oprright") {
            if (value != "0") {
                LastOperator.RightType = value;
            }
        } else if (key == "oprresult") {
            LastOperator.ResultType = value;
        } else if (key == "oprcode") {
            LastOperator.Code = value;
        }
    }

    void OnFinish() override {
        Operators[LastOperator.OperId] = LastOperator;
        LastOperator = TOperDesc();
    }

private:
    TOperators& Operators;
    TOperDesc LastOperator;
};

class TProcsParser : public TParser {
public:
    TProcsParser(TProcs& procs)
        : Procs(procs)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastProc.ProcId = FromString<ui32>(value);
        } else if (key == "provariadic") {
            IsSupported = false;
        } else if (key == "prorettype") {
            LastProc.ResultType = value;
        } else if (key == "proname") {
            LastProc.Name = value;
        } else if (key == "prosrc") {
            LastProc.Src = value;
        } else if (key == "prolang") {
            IsSupported = false;
        } else if (key == "proargtypes") {
            Split(value, " ", LastProc.ArgTypes);
        } else if (key == "proisstrict") {
            LastProc.IsStrict = (value == "t");
        } else if (key == "proretset") {
            IsSupported = false;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            Procs[LastProc.ProcId] = LastProc;
        }

        IsSupported = true;
    }

private:
    TProcs& Procs;
    TProcDesc LastProc;
    bool IsSupported = true;
};

TOperators ParseOperators(const TString& dat) {
    TOperators ret;
    TOperatorsParser parser(ret);
    parser.Do(dat);
    return ret;
}

TProcs ParseProcs(const TString& dat) {
    TProcs ret;
    TProcsParser parser(ret);
    parser.Do(dat);
    return ret;
}

struct TCatalog {
    TCatalog() {
        TString opData;
        Y_ENSURE(NResource::FindExact("pg_operator.dat", &opData));
        TString procData;
        Y_ENSURE(NResource::FindExact("pg_proc.dat", &procData));
        Operators = ParseOperators(opData);
        Procs = ParseProcs(procData);
        for (const auto& [k, v]: Procs) {
           ProcByName[v.Name].push_back(k);
           ProcBySrc[v.Src] = k;
        }
    }

    static const TCatalog& Instance() {
        return *Singleton<TCatalog>();
    }

    TOperators Operators;
    TProcs Procs;
    THashMap<TString, TVector<ui32>> ProcByName;
    THashMap<TString, ui32> ProcBySrc;
};

TMaybe<TString> LookupFunctionSignature(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    auto srcIdPtr = catalog.ProcBySrc.FindPtr(name);
    ui32 procId;
    if (srcIdPtr) {
        procId = *srcIdPtr;
    } else {
        auto procIdPtr = catalog.ProcByName.FindPtr(name);
        if (!procIdPtr) {
            return Nothing();
        }

        if (procIdPtr->size() != 1) {
            throw yexception() << "Ambiguous name: " << name;
        }

        procId = procIdPtr->at(0);
    }

    auto desc = catalog.Procs.FindPtr(procId);
    Y_ENSURE(desc);
    TStringBuilder builder;
    builder << "Callable<(";
    for (size_t i = 0; i < desc->ArgTypes.size(); ++i) {
        if (i) {
            builder << ',';
        }

        builder << "pg_" << desc->ArgTypes[i];
    }

    builder << ")->";
    builder << "pg_" << desc->ResultType;
    builder << ">";

    return builder;
}

}
