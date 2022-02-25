#include "catalog.h"
#include <util/generic/utility.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <library/cpp/resource/resource.h>

namespace NYql::NPg {

using TOperators = THashMap<ui32, TOperDesc>;

using TProcs = THashMap<ui32, TProcDesc>;

using TTypes = THashMap<ui32, TTypeDesc>;

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
        bool AfterBackSlash = false;
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
                    if (c == '\\' && !AfterBackSlash) {
                        AfterBackSlash = true;
                        continue;
                    }

                    if (AfterBackSlash) {
                        AfterBackSlash = false;
                        value << c;
                        continue;
                    }

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
    TOperatorsParser(TOperators& operators, const THashMap<TString, ui32>& typeByName)
        : Operators(operators)
        , TypeByName(typeByName)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastOperator.OperId = FromString<ui32>(value);
        } else if (key == "oprname") {
            LastOperator.Name = value;
        } else if (key == "oprkind") {
            if (value == "r") {
                LastOperator.Kind = EOperKind::RightUnary;
            } else if (value == "l") {
                LastOperator.Kind = EOperKind::LeftUnary;
            }
        } else if (key == "oprleft") {
            if (value != "0") {
                auto typeIdPtr = TypeByName.FindPtr(value);
                Y_ENSURE(typeIdPtr);
                LastOperator.LeftType = *typeIdPtr;
            }
        } else if (key == "oprright") {
            if (value != "0") {
                auto typeIdPtr = TypeByName.FindPtr(value);
                Y_ENSURE(typeIdPtr);
                LastOperator.RightType = *typeIdPtr;
            }
        } else if (key == "oprresult") {
            auto typeIdPtr = TypeByName.FindPtr(value);
            Y_ENSURE(typeIdPtr);
            LastOperator.ResultType = *typeIdPtr;
        } else if (key == "oprcode") {
            LastOperator.Code = value;
        }
    }

    void OnFinish() override {
        Y_ENSURE(!LastOperator.Name.empty());
        Operators[LastOperator.OperId] = LastOperator;
        LastOperator = TOperDesc();
    }

private:
    TOperators& Operators;
    const THashMap<TString, ui32>& TypeByName;
    TOperDesc LastOperator;
};

class TProcsParser : public TParser {
public:
    TProcsParser(TProcs& procs, const THashMap<TString, ui32>& typeByName)
        : Procs(procs)
        , TypeByName(typeByName)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastProc.ProcId = FromString<ui32>(value);
        } else if (key == "provariadic") {
            IsSupported = false;
        } else if (key == "prorettype") {
            auto idPtr = TypeByName.FindPtr(value);
            Y_ENSURE(idPtr);
            LastProc.ResultType = *idPtr;
        } else if (key == "proname") {
            LastProc.Name = value;
        } else if (key == "prosrc") {
            LastProc.Src = value;
        } else if (key == "prolang") {
            IsSupported = false;
        } else if (key == "proargtypes") {
            TVector<TString> strArgs;
            Split(value, " ", strArgs);
            LastProc.ArgTypes.reserve(strArgs.size());
            for (const auto& s : strArgs) {
                auto idPtr = TypeByName.FindPtr(s);
                Y_ENSURE(idPtr);
                LastProc.ArgTypes.push_back(*idPtr);
            }
        } else if (key == "proisstrict") {
            LastProc.IsStrict = (value == "t");
        } else if (key == "proretset") {
            IsSupported = false;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            Y_ENSURE(!LastProc.Name.empty());
            Procs[LastProc.ProcId] = LastProc;
        }

        IsSupported = true;
        LastProc = TProcDesc();
    }

private:
    TProcs& Procs;
    const THashMap<TString, ui32>& TypeByName;
    TProcDesc LastProc;
    bool IsSupported = true;
};

class TTypesParser : public TParser {
public:
    TTypesParser(TTypes& types)
        : Types(types)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastType.TypeId = FromString<ui32>(value);
        } else if (key == "array_type_oid") {
            LastType.ArrayTypeId = FromString<ui32>(value);
        } else if (key == "typname") {
            LastType.Name = value;
        } else if (key == "typelem") {
            LastType.ElementType = value;
        }
    }

    void OnFinish() override {
        Y_ENSURE(!LastType.Name.empty());
        Types[LastType.TypeId] = LastType;
        if (LastType.ArrayTypeId) {
            Types[LastType.ArrayTypeId] = LastType;
        }

        LastType = TTypeDesc();
    }

private:
    TTypes& Types;
    TTypeDesc LastType;
};

TOperators ParseOperators(const TString& dat, const THashMap<TString, ui32>& typeByName) {
    TOperators ret;
    TOperatorsParser parser(ret, typeByName);
    parser.Do(dat);
    return ret;
}

TProcs ParseProcs(const TString& dat, const THashMap<TString, ui32>& typeByName) {
    TProcs ret;
    TProcsParser parser(ret, typeByName);
    parser.Do(dat);
    return ret;
}

TTypes ParseTypes(const TString& dat) {
    TTypes ret;
    TTypesParser parser(ret);
    parser.Do(dat);
    return ret;
}

struct TCatalog {
    TCatalog() {
        TString typeData;
        Y_ENSURE(NResource::FindExact("pg_type.dat", &typeData));
        TString opData;
        Y_ENSURE(NResource::FindExact("pg_operator.dat", &opData));
        TString procData;
        Y_ENSURE(NResource::FindExact("pg_proc.dat", &procData));
        Types = ParseTypes(typeData);
        for (const auto&[k, v] : Types) {
            if (k == v.TypeId) {
                Y_ENSURE(TypeByName.insert(std::make_pair(v.Name, k)).second);
            }

            if (k == v.ArrayTypeId) {
                Y_ENSURE(TypeByName.insert(std::make_pair("_" + v.Name, k)).second);
            }
        }

        Operators = ParseOperators(opData, TypeByName);
        Procs = ParseProcs(procData, TypeByName);

        for (const auto& [k, v]: Procs) {
            ProcByName[v.Name].push_back(k);
        }
    }

    static const TCatalog& Instance() {
        return *Singleton<TCatalog>();
    }

    TOperators Operators;
    TProcs Procs;
    TTypes Types;
    THashMap<TString, TVector<ui32>> ProcByName;
    THashMap<TString, ui32> TypeByName;
};

bool ValidateArgs(const TProcDesc& d, const TVector<ui32>& argTypeIds) {
    if (argTypeIds.size() != d.ArgTypes.size()) {
        return false;
    }

    bool found = true;
    for (size_t i = 0; i < argTypeIds.size(); ++i) {
        if (argTypeIds[i] == 0) {
            continue; // NULL
        }

        if (argTypeIds[i] != d.ArgTypes[i]) {
            found = false;
            break;
        }
    }

    return found;
}

const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto procPtr = catalog.Procs.FindPtr(procId);
    if (!procPtr) {
        throw yexception() << "No such proc: " << procId;
    }

    if (!ValidateArgs(*procPtr, argTypeIds)) {
        throw yexception() << "Unable to find an overload for with oid " << procId << " with given argument types";
    }

    return *procPtr;
}

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto procIdPtr = catalog.ProcByName.FindPtr(name);
    if (!procIdPtr) {
        throw yexception() << "No such function: " << name;
    }

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.Procs.FindPtr(id);
        Y_ENSURE(d);
        if (!ValidateArgs(*d, argTypeIds)) {
            continue;
        }

        return *d;
    }

    throw yexception() << "Unable to find an overload for function " << name << " with given argument types";
}

const TProcDesc& LookupProc(ui32 procId) {
    const auto& catalog = TCatalog::Instance();
    auto procPtr = catalog.Procs.FindPtr(procId);
    if (!procPtr) {
        throw yexception() << "No such proc: " << procId;
    }

    return *procPtr;
}

const TTypeDesc& LookupType(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    auto typeIdPtr = catalog.TypeByName.FindPtr(name);
    if (!typeIdPtr) {
        throw yexception() << "No such type: " << name;
    }

    auto typePtr = catalog.Types.FindPtr(*typeIdPtr);
    Y_ENSURE(typePtr);
    return *typePtr;
}

const TTypeDesc& LookupType(ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    auto typePtr = catalog.Types.FindPtr(typeId);
    if (!typePtr) {
        throw yexception() << "No such type: " << typeId;
    }

    return *typePtr;
}

}
