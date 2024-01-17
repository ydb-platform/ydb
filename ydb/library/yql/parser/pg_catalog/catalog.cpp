#include "catalog.h"
#include <util/generic/utility.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <library/cpp/resource/resource.h>

namespace NYql::NPg {

constexpr ui32 InvalidOid = 0;
constexpr ui32 Int2VectorOid = 22;
constexpr ui32 OidVectorOid = 30;
//constexpr ui32 AnyElementOid = 2283;
//constexpr ui32 AnyNonArrayOid = 2776;
//constexpr ui32 AnyCompatibleOid = 5077;
//constexpr ui32 AnyCompatibleArrayOid = 5078;
//constexpr ui32 AnyCompatibleNonArrayOid = 5079;

using TOperators = THashMap<ui32, TOperDesc>;

using TProcs = THashMap<ui32, TProcDesc>;

using TTypes = THashMap<ui32, TTypeDesc>;

using TCasts = THashMap<ui32, TCastDesc>;

using TAggregations = THashMap<ui32, TAggregateDesc>;

// We parse OpFamilies' IDs for now. If we ever needed oid_symbol,
// create TOpFamilyDesc class alike other catalogs
using TOpFamilies = THashMap<TString, ui32>;

using TOpClasses = THashMap<std::pair<EOpClassMethod, ui32>, TOpClassDesc>;

using TAmOps = THashMap<std::tuple<ui32, ui32, ui32, ui32>, TAmOpDesc>;

using TAmProcs = THashMap<std::tuple<ui32, ui32, ui32, ui32>, TAmProcDesc>;

using TConversions = THashMap<std::pair<TString, TString>, TConversionDesc>;

bool IsCompatibleTo(ui32 actualTypeId, ui32 expectedTypeId, const TTypes& types) {
    if (actualTypeId == expectedTypeId) {
        return true;
    }

    if (actualTypeId == InvalidOid) {
        return true;
    }

    if (actualTypeId == UnknownOid) {
        return true;
    }

    if (expectedTypeId == AnyOid) {
        return true;
    }

    // TODO: add checks for polymorphic types

    if (expectedTypeId == UnknownOid) {
        return true;
    }

    if (expectedTypeId == AnyArrayOid) {
        const auto& actualDescPtr = types.FindPtr(actualTypeId);
        Y_ENSURE(actualDescPtr);
        return actualDescPtr->ArrayTypeId == actualDescPtr->TypeId;
    }

    return false;
}

TString ArgTypesList(const TVector<ui32>& ids) {
    TStringBuilder str;
    str << '(';
    for (ui32 i = 0; i < ids.size(); ++i) {
        if (i > 0) {
            str << ',';
        }

        str << (ids[i] ? LookupType(ids[i]).Name : "NULL");
    }

    str << ')';
    return str;
}

TStringBuf GetCanonicalTypeName(TStringBuf name) {
    if (name == "boolean") {
        return "bool";
    }

    if (name == "character") {
        return "bpchar";
    }

    if (name == "charactervarying") {
        return "varchar";
    }

    if (name == "bitvarying") {
        return "varbit";
    }

    if (name == "real") {
        return "float4";
    }

    if (name == "doubleprecision") {
        return "float8";
    }

    if (name == "smallint") {
        return "int2";
    }

    if (name == "integer" || name == "int") {
        return "int4";
    }

    if (name == "bigint") {
        return "int8";
    }

    if (name == "timewithouttimezone") {
        return "time";
    }

    if (name == "timewithtimezone") {
        return "timetz";
    }

    if (name == "timestampwithouttimezone") {
        return "timestamp";
    }

    if (name == "timestampwithtimezone") {
        return "timestamptz";
    }

    if (name == "decimal") {
        return "numeric";
    }

    return name;
}

class TParser {
public:
    virtual ~TParser() = default;
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

bool ValidateOperArgs(const TOperDesc& d, const TVector<ui32>& argTypeIds, const TTypes& types) {
    ui32 size = d.Kind == EOperKind::Binary ? 2 : 1;
    if (argTypeIds.size() != size) {
        return false;
    }

    for (size_t i = 0; i < argTypeIds.size(); ++i) {
        ui32 expectedArgType;
        if (d.Kind == EOperKind::RightUnary || (d.Kind == EOperKind::Binary && i == 0)) {
            expectedArgType = d.LeftType;
        }
        else {
            expectedArgType = d.RightType;
        }

        if (!IsCompatibleTo(argTypeIds[i], expectedArgType, types)) {
            return false;
        }
    }

    return true;
}

class TOperatorsParser : public TParser {
public:
    TOperatorsParser(TOperators& operators, const THashMap<TString, ui32>& typeByName, const TTypes& types,
        const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs)
        : Operators(operators)
        , TypeByName(typeByName)
        , Types(types)
        , ProcByName(procByName)
        , Procs(procs)
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
            LastCode = value;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            auto pos = LastCode.find('(');
            auto code = LastCode.substr(0, pos);
            auto procIdPtr = ProcByName.FindPtr(code);
            // skip operator if proc isn't builtin, e.g. path_contain_pt
            if (!procIdPtr) {
                IsSupported = false;
            } else {
                for (auto procId : *procIdPtr) {
                    auto procPtr = Procs.FindPtr(procId);
                    Y_ENSURE(procPtr);
                    if (ValidateOperArgs(LastOperator, procPtr->ArgTypes, Types)) {
                        Y_ENSURE(!LastOperator.ProcId);
                        LastOperator.ProcId = procId;
                    }
                }

                // can be missing for example jsonb - _text
                if (LastOperator.ProcId) {
                    Y_ENSURE(!LastOperator.Name.empty());
                    Operators[LastOperator.OperId] = LastOperator;
                }
            }
        }

        LastOperator = TOperDesc();
        LastCode = "";
        IsSupported = true;
    }

private:
    TOperators& Operators;
    const THashMap<TString, ui32>& TypeByName;
    const TTypes& Types;
    const THashMap<TString, TVector<ui32>>& ProcByName;
    const TProcs& Procs;
    TOperDesc LastOperator;
    bool IsSupported = true;
    TString LastCode;
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
        } else if (key == "prokind") {
            if (value == "f") {
                LastProc.Kind = EProcKind::Function;
            } else if (value == "a") {
                LastProc.Kind = EProcKind::Aggregate;
            } else if (value == "w") {
                LastProc.Kind = EProcKind::Window;
            } else {
                IsSupported = false;
            }
        } else if (key == "prorettype") {
            auto idPtr = TypeByName.FindPtr(value);
            Y_ENSURE(idPtr);
            LastProc.ResultType = *idPtr;
        } else if (key == "proname") {
            LastProc.Name = value;
        } else if (key == "prosrc") {
            LastProc.Src = value;
        } else if (key == "prolang") {
            if (value != "c") {
                IsSupported = false;
            }
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
            LastProc.ReturnSet = (value == "t");
        } else if (key == "proallargtypes") {
            AllArgTypesStr = value;
        } else if (key == "proargmodes") {
            ArgModesStr = value;
        } else if (key == "proargnames") {
            ArgNamesStr = value;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            if (!ArgModesStr.empty()) {
                Y_ENSURE(!AllArgTypesStr.empty());
                Y_ENSURE(ArgModesStr.front() == '{');
                Y_ENSURE(ArgModesStr.back() == '}');
                TVector<TString> modes;
                Split(ArgModesStr.substr(1, ArgModesStr.size() - 2), ",", modes);
                Y_ENSURE(modes.size() >= LastProc.ArgTypes.size());
                for (size_t i = 0; i < modes.size(); ++i) {
                    if (i < LastProc.ArgTypes.size()) {
                        if (modes[i] != "i") {
                            IsSupported = false;
                            break;
                        }
                    } else if (modes[i] != "o") {
                        IsSupported = false;
                        break;
                    }
                }
            }
        }

        if (IsSupported) {
            if (!ArgNamesStr.empty()) {
                Y_ENSURE(ArgNamesStr.front() == '{');
                Y_ENSURE(ArgNamesStr.back() == '}');
                TVector<TString> names;
                Split(ArgNamesStr.substr(1, ArgNamesStr.size() - 2), ",", names);
                Y_ENSURE(names.size() >= LastProc.ArgTypes.size());
                LastProc.OutputArgNames.insert(LastProc.OutputArgNames.begin(), names.begin() + LastProc.ArgTypes.size(), names.end());
            }

            if (!AllArgTypesStr.empty()) {
                Y_ENSURE(!ArgModesStr.empty());
                Y_ENSURE(AllArgTypesStr.front() == '{');
                Y_ENSURE(AllArgTypesStr.back() == '}');
                TVector<TString> types;
                Split(AllArgTypesStr.substr(1, AllArgTypesStr.size() - 2), ",", types);
                Y_ENSURE(types.size() >= LastProc.ArgTypes.size());

                for (size_t i = LastProc.ArgTypes.size(); i < types.size(); ++i) {
                    auto idPtr = TypeByName.FindPtr(types[i]);
                    Y_ENSURE(idPtr);
                    LastProc.OutputArgTypes.push_back(*idPtr);
                }
            }
        }

        if (IsSupported) {
            Y_ENSURE(!LastProc.Name.empty());
            Procs[LastProc.ProcId] = LastProc;
        }

        IsSupported = true;
        LastProc = TProcDesc();
        AllArgTypesStr = "";
        ArgModesStr = "";
        ArgNamesStr = "";
    }

private:
    TProcs& Procs;
    const THashMap<TString, ui32>& TypeByName;
    TProcDesc LastProc;
    bool IsSupported = true;
    TString AllArgTypesStr;
    TString ArgModesStr;
    TString ArgNamesStr;
};

struct TLazyTypeInfo {
    TString ElementType;
    TString InFunc;
    TString OutFunc;
    TString SendFunc;
    TString ReceiveFunc;
    TString ModInFunc;
    TString ModOutFunc;
    TString SubscriptFunc;
};

class TTypesParser : public TParser {
public:
    TTypesParser(TTypes& types, THashMap<ui32, TLazyTypeInfo>& lazyInfos)
        : Types(types)
        , LazyInfos(lazyInfos)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastType.TypeId = FromString<ui32>(value);
        } else if (key == "array_type_oid") {
            LastType.ArrayTypeId = FromString<ui32>(value);
        } else if (key == "typname") {
            LastType.Name = value;
        } else if (key == "typcategory") {
            Y_ENSURE(value.size() == 1);
            LastType.Category = value[0];
        } else if (key == "typlen") {
            if (value == "NAMEDATALEN") {
                LastType.TypeLen = 64;
            } else if (value == "SIZEOF_POINTER") {
                LastType.TypeLen = 8;
            } else {
                LastType.TypeLen = FromString<i32>(value);
            }
        } else if (key == "typalign") {
            if (value == "ALIGNOF_POINTER") {
                LastType.TypeAlign = 'i'; // doesn't matter for pointers
            } else {
                Y_ENSURE(value.size() == 1);
                LastType.TypeAlign = value[0];
            }
        } else if (key == "typdelim") {
            Y_ENSURE(value.size() == 1);
            LastType.TypeDelim = value[0];
        } else if (key == "typtype") {
            Y_ENSURE(value.size() == 1);
            const auto typType = value[0];

            LastType.TypType =
                    (typType == 'b') ? ETypType::Base :
                    (typType == 'c') ? ETypType::Composite :
                    (typType == 'd') ? ETypType::Domain :
                    (typType == 'e') ? ETypType::Enum :
                    (typType == 'm') ? ETypType::Multirange :
                    (typType == 'p') ? ETypType::Pseudo :
                    (typType == 'r') ? ETypType::Range :
                    ythrow yexception() << "Unknown typtype value: " << value;
        } else if (key == "typcollation") {
            // hardcode collations for now. There are only three of 'em in .dat file
            LastType.TypeCollation =
                    (value == "default") ? DefaultCollationOid :
                    (value == "C") ? C_CollationOid :
                    (value == "POSIX") ? PosixCollationOid :
                    ythrow yexception() << "Unknown typcollation value: " << value;
        } else if (key == "typelem") {
            LastLazyTypeInfo.ElementType = value; // resolve later
        } else if (key == "typinput") {
            LastLazyTypeInfo.InFunc = value; // resolve later
        } else if (key == "typoutput") {
            LastLazyTypeInfo.OutFunc = value; // resolve later
        } else if (key == "typsend") {
            LastLazyTypeInfo.SendFunc = value; // resolve later
        } else if (key == "typreceive") {
            LastLazyTypeInfo.ReceiveFunc = value; // resolve later
        } else if (key == "typmodin") {
            LastLazyTypeInfo.ModInFunc = value; // resolve later
        } else if (key == "typmodout") {
            LastLazyTypeInfo.ModOutFunc = value; // resolve later
        } else if (key == "typsubscript") {
            LastLazyTypeInfo.SubscriptFunc = value; // resolve later
        } else if (key == "typbyval") {
            if (value == "f") {
                LastType.PassByValue = false;
            } else if (value == "t" || value == "FLOAT8PASSBYVAL") {
                LastType.PassByValue = true;
            } else {
                ythrow yexception() << "Unknown typbyval value: " << value;
            }
        } else if (key == "typispreferred") {
            LastType.IsPreferred = (value == "t");
        }
    }

    void OnFinish() override {
        Y_ENSURE(!LastType.Name.empty());
        Y_ENSURE(LastType.TypeLen != 0);
        if (LastType.TypeLen < 0 || LastType.TypeLen > 8) {
            Y_ENSURE(!LastType.PassByValue);
        }

        Types[LastType.TypeId] = LastType;
        if (LastType.ArrayTypeId) {
            auto arrayType = LastType;
            arrayType.Name = "_" + arrayType.Name;
            arrayType.ElementTypeId = arrayType.TypeId;
            arrayType.TypeId = LastType.ArrayTypeId;
            arrayType.PassByValue = false;
            arrayType.TypeLen = -1;
            Types[LastType.ArrayTypeId] = arrayType;
        }

        LazyInfos[LastType.TypeId] = LastLazyTypeInfo;

        LastType = TTypeDesc();
        LastLazyTypeInfo = TLazyTypeInfo();
    }

private:
    TTypes& Types;
    THashMap<ui32, TLazyTypeInfo>& LazyInfos;
    TTypeDesc LastType;
    TLazyTypeInfo LastLazyTypeInfo;
};

class TCastsParser : public TParser {
public:
    TCastsParser(TCasts& casts, const THashMap<TString, ui32>& typeByName, const TTypes& types,
        const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs)
        : Casts(casts)
        , TypeByName(typeByName)
        , Types(types)
        , ProcByName(procByName)
        , Procs(procs)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "castsource") {
            auto typePtr = TypeByName.FindPtr(value);
            Y_ENSURE(typePtr);
            LastCast.SourceId = *typePtr;
        } else if (key == "casttarget") {
            auto typePtr = TypeByName.FindPtr(value);
            Y_ENSURE(typePtr);
            LastCast.TargetId = *typePtr;
        } else if (key == "castfunc") {
            if (value != "0") {
                if (value.Contains('(')) {
                    auto pos1 = value.find('(');
                    auto pos2 = value.find(')');
                    auto pos3 = value.find(',');
                    Y_ENSURE(pos1 != TString::npos);
                    Y_ENSURE(pos2 != TString::npos);
                    if (pos3 != TString::npos) {
                        pos2 = pos3;
                    }

                    auto funcName = value.substr(0, pos1);
                    auto inputType = value.substr(pos1 + 1, pos2 - pos1 - 1);
                    auto inputTypeIdPtr = TypeByName.FindPtr(inputType);
                    Y_ENSURE(inputTypeIdPtr);
                    auto procIdPtr = ProcByName.FindPtr(funcName);
                    Y_ENSURE(procIdPtr);
                    bool found = false;
                    for (const auto& procId : *procIdPtr) {
                        auto procPtr = Procs.FindPtr(procId);
                        Y_ENSURE(procPtr);
                        if (procPtr->ArgTypes.size() < 1) {
                            continue;
                        }

                        if (IsCompatibleTo(*inputTypeIdPtr, procPtr->ArgTypes[0], Types)) {
                            LastCast.FunctionId = procPtr->ProcId;
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        // e.g. convert circle to 12-vertex polygon, used sql proc
                        IsSupported = false;
                    }
                } else {
                    auto procIdPtr = ProcByName.FindPtr(value);
                    Y_ENSURE(procIdPtr);
                    Y_ENSURE(procIdPtr->size() == 1);
                    LastCast.FunctionId = procIdPtr->at(0);
                }
            }
        } else if (key == "castmethod") {
            if (value == "f") {
                LastCast.Method = ECastMethod::Function;
            } else if (value == "i") {
                LastCast.Method = ECastMethod::InOut;
            } else if (value == "b") {
                LastCast.Method = ECastMethod::Binary;
            } else {
                ythrow yexception() << "Unknown castmethod value: " << value;
            }
        } else if (key == "castcontext") {
            Y_ENSURE(value.size() == 1);
            const auto castCtx = value[0];

            LastCast.CoercionCode =
                    (castCtx == 'i') ? ECoercionCode::Implicit :
                    (castCtx == 'a') ? ECoercionCode::Assignment :
                    (castCtx == 'e') ? ECoercionCode::Explicit :
                    ythrow yexception() << "Unknown castcontext value: " << value;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            auto id = 1 + Casts.size();
            Casts[id] = LastCast;
        }

        LastCast = TCastDesc();
        IsSupported = true;
    }

private:
    TCasts& Casts;
    const THashMap<TString, ui32>& TypeByName;
    const TTypes& Types;
    const THashMap<TString, TVector<ui32>>& ProcByName;
    const TProcs& Procs;
    TCastDesc LastCast;
    bool IsSupported = true;
};

class TAggregationsParser : public TParser {
public:
    TAggregationsParser(TAggregations& aggregations, const THashMap<TString, ui32>& typeByName,
        const TTypes& types, const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs)
        : Aggregations(aggregations)
        , TypeByName(typeByName)
        , Types(types)
        , ProcByName(procByName)
        , Procs(procs)
    {}

    void OnKey(const TString& key, const TString& value) override {
        Y_UNUSED(ProcByName);
        if (key == "aggtranstype") {
            auto typeId = TypeByName.FindPtr(value);
            Y_ENSURE(typeId);
            LastAggregation.TransTypeId = *typeId;
        } else if (key == "aggfnoid") {
            LastOid = value;
        } else if (key == "aggtransfn") {
            LastTransFunc = value;
        } else if (key == "aggfinalfn") {
            LastFinalFunc = value;
        } else if (key == "aggcombinefn") {
            LastCombineFunc = value;
        } else if (key == "aggserialfn") {
            LastSerializeFunc = value;
        } else if (key == "aggdeserialfn") {
            LastDeserializeFunc = value;
        } else if (key == "aggkind") {
            if (value == "n") {
                LastAggregation.Kind = EAggKind::Normal;
            } else if (value == "o") {
                LastAggregation.Kind = EAggKind::OrderedSet;
            } else if (value == "h") {
                LastAggregation.Kind = EAggKind::Hypothetical;
            } else {
                ythrow yexception() << "Unknown aggkind value: " << value;
            }
        } else if (key == "agginitval") {
            LastAggregation.InitValue = value;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            if (FillSupported()) {
                auto id = Aggregations.size() + 1;
                LastAggregation.InternalId = id;
                Aggregations[id] = LastAggregation;
            }
        }

        LastAggregation = TAggregateDesc();
        IsSupported = true;
        LastOid = "";
        LastTransFunc = "";
        LastFinalFunc = "";
        LastCombineFunc = "";
        LastSerializeFunc = "";
        LastDeserializeFunc = "";
    }

    bool FillSupported() {
        Y_ENSURE(LastAggregation.TransTypeId);
        Y_ENSURE(LastOid);
        Y_ENSURE(LastTransFunc);
        auto transFuncIdsPtr = ProcByName.FindPtr(LastTransFunc);
        if (!transFuncIdsPtr) {
            // e.g. variadic ordered_set_transition_multi
            return false;
        }

        for (const auto id : *transFuncIdsPtr) {
            auto procPtr = Procs.FindPtr(id);
            Y_ENSURE(procPtr);
            if (procPtr->ArgTypes.size() >= 1 &&
                IsCompatibleTo(LastAggregation.TransTypeId, procPtr->ArgTypes[0], Types)) {
                Y_ENSURE(!LastAggregation.TransFuncId);
                LastAggregation.TransFuncId = id;
            }
        }

        Y_ENSURE(LastAggregation.TransFuncId);

        // oid format: name(arg1,arg2...)
        auto pos1 = LastOid.find('(');
        if (pos1 != TString::npos) {
            LastAggregation.Name = LastOid.substr(0, pos1);
            auto pos = pos1 + 1;
            for (;;) {
                auto nextPos = Min(LastOid.find(',', pos), LastOid.find(')', pos));
                Y_ENSURE(nextPos != TString::npos);
                if (pos == nextPos) {
                    break;
                }

                auto arg = LastOid.substr(pos, nextPos - pos);
                auto argTypeId = TypeByName.FindPtr(arg);
                Y_ENSURE(argTypeId);
                LastAggregation.ArgTypes.push_back(*argTypeId);
                pos = nextPos;
                if (LastOid[pos] == ')') {
                    break;
                } else {
                    ++pos;
                }
            }
        } else {
            // no signature in oid, use transfunc
            LastAggregation.Name = LastOid;
            auto procPtr = Procs.FindPtr(LastAggregation.TransFuncId);
            Y_ENSURE(procPtr);
            LastAggregation.ArgTypes = procPtr->ArgTypes;
            Y_ENSURE(LastAggregation.ArgTypes.size() >= 1);
            Y_ENSURE(IsCompatibleTo(LastAggregation.TransTypeId, LastAggregation.ArgTypes[0], Types));
            LastAggregation.ArgTypes.erase(LastAggregation.ArgTypes.begin());
        }

        Y_ENSURE(!LastAggregation.Name.empty());
        if (!ResolveFunc(LastFinalFunc, LastAggregation.FinalFuncId, 1)) {
            return false;
        }

        if (!ResolveFunc(LastCombineFunc, LastAggregation.CombineFuncId, 2)) {
            return false;
        }

        if (!ResolveFunc(LastSerializeFunc, LastAggregation.SerializeFuncId, 1)) {
            return false;
        }

        if (!ResolveFunc(LastDeserializeFunc, LastAggregation.DeserializeFuncId, 0)) {
            return false;
        }

        return true;
    }

    bool ResolveFunc(const TString& name, ui32& funcId, ui32 stateArgsCount) {
        if (name) {
            auto funcIdsPtr = ProcByName.FindPtr(name);
            if (!funcIdsPtr) {
                return false;
            }

            if (!stateArgsCount) {
                Y_ENSURE(funcIdsPtr->size() == 1);
            }

            for (const auto id : *funcIdsPtr) {
                auto procPtr = Procs.FindPtr(id);
                Y_ENSURE(procPtr);
                bool found = true;
                if (stateArgsCount > 0 && procPtr->ArgTypes.size() == stateArgsCount) {
                    for (ui32 i = 0; i < stateArgsCount; ++i) {
                        if (!IsCompatibleTo(LastAggregation.TransTypeId, procPtr->ArgTypes[i], Types)) {
                            found = false;
                            break;
                        }
                    }
                }

                if (found) {
                    Y_ENSURE(!funcId);
                    funcId = id;
                }
            }

            Y_ENSURE(funcId);
        }

        return true;
    }

private:
    TAggregations& Aggregations;
    const THashMap<TString, ui32>& TypeByName;
    const TTypes& Types;
    const THashMap<TString, TVector<ui32>>& ProcByName;
    const TProcs& Procs;
    TAggregateDesc LastAggregation;
    bool IsSupported = true;
    TString LastOid;
    TString LastTransFunc;
    TString LastFinalFunc;
    TString LastCombineFunc;
    TString LastSerializeFunc;
    TString LastDeserializeFunc;
};

class TOpFamiliesParser : public TParser {
public:
    TOpFamiliesParser(TOpFamilies& opFamilies)
        : OpFamilies(opFamilies)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastOpfId = FromString<ui32>(value);
        } else if (key == "opfmethod") {
            if (value == "btree" || value == "hash") {
                LastOpfMethod = value;
            } else {
                IsSupported = false;
            }
        } else if (key == "opfname") {
            LastOpfName = value;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            Y_ENSURE(LastOpfId != InvalidOid);

            // TODO: log or throw if dict keys aren't unique
            // opfamily references have opf_method/opf_name format in PG catalogs
            OpFamilies[LastOpfMethod.append('/').append(
                    LastOpfName)] = LastOpfId; // LastOpfMethod is modified here. Use with caution till its reinit
        }

        IsSupported = true;
        LastOpfId = InvalidOid;
        LastOpfMethod.clear();
        LastOpfName.clear();
    }

private:
    TOpFamilies& OpFamilies;

    ui32 LastOpfId = InvalidOid;

    TString LastOpfMethod;
    TString LastOpfName;
    bool IsSupported = true;
};

class TOpClassesParser : public TParser {
public:
    TOpClassesParser(TOpClasses& opClasses, const THashMap<TString, ui32>& typeByName,
                     const TOpFamilies &opFamilies)
            : OpClasses(opClasses)
            , TypeByName(typeByName)
            , OpFamilies(opFamilies)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "opcmethod") {
            if (value == "btree") {
                LastOpClass.Method = EOpClassMethod::Btree;
            } else if (value == "hash") {
                LastOpClass.Method = EOpClassMethod::Hash;
            } else {
                IsSupported = false;
            }
        } else if (key == "opcintype") {
            auto idPtr = TypeByName.FindPtr(value);
            Y_ENSURE(idPtr);
            LastOpClass.TypeId = *idPtr;
        } else if (key == "opcname") {
            LastOpClass.Name = value;
        } else if (key == "opcfamily") {
            LastOpClass.Family = value;
            auto opFamilyPtr = OpFamilies.FindPtr(value);

            if (opFamilyPtr) {
                LastOpClass.FamilyId = *opFamilyPtr;
            } else {
                IsSupported = false;
            }
        } else if (key == "opcdefault") {
            IsDefault = (value[0] != 'f');
        }
    }

void OnFinish() override {
    // Only default opclasses are used so far
    if (IsSupported && IsDefault) {
        Y_ENSURE(!LastOpClass.Name.empty());

        const auto key = std::make_pair(LastOpClass.Method, LastOpClass.TypeId);

        if (OpClasses.contains(key)) {
            throw yexception() << "Duplicate opclass: (" << (key.first == EOpClassMethod::Btree ? "btree" : "hash")
                               << ", " << key.second << ")";
        }
        OpClasses[key] = LastOpClass;
    }

    IsSupported = true;
    IsDefault = true;
    LastOpClass = TOpClassDesc();
}

private:
    TOpClasses& OpClasses;

    const THashMap<TString, ui32>& TypeByName;
    const TOpFamilies OpFamilies;

    TOpClassDesc LastOpClass;
    bool IsSupported = true;
    bool IsDefault = true;
};

class TAmOpsParser : public TParser {
public:
    TAmOpsParser(TAmOps& amOps, const THashMap<TString, ui32>& typeByName, const TTypes& types,
        const THashMap<TString, TVector<ui32>>& operatorsByName, const TOperators& operators,
        const TOpFamilies &opFamilies)
        : AmOps(amOps)
        , TypeByName(typeByName)
        , Types(types)
        , OperatorsByName(operatorsByName)
        , Operators(operators)
        , OpFamilies(opFamilies)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "amopfamily") {
            LastAmOp.Family = value;
            auto opFamilyPtr = OpFamilies.FindPtr(value);
            if (opFamilyPtr) {
                LastAmOp.FamilyId = *opFamilyPtr;
            } else {
                IsSupported = false;
            }
        } else if (key == "amoplefttype") {
            auto leftTypePtr = TypeByName.FindPtr(value);
            Y_ENSURE(leftTypePtr);
            LastAmOp.LeftType = *leftTypePtr;
        } else if (key == "amoprighttype") {
            auto rightTypePtr = TypeByName.FindPtr(value);
            Y_ENSURE(rightTypePtr);
            LastAmOp.RightType = *rightTypePtr;
        } else if (key == "amopstrategy") {
            LastAmOp.Strategy = FromString<ui32>(value);
        } else if (key == "amopopr") {
            auto pos = value.find('(');
            Y_ENSURE(pos != TString::npos);
            LastOp = value.substr(0, pos);
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            auto operIdPtr = OperatorsByName.FindPtr(LastOp);
            Y_ENSURE(operIdPtr);
            for (const auto& id : *operIdPtr) {
                const auto& d = Operators.FindPtr(id);
                Y_ENSURE(d);
                if (d->Kind == EOperKind::Binary &&
                    IsCompatibleTo(LastAmOp.LeftType, d->LeftType, Types) &&
                    IsCompatibleTo(LastAmOp.RightType, d->RightType, Types)) {
                    Y_ENSURE(!LastAmOp.OperId);
                    LastAmOp.OperId = d->OperId;
                }
            }

            Y_ENSURE(LastAmOp.OperId);
            AmOps[std::make_tuple(LastAmOp.FamilyId, LastAmOp.Strategy, LastAmOp.LeftType, LastAmOp.RightType)] = LastAmOp;
        }

        LastAmOp = TAmOpDesc();
        LastOp = "";
        IsSupported = true;
    }

private:
    TAmOps& AmOps;

    const THashMap<TString, ui32>& TypeByName;
    const TTypes& Types;
    const THashMap<TString, TVector<ui32>>& OperatorsByName;
    const TOperators& Operators;
    const TOpFamilies& OpFamilies;

    TAmOpDesc LastAmOp;
    TString LastOp;
    bool IsSupported = true;
};

class TAmProcsParser : public TParser {
public:
    TAmProcsParser(TAmProcs& amProcs, const THashMap<TString, ui32>& typeByName,
        const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs,
        const TOpFamilies& opFamilies)
        : AmProcs(amProcs)
        , TypeByName(typeByName)
        , ProcByName(procByName)
        , Procs(procs)
        , OpFamilies(opFamilies)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "amprocfamily") {
            LastAmProc.Family = value;
            auto opFamilyPtr = OpFamilies.FindPtr(value);

            if (opFamilyPtr) {
                LastAmProc.FamilyId = *opFamilyPtr;
            } else {
                IsSupported = false;
            }
        } else if (key == "amproclefttype") {
            auto leftTypePtr = TypeByName.FindPtr(value);
            Y_ENSURE(leftTypePtr);
            LastAmProc.LeftType = *leftTypePtr;
        } else if (key == "amprocrighttype") {
            auto rightTypePtr = TypeByName.FindPtr(value);
            Y_ENSURE(rightTypePtr);
            LastAmProc.RightType = *rightTypePtr;
        } else if (key == "amprocnum") {
            LastAmProc.ProcNum = FromString<ui32>(value);
        } else if (key == "amproc") {
            LastName = value;
        }
    }

    void OnFinish() override {
        if (IsSupported) {
            if (LastName.find('(') == TString::npos) {
                auto procIdPtr = ProcByName.FindPtr(LastName);
                Y_ENSURE(procIdPtr);
                for (const auto& id : *procIdPtr) {
                    const auto& d = Procs.FindPtr(id);
                    Y_ENSURE(d);
                    Y_ENSURE(!LastAmProc.ProcId);
                    LastAmProc.ProcId = d->ProcId;
                }

                Y_ENSURE(LastAmProc.ProcId);
                AmProcs[std::make_tuple(LastAmProc.FamilyId, LastAmProc.ProcNum, LastAmProc.LeftType, LastAmProc.RightType)] = LastAmProc;
            }
        }

        LastAmProc = TAmProcDesc();
        LastName = "";
        IsSupported = true;
    }

private:
    TAmProcs& AmProcs;

    const THashMap<TString, ui32>& TypeByName;
    const THashMap<TString, TVector<ui32>>& ProcByName;
    const TProcs& Procs;
    const TOpFamilies& OpFamilies;

    TAmProcDesc LastAmProc;
    TString LastName;
    bool IsSupported = true;
};

class TConversionsParser : public TParser {
public:
    TConversionsParser(TConversions& conversions, const THashMap<TString, TVector<ui32>>& procByName)
        : Conversions(conversions)
        , ProcByName(procByName)
    {}

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastConversion.ConversionId = FromString<ui32>(value);
        } else if (key == "conforencoding") {
            Y_ENSURE(value.StartsWith("PG_"));
            LastConversion.From = value.substr(3);
        } else if (key == "contoencoding") {
            Y_ENSURE(value.StartsWith("PG_"));
            LastConversion.To = value.substr(3);
        } else if (key == "conproc") {
            auto found = ProcByName.FindPtr(value);
            if (found && found->size() == 1) {
                LastConversion.ProcId = found->front();
            }
        }
    }

    void OnFinish() override {
        if (LastConversion.ProcId) {
            Conversions[std::make_pair(LastConversion.From, LastConversion.To)] = LastConversion;
        }

        LastConversion = TConversionDesc();
    }

private:
    TConversions& Conversions;

    const THashMap<TString, TVector<ui32>>& ProcByName;

    TConversionDesc LastConversion;
};

TOperators ParseOperators(const TString& dat, const THashMap<TString, ui32>& typeByName,
    const TTypes& types, const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs) {
    TOperators ret;
    TOperatorsParser parser(ret, typeByName, types, procByName, procs);
    parser.Do(dat);
    return ret;
}

TAggregations ParseAggregations(const TString& dat, const THashMap<TString, ui32>& typeByName,
    const TTypes& types, const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs) {
    TAggregations ret;
    TAggregationsParser parser(ret, typeByName, types, procByName, procs);
    parser.Do(dat);
    return ret;
}

TProcs ParseProcs(const TString& dat, const THashMap<TString, ui32>& typeByName) {
    TProcs ret;
    TProcsParser parser(ret, typeByName);
    parser.Do(dat);
    return ret;
}

TTypes ParseTypes(const TString& dat, THashMap<ui32, TLazyTypeInfo>& lazyInfos) {
    TTypes ret;
    TTypesParser parser(ret, lazyInfos);
    parser.Do(dat);
    return ret;
}

TCasts ParseCasts(const TString& dat, const THashMap<TString, ui32>& typeByName, const TTypes& types,
    const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs) {
    TCasts ret;
    TCastsParser parser(ret, typeByName, types, procByName, procs);
    parser.Do(dat);
    return ret;
}

TOpFamilies ParseOpFamilies(const TString& dat) {
    TOpFamilies ret;
    TOpFamiliesParser parser(ret);
    parser.Do(dat);
    return ret;
}

TOpClasses ParseOpClasses(const TString& dat, const THashMap<TString, ui32>& typeByName,
    const TOpFamilies& opFamilies) {
    TOpClasses ret;
    TOpClassesParser parser(ret, typeByName, opFamilies);
    parser.Do(dat);
    return ret;
}

TAmOps ParseAmOps(const TString& dat, const THashMap<TString, ui32>& typeByName, const TTypes& types,
    const THashMap<TString, TVector<ui32>>& operatorsByName, const TOperators& operators,
    const TOpFamilies& opFamilies) {
    TAmOps ret;
    TAmOpsParser parser(ret, typeByName, types, operatorsByName, operators, opFamilies);
    parser.Do(dat);
    return ret;
}

TAmProcs ParseAmProcs(const TString& dat, const THashMap<TString, ui32>& typeByName,
    const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs,
    const TOpFamilies& opFamilies) {
    TAmProcs ret;
    TAmProcsParser parser(ret, typeByName, procByName, procs, opFamilies);
    parser.Do(dat);
    return ret;
}

TConversions ParseConversions(const TString& dat, const THashMap<TString, TVector<ui32>>& procByName) {
    TConversions ret;
    TConversionsParser parser(ret, procByName);
    parser.Do(dat);
    return ret;
}

struct TCatalog {
    TCatalog()
        : ProhibitedProcs({
            // revoked from public
            "pg_start_backup",
            "pg_stop_backup",
            "pg_create_restore_point",
            "pg_switch_wal",
            "pg_wal_replay_pause",
            "pg_wal_replay_resume",
            "pg_rotate_logfile",
            "pg_reload_conf",
            "pg_current_logfile",
            "pg_promote",
            "pg_stat_reset",
            "pg_stat_reset_shared",
            "pg_stat_reset_slru",
            "pg_stat_reset_single_table_counters",
            "pg_stat_reset_single_function_counters",
            "pg_stat_reset_replication_slot",
            "lo_import",
            "lo_export",
            "pg_ls_logdir",
            "pg_ls_waldir",
            "pg_ls_archive_statusdir",
            "pg_ls_tmpdir",
            "pg_read_file",
            "pg_read_binary_file",
            "pg_replication_origin_advance",
            "pg_replication_origin_create",
            "pg_replication_origin_drop",
            "pg_replication_origin_oid",
            "pg_replication_origin_progress",
            "pg_replication_origin_session_is_setup",
            "pg_replication_origin_session_progress",
            "pg_replication_origin_session_reset",
            "pg_replication_origin_session_setup",
            "pg_replication_origin_xact_reset",
            "pg_replication_origin_xact_setup",
            "pg_show_replication_origin_status",
            "pg_stat_file",
            "pg_ls_dir",
            // transactions
            "pg_last_committed_xact",
            "pg_current_wal_lsn",
            // large_objects
            "lo_creat",
            "lo_create",
            "lo_import",
            "lo_import_with_oid",
            "lo_export",
            "lo_open",
            "lo_write",
            "lo_read",
            "lo_lseek",
            "lo_lseek64",
            "lo_tell",
            "lo_tell64",
            "lo_truncate",
            "lo_truncate64",
            "lo_close",
            "lo_unlink"
        })
    {
        TString typeData;
        Y_ENSURE(NResource::FindExact("pg_type.dat", &typeData));
        TString opData;
        Y_ENSURE(NResource::FindExact("pg_operator.dat", &opData));
        TString procData;
        Y_ENSURE(NResource::FindExact("pg_proc.dat", &procData));
        TString castData;
        Y_ENSURE(NResource::FindExact("pg_cast.dat", &castData));
        TString aggData;
        Y_ENSURE(NResource::FindExact("pg_aggregate.dat", &aggData));
        TString opFamiliesData;
        Y_ENSURE(NResource::FindExact("pg_opfamily.dat", &opFamiliesData));
        TString opClassData;
        Y_ENSURE(NResource::FindExact("pg_opclass.dat", &opClassData));
        TString amProcData;
        Y_ENSURE(NResource::FindExact("pg_amproc.dat", &amProcData));
        TString amOpData;
        Y_ENSURE(NResource::FindExact("pg_amop.dat", &amOpData));
        TString conversionData;
        Y_ENSURE(NResource::FindExact("pg_conversion.dat", &conversionData));
        THashMap<ui32, TLazyTypeInfo> lazyTypeInfos;
        Types = ParseTypes(typeData, lazyTypeInfos);
        for (const auto& [k, v] : Types) {
            if (k == v.TypeId) {
                Y_ENSURE(TypeByName.insert(std::make_pair(v.Name, k)).second);
            }

            if (k == v.ArrayTypeId) {
                Y_ENSURE(TypeByName.insert(std::make_pair("_" + v.Name, k)).second);
            }
        }

        Procs = ParseProcs(procData, TypeByName);

        for (const auto& [k, v] : Procs) {
            ProcByName[v.Name].push_back(k);
        }

        const ui32 cstringId = 2275;
        const ui32 byteaId = 17;
        const ui32 internalId = 2281;
        for (const auto& [k, v] : lazyTypeInfos) {
            auto typePtr = Types.FindPtr(k);
            Y_ENSURE(typePtr);

            auto inFuncIdPtr = ProcByName.FindPtr(v.InFunc);
            Y_ENSURE(inFuncIdPtr);
            Y_ENSURE(inFuncIdPtr->size() == 1);
            auto inFuncPtr = Procs.FindPtr(inFuncIdPtr->at(0));
            Y_ENSURE(inFuncPtr);
            Y_ENSURE(inFuncPtr->ArgTypes.size() >= 1); // may have mods
            Y_ENSURE(inFuncPtr->ArgTypes[0] == cstringId);
            typePtr->InFuncId = inFuncIdPtr->at(0);

            auto outFuncIdPtr = ProcByName.FindPtr(v.OutFunc);
            Y_ENSURE(outFuncIdPtr);
            Y_ENSURE(outFuncIdPtr->size() == 1);
            auto outFuncPtr = Procs.FindPtr(outFuncIdPtr->at(0));
            Y_ENSURE(outFuncPtr);
            Y_ENSURE(outFuncPtr->ArgTypes.size() == 1);
            Y_ENSURE(outFuncPtr->ResultType == cstringId);
            typePtr->OutFuncId = outFuncIdPtr->at(0);

            if (v.ReceiveFunc != "-") {
                auto receiveFuncIdPtr = ProcByName.FindPtr(v.ReceiveFunc);
                Y_ENSURE(receiveFuncIdPtr);
                Y_ENSURE(receiveFuncIdPtr->size() == 1);
                auto receiveFuncPtr = Procs.FindPtr(receiveFuncIdPtr->at(0));
                Y_ENSURE(receiveFuncPtr);
                Y_ENSURE(receiveFuncPtr->ArgTypes.size() >= 1);
                Y_ENSURE(receiveFuncPtr->ArgTypes[0] == internalId); // mutable StringInfo
                typePtr->ReceiveFuncId = receiveFuncIdPtr->at(0);
            }

            if (v.SendFunc != "-") {
                auto sendFuncIdPtr = ProcByName.FindPtr(v.SendFunc);
                Y_ENSURE(sendFuncIdPtr);
                Y_ENSURE(sendFuncIdPtr->size() == 1);
                auto sendFuncPtr = Procs.FindPtr(sendFuncIdPtr->at(0));
                Y_ENSURE(sendFuncPtr);
                Y_ENSURE(sendFuncPtr->ArgTypes.size() == 1);
                Y_ENSURE(sendFuncPtr->ResultType == byteaId);
                typePtr->SendFuncId = sendFuncIdPtr->at(0);
            }

            if (v.ModInFunc) {
                auto modInFuncIdPtr = ProcByName.FindPtr(v.ModInFunc);
                Y_ENSURE(modInFuncIdPtr);
                Y_ENSURE(modInFuncIdPtr->size() == 1);
                auto modInFuncPtr = Procs.FindPtr(modInFuncIdPtr->at(0));
                Y_ENSURE(modInFuncPtr);
                Y_ENSURE(modInFuncPtr->ArgTypes.size() == 1);
                typePtr->TypeModInFuncId = modInFuncIdPtr->at(0);
            }

            if (v.ModOutFunc) {
                auto modOutFuncIdPtr = ProcByName.FindPtr(v.ModOutFunc);
                Y_ENSURE(modOutFuncIdPtr);
                Y_ENSURE(modOutFuncIdPtr->size() == 1);
                auto modOutFuncPtr = Procs.FindPtr(modOutFuncIdPtr->at(0));
                Y_ENSURE(modOutFuncPtr);
                Y_ENSURE(modOutFuncPtr->ArgTypes.size() == 1);
                typePtr->TypeModOutFuncId = modOutFuncIdPtr->at(0);
            }

            if (v.SubscriptFunc) {
                auto subscriptFuncIdPtr = ProcByName.FindPtr(v.SubscriptFunc);
                Y_ENSURE(subscriptFuncIdPtr);
                Y_ENSURE(subscriptFuncIdPtr->size() == 1);
                auto subscriptFuncPtr = Procs.FindPtr(subscriptFuncIdPtr->at(0));
                Y_ENSURE(subscriptFuncPtr);
                Y_ENSURE(subscriptFuncPtr->ArgTypes.size() == 1);
                typePtr->TypeSubscriptFuncId = subscriptFuncIdPtr->at(0);
            }

            if (v.ElementType) {
                auto elemTypePtr = TypeByName.FindPtr(v.ElementType);
                Y_ENSURE(elemTypePtr);

                typePtr->ElementTypeId = *elemTypePtr;
            }
        }

        Casts = ParseCasts(castData, TypeByName, Types, ProcByName, Procs);
        for (const auto&[k, v] : Casts) {
            Y_ENSURE(CastsByDir.insert(std::make_pair(std::make_pair(v.SourceId, v.TargetId), k)).second);
        }

        Operators = ParseOperators(opData, TypeByName, Types, ProcByName, Procs);
        for (const auto&[k, v] : Operators) {
            OperatorsByName[v.Name].push_back(k);
        }

        Aggregations = ParseAggregations(aggData, TypeByName, Types, ProcByName, Procs);
        for (const auto&[k, v] : Aggregations) {
            AggregationsByName[v.Name].push_back(k);
        }

        TOpFamilies opFamilies = ParseOpFamilies(opFamiliesData);
        OpClasses = ParseOpClasses(opClassData, TypeByName, opFamilies);
        AmOps = ParseAmOps(amOpData, TypeByName, Types, OperatorsByName, Operators, opFamilies);
        AmProcs = ParseAmProcs(amProcData, TypeByName, ProcByName, Procs, opFamilies);
        for (auto& [k, v] : Types) {
            if (v.TypeId != v.ArrayTypeId) {
                auto lookupId = (v.TypeId == VarcharOid ? TextOid : v.TypeId);
                auto btreeOpClassPtr = OpClasses.FindPtr(std::make_pair(EOpClassMethod::Btree, lookupId));
                if (btreeOpClassPtr) {
                    auto lessAmOpPtr = AmOps.FindPtr(std::make_tuple(btreeOpClassPtr->FamilyId, ui32(EBtreeAmStrategy::Less), lookupId, lookupId));
                    Y_ENSURE(lessAmOpPtr);
                    auto equalAmOpPtr = AmOps.FindPtr(std::make_tuple(btreeOpClassPtr->FamilyId, ui32(EBtreeAmStrategy::Equal), lookupId, lookupId));
                    Y_ENSURE(equalAmOpPtr);
                    auto lessOperPtr = Operators.FindPtr(lessAmOpPtr->OperId);
                    Y_ENSURE(lessOperPtr);
                    auto equalOperPtr = Operators.FindPtr(equalAmOpPtr->OperId);
                    Y_ENSURE(equalOperPtr);
                    v.LessProcId = lessOperPtr->ProcId;
                    v.EqualProcId = equalOperPtr->ProcId;

                    auto compareAmProcPtr = AmProcs.FindPtr(std::make_tuple(btreeOpClassPtr->FamilyId, ui32(EBtreeAmProcNum::Compare), lookupId, lookupId));
                    Y_ENSURE(compareAmProcPtr);
                    v.CompareProcId = compareAmProcPtr->ProcId;
                }

                auto hashOpClassPtr = OpClasses.FindPtr(std::make_pair(EOpClassMethod::Hash, lookupId));
                if (hashOpClassPtr) {
                    auto hashAmProcPtr = AmProcs.FindPtr(std::make_tuple(hashOpClassPtr->FamilyId, ui32(EHashAmProcNum::Hash), lookupId, lookupId));
                    Y_ENSURE(hashAmProcPtr);
                    v.HashProcId = hashAmProcPtr->ProcId;
                }
            }
        }

        Conversions = ParseConversions(conversionData, ProcByName);
    }

    static const TCatalog& Instance() {
        return *Singleton<TCatalog>();
    }

    TOperators Operators;
    TProcs Procs;
    TTypes Types;
    TCasts Casts;
    TAggregations Aggregations;
    TOpClasses OpClasses;
    TAmOps AmOps;
    TAmProcs AmProcs;
    TConversions Conversions;
    THashMap<TString, TVector<ui32>> ProcByName;
    THashMap<TString, ui32> TypeByName;
    THashMap<std::pair<ui32, ui32>, ui32> CastsByDir;
    THashMap<TString, TVector<ui32>> OperatorsByName;
    THashMap<TString, TVector<ui32>> AggregationsByName;
    THashSet<TString> ProhibitedProcs;
};

bool ValidateArgs(const TVector<ui32>& descArgTypeIds, const TVector<ui32>& argTypeIds) {
    if (argTypeIds.size() != descArgTypeIds.size()) {
        return false;
    }

    for (size_t i = 0; i < argTypeIds.size(); ++i) {
        if (!IsCompatibleTo(argTypeIds[i], descArgTypeIds[i])) {
            return false;
        }
    }

    return true;
}

bool ValidateProcArgs(const TProcDesc& d, const TVector<ui32>& argTypeIds) {
    return ValidateArgs(d.ArgTypes, argTypeIds);
}

const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto procPtr = catalog.Procs.FindPtr(procId);
    if (!procPtr) {
        throw yexception() << "No such proc: " << procId;
    }

    if (!ValidateProcArgs(*procPtr, argTypeIds)) {
        throw yexception() << "Unable to find an overload for proc with oid " << procId << " with given argument types: " <<
            ArgTypesList(argTypeIds);
    }

    return *procPtr;
}

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto lower = to_lower(name);
    if (catalog.ProhibitedProcs.contains(lower)) {
        throw yexception() << "No access to proc: " << name;
    }

    auto procIdPtr = catalog.ProcByName.FindPtr(lower);
    if (!procIdPtr) {
        throw yexception() << "No such proc: " << name;
    }

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.Procs.FindPtr(id);
        Y_ENSURE(d);
        if (!ValidateProcArgs(*d, argTypeIds)) {
            continue;
        }

        return *d;
    }

    throw yexception() << "Unable to find an overload for proc " << name << " with given argument types: "
        << ArgTypesList(argTypeIds);
}

const TProcDesc& LookupProc(ui32 procId) {
    const auto& catalog = TCatalog::Instance();
    auto procPtr = catalog.Procs.FindPtr(procId);
    if (!procPtr) {
        throw yexception() << "No such proc: " << procId;
    }

    return *procPtr;
}

void EnumProc(std::function<void(ui32, const TProcDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& x : catalog.Procs) {
        f(x.first, x.second);
    }
}

bool HasReturnSetProc(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    auto procIdPtr = catalog.ProcByName.FindPtr(to_lower(name));
    if (!procIdPtr) {
        return false;
    }

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.Procs.FindPtr(id);
        Y_ENSURE(d);
        if (d->ReturnSet) {
            return true;
        }
    }

    return false;
}

bool HasType(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    return catalog.TypeByName.contains(GetCanonicalTypeName(to_lower(name)));
}

const TTypeDesc& LookupType(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    const auto typeIdPtr = catalog.TypeByName.FindPtr(GetCanonicalTypeName(to_lower(name)));
    if (!typeIdPtr) {
        throw yexception() << "No such type: " << name;
    }

    const auto typePtr = catalog.Types.FindPtr(*typeIdPtr);
    Y_ENSURE(typePtr);
    return *typePtr;
}

const TTypeDesc& LookupType(ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    const auto typePtr = catalog.Types.FindPtr(typeId);
    if (!typePtr) {
        throw yexception() << "No such type: " << typeId;
    }

    return *typePtr;
}

void EnumTypes(std::function<void(ui32, const TTypeDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& [typeId, desc] : catalog.Types) {
        f(typeId, desc);
    }
}

bool HasCast(ui32 sourceId, ui32 targetId) {
    const auto& catalog = TCatalog::Instance();
    return catalog.CastsByDir.contains(std::make_pair(sourceId, targetId));
}

const TCastDesc& LookupCast(ui32 sourceId, ui32 targetId) {
    const auto& catalog = TCatalog::Instance();
    auto castByDirPtr = catalog.CastsByDir.FindPtr(std::make_pair(sourceId, targetId));
    if (!castByDirPtr) {
        throw yexception() << "No such cast";
    }

    auto castPtr = catalog.Casts.FindPtr(*castByDirPtr);
    Y_ENSURE(castPtr);
    return *castPtr;
}

namespace NPrivate {

constexpr ui64 NoFitScore = 0;

bool CanUseCoercionType(ECoercionCode requiredCoercionLevel, ECoercionCode actualCoercionLevel) {
    switch (requiredCoercionLevel) {
        case NYql::NPg::ECoercionCode::Implicit:
           return actualCoercionLevel == ECoercionCode::Implicit;

        case NYql::NPg::ECoercionCode::Assignment:
           return (actualCoercionLevel == ECoercionCode::Implicit) || (actualCoercionLevel == ECoercionCode::Assignment);

        case NYql::NPg::ECoercionCode::Explicit:
           return (actualCoercionLevel != ECoercionCode::Unknown);

        case NYql::NPg::ECoercionCode::Unknown:
            return false;
    }
}

enum class ECoercionSearchResult
{
	None,
	Func,
	BinaryCompatible,
	ArrayCoerce,
	IOCoerce,
};

ECoercionSearchResult FindCoercionPath(ui32 fromTypeId, ui32 toTypeId, ECoercionCode coercionType, const TCatalog& catalog) {
    if (fromTypeId == toTypeId) {
        return ECoercionSearchResult::BinaryCompatible;
    }

    const auto* castId = catalog.CastsByDir.FindPtr(std::make_pair(fromTypeId, toTypeId));
    if (castId != nullptr) {
        const auto* castPtr = catalog.Casts.FindPtr(*castId);
        Y_ENSURE(castPtr);

        if (!CanUseCoercionType(coercionType, castPtr->CoercionCode)) {
            return ECoercionSearchResult::None;
        }
        switch (castPtr->Method) {
            case ECastMethod::Function:
                return ECoercionSearchResult::Func;

            case ECastMethod::Binary:
                return ECoercionSearchResult::BinaryCompatible;

            case ECastMethod::InOut:
                return ECoercionSearchResult::IOCoerce;
        }
    }

    if (toTypeId != OidVectorOid && toTypeId != Int2VectorOid) {
        const auto* toTypePtr = catalog.Types.FindPtr(toTypeId);
        Y_ENSURE(toTypePtr);

        const auto* fromTypePtr = catalog.Types.FindPtr(fromTypeId);
        Y_ENSURE(fromTypePtr);

        if (IsArrayType(*toTypePtr) && IsArrayType(*fromTypePtr)) {
            if (FindCoercionPath(fromTypePtr->ElementTypeId, toTypePtr->ElementTypeId, coercionType, catalog) != ECoercionSearchResult::None) {
                return ECoercionSearchResult::ArrayCoerce;
            }
        }
    }

    if (coercionType == ECoercionCode::Assignment || coercionType == ECoercionCode::Explicit) {
        const auto* toTypePtr = catalog.Types.FindPtr(toTypeId);
        Y_ENSURE(toTypePtr);

        if (toTypePtr->Category == 'S') {
            return ECoercionSearchResult::IOCoerce;
        }

        if (coercionType == ECoercionCode::Explicit) {
            const auto* fromTypePtr = catalog.Types.FindPtr(fromTypeId);
            Y_ENSURE(fromTypePtr);

            if (fromTypePtr->Category == 'S') {
                return ECoercionSearchResult::IOCoerce;
            }
        }
    }

    return ECoercionSearchResult::None;
}

bool IsCoercible(ui32 fromTypeId, ui32 toTypeId, ECoercionCode coercionType, const TCatalog& catalog) {
    if (fromTypeId == toTypeId) {
        return true;
    }
    if (toTypeId == AnyOid) {
        return true;
    }

    //TODO: support polymorphic types

    if (fromTypeId == UnknownOid) {
        return true;
    }

    if (FindCoercionPath(fromTypeId, toTypeId, coercionType, catalog) != ECoercionSearchResult::None ) {
        return true;
    }

    // TODO: support record & complex types

    // TODO: support record array

    // TODO: support inheritance

    if (toTypeId == AnyArrayOid) {
        const auto& actualDescPtr = catalog.Types.FindPtr(fromTypeId);
        Y_ENSURE(actualDescPtr);
        return actualDescPtr->ArrayTypeId == actualDescPtr->TypeId;
    }

    return false;
}

bool IsPreferredType(char categoryId, const TTypeDesc& type) {
    Y_ENSURE(type.Category != InvalidCategory);

    return (categoryId == type.Category) ? type.IsPreferred : false;
}

constexpr ui32 CoercibleMatchShift = 16;
constexpr ui64 ArgExactTypeMatch = 1ULL << 2*CoercibleMatchShift;
constexpr ui64 ArgPreferredTypeMatch = 1ULL << CoercibleMatchShift;
constexpr ui64 ArgCoercibleTypeMatch = 1ULL;
constexpr ui64 ArgTypeMismatch = 0;

ui64 CalcArgumentMatchScore(ui32 operArgTypeId, ui32 argTypeId, const TCatalog& catalog) {
    Y_ENSURE(operArgTypeId != UnknownOid);

    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 2
    if (argTypeId == operArgTypeId) {
        return ArgExactTypeMatch;
    }
    if (argTypeId == UnknownOid) {
        return ArgCoercibleTypeMatch;
    }
    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 3.c
    if (IsCoercible(argTypeId, operArgTypeId, ECoercionCode::Implicit, catalog)) {
        // https://www.postgresql.org/docs/14/typeconv-oper.html, step 3.d
        const auto& argType = catalog.Types.FindPtr(argTypeId);
        Y_ENSURE(argType);
        const auto& operArgType = catalog.Types.FindPtr(operArgTypeId);
        Y_ENSURE(operArgType);

        return IsPreferredType(argType->Category, *operArgType)
                   ? ArgPreferredTypeMatch
                   : ArgCoercibleTypeMatch;
    }

    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 3.a
    return ArgTypeMismatch;
}

ui64 CalcBinaryOperatorScore(const TOperDesc& oper, ui32 leftArgTypeId, ui32 rightArgTypeId, const TCatalog& catalog) {
    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 2.a
    if (leftArgTypeId == UnknownOid && rightArgTypeId != InvalidOid) {
        if (oper.LeftType == rightArgTypeId && oper.RightType == rightArgTypeId) {
            return ArgExactTypeMatch + ArgExactTypeMatch;
        }
    }
    else if (rightArgTypeId == UnknownOid && leftArgTypeId != InvalidOid) {
        if (oper.LeftType == leftArgTypeId && oper.RightType == leftArgTypeId) {
            return ArgExactTypeMatch + ArgExactTypeMatch;
        }
    }

    // https://www.postgresql.org/docs/14/typeconv-oper.html, steps 3.a, 3.c, 3.d
    auto lscore = CalcArgumentMatchScore(oper.LeftType, leftArgTypeId, catalog);
    auto rscore = CalcArgumentMatchScore(oper.RightType, rightArgTypeId, catalog);

    if (lscore == ArgTypeMismatch || rscore == ArgTypeMismatch) {
        return ArgTypeMismatch;
    }

    return lscore + rscore;
}

ui64 CalcUnaryOperatorScore(const TOperDesc& oper, ui32 argTypeId, const TCatalog& catalog) {
    return CalcArgumentMatchScore(oper.RightType, argTypeId, catalog);
}

ui64 CalcProcScore(const TVector<ui32>& procArgTypes, const TVector<ui32>& argTypeIds, const TCatalog& catalog) {
    ui64 result = 0UL;

    if (argTypeIds.size() != procArgTypes.size()) {
        return ArgTypeMismatch;
    }

    for (size_t i = 0; i < argTypeIds.size(); ++i) {
        auto score = CalcArgumentMatchScore(procArgTypes[i], argTypeIds[i], catalog);

        if (score == ArgTypeMismatch) {
            return ArgTypeMismatch;
        }
        result += score;
    }
    return result;
}

[[noreturn]] void ThrowOperatorNotFound(const TString& name, const TVector<ui32>& argTypeIds) {
    throw yexception() << "Unable to find an overload for operator " << name << " with given argument type(s): "
        << ArgTypesList(argTypeIds);
}

[[noreturn]] void ThrowOperatorAmbiguity(const TString& name, const TVector<ui32>& argTypeIds) {
    throw yexception() << "Ambiguity for operator " << name << " with given argument type(s): "
        << ArgTypesList(argTypeIds);
}

[[noreturn]] void ThrowProcNotFound(const TString& name, const TVector<ui32>& argTypeIds) {
    throw yexception() << "Unable to find an overload for proc " << name << " with given argument types: "
        << ArgTypesList(argTypeIds);
}

[[noreturn]] void ThrowProcAmbiguity(const TString& name, const TVector<ui32>& argTypeIds) {
    throw yexception() << "Ambiguity for proc " << name << " with given argument type(s): "
        << ArgTypesList(argTypeIds);
}

[[noreturn]] void ThrowAggregateNotFound(const TString& name, const TVector<ui32>& argTypeIds) {
    throw yexception() << "Unable to find an overload for aggregate " << name << " with given argument types: "
        << ArgTypesList(argTypeIds);
}

[[noreturn]] void ThrowAggregateAmbiguity(const TString& name, const TVector<ui32>& argTypeIds) {
    throw yexception() << "Ambiguity for aggregate " << name << " with given argument type(s): "
        << ArgTypesList(argTypeIds);
}

struct TCommonCategoryDesc {
    size_t Position;
    char Category = InvalidCategory;
    bool IsPreferred = false;

    TCommonCategoryDesc(size_t position, char category, bool isPreferred)
    : Position(position), Category(category), IsPreferred(isPreferred) {}
};

template <class C>
char FindCommonCategory(const TVector<const C*> &candidates, std::function<ui32(const C*)> getTypeId, const TCatalog &catalog, bool &isPreferred) {
    char category = InvalidCategory;
    auto isConflict = false;
    isPreferred = false;

    for (const auto* candidate : candidates) {
        const auto argTypeId = getTypeId(candidate);
        const auto& argTypePtr = catalog.Types.FindPtr(argTypeId);
        Y_ENSURE(argTypePtr);

        if (InvalidCategory == category) {
            category = argTypePtr->Category;
            isPreferred = argTypePtr->IsPreferred;
        } else if (category == argTypePtr->Category) {
            isPreferred |= argTypePtr->IsPreferred;
        } else {
            if (argTypePtr->Category == 'S') {
                category = argTypePtr->Category;
                isPreferred = argTypePtr->IsPreferred;
            } else {
                isConflict = true;
            }
        }
    }
    if (isConflict && category != 'S') {
        isPreferred = false;
        return InvalidCategory;
    }
    return category;
}

template <class C>
TVector<const C*> TryResolveUnknownsByCategory(const TVector<const C*>& candidates, const TVector<ui32>& argTypeIds, const TCatalog& catalog) {
    TVector<NPrivate::TCommonCategoryDesc> argCommonCategory;

    size_t unknownsCnt = 0;

    for (size_t i = 0; i < argTypeIds.size(); ++i) {
        if (argTypeIds[i] != UnknownOid) {
            continue;
        }
        ++unknownsCnt;

        char category = InvalidCategory;
        bool isPreferred = false;

        std::function<ui32(const C *)> typeGetter = [i] (const auto* candidate) {
            return candidate->ArgTypes[i];
        };

        if (InvalidCategory != (category = NPrivate::FindCommonCategory<C>(candidates, typeGetter, catalog, isPreferred))) {
            argCommonCategory.emplace_back(i, category, isPreferred);
        }
    }
    if (argCommonCategory.size() < unknownsCnt) {
        return candidates;
    }

    TVector<const C*> filteredCandidates;

    for (const auto* candidate : candidates) {
        auto keepIt = true;

        for (const auto& category : argCommonCategory) {
            const auto argTypeId = candidate->ArgTypes[category.Position];

            const auto& argTypePtr = catalog.Types.FindPtr(argTypeId);
            Y_ENSURE(argTypePtr);

            if (argTypePtr->Category != category.Category) {
                keepIt = false;
                break;
            }
            if (category.IsPreferred && !argTypePtr->IsPreferred) {
                keepIt = false;
                break;
            }
        }
        if (keepIt) {
            filteredCandidates.push_back(candidate);
        }
    }

    return filteredCandidates.empty() ? candidates : filteredCandidates;
}

template <>
TVector<const TOperDesc*> TryResolveUnknownsByCategory<TOperDesc>(const TVector<const TOperDesc*>& candidates, const TVector<ui32>& argTypeIds, const TCatalog& catalog) {
    TVector<NPrivate::TCommonCategoryDesc> argCommonCategory;

    size_t unknownsCnt = 0;

    for (size_t i = 0; i < argTypeIds.size(); ++i) {
        if (argTypeIds[i] != UnknownOid) {
            continue;
        }
        ++unknownsCnt;

        char category = InvalidCategory;
        bool isPreferred = false;

        std::function <ui32(const TOperDesc*)> typeGetter;
        if (i == 1) {
            typeGetter = [] (const auto* candidate) {
                return candidate->RightType;
            };
        } else {
            typeGetter = [] (const auto* candidate) {
                return (candidate->Kind == EOperKind::Binary)
                    ? candidate->LeftType
                    : candidate->RightType;
            };
        }

        if (InvalidCategory != (category = NPrivate::FindCommonCategory<TOperDesc>(candidates, typeGetter, catalog, isPreferred))) {
            argCommonCategory.emplace_back(i, category, isPreferred);
        }
    }
    if (argCommonCategory.size() < unknownsCnt) {
        return candidates;
    }

    TVector<const TOperDesc*> filteredCandidates;

    for (const auto* candidate : candidates) {
        auto keepIt = true;

        for (const auto& category : argCommonCategory) {
            const auto argTypeId = (category.Position == 1)
                ? candidate->RightType
                : (candidate->Kind == EOperKind::Binary) ? candidate->LeftType : candidate->RightType;

            const auto& argTypePtr = catalog.Types.FindPtr(argTypeId);
            Y_ENSURE(argTypePtr);

            if (argTypePtr->Category != category.Category) {
                keepIt = false;
                break;
            }
            if (category.IsPreferred && !argTypePtr->IsPreferred) {
                keepIt = false;
                break;
            }
        }
        if (keepIt) {
            filteredCandidates.push_back(candidate);
        }
    }

    return filteredCandidates.empty() ? candidates : filteredCandidates;
}

bool CanCastImplicitly(ui32 fromTypeId, ui32 toTypeId, const TCatalog& catalog) {
    const auto* castId = catalog.CastsByDir.FindPtr(std::make_pair(fromTypeId, toTypeId));
    if (!castId) {
        return false;
    }
    const auto* castPtr = catalog.Casts.FindPtr(*castId);
    Y_ENSURE(castPtr);

    return (castPtr->CoercionCode == ECoercionCode::Implicit);
}

}  // NPrivate

bool IsCoercible(ui32 fromTypeId, ui32 toTypeId, ECoercionCode coercionType) {
    const auto& catalog = TCatalog::Instance();

    return NPrivate::IsCoercible(fromTypeId, toTypeId, coercionType, catalog);
}

std::variant<const TProcDesc*, const TTypeDesc*> LookupProcWithCasts(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto lower = to_lower(name);
    if (catalog.ProhibitedProcs.contains(lower)) {
        throw yexception() << "No access to proc: " << name;
    }

    auto procIdPtr = catalog.ProcByName.FindPtr(lower);
    if (!procIdPtr) {
        throw yexception() << "No such proc: " << name;
    }

    auto bestScore = NPrivate::NoFitScore;
    TVector<const TProcDesc*> candidates;

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.Procs.FindPtr(id);
        Y_ENSURE(d);

        if (argTypeIds == d->ArgTypes) {
            // At most one exact match is possible, so look no further
            // https://www.postgresql.org/docs/14/typeconv-func.html, step 2
            return d;
        }

        // https://www.postgresql.org/docs/14/typeconv-func.html, steps 4.a, 4.c, 4.d
        auto score = NPrivate::CalcProcScore(d->ArgTypes, argTypeIds, catalog);

        if (bestScore < score) {
            bestScore = score;

            candidates.clear();
            candidates.push_back(d);
        } else if (bestScore == score && NPrivate::NoFitScore < score) {
            candidates.push_back(d);
        }
    }

    // check, if it's a form of typecast
    // https://www.postgresql.org/docs/14/typeconv-func.html, step 3
    if (argTypeIds.size() == 1) {
        const auto typeIdPtr = catalog.TypeByName.FindPtr(to_lower(name));

        if (typeIdPtr) {
            const auto typePtr = catalog.Types.FindPtr(*typeIdPtr);
            Y_ENSURE(typePtr);

            const auto fromTypeId = argTypeIds[0];

            if (fromTypeId == UnknownOid) {
                return typePtr;
            }

            const auto coercionType = NPrivate::FindCoercionPath(fromTypeId, typePtr->TypeId,
                ECoercionCode::Explicit, catalog);

            switch (coercionType) {
                case NPrivate::ECoercionSearchResult::BinaryCompatible:
                    return typePtr;

                case NPrivate::ECoercionSearchResult::IOCoerce:
                    if (!(fromTypeId == RecordOid /* || TODO: IsComplex */ && typePtr->Category == 'S')) {
                        return typePtr;
                    }
                default:
                    break;
            }
        }
    }

    switch (candidates.size()) {
        case 1:
            // https://www.postgresql.org/docs/14/typeconv-func.html, end of steps 4.a, 4.c or 4.d
            return candidates[0];

        case 0:
            NPrivate::ThrowProcNotFound(name, argTypeIds);
    }

    // https://www.postgresql.org/docs/14/typeconv-func.html, step 4.e
    const size_t unknownsCount = std::count(argTypeIds.cbegin(), argTypeIds.cend(), UnknownOid);
    if (0 == unknownsCount) {
        NPrivate::ThrowProcNotFound(name, argTypeIds);
    }

    // https://www.postgresql.org/docs/14/typeconv-funcr.html, step 4.e
    candidates = NPrivate::TryResolveUnknownsByCategory<TProcDesc>(candidates, argTypeIds, catalog);

    if (1 == candidates.size()) {
        // https://www.postgresql.org/docs/14/typeconv-func.html, end of step 4.e
        return candidates[0];
    }

    // https://www.postgresql.org/docs/14/typeconv-func.html, step 4.f
    if (unknownsCount < argTypeIds.size()) {
        ui32 commonType = UnknownOid;

        for (const auto argType: argTypeIds) {
            if (argType == UnknownOid) {
                continue;
            }
            if (commonType == UnknownOid) {
                commonType = argType;
                continue;
            }
            if (argType != commonType) {
                commonType = UnknownOid;
                break;
            }
        }

        if (commonType != UnknownOid) {
            const TProcDesc* finalCandidate = nullptr;

            for (const auto* candidate : candidates) {
                for (size_t i = 0; i < argTypeIds.size(); ++i) {
                    if (NPrivate::IsCoercible(commonType, candidate->ArgTypes[i], ECoercionCode::Implicit, catalog)) {
                        if (finalCandidate) {
                            NPrivate::ThrowProcAmbiguity(name, argTypeIds);
                        }
                        finalCandidate = candidate;
                    }
                }
            }

            if (finalCandidate) {
                return finalCandidate;
            }
        }
    }
    NPrivate::ThrowProcNotFound(name, argTypeIds);
}

TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>& GetPosition, const TTypeDesc*& typeDesc, bool& castsNeeded) {
    Y_ENSURE(0 != typeIds.size());

    const auto& catalog = TCatalog::Instance();

    const TTypeDesc* commonType = &LookupType(typeIds[0]);
    char commonCategory = commonType->Category;
    size_t unknownsCnt = (commonType->TypeId == UnknownOid) ? 1 : 0;
    castsNeeded = (unknownsCnt != 0);
    size_t i = 1;
    for (auto typeId = typeIds.cbegin() + 1; typeId != typeIds.cend(); ++typeId, ++i) {
        if (*typeId == UnknownOid || *typeId == InvalidOid) {
            ++unknownsCnt;
            castsNeeded = true;
            continue;
        }
        if (Y_LIKELY(*typeId == commonType->TypeId)) {
            continue;
        }
        const TTypeDesc& otherType = LookupType(*typeId);
        if (commonType->TypeId == UnknownOid) {
            commonType = &otherType;
            commonCategory = otherType.Category;
            continue;
        }
        if (otherType.Category != commonCategory) {
            // https://www.postgresql.org/docs/14/typeconv-union-case.html, step 4
            return TIssue(GetPosition(i), TStringBuilder() << "Cannot infer common type for types "
                << commonType->TypeId << " and " << otherType.TypeId);
        }
        castsNeeded = true;
        if (NPrivate::CanCastImplicitly(otherType.TypeId, commonType->TypeId, catalog)) {
            continue;
        }
        if (commonType->IsPreferred || !NPrivate::CanCastImplicitly(commonType->TypeId, otherType.TypeId, catalog)) {
            return TIssue(GetPosition(i), TStringBuilder() << "Cannot infer common type for types "
                << commonType->TypeId << " and " << otherType.TypeId);
        }
        commonType = &otherType;
    }

    // https://www.postgresql.org/docs/14/typeconv-union-case.html, step 3
    if (unknownsCnt == typeIds.size()) {
        castsNeeded = true;
        typeDesc = &LookupType("text");
    } else {
        typeDesc = commonType;
    }
    return {};
}

TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>&GetPosition, const TTypeDesc*& typeDesc) {
    bool _;
    return LookupCommonType(typeIds, GetPosition, typeDesc, _);
}


const TOperDesc& LookupOper(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();

    auto operIdPtr = catalog.OperatorsByName.FindPtr(to_lower(name));
    if (!operIdPtr) {
        throw yexception() << "No such operator: " << name;
    }

    EOperKind expectedOpKind;
    std::function<ui64(const TOperDesc*)> calcScore;

    switch (argTypeIds.size()) {
        case 2:
            expectedOpKind = EOperKind::Binary;
            calcScore = [&] (const auto* d) {
                 return NPrivate::CalcBinaryOperatorScore(*d, argTypeIds[0], argTypeIds[1], catalog);
                 };
            break;

        case 1:
            expectedOpKind = EOperKind::LeftUnary;
            calcScore = [&] (const auto* d) {
                return NPrivate::CalcUnaryOperatorScore(*d, argTypeIds[0], catalog);
                };
            break;

        default:
            throw yexception() << "Operator's number of arguments should be 1 or 2, got: " << argTypeIds.size();
    }

    auto bestScore = NPrivate::NoFitScore;
    TVector<const TOperDesc*> candidates;
    const ui64 maxPossibleScore = NPrivate::ArgExactTypeMatch * argTypeIds.size();

    for (const auto& operId : *operIdPtr) {
        const auto& oper = catalog.Operators.FindPtr(operId);
        Y_ENSURE(oper);

        if (expectedOpKind != oper->Kind) {
            continue;
        }

        auto score = calcScore(oper);

        if (maxPossibleScore == score) {
            // The only exact match is possible, so look no further
            // https://www.postgresql.org/docs/14/typeconv-oper.html, step 2
            return *oper;
        }

        // https://www.postgresql.org/docs/14/typeconv-oper.html, steps 3.a, 3.c, 3.d
        if (bestScore < score) {
            bestScore = score;

            candidates.clear();
            candidates.push_back(oper);
        } else if (bestScore == score && NPrivate::NoFitScore < score) {
            candidates.push_back(oper);
        }
    }

    switch (candidates.size()) {
        case 1:
            // https://www.postgresql.org/docs/14/typeconv-oper.html, end of steps 3.a, 3.c or 3.d
            return *candidates[0];

        case 0:
            NPrivate::ThrowOperatorNotFound(name, argTypeIds);
    }

    if (!(argTypeIds[0] == UnknownOid || (2 == argTypeIds.size() && argTypeIds[1] == UnknownOid))) {
        NPrivate::ThrowOperatorNotFound(name, argTypeIds);
    }

    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 3.e
    candidates = NPrivate::TryResolveUnknownsByCategory<TOperDesc>(candidates, argTypeIds, catalog);

    if (1 == candidates.size()) {
        // https://www.postgresql.org/docs/14/typeconv-oper.html, end of step 3.e
        return *candidates[0];
    }

    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 3.f
    if (EOperKind::LeftUnary == expectedOpKind) {
        NPrivate::ThrowOperatorNotFound(name, argTypeIds);
    }

    auto TryResolveUnknownsBySpreadingType = [&] (ui32 argTypeId, std::function<ui32(const TOperDesc*)> getArgType) {
        const TOperDesc* finalCandidate = nullptr;

        for (const auto* candidate : candidates) {
            if (NPrivate::IsCoercible(argTypeId, getArgType(candidate), ECoercionCode::Implicit, catalog)) {
                if (finalCandidate) {
                    NPrivate::ThrowOperatorAmbiguity(name, argTypeIds);
                }
                finalCandidate = candidate;
            }
        }
        return finalCandidate;
    };

    const TOperDesc* finalCandidate = nullptr;
    if (argTypeIds[0] == UnknownOid) {
        finalCandidate = TryResolveUnknownsBySpreadingType(argTypeIds[1], [&] (const auto* oper) { return oper->LeftType; });
    } else if (argTypeIds[1] == UnknownOid) {
        finalCandidate = TryResolveUnknownsBySpreadingType(argTypeIds[0], [&] (const auto* oper) { return oper->RightType; });
    }

    if (finalCandidate) {
        return *finalCandidate;
    }
    NPrivate::ThrowOperatorNotFound(name, argTypeIds);
}

const TOperDesc& LookupOper(ui32 operId, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto operPtr = catalog.Operators.FindPtr(operId);
    if (!operPtr) {
        throw yexception() << "No such oper: " << operId;
    }

    if (!ValidateOperArgs(*operPtr, argTypeIds, catalog.Types)) {
        throw yexception() << "Unable to find an overload for operator with oid " << operId << " with given argument types: "
            << ArgTypesList(argTypeIds);
    }

    return *operPtr;
}

const TOperDesc& LookupOper(ui32 operId) {
    const auto& catalog = TCatalog::Instance();
    auto operPtr = catalog.Operators.FindPtr(operId);
    if (!operPtr) {
        throw yexception() << "No such oper: " << operId;
    }

    return *operPtr;
}

bool HasAggregation(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    return catalog.AggregationsByName.contains(to_lower(name));
}

bool ValidateAggregateArgs(const TAggregateDesc& d, const TVector<ui32>& argTypeIds) {
    return ValidateArgs(d.ArgTypes, argTypeIds);
}

bool ValidateAggregateArgs(const TAggregateDesc& d, ui32 stateType, ui32 resultType) {
    auto expectedStateType = LookupProc(d.SerializeFuncId ? d.SerializeFuncId : d.TransFuncId).ResultType;
    if (stateType != expectedStateType) {
        return false;
    }

    auto expectedResultType = LookupProc(d.FinalFuncId ? d.FinalFuncId : d.TransFuncId).ResultType;
    if (resultType != expectedResultType) {
        return false;
    }

    return true;
}

const TAggregateDesc& LookupAggregation(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto aggIdPtr = catalog.AggregationsByName.FindPtr(to_lower(name));
    if (!aggIdPtr) {
        throw yexception() << "No such aggregate: " << name;
    }

    auto bestScore = NPrivate::NoFitScore;
    TVector<const TAggregateDesc*> candidates;

    for (const auto& id : *aggIdPtr) {
        const auto& d = catalog.Aggregations.FindPtr(id);
        Y_ENSURE(d);

        if (argTypeIds == d->ArgTypes) {
            // At most one exact match is possible, so look no further
            // https://www.postgresql.org/docs/14/typeconv-func.html, step 2
            return *d;
        }

        // https://www.postgresql.org/docs/14/typeconv-func.html, steps 4.a, 4.c, 4.d
        auto score = NPrivate::CalcProcScore(d->ArgTypes, argTypeIds, catalog);

        if (bestScore < score) {
            bestScore = score;

            candidates.clear();
            candidates.push_back(d);
        } else if (bestScore == score && NPrivate::NoFitScore < score) {
            candidates.push_back(d);
        }
    }

    switch (candidates.size()) {
        case 1:
            // https://www.postgresql.org/docs/14/typeconv-func.html, end of steps 4.a, 4.c or 4.d
            return *candidates[0];

        case 0:
            NPrivate::ThrowAggregateNotFound(name, argTypeIds);
    }

    // https://www.postgresql.org/docs/14/typeconv-func.html, step 4.e
    const size_t unknownsCount = std::count(argTypeIds.cbegin(), argTypeIds.cend(), UnknownOid);
    if (0 == unknownsCount) {
        NPrivate::ThrowAggregateNotFound(name, argTypeIds);
    }

    // https://www.postgresql.org/docs/14/typeconv-funcr.html, step 4.e
    candidates = NPrivate::TryResolveUnknownsByCategory<TAggregateDesc>(candidates, argTypeIds, catalog);

    if (1 == candidates.size()) {
        // https://www.postgresql.org/docs/14/typeconv-func.html, end of step 4.e
        return *candidates[0];
    }

    // https://www.postgresql.org/docs/14/typeconv-func.html, step 4.f
    if (unknownsCount < argTypeIds.size()) {
        ui32 commonType = UnknownOid;

        for (const auto argType: argTypeIds) {
            if (argType == UnknownOid) {
                continue;
            }
            if (commonType == UnknownOid) {
                commonType = argType;
                continue;
            }
            if (argType != commonType) {
                commonType = UnknownOid;
                break;
            }
        }

        if (commonType != UnknownOid) {
            const TAggregateDesc* finalCandidate = nullptr;

            for (const auto* candidate : candidates) {
                for (size_t i = 0; i < argTypeIds.size(); ++i) {
                    if (NPrivate::IsCoercible(commonType, candidate->ArgTypes[i], ECoercionCode::Implicit, catalog)) {
                        if (finalCandidate) {
                            NPrivate::ThrowAggregateAmbiguity(name, argTypeIds);
                        }
                        finalCandidate = candidate;
                    }
                }
            }

            if (finalCandidate) {
                return *finalCandidate;
            }
        }
    }
    NPrivate::ThrowAggregateNotFound(name, argTypeIds);
}

const TAggregateDesc& LookupAggregation(const TString& name, ui32 stateType, ui32 resultType) {
    const auto& catalog = TCatalog::Instance();
    auto aggIdPtr = catalog.AggregationsByName.FindPtr(to_lower(name));
    if (!aggIdPtr) {
        throw yexception() << "No such aggregate: " << name;
    }

    for (const auto& id : *aggIdPtr) {
        const auto& d = catalog.Aggregations.FindPtr(id);
        Y_ENSURE(d);
        if (!ValidateAggregateArgs(*d, stateType, resultType)) {
            continue;
        }

        return *d;
    }

    throw yexception() << "Unable to find an overload for aggregate " << name << " with given state type: " <<
        NPg::LookupType(stateType).Name << " and result type: " <<
        NPg::LookupType(resultType).Name;
}

void EnumAggregation(std::function<void(ui32, const TAggregateDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& x : catalog.Aggregations) {
        f(x.first, x.second);
    }
}

bool HasOpClass(EOpClassMethod method, ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    return catalog.OpClasses.contains(std::make_pair(method, typeId));
}

const TOpClassDesc* LookupDefaultOpClass(EOpClassMethod method, ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    auto lookupId = (typeId == VarcharOid ? TextOid : typeId);
    const auto opClassPtr = catalog.OpClasses.FindPtr(std::make_pair(method, lookupId));
    if (opClassPtr)
        return opClassPtr;

    throw yexception() << "No such opclass";

    // TODO: support binary coercible and preferred types as define in PG's GetDefaultOpClass()
}

bool HasAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType) {
    const auto &catalog = TCatalog::Instance();
    return catalog.AmOps.contains(std::make_tuple(familyId, strategy, leftType, rightType));
}

const TAmOpDesc& LookupAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType) {
    const auto& catalog = TCatalog::Instance();
    const auto amOpPtr = catalog.AmOps.FindPtr(std::make_tuple(familyId, strategy, leftType, rightType));
    if (!amOpPtr) {
        throw yexception() << "No such amop";
    }

    return *amOpPtr;
}

bool HasAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType) {
    const auto &catalog = TCatalog::Instance();
    return catalog.AmProcs.contains(std::make_tuple(familyId, num, leftType, rightType));
}

const TAmProcDesc& LookupAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType) {
    const auto& catalog = TCatalog::Instance();
    auto amProcPtr = catalog.AmProcs.FindPtr(std::make_tuple(familyId, num, leftType, rightType));
    if (!amProcPtr) {
        throw yexception() << "No such amproc";
    }

    return *amProcPtr;
}

bool HasConversion(const TString& from, const TString& to) {
    const auto &catalog = TCatalog::Instance();
    return catalog.Conversions.contains(std::make_pair(from, to));
}

const TConversionDesc& LookupConversion(const TString& from, const TString& to) {
    const auto& catalog = TCatalog::Instance();
    auto convPtr = catalog.Conversions.FindPtr(std::make_pair(from, to));
    if (!convPtr) {
        throw yexception() << "No such conversion from " << from << " to " << to;
    }

    return *convPtr;
}

bool IsCompatibleTo(ui32 actualType, ui32 expectedType) {
    const auto& catalog = TCatalog::Instance();
    return IsCompatibleTo(actualType, expectedType, catalog.Types);
}

}
