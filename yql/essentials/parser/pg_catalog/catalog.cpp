#include "catalog.h"
#include <yql/essentials/parser/pg_catalog/proto/pg_catalog.pb.h>
#include <yql/essentials/utils/log/profile.h>
#include <util/generic/array_size.h>
#include <util/generic/utility.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/mutex.h>
#include <util/system/tempfile.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/digest/md5/md5.h>

namespace NYql::NPg {

const ui32 MaximumExtensionsCount = 64; // see TTypeAnnotationNode::GetUsedPgExtensions

constexpr ui32 FuncMaxArgs = 100;
constexpr ui32 InvalidOid = 0;
constexpr ui32 Int2VectorOid = 22;
constexpr ui32 RegProcOid = 24;
constexpr ui32 OidOid = 26;
constexpr ui32 OidVectorOid = 30;
constexpr ui32 RegProcedureOid = 2202;
constexpr ui32 RegOperOid = 2203;
constexpr ui32 RegOperatorOid = 2204;
constexpr ui32 RegClassOid = 2205;
constexpr ui32 RegTypeOid = 2206;
// constexpr ui32 AnyElementOid = 2283;
// constexpr ui32 AnyNonArrayOid = 2776;
constexpr ui32 RegConfigOid = 3734;
constexpr ui32 RegDictionaryOid = 3769;
constexpr ui32 RegNamespaceOid = 4089;
constexpr ui32 RegRoleOid = 4096;
// constexpr ui32 AnyCompatibleOid = 5077;
// constexpr ui32 AnyCompatibleArrayOid = 5078;
// constexpr ui32 AnyCompatibleNonArrayOid = 5079;

// See GetCCHashEqFuncs in PG sources
// https://doxygen.postgresql.org/catcache_8c.html#a8a2dc395011dba02c083bfbf6b87ce6c
const THashSet<ui32> regClasses({RegProcOid, RegProcedureOid, RegOperOid, RegOperatorOid, RegClassOid, RegTypeOid,
                                 RegConfigOid, RegDictionaryOid, RegRoleOid, RegNamespaceOid});

using TOperators = THashMap<ui32, TOperDesc>;

using TProcs = THashMap<ui32, TProcDesc>;

using TTypes = THashMap<ui32, TTypeDesc>;

using TCasts = THashMap<ui32, TCastDesc>;

using TAggregations = THashMap<ui32, TAggregateDesc>;

using TAms = THashMap<ui32, TAmDesc>;

using TNamespaces = THashMap<decltype(TNamespaceDesc::Oid), TNamespaceDesc>;

using TOpFamilies = THashMap<TString, TOpFamilyDesc>;

using TOpClasses = THashMap<std::pair<EOpClassMethod, ui32>, TOpClassDesc>;

using TAmOps = THashMap<std::tuple<ui32, ui32, ui32, ui32>, TAmOpDesc>;

using TAmProcs = THashMap<std::tuple<ui32, ui32, ui32, ui32>, TAmProcDesc>;

using TConversions = THashMap<std::pair<TString, TString>, TConversionDesc>;

using TLanguages = THashMap<ui32, TLanguageDesc>;

using TExtensions = TVector<TExtensionDesc>;
using TExtensionsByName = THashMap<TString, ui32>;

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
        return actualDescPtr->ArrayTypeId && actualDescPtr->ArrayTypeId == actualDescPtr->TypeId;
    }

    if (expectedTypeId == AnyNonArrayOid) {
        const auto& actualDescPtr = types.FindPtr(actualTypeId);
        Y_ENSURE(actualDescPtr);
        return actualDescPtr->ArrayTypeId && actualDescPtr->ArrayTypeId != actualDescPtr->TypeId;
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

bool ValidateArgs(const TVector<ui32>& descArgTypeIds, const TVector<ui32>& argTypeIds, const TTypes& types, ui32 variadicArgType = 0) {
    if (argTypeIds.size() > FuncMaxArgs) {
        return false;
    }

    if (argTypeIds.size() < descArgTypeIds.size()) {
        return false;
    }

    if (!variadicArgType && argTypeIds.size() > descArgTypeIds.size()) {
        return false;
    }

    if (variadicArgType && argTypeIds.size() == descArgTypeIds.size()) {
        // at least one variadic argument is required
        return false;
    }

    for (size_t i = 0; i < descArgTypeIds.size(); ++i) {
        if (!IsCompatibleTo(argTypeIds[i], descArgTypeIds[i], types)) {
            return false;
        }
    }

    for (size_t i = descArgTypeIds.size(); i < argTypeIds.size(); ++i) {
        if (!IsCompatibleTo(argTypeIds[i], variadicArgType, types)) {
            return false;
        }
    }

    return true;
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
        } else {
            expectedArgType = d.RightType;
        }

        if (!IsCompatibleTo(argTypeIds[i], expectedArgType, types)) {
            return false;
        }
    }

    return true;
}

struct TLazyOperInfo {
    TString Com;
    TString Negate;
};

class TOperatorsParser: public TParser {
public:
    TOperatorsParser(TOperators& operators, const THashMap<TString, ui32>& typeByName, const TTypes& types,
                     const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs, THashMap<ui32, TLazyOperInfo>& lazyInfos)
        : Operators_(operators)
        , TypeByName_(typeByName)
        , Types_(types)
        , ProcByName_(procByName)
        , Procs_(procs)
        , LazyInfos_(lazyInfos)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastOperator_.OperId = FromString<ui32>(value);
        } else if (key == "oprname") {
            LastOperator_.Name = value;
        } else if (key == "descr") {
            LastOperator_.Descr = value;
        } else if (key == "oprkind") {
            if (value == "r") {
                LastOperator_.Kind = EOperKind::RightUnary;
            } else if (value == "l") {
                LastOperator_.Kind = EOperKind::LeftUnary;
            }
        } else if (key == "oprleft") {
            if (value != "0") {
                auto typeIdPtr = TypeByName_.FindPtr(value);
                Y_ENSURE(typeIdPtr);
                LastOperator_.LeftType = *typeIdPtr;
            }
        } else if (key == "oprright") {
            if (value != "0") {
                auto typeIdPtr = TypeByName_.FindPtr(value);
                Y_ENSURE(typeIdPtr);
                LastOperator_.RightType = *typeIdPtr;
            }
        } else if (key == "oprresult") {
            auto typeIdPtr = TypeByName_.FindPtr(value);
            Y_ENSURE(typeIdPtr);
            LastOperator_.ResultType = *typeIdPtr;
        } else if (key == "oprcode") {
            LastCode_ = value;
        } else if (key == "oprnegate") {
            LastNegate_ = value;
        } else if (key == "oprcom") {
            LastCom_ = value;
        }
    }

    void OnFinish() override {
        if (IsSupported_) {
            auto pos = LastCode_.find('(');
            auto code = LastCode_.substr(0, pos);
            auto procIdPtr = ProcByName_.FindPtr(code);
            // skip operator if proc isn't builtin, e.g. path_contain_pt
            if (!procIdPtr) {
                IsSupported_ = false;
            } else {
                for (auto procId : *procIdPtr) {
                    auto procPtr = Procs_.FindPtr(procId);
                    Y_ENSURE(procPtr);
                    if (ValidateOperArgs(LastOperator_, procPtr->ArgTypes, Types_)) {
                        Y_ENSURE(!LastOperator_.ProcId);
                        LastOperator_.ProcId = procId;
                    }
                }

                // can be missing for example jsonb - _text
                if (LastOperator_.ProcId) {
                    Y_ENSURE(!LastOperator_.Name.empty());
                    Operators_[LastOperator_.OperId] = LastOperator_;
                }

                if (!LastCom_.empty()) {
                    LazyInfos_[LastOperator_.OperId].Com = LastCom_;
                }

                if (!LastNegate_.empty()) {
                    LazyInfos_[LastOperator_.OperId].Negate = LastNegate_;
                }
            }
        }

        LastOperator_ = TOperDesc();
        LastCode_ = "";
        LastNegate_ = "";
        LastCom_ = "";
        IsSupported_ = true;
    }

private:
    TOperators& Operators_;
    const THashMap<TString, ui32>& TypeByName_;
    const TTypes& Types_;
    const THashMap<TString, TVector<ui32>>& ProcByName_;
    const TProcs& Procs_;
    THashMap<ui32, TLazyOperInfo>& LazyInfos_;
    TOperDesc LastOperator_;
    bool IsSupported_ = true;
    TString LastCode_;
    TString LastNegate_;
    TString LastCom_;
};

class TProcsParser: public TParser {
public:
    TProcsParser(TProcs& procs, const THashMap<TString, ui32>& typeByName)
        : Procs_(procs)
        , TypeByName_(typeByName)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastProc_.ProcId = FromString<ui32>(value);
        } else if (key == "provariadic") {
            auto idPtr = TypeByName_.FindPtr(value);
            Y_ENSURE(idPtr);
            LastProc_.VariadicType = *idPtr;
        } else if (key == "descr") {
            LastProc_.Descr = value;
        } else if (key == "prokind") {
            if (value == "f") {
                LastProc_.Kind = EProcKind::Function;
            } else if (value == "a") {
                LastProc_.Kind = EProcKind::Aggregate;
            } else if (value == "w") {
                LastProc_.Kind = EProcKind::Window;
            } else {
                IsSupported_ = false;
            }
        } else if (key == "prorettype") {
            auto idPtr = TypeByName_.FindPtr(value);
            Y_ENSURE(idPtr);
            LastProc_.ResultType = *idPtr;
        } else if (key == "proname") {
            LastProc_.Name = value;
        } else if (key == "prosrc") {
            LastProc_.Src = value;
        } else if (key == "prolang") {
            if (value == "sql") {
                LastProc_.Lang = LangSQL;
            } else if (value == "c") {
                LastProc_.Lang = LangC;
            } else {
                IsSupported_ = false;
            }
        } else if (key == "proargtypes") {
            TVector<TString> strArgs;
            Split(value, " ", strArgs);
            LastProc_.ArgTypes.reserve(strArgs.size());
            for (const auto& s : strArgs) {
                auto idPtr = TypeByName_.FindPtr(s);
                Y_ENSURE(idPtr);
                LastProc_.ArgTypes.push_back(*idPtr);
            }
        } else if (key == "proisstrict") {
            LastProc_.IsStrict = (value == "t");
        } else if (key == "proretset") {
            LastProc_.ReturnSet = (value == "t");
        } else if (key == "proallargtypes") {
            AllArgTypesStr_ = value;
        } else if (key == "proargmodes") {
            ArgModesStr_ = value;
        } else if (key == "proargnames") {
            ArgNamesStr_ = value;
        }
    }

    void OnFinish() override {
        if (IsSupported_) {
            if (LastProc_.VariadicType) {
                Y_ENSURE(!ArgModesStr_.empty());
            }

            if (!ArgModesStr_.empty()) {
                Y_ENSURE(ArgModesStr_.front() == '{');
                Y_ENSURE(ArgModesStr_.back() == '}');
                TVector<TString> modes;
                Split(ArgModesStr_.substr(1, ArgModesStr_.size() - 2), ",", modes);
                Y_ENSURE(modes.size() >= LastProc_.ArgTypes.size());
                ui32 inputArgsCount = 0;
                bool startedVarArgs = false;
                bool startedOutArgs = false;
                for (size_t i = 0; i < modes.size(); ++i) {
                    if (modes[i] == "i") {
                        Y_ENSURE(!startedVarArgs && !startedOutArgs);
                        inputArgsCount = i + 1;
                    } else if (modes[i] == "o") {
                        startedOutArgs = true;
                    } else {
                        Y_ENSURE(!startedVarArgs && !startedOutArgs);
                        Y_ENSURE(modes[i] == "v");
                        Y_ENSURE(LastProc_.VariadicType);
                        startedVarArgs = true;
                    }
                }

                if (LastProc_.VariadicType) {
                    Y_ENSURE(LastProc_.ArgTypes.size() > inputArgsCount);
                    LastProc_.VariadicArgType = LastProc_.ArgTypes[inputArgsCount];
                    Y_ENSURE(LastProc_.VariadicArgType);
                }

                LastProc_.ArgTypes.resize(inputArgsCount);
            }
        }

        if (IsSupported_) {
            auto variadicDelta = LastProc_.VariadicType ? 1 : 0;
            if (!ArgNamesStr_.empty()) {
                Y_ENSURE(ArgNamesStr_.front() == '{');
                Y_ENSURE(ArgNamesStr_.back() == '}');
                TVector<TString> names;
                Split(ArgNamesStr_.substr(1, ArgNamesStr_.size() - 2), ",", names);
                Y_ENSURE(names.size() >= LastProc_.ArgTypes.size() + variadicDelta);
                LastProc_.OutputArgNames.insert(LastProc_.OutputArgNames.begin(), names.begin() + LastProc_.ArgTypes.size() + variadicDelta, names.end());
                if (LastProc_.VariadicType) {
                    LastProc_.VariadicArgName = names[LastProc_.ArgTypes.size()];
                }

                LastProc_.InputArgNames.insert(LastProc_.InputArgNames.begin(), names.begin(), names.begin() + LastProc_.ArgTypes.size());
            }

            if (!AllArgTypesStr_.empty()) {
                Y_ENSURE(!ArgModesStr_.empty());
                Y_ENSURE(AllArgTypesStr_.front() == '{');
                Y_ENSURE(AllArgTypesStr_.back() == '}');
                TVector<TString> types;
                Split(AllArgTypesStr_.substr(1, AllArgTypesStr_.size() - 2), ",", types);
                Y_ENSURE(types.size() >= LastProc_.ArgTypes.size() + variadicDelta);

                for (size_t i = LastProc_.ArgTypes.size() + variadicDelta; i < types.size(); ++i) {
                    auto idPtr = TypeByName_.FindPtr(types[i]);
                    Y_ENSURE(idPtr);
                    LastProc_.OutputArgTypes.push_back(*idPtr);
                }
            }
        }

        if (IsSupported_) {
            Y_ENSURE(!LastProc_.Name.empty());
            Procs_[LastProc_.ProcId] = LastProc_;
        }

        IsSupported_ = true;
        LastProc_ = TProcDesc();
        AllArgTypesStr_ = "";
        ArgModesStr_ = "";
        ArgNamesStr_ = "";
    }

private:
    TProcs& Procs_;
    const THashMap<TString, ui32>& TypeByName_;
    TProcDesc LastProc_;
    bool IsSupported_ = true;
    TString AllArgTypesStr_;
    TString ArgModesStr_;
    TString ArgNamesStr_;
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

class TTypesParser: public TParser {
public:
    TTypesParser(TTypes& types, THashMap<ui32, TLazyTypeInfo>& lazyInfos)
        : Types_(types)
        , LazyInfos_(lazyInfos)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastType_.TypeId = FromString<ui32>(value);
        } else if (key == "array_type_oid") {
            LastType_.ArrayTypeId = FromString<ui32>(value);
        } else if (key == "descr") {
            LastType_.Descr = value;
        } else if (key == "typname") {
            LastType_.Name = value;
        } else if (key == "typcategory") {
            Y_ENSURE(value.size() == 1);
            LastType_.Category = value[0];
        } else if (key == "typlen") {
            if (value == "NAMEDATALEN") {
                LastType_.TypeLen = 64;
            } else if (value == "SIZEOF_POINTER") {
                LastType_.TypeLen = 8;
            } else {
                LastType_.TypeLen = FromString<i32>(value);
            }
        } else if (key == "typalign") {
            if (value == "ALIGNOF_POINTER") {
                LastType_.TypeAlign = 'i'; // doesn't matter for pointers
            } else {
                Y_ENSURE(value.size() == 1);
                LastType_.TypeAlign = value[0];
            }
        } else if (key == "typdelim") {
            Y_ENSURE(value.size() == 1);
            LastType_.TypeDelim = value[0];
        } else if (key == "typtype") {
            Y_ENSURE(value.size() == 1);
            const auto typType = value[0];

            switch (typType) {
                case 'b':
                case 'c':
                case 'd':
                case 'e':
                case 'm':
                case 'p':
                case 'r':
                    LastType_.TypType = (ETypType)typType;
                    break;
                default:
                    throw yexception() << "Unknown typtype value: " << typType;
            }
        } else if (key == "typcollation") {
            // hardcode collations for now. There are only three of 'em in .dat file
            if (value == "default") {
                LastType_.TypeCollation = DefaultCollationOid;
            } else if (value == "C") {
                LastType_.TypeCollation = C_CollationOid;
            } else if (value == "POSIX") {
                LastType_.TypeCollation = PosixCollationOid;
            } else {
                throw yexception() << "Unknown typcollation value: " << value;
            }
        } else if (key == "typelem") {
            LastLazyTypeInfo_.ElementType = value; // resolve later
        } else if (key == "typinput") {
            LastLazyTypeInfo_.InFunc = value; // resolve later
        } else if (key == "typoutput") {
            LastLazyTypeInfo_.OutFunc = value; // resolve later
        } else if (key == "typsend") {
            LastLazyTypeInfo_.SendFunc = value; // resolve later
        } else if (key == "typreceive") {
            LastLazyTypeInfo_.ReceiveFunc = value; // resolve later
        } else if (key == "typmodin") {
            LastLazyTypeInfo_.ModInFunc = value; // resolve later
        } else if (key == "typmodout") {
            LastLazyTypeInfo_.ModOutFunc = value; // resolve later
        } else if (key == "typsubscript") {
            LastLazyTypeInfo_.SubscriptFunc = value; // resolve later
        } else if (key == "typbyval") {
            if (value == "f") {
                LastType_.PassByValue = false;
            } else if (value == "t" || value == "FLOAT8PASSBYVAL") {
                LastType_.PassByValue = true;
            } else {
                throw yexception() << "Unknown typbyval value: " << value;
            }
        } else if (key == "typispreferred") {
            LastType_.IsPreferred = (value == "t");
        }
    }

    void OnFinish() override {
        Y_ENSURE(!LastType_.Name.empty());
        Y_ENSURE(LastType_.TypeLen != 0);
        if (LastType_.TypeLen < 0 || LastType_.TypeLen > 8) {
            Y_ENSURE(!LastType_.PassByValue);
        }

        Types_[LastType_.TypeId] = LastType_;
        if (LastType_.ArrayTypeId) {
            auto arrayType = LastType_;
            arrayType.Name = "_" + arrayType.Name;
            arrayType.ElementTypeId = arrayType.TypeId;
            arrayType.TypeId = LastType_.ArrayTypeId;
            arrayType.PassByValue = false;
            arrayType.TypeLen = -1;
            Types_[LastType_.ArrayTypeId] = arrayType;
        }

        LazyInfos_[LastType_.TypeId] = LastLazyTypeInfo_;
        if (LastType_.ArrayTypeId) {
            LastLazyTypeInfo_.OutFunc = "array_out";
            LastLazyTypeInfo_.InFunc = "array_in";
            LastLazyTypeInfo_.SendFunc = "array_send";
            LastLazyTypeInfo_.ReceiveFunc = "array_recv";
            LastLazyTypeInfo_.ElementType = "";
            LazyInfos_[LastType_.ArrayTypeId] = LastLazyTypeInfo_;
        }

        LastType_ = TTypeDesc();
        LastLazyTypeInfo_ = TLazyTypeInfo();
    }

private:
    TTypes& Types_;
    THashMap<ui32, TLazyTypeInfo>& LazyInfos_;
    TTypeDesc LastType_;
    TLazyTypeInfo LastLazyTypeInfo_;
};

class TCastsParser: public TParser {
public:
    TCastsParser(TCasts& casts, const THashMap<TString, ui32>& typeByName, const TTypes& types,
                 const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs)
        : Casts_(casts)
        , TypeByName_(typeByName)
        , Types_(types)
        , ProcByName_(procByName)
        , Procs_(procs)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "castsource") {
            auto typePtr = TypeByName_.FindPtr(value);
            Y_ENSURE(typePtr);
            LastCast_.SourceId = *typePtr;
        } else if (key == "casttarget") {
            auto typePtr = TypeByName_.FindPtr(value);
            Y_ENSURE(typePtr);
            LastCast_.TargetId = *typePtr;
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
                    auto inputTypeIdPtr = TypeByName_.FindPtr(inputType);
                    Y_ENSURE(inputTypeIdPtr);
                    auto procIdPtr = ProcByName_.FindPtr(funcName);
                    Y_ENSURE(procIdPtr);
                    bool found = false;
                    for (const auto& procId : *procIdPtr) {
                        auto procPtr = Procs_.FindPtr(procId);
                        Y_ENSURE(procPtr);
                        if (procPtr->ArgTypes.size() < 1) {
                            continue;
                        }

                        if (IsCompatibleTo(*inputTypeIdPtr, procPtr->ArgTypes[0], Types_)) {
                            LastCast_.FunctionId = procPtr->ProcId;
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        // e.g. convert circle to 12-vertex polygon, used sql proc
                        IsSupported_ = false;
                    }
                } else {
                    auto procIdPtr = ProcByName_.FindPtr(value);
                    Y_ENSURE(procIdPtr);
                    Y_ENSURE(procIdPtr->size() == 1);
                    LastCast_.FunctionId = procIdPtr->at(0);
                }
            }
        } else if (key == "castmethod") {
            if (value == "f") {
                LastCast_.Method = ECastMethod::Function;
            } else if (value == "i") {
                LastCast_.Method = ECastMethod::InOut;
            } else if (value == "b") {
                LastCast_.Method = ECastMethod::Binary;
            } else {
                throw yexception() << "Unknown castmethod value: " << value;
            }
        } else if (key == "castcontext") {
            Y_ENSURE(value.size() == 1);
            const auto castCtx = value[0];

            switch (castCtx) {
                case 'i':
                case 'a':
                case 'e':
                    LastCast_.CoercionCode = (ECoercionCode)castCtx;
                    break;
                default:
                    throw yexception() << "Unknown castcontext value: " << castCtx;
            }
        }
    }

    void OnFinish() override {
        if (IsSupported_) {
            auto id = 1 + Casts_.size();
            Casts_[id] = LastCast_;
        }

        LastCast_ = TCastDesc();
        IsSupported_ = true;
    }

private:
    TCasts& Casts_;
    const THashMap<TString, ui32>& TypeByName_;
    const TTypes& Types_;
    const THashMap<TString, TVector<ui32>>& ProcByName_;
    const TProcs& Procs_;
    TCastDesc LastCast_;
    bool IsSupported_ = true;
};

class TAggregationsParser: public TParser {
public:
    TAggregationsParser(TAggregations& aggregations, const THashMap<TString, ui32>& typeByName,
                        const TTypes& types, const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs)
        : Aggregations_(aggregations)
        , TypeByName_(typeByName)
        , Types_(types)
        , ProcByName_(procByName)
        , Procs_(procs)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        Y_UNUSED(ProcByName_);
        if (key == "aggtranstype") {
            auto typeId = TypeByName_.FindPtr(value);
            Y_ENSURE(typeId);
            LastAggregation_.TransTypeId = *typeId;
        } else if (key == "aggfnoid") {
            LastOid_ = value;
        } else if (key == "aggtransfn") {
            LastTransFunc_ = value;
        } else if (key == "aggfinalfn") {
            LastFinalFunc_ = value;
        } else if (key == "aggcombinefn") {
            LastCombineFunc_ = value;
        } else if (key == "aggserialfn") {
            LastSerializeFunc_ = value;
        } else if (key == "aggdeserialfn") {
            LastDeserializeFunc_ = value;
        } else if (key == "aggkind") {
            if (value == "n") {
                LastAggregation_.Kind = EAggKind::Normal;
            } else if (value == "o") {
                LastAggregation_.Kind = EAggKind::OrderedSet;
            } else if (value == "h") {
                LastAggregation_.Kind = EAggKind::Hypothetical;
            } else {
                throw yexception() << "Unknown aggkind value: " << value;
            }
        } else if (key == "agginitval") {
            LastAggregation_.InitValue = value;
        } else if (key == "aggfinalextra") {
            LastAggregation_.FinalExtra = (value == "t");
            ;
        } else if (key == "aggnumdirectargs") {
            LastAggregation_.NumDirectArgs = FromString<ui32>(value);
        }
    }

    void OnFinish() override {
        if (IsSupported_) {
            if (FillSupported()) {
                Aggregations_[LastAggregation_.AggId] = LastAggregation_;
            }
        }

        LastAggregation_ = TAggregateDesc();
        IsSupported_ = true;
        LastOid_ = "";
        LastTransFunc_ = "";
        LastFinalFunc_ = "";
        LastCombineFunc_ = "";
        LastSerializeFunc_ = "";
        LastDeserializeFunc_ = "";
    }

    bool FillSupported() {
        Y_ENSURE(LastAggregation_.TransTypeId);
        Y_ENSURE(LastOid_);
        Y_ENSURE(LastTransFunc_);
        auto transFuncIdsPtr = ProcByName_.FindPtr(LastTransFunc_);
        if (!transFuncIdsPtr) {
            // e.g. variadic ordered_set_transition_multi
            return false;
        }

        for (const auto id : *transFuncIdsPtr) {
            auto procPtr = Procs_.FindPtr(id);
            Y_ENSURE(procPtr);
            if (procPtr->ArgTypes.size() >= 1 &&
                IsCompatibleTo(LastAggregation_.TransTypeId, procPtr->ArgTypes[0], Types_)) {
                Y_ENSURE(!LastAggregation_.TransFuncId);
                LastAggregation_.TransFuncId = id;
            }
        }

        Y_ENSURE(LastAggregation_.TransFuncId);

        // oid format: name(arg1,arg2...)
        auto pos1 = LastOid_.find('(');
        if (pos1 != TString::npos) {
            LastAggregation_.Name = LastOid_.substr(0, pos1);
            auto pos = pos1 + 1;
            for (;;) {
                auto nextPos = Min(LastOid_.find(',', pos), LastOid_.find(')', pos));
                Y_ENSURE(nextPos != TString::npos);
                if (pos == nextPos) {
                    break;
                }

                auto arg = LastOid_.substr(pos, nextPos - pos);
                auto argTypeId = TypeByName_.FindPtr(arg);
                Y_ENSURE(argTypeId);
                LastAggregation_.ArgTypes.push_back(*argTypeId);
                pos = nextPos;
                if (LastOid_[pos] == ')') {
                    break;
                } else {
                    ++pos;
                }
            }
        } else {
            // no signature in oid, use transfunc
            LastAggregation_.Name = LastOid_;
            auto procPtr = Procs_.FindPtr(LastAggregation_.TransFuncId);
            Y_ENSURE(procPtr);
            LastAggregation_.ArgTypes = procPtr->ArgTypes;
            Y_ENSURE(LastAggregation_.ArgTypes.size() >= 1);
            Y_ENSURE(IsCompatibleTo(LastAggregation_.TransTypeId, LastAggregation_.ArgTypes[0], Types_));
            LastAggregation_.ArgTypes.erase(LastAggregation_.ArgTypes.begin());
        }

        Y_ENSURE(!LastAggregation_.Name.empty());
        auto funcIdsPtr = ProcByName_.FindPtr(LastAggregation_.Name);
        Y_ENSURE(funcIdsPtr);
        if (funcIdsPtr->size() == 1) {
            LastAggregation_.AggId = funcIdsPtr->front();
        } else {
            for (const auto id : *funcIdsPtr) {
                auto procPtr = Procs_.FindPtr(id);
                Y_ENSURE(procPtr);
                if (ValidateArgs(procPtr->ArgTypes, LastAggregation_.ArgTypes, Types_, procPtr->VariadicType)) {
                    LastAggregation_.AggId = id;
                    break;
                }
            }
        }

        Y_ENSURE(LastAggregation_.AggId);
        if (!ResolveFunc(LastFinalFunc_, LastAggregation_.FinalFuncId, 1)) {
            return false;
        }

        if (!ResolveFunc(LastCombineFunc_, LastAggregation_.CombineFuncId, 2)) {
            return false;
        }

        if (!ResolveFunc(LastSerializeFunc_, LastAggregation_.SerializeFuncId, 1)) {
            return false;
        }

        if (!ResolveFunc(LastDeserializeFunc_, LastAggregation_.DeserializeFuncId, 0)) {
            return false;
        }

        return true;
    }

    bool ResolveFunc(const TString& name, ui32& funcId, ui32 stateArgsCount) {
        if (name) {
            auto funcIdsPtr = ProcByName_.FindPtr(name);
            if (!funcIdsPtr) {
                return false;
            }

            if (!stateArgsCount) {
                Y_ENSURE(funcIdsPtr->size() == 1);
            }

            for (const auto id : *funcIdsPtr) {
                auto procPtr = Procs_.FindPtr(id);
                Y_ENSURE(procPtr);
                bool found = true;
                if (stateArgsCount > 0 && procPtr->ArgTypes.size() == stateArgsCount) {
                    for (ui32 i = 0; i < stateArgsCount; ++i) {
                        if (!IsCompatibleTo(LastAggregation_.TransTypeId, procPtr->ArgTypes[i], Types_)) {
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
    TAggregations& Aggregations_;
    const THashMap<TString, ui32>& TypeByName_;
    const TTypes& Types_;
    const THashMap<TString, TVector<ui32>>& ProcByName_;
    const TProcs& Procs_;
    TAggregateDesc LastAggregation_;
    bool IsSupported_ = true;
    TString LastOid_;
    TString LastTransFunc_;
    TString LastFinalFunc_;
    TString LastCombineFunc_;
    TString LastSerializeFunc_;
    TString LastDeserializeFunc_;
};

class TOpFamiliesParser: public TParser {
public:
    explicit TOpFamiliesParser(TOpFamilies& opFamilies)
        : OpFamilies_(opFamilies)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastOpfId_ = FromString<ui32>(value);
        } else if (key == "opfmethod") {
            if (value == "btree" || value == "hash") {
                LastOpfMethod_ = value;
            } else {
                IsSupported_ = false;
            }
        } else if (key == "opfname") {
            LastOpfName_ = value;
        }
    }

    void OnFinish() override {
        if (IsSupported_) {
            Y_ENSURE(LastOpfId_ != InvalidOid);

            // opfamily references have opf_method/opf_name format in PG catalogs
            TOpFamilyDesc desc;
            desc.Name = LastOpfMethod_ + "/" + LastOpfName_;
            desc.FamilyId = LastOpfId_;
            Y_ENSURE(OpFamilies_.emplace(desc.Name, desc).second);
        }

        IsSupported_ = true;
        LastOpfId_ = InvalidOid;
        LastOpfMethod_.clear();
        LastOpfName_.clear();
    }

private:
    TOpFamilies& OpFamilies_;

    ui32 LastOpfId_ = InvalidOid;

    TString LastOpfMethod_;
    TString LastOpfName_;
    bool IsSupported_ = true;
};

class TOpClassesParser: public TParser {
public:
    TOpClassesParser(TOpClasses& opClasses, const THashMap<TString, ui32>& typeByName,
                     const TOpFamilies& opFamilies)
        : OpClasses_(opClasses)
        , TypeByName_(typeByName)
        , OpFamilies_(opFamilies)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "opcmethod") {
            if (value == "btree") {
                LastOpClass_.Method = EOpClassMethod::Btree;
            } else if (value == "hash") {
                LastOpClass_.Method = EOpClassMethod::Hash;
            } else {
                IsSupported_ = false;
            }
        } else if (key == "opcintype") {
            auto idPtr = TypeByName_.FindPtr(value);
            Y_ENSURE(idPtr);
            LastOpClass_.TypeId = *idPtr;
        } else if (key == "opcname") {
            LastOpClass_.Name = value;
        } else if (key == "opcfamily") {
            LastOpClass_.Family = value;
            auto opFamilyPtr = OpFamilies_.FindPtr(value);

            if (opFamilyPtr) {
                LastOpClass_.FamilyId = opFamilyPtr->FamilyId;
            } else {
                IsSupported_ = false;
            }
        } else if (key == "opcdefault") {
            IsDefault_ = (value[0] != 'f');
        }
    }

    void OnFinish() override {
        // Only default opclasses are used so far
        if (IsSupported_ && IsDefault_) {
            Y_ENSURE(!LastOpClass_.Name.empty());

            const auto key = std::make_pair(LastOpClass_.Method, LastOpClass_.TypeId);

            if (OpClasses_.contains(key)) {
                throw yexception() << "Duplicate opclass: (" << (key.first == EOpClassMethod::Btree ? "btree" : "hash")
                                   << ", " << key.second << ")";
            }
            OpClasses_[key] = LastOpClass_;
        }

        IsSupported_ = true;
        IsDefault_ = true;
        LastOpClass_ = TOpClassDesc();
    }

private:
    TOpClasses& OpClasses_;

    const THashMap<TString, ui32>& TypeByName_;
    const TOpFamilies OpFamilies_;

    TOpClassDesc LastOpClass_;
    bool IsSupported_ = true;
    bool IsDefault_ = true;
};

class TAmOpsParser: public TParser {
public:
    TAmOpsParser(TAmOps& amOps, const THashMap<TString, ui32>& typeByName, const TTypes& types,
                 const THashMap<TString, TVector<ui32>>& operatorsByName, const TOperators& operators,
                 const TOpFamilies& opFamilies)
        : AmOps_(amOps)
        , TypeByName_(typeByName)
        , Types_(types)
        , OperatorsByName_(operatorsByName)
        , Operators_(operators)
        , OpFamilies_(opFamilies)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "amopfamily") {
            LastAmOp_.Family = value;
            auto opFamilyPtr = OpFamilies_.FindPtr(value);
            if (opFamilyPtr) {
                LastAmOp_.FamilyId = opFamilyPtr->FamilyId;
            } else {
                IsSupported_ = false;
            }
        } else if (key == "amoplefttype") {
            auto leftTypePtr = TypeByName_.FindPtr(value);
            Y_ENSURE(leftTypePtr);
            LastAmOp_.LeftType = *leftTypePtr;
        } else if (key == "amoprighttype") {
            auto rightTypePtr = TypeByName_.FindPtr(value);
            Y_ENSURE(rightTypePtr);
            LastAmOp_.RightType = *rightTypePtr;
        } else if (key == "amopstrategy") {
            LastAmOp_.Strategy = FromString<ui32>(value);
        } else if (key == "amopopr") {
            auto pos = value.find('(');
            Y_ENSURE(pos != TString::npos);
            LastOp_ = value.substr(0, pos);
        }
    }

    void OnFinish() override {
        if (IsSupported_) {
            auto operIdPtr = OperatorsByName_.FindPtr(LastOp_);
            Y_ENSURE(operIdPtr);
            for (const auto& id : *operIdPtr) {
                const auto& d = Operators_.FindPtr(id);
                Y_ENSURE(d);
                if (d->Kind == EOperKind::Binary &&
                    IsCompatibleTo(LastAmOp_.LeftType, d->LeftType, Types_) &&
                    IsCompatibleTo(LastAmOp_.RightType, d->RightType, Types_)) {
                    Y_ENSURE(!LastAmOp_.OperId);
                    LastAmOp_.OperId = d->OperId;
                }
            }

            Y_ENSURE(LastAmOp_.OperId);
            AmOps_[std::make_tuple(LastAmOp_.FamilyId, LastAmOp_.Strategy, LastAmOp_.LeftType, LastAmOp_.RightType)] = LastAmOp_;
        }

        LastAmOp_ = TAmOpDesc();
        LastOp_ = "";
        IsSupported_ = true;
    }

private:
    TAmOps& AmOps_;

    const THashMap<TString, ui32>& TypeByName_;
    const TTypes& Types_;
    const THashMap<TString, TVector<ui32>>& OperatorsByName_;
    const TOperators& Operators_;
    const TOpFamilies& OpFamilies_;

    TAmOpDesc LastAmOp_;
    TString LastOp_;
    bool IsSupported_ = true;
};

class TAmsParser: public TParser {
public:
    explicit TAmsParser(TAms& ams)
        : Ams_(ams)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            CurrDesc_.Oid = FromString<ui32>(value);
        } else if (key == "descr") {
            CurrDesc_.Descr = value;
        } else if (key == "amname") {
            CurrDesc_.AmName = value;
        } else if (key == "amtype") {
            Y_ENSURE(value.size() == 1);
            if ((char)EAmType::Index == value[0]) {
                CurrDesc_.AmType = EAmType::Index;
            } else if ((char)EAmType::Table == value[0]) {
                CurrDesc_.AmType = EAmType::Table;
            } else {
                Y_ENSURE(false, "Expected correct AmType");
            }
        }
    }

    void OnFinish() override {
        Ams_[CurrDesc_.Oid] = std::move(CurrDesc_);
        CurrDesc_ = TAmDesc();
    }

private:
    TAmDesc CurrDesc_;
    TAms& Ams_;
};

class TAmProcsParser: public TParser {
public:
    TAmProcsParser(TAmProcs& amProcs, const THashMap<TString, ui32>& typeByName,
                   const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs,
                   const TOpFamilies& opFamilies)
        : AmProcs_(amProcs)
        , TypeByName_(typeByName)
        , ProcByName_(procByName)
        , Procs_(procs)
        , OpFamilies_(opFamilies)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "amprocfamily") {
            LastAmProc_.Family = value;
            auto opFamilyPtr = OpFamilies_.FindPtr(value);

            if (opFamilyPtr) {
                LastAmProc_.FamilyId = opFamilyPtr->FamilyId;
            } else {
                IsSupported_ = false;
            }
        } else if (key == "amproclefttype") {
            auto leftTypePtr = TypeByName_.FindPtr(value);
            Y_ENSURE(leftTypePtr);
            LastAmProc_.LeftType = *leftTypePtr;
        } else if (key == "amprocrighttype") {
            auto rightTypePtr = TypeByName_.FindPtr(value);
            Y_ENSURE(rightTypePtr);
            LastAmProc_.RightType = *rightTypePtr;
        } else if (key == "amprocnum") {
            LastAmProc_.ProcNum = FromString<ui32>(value);
        } else if (key == "amproc") {
            LastName_ = value;
        }
    }

    void OnFinish() override {
        if (IsSupported_) {
            if (LastName_.find('(') == TString::npos) {
                auto procIdPtr = ProcByName_.FindPtr(LastName_);
                Y_ENSURE(procIdPtr);
                for (const auto& id : *procIdPtr) {
                    const auto& d = Procs_.FindPtr(id);
                    Y_ENSURE(d);
                    Y_ENSURE(!LastAmProc_.ProcId);
                    LastAmProc_.ProcId = d->ProcId;
                }

                Y_ENSURE(LastAmProc_.ProcId);
                AmProcs_[std::make_tuple(LastAmProc_.FamilyId, LastAmProc_.ProcNum, LastAmProc_.LeftType, LastAmProc_.RightType)] = LastAmProc_;
            }
        }

        LastAmProc_ = TAmProcDesc();
        LastName_ = "";
        IsSupported_ = true;
    }

private:
    TAmProcs& AmProcs_;

    const THashMap<TString, ui32>& TypeByName_;
    const THashMap<TString, TVector<ui32>>& ProcByName_;
    const TProcs& Procs_;
    const TOpFamilies& OpFamilies_;

    TAmProcDesc LastAmProc_;
    TString LastName_;
    bool IsSupported_ = true;
};

class TConversionsParser: public TParser {
public:
    TConversionsParser(TConversions& conversions, const THashMap<TString, TVector<ui32>>& procByName)
        : Conversions_(conversions)
        , ProcByName_(procByName)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastConversion_.ConversionId = FromString<ui32>(value);
        } else if (key == "conforencoding") {
            Y_ENSURE(value.StartsWith("PG_"));
            LastConversion_.From = value.substr(3);
        } else if (key == "descr") {
            LastConversion_.Descr = value;
        } else if (key == "contoencoding") {
            Y_ENSURE(value.StartsWith("PG_"));
            LastConversion_.To = value.substr(3);
        } else if (key == "conproc") {
            auto found = ProcByName_.FindPtr(value);
            if (found && found->size() == 1) {
                LastConversion_.ProcId = found->front();
            }
        }
    }

    void OnFinish() override {
        if (LastConversion_.ProcId) {
            Conversions_[std::make_pair(LastConversion_.From, LastConversion_.To)] = LastConversion_;
        }

        LastConversion_ = TConversionDesc();
    }

private:
    TConversions& Conversions_;

    const THashMap<TString, TVector<ui32>>& ProcByName_;

    TConversionDesc LastConversion_;
};

class TLanguagesParser: public TParser {
public:
    explicit TLanguagesParser(TLanguages& languages)
        : Languages_(languages)
    {
    }

    void OnKey(const TString& key, const TString& value) override {
        if (key == "oid") {
            LastLanguage_.LangId = FromString<ui32>(value);
        } else if (key == "lanname") {
            LastLanguage_.Name = value;
        } else if (key == "descr") {
            LastLanguage_.Descr = value;
        }
    }

    void OnFinish() override {
        Languages_[LastLanguage_.LangId] = LastLanguage_;
        LastLanguage_ = TLanguageDesc();
    }

private:
    TLanguages& Languages_;

    TLanguageDesc LastLanguage_;
};

TOperators ParseOperators(const TString& dat, const THashMap<TString, ui32>& typeByName,
                          const TTypes& types, const THashMap<TString, TVector<ui32>>& procByName, const TProcs& procs, THashMap<ui32, TLazyOperInfo>& lazyInfos) {
    TOperators ret;
    TOperatorsParser parser(ret, typeByName, types, procByName, procs, lazyInfos);
    parser.Do(dat);
    return ret;
}

ui32 FindOperator(const THashMap<TString, TVector<ui32>>& operatorsByName, const THashMap<TString, ui32>& typeByName, TOperators& operators, const TString& signature) {
    auto pos1 = signature.find('(');
    auto pos2 = signature.find(')');
    Y_ENSURE(pos1 != TString::npos && pos1 > 0);
    Y_ENSURE(pos2 != TString::npos && pos2 > pos1);
    auto name = signature.substr(0, pos1);
    auto operIdsPtr = operatorsByName.FindPtr(name);
    Y_ENSURE(operIdsPtr);
    TVector<TString> strArgs;
    Split(signature.substr(pos1 + 1, pos2 - pos1 - 1), ",", strArgs);
    Y_ENSURE(strArgs.size() >= 1 && strArgs.size() <= 2);
    TVector<ui32> argTypes;
    for (const auto& str : strArgs) {
        auto typePtr = typeByName.FindPtr(str);
        Y_ENSURE(typePtr);
        argTypes.push_back(*typePtr);
    }

    for (const auto& operId : *operIdsPtr) {
        auto operPtr = operators.FindPtr(operId);
        Y_ENSURE(operPtr);
        if (argTypes.size() == 1) {
            if (operPtr->RightType != argTypes[0]) {
                continue;
            }
        } else {
            if (operPtr->LeftType != argTypes[0]) {
                continue;
            }

            if (operPtr->RightType != argTypes[1]) {
                continue;
            }
        }

        return operId;
    }

    // for example, some operators are based on SQL system_functions.sql
    return 0;
}

void ApplyLazyOperInfos(TOperators& operators, const THashMap<TString, TVector<ui32>>& operatorsByName, const THashMap<TString, ui32>& typeByName, const THashMap<ui32, TLazyOperInfo>& lazyInfos) {
    for (const auto& x : lazyInfos) {
        if (!x.second.Com.empty()) {
            operators[x.first].ComId = FindOperator(operatorsByName, typeByName, operators, x.second.Com);
        }

        if (!x.second.Negate.empty()) {
            operators[x.first].NegateId = FindOperator(operatorsByName, typeByName, operators, x.second.Negate);
        }
    }
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

TAms ParseAms(const TString& dat) {
    TAms ret;
    TAmsParser parser(ret);
    parser.Do(dat);
    return ret;
}

TLanguages ParseLanguages(const TString& dat) {
    TLanguages ret;
    TLanguagesParser parser(ret);
    parser.Do(dat);
    return ret;
}

TNamespaces FillNamespaces() {
    const ui32 PgInformationSchemaNamepace = 1;
    const ui32 PgCatalogNamepace = 11;
    const ui32 PgPublicNamepace = 2200;
    return TNamespaces{
        {PgInformationSchemaNamepace, TNamespaceDesc{PgInformationSchemaNamepace, "information_schema", "information_schema namespace"}},
        {PgPublicNamepace, TNamespaceDesc{PgPublicNamepace, "public", "public namespace"}},
        {PgCatalogNamepace, TNamespaceDesc{PgCatalogNamepace, "pg_catalog", "pg_catalog namespace"}},
    };
}

struct TTableInfoKeyRaw {
    const char* Schema;
    const char* Name;
};

struct TTableInfoRaw: public TTableInfoKeyRaw {
    ERelKind Kind;
    ui32 Oid;
};

struct TColumnInfoRaw {
    const char* Schema;
    const char* TableName;
    const char* Name;
    const char* UdtType;
};

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
const TTableInfoRaw AllStaticTablesRaw[] = {
#include "pg_class.generated.h"
};

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
const TColumnInfoRaw AllStaticColumnsRaw[] = {
#include "columns.generated.h"
};

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
const char* AllowedProcsRaw[] = {
#include "safe_procs.h"
#include "used_procs.h"
#include "postgis_procs.h"
};

struct TCatalog: public IExtensionSqlBuilder {
    TCatalog() {
        Init();
    }

    void Clear() {
        State.Clear();
    }

    void Init() {
        Clear();
        State.ConstructInPlace();
        for (size_t i = 0; i < Y_ARRAY_SIZE(AllStaticTablesRaw); ++i) {
            const auto& raw = AllStaticTablesRaw[i];
            State->AllStaticTables.push_back(
                {{TString(raw.Schema), TString(raw.Name)}, raw.Kind, raw.Oid});
        }

        for (size_t i = 0; i < Y_ARRAY_SIZE(AllStaticColumnsRaw); ++i) {
            const auto& raw = AllStaticColumnsRaw[i];
            State->AllStaticColumns.push_back(
                {TString(raw.Schema), TString(raw.TableName), TString(raw.Name), TString(raw.UdtType)});
        }

        if (GetEnv("YDB_EXPERIMENTAL_PG") == "1") {
            // grafana migration_log
            State->AllStaticTables.push_back(
                {{"public", "migration_log"}, ERelKind::Relation, 100001});
            State->AllStaticColumns.push_back(
                {"public", "migration_log", "id", "int"});
            State->AllStaticColumns.push_back(
                {"public", "migration_log", "migration_id", "character varying(255)"});
            State->AllStaticColumns.push_back(
                {"public", "migration_log", "sql", "text"});
            State->AllStaticColumns.push_back(
                {"public", "migration_log", "success", "boolean"});
            State->AllStaticColumns.push_back(
                {"public", "migration_log", "error", "text"});
            State->AllStaticColumns.push_back(
                {"public", "migration_log", "timestamp", "timestamp without time zone"});

            // zabbix config
            State->AllStaticTables.push_back(
                {{"public", "config"}, ERelKind::Relation, 100001});
            State->AllStaticColumns.push_back(
                {"public", "config", "configid", "bigint"});
            State->AllStaticColumns.push_back(
                {"public", "config", "server_check_interval", "integer"});

            State->AllStaticColumns.push_back(
                {"public", "config", "dbversion_status", "text"});

            // zabbix dbversion
            State->AllStaticTables.push_back(
                {{"public", "dbversion"}, ERelKind::Relation, 100002});
            State->AllStaticColumns.push_back(
                {"public", "dbversion", "dbversionid", "bigint"});
            State->AllStaticColumns.push_back(
                {"public", "dbversion", "mandatory", "integer"});
            State->AllStaticColumns.push_back(
                {"public", "dbversion", "mandatory", "optional"});
        }
        THashSet<ui32> usedTableOids;
        for (const auto& t : State->AllStaticTables) {
            State->StaticColumns.insert(std::make_pair(t, TVector<TColumnInfo>()));
            Y_ENSURE(usedTableOids.insert(t.Oid).first);
            State->StaticTables.insert(std::make_pair(TTableInfoKey(t), t));
        }

        for (const auto& c : State->AllStaticColumns) {
            auto tablePtr = State->StaticColumns.FindPtr(TTableInfoKey{c.Schema, c.TableName});
            Y_ENSURE(tablePtr);
            tablePtr->push_back(c);
        }

        for (const auto& t : State->StaticColumns) {
            Y_ENSURE(!t.second.empty());
        }

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
        TString amData;
        Y_ENSURE(NResource::FindExact("pg_am.dat", &amData));
        TString languagesData;
        Y_ENSURE(NResource::FindExact("pg_language.dat", &languagesData));
        THashMap<ui32, TLazyTypeInfo> lazyTypeInfos;
        State->Types = ParseTypes(typeData, lazyTypeInfos);
        for (const auto& [k, v] : State->Types) {
            if (k == v.TypeId) {
                Y_ENSURE(State->TypeByName.insert(std::make_pair(v.Name, k)).second);
            }

            if (k == v.ArrayTypeId) {
                Y_ENSURE(State->TypeByName.insert(std::make_pair("_" + v.Name, k)).second);
            }
        }

        State->Procs = ParseProcs(procData, State->TypeByName);

        for (const auto& [k, v] : State->Procs) {
            State->ProcByName[v.Name].push_back(k);
        }

        const ui32 cstringId = 2275;
        const ui32 byteaId = 17;
        const ui32 internalId = 2281;
        for (const auto& [k, v] : lazyTypeInfos) {
            auto typePtr = State->Types.FindPtr(k);
            Y_ENSURE(typePtr);

            auto inFuncIdPtr = State->ProcByName.FindPtr(v.InFunc);
            Y_ENSURE(inFuncIdPtr);
            Y_ENSURE(inFuncIdPtr->size() == 1);
            auto inFuncPtr = State->Procs.FindPtr(inFuncIdPtr->at(0));
            Y_ENSURE(inFuncPtr);
            Y_ENSURE(inFuncPtr->ArgTypes.size() >= 1); // may have mods
            Y_ENSURE(inFuncPtr->ArgTypes[0] == cstringId);
            typePtr->InFuncId = inFuncIdPtr->at(0);

            auto outFuncIdPtr = State->ProcByName.FindPtr(v.OutFunc);
            Y_ENSURE(outFuncIdPtr);
            Y_ENSURE(outFuncIdPtr->size() == 1);
            auto outFuncPtr = State->Procs.FindPtr(outFuncIdPtr->at(0));
            Y_ENSURE(outFuncPtr);
            Y_ENSURE(outFuncPtr->ArgTypes.size() == 1);
            Y_ENSURE(outFuncPtr->ResultType == cstringId);
            typePtr->OutFuncId = outFuncIdPtr->at(0);

            if (v.ReceiveFunc != "-") {
                auto receiveFuncIdPtr = State->ProcByName.FindPtr(v.ReceiveFunc);
                Y_ENSURE(receiveFuncIdPtr);
                Y_ENSURE(receiveFuncIdPtr->size() == 1);
                auto receiveFuncPtr = State->Procs.FindPtr(receiveFuncIdPtr->at(0));
                Y_ENSURE(receiveFuncPtr);
                Y_ENSURE(receiveFuncPtr->ArgTypes.size() >= 1);
                Y_ENSURE(receiveFuncPtr->ArgTypes[0] == internalId); // mutable StringInfo
                typePtr->ReceiveFuncId = receiveFuncIdPtr->at(0);
            }

            if (v.SendFunc != "-") {
                auto sendFuncIdPtr = State->ProcByName.FindPtr(v.SendFunc);
                Y_ENSURE(sendFuncIdPtr);
                Y_ENSURE(sendFuncIdPtr->size() == 1);
                auto sendFuncPtr = State->Procs.FindPtr(sendFuncIdPtr->at(0));
                Y_ENSURE(sendFuncPtr);
                Y_ENSURE(sendFuncPtr->ArgTypes.size() == 1);
                Y_ENSURE(sendFuncPtr->ResultType == byteaId);
                typePtr->SendFuncId = sendFuncIdPtr->at(0);
            }

            if (v.ModInFunc) {
                auto modInFuncIdPtr = State->ProcByName.FindPtr(v.ModInFunc);
                Y_ENSURE(modInFuncIdPtr);
                Y_ENSURE(modInFuncIdPtr->size() == 1);
                auto modInFuncPtr = State->Procs.FindPtr(modInFuncIdPtr->at(0));
                Y_ENSURE(modInFuncPtr);
                Y_ENSURE(modInFuncPtr->ArgTypes.size() == 1);
                typePtr->TypeModInFuncId = modInFuncIdPtr->at(0);
            }

            if (v.ModOutFunc) {
                auto modOutFuncIdPtr = State->ProcByName.FindPtr(v.ModOutFunc);
                Y_ENSURE(modOutFuncIdPtr);
                Y_ENSURE(modOutFuncIdPtr->size() == 1);
                auto modOutFuncPtr = State->Procs.FindPtr(modOutFuncIdPtr->at(0));
                Y_ENSURE(modOutFuncPtr);
                Y_ENSURE(modOutFuncPtr->ArgTypes.size() == 1);
                typePtr->TypeModOutFuncId = modOutFuncIdPtr->at(0);
            }

            if (v.SubscriptFunc) {
                auto subscriptFuncIdPtr = State->ProcByName.FindPtr(v.SubscriptFunc);
                Y_ENSURE(subscriptFuncIdPtr);
                Y_ENSURE(subscriptFuncIdPtr->size() == 1);
                auto subscriptFuncPtr = State->Procs.FindPtr(subscriptFuncIdPtr->at(0));
                Y_ENSURE(subscriptFuncPtr);
                Y_ENSURE(subscriptFuncPtr->ArgTypes.size() == 1);
                typePtr->TypeSubscriptFuncId = subscriptFuncIdPtr->at(0);
            }

            if (v.ElementType) {
                auto elemTypePtr = State->TypeByName.FindPtr(v.ElementType);
                Y_ENSURE(elemTypePtr);

                typePtr->ElementTypeId = *elemTypePtr;
            }
        }

        State->Casts = ParseCasts(castData, State->TypeByName, State->Types, State->ProcByName, State->Procs);
        for (const auto& [k, v] : State->Casts) {
            Y_ENSURE(State->CastsByDir.insert(std::make_pair(std::make_pair(v.SourceId, v.TargetId), k)).second);
        }

        THashMap<ui32, TLazyOperInfo> lazyOperInfos;
        State->Operators = ParseOperators(opData, State->TypeByName, State->Types, State->ProcByName, State->Procs, lazyOperInfos);
        for (const auto& [k, v] : State->Operators) {
            State->OperatorsByName[v.Name].push_back(k);
        }

        ApplyLazyOperInfos(State->Operators, State->OperatorsByName, State->TypeByName, lazyOperInfos);
        State->Aggregations = ParseAggregations(aggData, State->TypeByName, State->Types, State->ProcByName, State->Procs);
        for (const auto& [k, v] : State->Aggregations) {
            State->AggregationsByName[v.Name].push_back(k);
        }

        State->OpFamilies = ParseOpFamilies(opFamiliesData);
        State->OpClasses = ParseOpClasses(opClassData, State->TypeByName, State->OpFamilies);
        State->AmOps = ParseAmOps(amOpData, State->TypeByName, State->Types, State->OperatorsByName, State->Operators, State->OpFamilies);
        State->AmProcs = ParseAmProcs(amProcData, State->TypeByName, State->ProcByName, State->Procs, State->OpFamilies);
        State->Ams = ParseAms(amData);
        State->Namespaces = FillNamespaces();
        for (auto& [k, v] : State->Types) {
            if (v.TypeId != v.ArrayTypeId) {
                auto lookupId = (v.TypeId == VarcharOid ? TextOid : v.TypeId);
                if (regClasses.contains(lookupId)) {
                    lookupId = OidOid;
                }

                CacheAmFuncs(lookupId, v);
            }
        }

        State->Conversions = ParseConversions(conversionData, State->ProcByName);
        State->Languages = ParseLanguages(languagesData);

        if (GetEnv("YQL_ALLOW_ALL_PG_FUNCTIONS")) {
            State->AllowAllFunctions = true;
        } else if (auto exportDir = GetEnv("YQL_EXPORT_PG_FUNCTIONS_DIR")) {
            State->AllowAllFunctions = true;
            ExportFile.ConstructInPlace(MakeTempName(exportDir.c_str(), "procs"), CreateAlways | RdWr);
            for (const auto& a : State->Aggregations) {
                const auto& desc = a.second;
                ExportFunction(desc.TransFuncId);
                ExportFunction(desc.FinalFuncId);
                ExportFunction(desc.CombineFuncId);
                ExportFunction(desc.SerializeFuncId);
                ExportFunction(desc.DeserializeFuncId);
            }

            for (const auto& t : State->Types) {
                const auto& desc = t.second;
                ExportFunction(desc.InFuncId);
                ExportFunction(desc.OutFuncId);
                ExportFunction(desc.SendFuncId);
                ExportFunction(desc.ReceiveFuncId);
                ExportFunction(desc.TypeModInFuncId);
                ExportFunction(desc.TypeModOutFuncId);
                ExportFunction(desc.TypeSubscriptFuncId);
                ExportFunction(desc.LessProcId);
                ExportFunction(desc.EqualProcId);
                ExportFunction(desc.CompareProcId);
                ExportFunction(desc.HashProcId);
            }

            for (const auto& o : State->Operators) {
                const auto& desc = o.second;
                ExportFunction(desc.ProcId);
            }

            for (const auto& c : State->Casts) {
                const auto& desc = c.second;
                ExportFunction(desc.FunctionId);
            }
        } else {
            for (size_t i = 0; i < Y_ARRAY_SIZE(AllowedProcsRaw); ++i) {
                const auto& raw = AllowedProcsRaw[i];
                State->AllowedProcs.insert(raw);
            }

            for (const auto& t : State->Types) {
                State->AllowedProcs.insert(t.second.Name);
            }
        }
    }

    void ExportFunction(ui32 procOid) const {
        if (!procOid || !ExportFile) {
            return;
        }

        auto procPtr = State->Procs.FindPtr(procOid);
        Y_ENSURE(procPtr);
        ExportFunction(procPtr->Name);
    }

    void ExportFunction(const TString& name) const {
        if (!ExportFile) {
            return;
        }

        TString line = TStringBuilder() << "\"" << name << "\",\n";
        with_lock (ExportGuard) {
            ExportFile->Write(line.data(), line.size());
        }
    }

    void CacheAmFuncs(ui32 typeId, TTypeDesc& v) {
        auto btreeOpClassPtr = State->OpClasses.FindPtr(std::make_pair(EOpClassMethod::Btree, typeId));
        if (btreeOpClassPtr) {
            auto lessAmOpPtr = State->AmOps.FindPtr(std::make_tuple(btreeOpClassPtr->FamilyId, ui32(EBtreeAmStrategy::Less), typeId, typeId));
            Y_ENSURE(lessAmOpPtr);
            auto equalAmOpPtr = State->AmOps.FindPtr(std::make_tuple(btreeOpClassPtr->FamilyId, ui32(EBtreeAmStrategy::Equal), typeId, typeId));
            Y_ENSURE(equalAmOpPtr);
            auto lessOperPtr = State->Operators.FindPtr(lessAmOpPtr->OperId);
            Y_ENSURE(lessOperPtr);
            auto equalOperPtr = State->Operators.FindPtr(equalAmOpPtr->OperId);
            Y_ENSURE(equalOperPtr);
            v.LessProcId = lessOperPtr->ProcId;
            v.EqualProcId = equalOperPtr->ProcId;

            auto compareAmProcPtr = State->AmProcs.FindPtr(std::make_tuple(btreeOpClassPtr->FamilyId, ui32(EBtreeAmProcNum::Compare), typeId, typeId));
            Y_ENSURE(compareAmProcPtr);
            v.CompareProcId = compareAmProcPtr->ProcId;
        }

        auto hashOpClassPtr = State->OpClasses.FindPtr(std::make_pair(EOpClassMethod::Hash, typeId));
        if (hashOpClassPtr) {
            auto hashAmProcPtr = State->AmProcs.FindPtr(std::make_tuple(hashOpClassPtr->FamilyId, ui32(EHashAmProcNum::Hash), typeId, typeId));
            Y_ENSURE(hashAmProcPtr);
            v.HashProcId = hashAmProcPtr->ProcId;
        }
    }

    void CreateProc(const TProcDesc& desc) final {
        Y_ENSURE(desc.ExtensionIndex);
        TProcDesc newDesc = desc;
        newDesc.Name = to_lower(newDesc.Name);
        newDesc.ProcId = 16000 + State->Procs.size();
        State->Procs[newDesc.ProcId] = newDesc;
        State->ProcByName[newDesc.Name].push_back(newDesc.ProcId);
    }

    void PrepareType(ui32 extensionIndex, const TString& name) final {
        Y_ENSURE(extensionIndex);
        auto lowerName = to_lower(name);
        if (auto idPtr = State->TypeByName.FindPtr(lowerName)) {
            auto typePtr = State->Types.FindPtr(*idPtr);
            Y_ENSURE(typePtr);
            Y_ENSURE(!typePtr->ExtensionIndex || typePtr->ExtensionIndex == extensionIndex);
            return;
        }

        TTypeDesc newDesc;
        newDesc.Name = lowerName;
        newDesc.TypeId = 16000 + State->Types.size();
        newDesc.ExtensionIndex = extensionIndex;
        newDesc.ArrayTypeId = newDesc.TypeId + 1;
        newDesc.Category = 'U';
        State->Types[newDesc.TypeId] = newDesc;
        State->TypeByName[newDesc.Name] = newDesc.TypeId;
        TTypeDesc newArrayDesc = newDesc;
        newArrayDesc.TypeId += 1;
        newArrayDesc.Name = "_" + newArrayDesc.Name;
        newArrayDesc.ElementTypeId = newDesc.TypeId;
        newArrayDesc.ArrayTypeId = newArrayDesc.TypeId;
        newArrayDesc.PassByValue = false;
        newArrayDesc.TypeLen = -1;
        newArrayDesc.SendFuncId = (*State->ProcByName.FindPtr("array_send"))[0];
        newArrayDesc.ReceiveFuncId = (*State->ProcByName.FindPtr("array_recv"))[0];
        newArrayDesc.InFuncId = (*State->ProcByName.FindPtr("array_in"))[0];
        newArrayDesc.OutFuncId = (*State->ProcByName.FindPtr("array_out"))[0];
        newArrayDesc.Category = 'A';
        State->Types[newArrayDesc.TypeId] = newArrayDesc;
        State->TypeByName[newArrayDesc.Name] = newArrayDesc.TypeId;
    }

    void UpdateType(const TTypeDesc& desc) final {
        Y_ENSURE(desc.ExtensionIndex);
        auto byIdPtr = State->Types.FindPtr(desc.TypeId);
        Y_ENSURE(byIdPtr);
        Y_ENSURE(byIdPtr->Name == desc.Name);
        Y_ENSURE(byIdPtr->ArrayTypeId == desc.ArrayTypeId);
        Y_ENSURE(byIdPtr->TypeId == desc.TypeId);
        Y_ENSURE(byIdPtr->ExtensionIndex == desc.ExtensionIndex);
        if (desc.InFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.InFuncId)->Name);
        }

        if (desc.OutFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.OutFuncId)->Name);
        }

        if (desc.SendFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.SendFuncId)->Name);
        }

        if (desc.ReceiveFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.ReceiveFuncId)->Name);
        }

        if (desc.TypeModInFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.TypeModInFuncId)->Name);
        }

        if (desc.TypeModOutFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.TypeModOutFuncId)->Name);
        }

        *byIdPtr = desc;
    }

    void CreateTable(const TTableInfo& table, const TVector<TColumnInfo>& columns) final {
        Y_ENSURE(table.ExtensionIndex);
        Y_ENSURE(!columns.empty());
        THashSet<TString> usedColumns;
        for (const auto& c : columns) {
            Y_ENSURE(c.Schema == table.Schema);
            Y_ENSURE(c.TableName == table.Name);
            Y_ENSURE(c.ExtensionIndex == table.ExtensionIndex);
            Y_ENSURE(usedColumns.insert(c.Name).second);
        }

        TTableInfoKey key{table};
        TTableInfo value = table;
        value.Oid = 16000 + State->StaticTables.size();
        Y_ENSURE(State->StaticTables.emplace(key, value).second);
        Y_ENSURE(State->StaticColumns.emplace(key, columns).second);

        State->AllStaticTables.push_back(value);
        for (const auto& c : columns) {
            State->AllStaticColumns.push_back(c);
        }
    }

    void InsertValues(const TTableInfoKey& table, const TVector<TString>& columns,
                      const TVector<TMaybe<TString>>& data) final {
        Y_ENSURE(State->StaticTables.contains(table));
        const auto& columnDefs = *State->StaticColumns.FindPtr(table);
        Y_ENSURE(columnDefs.size() == columns.size());
        Y_ENSURE(data.size() % columns.size() == 0);
        THashMap<TString, ui32> columnToIndex;
        for (ui32 i = 0; i < columnDefs.size(); ++i) {
            columnToIndex[columnDefs[i].Name] = i;
        }

        THashSet<TString> usedColumns;
        TVector<ui32> dataColumnRemap;
        for (const auto& c : columns) {
            Y_ENSURE(usedColumns.insert(c).second);
            dataColumnRemap.push_back(*columnToIndex.FindPtr(c));
        }

        auto& tableData = State->StaticTablesData[table];
        size_t writePos = tableData.size();
        tableData.resize(tableData.size() + data.size());
        size_t readRowPos = 0;
        while (writePos < tableData.size()) {
            for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
                tableData[writePos++] = data[readRowPos + dataColumnRemap[colIdx]];
            }

            readRowPos += columns.size();
        }

        Y_ENSURE(readRowPos == data.size());
    }

    void CreateCast(const TCastDesc& desc) final {
        Y_ENSURE(desc.ExtensionIndex);
        auto id = 1 + State->Casts.size();
        State->Casts[id] = desc;
        Y_ENSURE(State->CastsByDir.insert(std::make_pair(std::make_pair(desc.SourceId, desc.TargetId), id)).second);
        if (desc.FunctionId) {
            auto funcPtr = State->Procs.FindPtr(desc.FunctionId);
            Y_ENSURE(funcPtr);
            State->AllowedProcs.insert(funcPtr->Name);
        }
    }

    void PrepareOper(ui32 extensionIndex, const TString& name, const TVector<ui32>& args) final {
        Y_ENSURE(args.size() >= 1 && args.size() <= 2);
        Y_ENSURE(extensionIndex);
        auto lowerName = to_lower(name);
        auto operIdPtr = State->OperatorsByName.FindPtr(lowerName);
        if (operIdPtr) {
            for (const auto& id : *operIdPtr) {
                const auto& d = State->Operators.FindPtr(id);
                Y_ENSURE(d);
                if (d->LeftType == args[0] && (args.size() == 1 || d->RightType == args[1])) {
                    Y_ENSURE(!d->ExtensionIndex || d->ExtensionIndex == extensionIndex);
                    return;
                }
            }
        }

        if (!operIdPtr) {
            operIdPtr = &State->OperatorsByName[lowerName];
        }

        TOperDesc desc;
        desc.Name = name;
        desc.LeftType = args[0];
        if (args.size() == 1) {
            desc.Kind = EOperKind::LeftUnary;
        } else {
            desc.RightType = args[1];
        }

        auto id = 16000 + State->Operators.size();
        desc.OperId = id;
        desc.ExtensionIndex = extensionIndex;
        Y_ENSURE(State->Operators.emplace(id, desc).second);
        operIdPtr->push_back(id);
    }

    void UpdateOper(const TOperDesc& desc) final {
        Y_ENSURE(desc.ExtensionIndex);
        const auto& d = State->Operators.FindPtr(desc.OperId);
        Y_ENSURE(d);
        Y_ENSURE(d->Name == desc.Name);
        Y_ENSURE(d->ExtensionIndex == desc.ExtensionIndex);
        Y_ENSURE(d->LeftType == desc.LeftType);
        Y_ENSURE(d->RightType == desc.RightType);
        Y_ENSURE(d->Kind == desc.Kind);
        d->ProcId = desc.ProcId;
        d->ComId = desc.ComId;
        d->NegateId = desc.NegateId;
        d->ResultType = desc.ResultType;
        auto procPtr = State->Procs.FindPtr(desc.ProcId);
        Y_ENSURE(procPtr);
        State->AllowedProcs.insert(procPtr->Name);
    }

    void CreateAggregate(const TAggregateDesc& desc) final {
        Y_ENSURE(desc.ExtensionIndex);
        auto id = 16000 + State->Aggregations.size();
        auto newDesc = desc;
        newDesc.Name = to_lower(newDesc.Name);
        newDesc.AggId = id;
        Y_ENSURE(State->Aggregations.emplace(id, newDesc).second);
        State->AggregationsByName[newDesc.Name].push_back(id);
        if (desc.CombineFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.CombineFuncId)->Name);
        }

        if (desc.DeserializeFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.DeserializeFuncId)->Name);
        }

        if (desc.SerializeFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.SerializeFuncId)->Name);
        }

        if (desc.FinalFuncId) {
            State->AllowedProcs.insert(State->Procs.FindPtr(desc.FinalFuncId)->Name);
        }

        State->AllowedProcs.insert(State->Procs.FindPtr(desc.TransFuncId)->Name);
    }

    void CreateOpClass(const TOpClassDesc& opclass, const TVector<TAmOpDesc>& ops, const TVector<TAmProcDesc>& procs) final {
        Y_ENSURE(opclass.ExtensionIndex);
        auto newDesc = opclass;
        newDesc.Family = to_lower(newDesc.Family);
        newDesc.Name = to_lower(newDesc.Name);
        auto newFamilyId = 16000 + State->OpFamilies.size();
        TOpFamilyDesc opFamilyDesc;
        opFamilyDesc.FamilyId = newFamilyId;
        opFamilyDesc.Name = newDesc.Family;
        opFamilyDesc.ExtensionIndex = opclass.ExtensionIndex;
        Y_ENSURE(State->OpFamilies.emplace(newDesc.Family, opFamilyDesc).second);
        newDesc.FamilyId = newFamilyId;
        const auto key = std::make_pair(newDesc.Method, newDesc.TypeId);
        Y_ENSURE(State->OpClasses.emplace(key, newDesc).second);
        for (const auto& o : ops) {
            Y_ENSURE(opclass.ExtensionIndex == o.ExtensionIndex);
            Y_ENSURE(opclass.Family == o.Family);
            auto newOpDesc = o;
            newOpDesc.FamilyId = newFamilyId;
            newOpDesc.Family = newDesc.Name;
            Y_ENSURE(State->AmOps.emplace(std::make_tuple(newFamilyId, o.Strategy, o.LeftType, o.RightType), newOpDesc).second);
            auto operPtr = State->Operators.FindPtr(o.OperId);
            Y_ENSURE(operPtr);
            auto procPtr = State->Procs.FindPtr(operPtr->ProcId);
            Y_ENSURE(procPtr);
            State->AllowedProcs.emplace(procPtr->Name);
        }

        for (const auto& p : procs) {
            Y_ENSURE(opclass.ExtensionIndex == p.ExtensionIndex);
            Y_ENSURE(opclass.Family == p.Family);
            auto newProcDesc = p;
            newProcDesc.FamilyId = newFamilyId;
            newProcDesc.Family = newDesc.Name;
            Y_ENSURE(State->AmProcs.emplace(std::make_tuple(newFamilyId, p.ProcNum, p.LeftType, p.RightType), newProcDesc).second);
            auto procPtr = State->Procs.FindPtr(p.ProcId);
            Y_ENSURE(procPtr);
            State->AllowedProcs.emplace(procPtr->Name);
        }

        auto typePtr = State->Types.FindPtr(opclass.TypeId);
        Y_ENSURE(typePtr);
        Y_ENSURE(typePtr->ExtensionIndex == opclass.ExtensionIndex);
        CacheAmFuncs(opclass.TypeId, *typePtr);
    }

    static const TCatalog& Instance() {
        return *Singleton<TCatalog>();
    }

    static TCatalog& MutableInstance() {
        return *Singleton<TCatalog>();
    }

    struct TState {
        TExtensionsByName ExtensionsByName, ExtensionsByInstallName;
        TExtensions Extensions;

        TOperators Operators;
        TProcs Procs;
        TTypes Types;
        TCasts Casts;
        TAggregations Aggregations;
        TAms Ams;
        TNamespaces Namespaces;
        TOpFamilies OpFamilies;
        TOpClasses OpClasses;
        TAmOps AmOps;
        TAmProcs AmProcs;
        TConversions Conversions;
        TLanguages Languages;
        THashMap<TString, TVector<ui32>> ProcByName;
        THashMap<TString, ui32> TypeByName;
        THashMap<std::pair<ui32, ui32>, ui32> CastsByDir;
        THashMap<TString, TVector<ui32>> OperatorsByName;
        THashMap<TString, TVector<ui32>> AggregationsByName;

        TVector<TTableInfo> AllStaticTables;
        TVector<TColumnInfo> AllStaticColumns;
        THashMap<TTableInfoKey, TTableInfo> StaticTables;
        THashMap<TTableInfoKey, TVector<TColumnInfo>> StaticColumns;
        THashMap<TTableInfoKey, TVector<TMaybe<TString>>> StaticTablesData;

        bool AllowAllFunctions = false;
        THashSet<TString> AllowedProcs;

        bool SystemFunctionInit = false;
    };

    mutable TMaybe<TFile> ExportFile;
    TMutex ExportGuard;
    TMaybe<TState> State;

    TMutex ExtensionsGuard;
    bool ExtensionsInit = false;
};

bool ValidateProcArgs(const TProcDesc& d, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    return ValidateArgs(d.ArgTypes, argTypeIds, catalog.State->Types, d.VariadicType);
}

const TProcDesc& LookupProc(ui32 procId, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto procPtr = catalog.State->Procs.FindPtr(procId);
    if (!procPtr) {
        throw yexception() << "No such proc: " << procId;
    }

    if (!catalog.State->AllowAllFunctions && !catalog.State->AllowedProcs.contains(procPtr->Name)) {
        throw yexception() << "No access to proc: " << procPtr->Name;
    }

    if (!ValidateProcArgs(*procPtr, argTypeIds)) {
        throw yexception() << "Unable to find an overload for proc with oid " << procId << " with given argument types: " << ArgTypesList(argTypeIds);
    }

    catalog.ExportFunction(procId);
    return *procPtr;
}

const TProcDesc& LookupProc(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto lower = to_lower(name);
    auto procIdPtr = catalog.State->ProcByName.FindPtr(lower);
    if (!procIdPtr) {
        throw yexception() << "No such proc: " << name;
    }

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.State->Procs.FindPtr(id);
        Y_ENSURE(d);
        if (!catalog.State->AllowAllFunctions && !catalog.State->AllowedProcs.contains(d->Name)) {
            throw yexception() << "No access to proc: " << d->Name;
        }

        if (!ValidateProcArgs(*d, argTypeIds)) {
            continue;
        }

        catalog.ExportFunction(d->Name);
        return *d;
    }

    throw yexception() << "Unable to find an overload for proc " << name << " with given argument types: "
                       << ArgTypesList(argTypeIds);
}

const TProcDesc& LookupProc(ui32 procId) {
    const auto& catalog = TCatalog::Instance();
    auto procPtr = catalog.State->Procs.FindPtr(procId);
    if (!procPtr) {
        throw yexception() << "No such proc: " << procId;
    }

    if (!catalog.State->AllowAllFunctions && !catalog.State->AllowedProcs.contains(procPtr->Name)) {
        throw yexception() << "No access to proc: " << procPtr->Name;
    }

    catalog.ExportFunction(procId);
    return *procPtr;
}

void EnumProc(std::function<void(ui32, const TProcDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& x : catalog.State->Procs) {
        if (catalog.State->AllowAllFunctions || catalog.State->AllowedProcs.contains(x.second.Name)) {
            f(x.first, x.second);
        }
    }
}

bool HasProc(const TString& name, EProcKind kind) {
    const auto& catalog = TCatalog::Instance();
    auto procIdPtr = catalog.State->ProcByName.FindPtr(to_lower(name));
    if (!procIdPtr) {
        return false;
    }

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.State->Procs.FindPtr(id);
        Y_ENSURE(d);
        if (d->Kind == kind) {
            return true;
        }
    }

    return false;
}

bool HasReturnSetProc(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    auto procIdPtr = catalog.State->ProcByName.FindPtr(to_lower(name));
    if (!procIdPtr) {
        return false;
    }

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.State->Procs.FindPtr(id);
        Y_ENSURE(d);
        if (d->ReturnSet) {
            return true;
        }
    }

    return false;
}

bool HasType(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->TypeByName.contains(GetCanonicalTypeName(to_lower(name)));
}

const TTypeDesc& LookupType(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    const auto typeIdPtr = catalog.State->TypeByName.FindPtr(GetCanonicalTypeName(to_lower(name)));
    if (!typeIdPtr) {
        throw yexception() << "No such type: " << name;
    }

    const auto typePtr = catalog.State->Types.FindPtr(*typeIdPtr);
    Y_ENSURE(typePtr);
    return *typePtr;
}

bool HasType(ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->Types.contains(typeId);
}

const TTypeDesc& LookupType(ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    const auto typePtr = catalog.State->Types.FindPtr(typeId);
    if (!typePtr) {
        throw yexception() << "No such type: " << typeId;
    }

    return *typePtr;
}

void EnumTypes(std::function<void(ui32, const TTypeDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& [typeId, desc] : catalog.State->Types) {
        f(typeId, desc);
    }
}

const TAmDesc& LookupAm(ui32 oid) {
    const auto& catalog = TCatalog::Instance();
    const auto typePtr = catalog.State->Ams.FindPtr(oid);
    if (!typePtr) {
        throw yexception() << "No such am: " << oid;
    }

    return *typePtr;
}

void EnumAm(std::function<void(ui32, const TAmDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& [oid, desc] : catalog.State->Ams) {
        f(oid, desc);
    }
}

void EnumConversions(std::function<void(const TConversionDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& [_, desc] : catalog.State->Conversions) {
        f(desc);
    }
}

const TNamespaceDesc& LookupNamespace(ui32 oid) {
    const auto& catalog = TCatalog::Instance();
    const auto typePtr = catalog.State->Namespaces.FindPtr(oid);
    if (!typePtr) {
        throw yexception() << "No such namespace: " << oid;
    }

    return *typePtr;
}

void EnumNamespace(std::function<void(ui32, const TNamespaceDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& [oid, desc] : catalog.State->Namespaces) {
        f(oid, desc);
    }
}

void EnumOperators(std::function<void(const TOperDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& [_, desc] : catalog.State->Operators) {
        f(desc);
    }
}

bool HasCast(ui32 sourceId, ui32 targetId) {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->CastsByDir.contains(std::make_pair(sourceId, targetId));
}

const TCastDesc& LookupCast(ui32 sourceId, ui32 targetId) {
    const auto& catalog = TCatalog::Instance();
    auto castByDirPtr = catalog.State->CastsByDir.FindPtr(std::make_pair(sourceId, targetId));
    if (!castByDirPtr) {
        throw yexception() << "No such cast";
    }

    auto castPtr = catalog.State->Casts.FindPtr(*castByDirPtr);
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

enum class ECoercionSearchResult {
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

    const auto* castId = catalog.State->CastsByDir.FindPtr(std::make_pair(fromTypeId, toTypeId));
    if (castId != nullptr) {
        const auto* castPtr = catalog.State->Casts.FindPtr(*castId);
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
        const auto* toTypePtr = catalog.State->Types.FindPtr(toTypeId);
        Y_ENSURE(toTypePtr);

        const auto* fromTypePtr = catalog.State->Types.FindPtr(fromTypeId);
        Y_ENSURE(fromTypePtr);

        if (IsArrayType(*toTypePtr) && IsArrayType(*fromTypePtr)) {
            if (FindCoercionPath(fromTypePtr->ElementTypeId, toTypePtr->ElementTypeId, coercionType, catalog) != ECoercionSearchResult::None) {
                return ECoercionSearchResult::ArrayCoerce;
            }
        }
    }

    if (coercionType == ECoercionCode::Assignment || coercionType == ECoercionCode::Explicit) {
        const auto* toTypePtr = catalog.State->Types.FindPtr(toTypeId);
        Y_ENSURE(toTypePtr);

        if (toTypePtr->Category == 'S') {
            return ECoercionSearchResult::IOCoerce;
        }

        if (coercionType == ECoercionCode::Explicit) {
            const auto* fromTypePtr = catalog.State->Types.FindPtr(fromTypeId);
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
    // TODO: support polymorphic types

    if (fromTypeId == UnknownOid) {
        return true;
    }

    if (FindCoercionPath(fromTypeId, toTypeId, coercionType, catalog) != ECoercionSearchResult::None) {
        return true;
    }

    // TODO: support record & complex types

    // TODO: support record array

    // TODO: support inheritance

    if (toTypeId == AnyArrayOid) {
        const auto& actualDescPtr = catalog.State->Types.FindPtr(fromTypeId);
        Y_ENSURE(actualDescPtr);
        return actualDescPtr->ArrayTypeId == actualDescPtr->TypeId;
    }

    if (toTypeId == AnyNonArrayOid) {
        const auto& actualDescPtr = catalog.State->Types.FindPtr(fromTypeId);
        Y_ENSURE(actualDescPtr);
        return actualDescPtr->ArrayTypeId != actualDescPtr->TypeId;
    }

    return false;
}

bool IsPreferredType(char categoryId, const TTypeDesc& type) {
    Y_ENSURE(type.Category != InvalidCategory);

    return (categoryId == type.Category) ? type.IsPreferred : false;
}

constexpr ui32 CoercibleMatchShift = 16;
constexpr ui64 ArgExactTypeMatch = 1ULL << 2 * CoercibleMatchShift;
constexpr ui64 ArgPreferredTypeMatch = 1ULL << CoercibleMatchShift;
constexpr ui64 ArgCoercibleTypeMatch = 1ULL;
constexpr ui64 ArgTypeMismatch = 0;

ui64 CalcArgumentMatchScore(ui32 operArgTypeId, ui32 argTypeId, const TCatalog& catalog) {
    Y_ENSURE(operArgTypeId != UnknownOid);

    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 2
    if (argTypeId == operArgTypeId) {
        return ArgExactTypeMatch;
    }
    if (argTypeId == UnknownOid || argTypeId == InvalidOid) {
        return ArgCoercibleTypeMatch;
    }
    // https://www.postgresql.org/docs/14/typeconv-oper.html, step 3.c
    if (IsCoercible(argTypeId, operArgTypeId, ECoercionCode::Implicit, catalog)) {
        // https://www.postgresql.org/docs/14/typeconv-oper.html, step 3.d
        const auto& argType = catalog.State->Types.FindPtr(argTypeId);
        Y_ENSURE(argType);
        const auto& operArgType = catalog.State->Types.FindPtr(operArgTypeId);
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
    } else if (rightArgTypeId == UnknownOid && leftArgTypeId != InvalidOid) {
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

bool IsExactMatch(const TVector<ui32>& procArgTypes, ui32 procVariadicType, const TVector<ui32>& argTypeIds) {
    if (argTypeIds.size() < procArgTypes.size()) {
        return false;
    }

    if (!procVariadicType && argTypeIds.size() > procArgTypes.size()) {
        return false;
    }

    for (ui32 i = 0; i < procArgTypes.size(); ++i) {
        if (procArgTypes[i] != argTypeIds[i]) {
            return false;
        }
    }

    if (procVariadicType) {
        if (argTypeIds.size() == procArgTypes.size()) {
            return false;
        }

        for (ui32 i = procArgTypes.size(); i < argTypeIds.size(); ++i) {
            if (procVariadicType != argTypeIds[i]) {
                return false;
            }
        }
    }

    return true;
}

ui64 CalcProcScore(const TVector<ui32>& procArgTypes, ui32 procVariadicType, ui32 procDefArgs, const TVector<ui32>& argTypeIds, const TCatalog& catalog) {
    ui64 result = 0UL;
    if (!procVariadicType) {
        ++result;
    }

    Y_ENSURE(procArgTypes.size() >= procDefArgs);
    if (argTypeIds.size() < procArgTypes.size() - procDefArgs) {
        return ArgTypeMismatch;
    }

    if (!procVariadicType && argTypeIds.size() > procArgTypes.size()) {
        return ArgTypeMismatch;
    }

    if (procVariadicType && argTypeIds.size() == procArgTypes.size()) {
        return ArgTypeMismatch;
    }

    for (size_t i = 0; i < argTypeIds.size(); ++i) {
        auto score = CalcArgumentMatchScore(i >= procArgTypes.size() ? procVariadicType : procArgTypes[i], argTypeIds[i], catalog);

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
        : Position(position)
        , Category(category)
        , IsPreferred(isPreferred)
    {
    }
};

template <class C>
char FindCommonCategory(const TVector<const C*>& candidates, std::function<ui32(const C*)> getTypeId, const TCatalog& catalog, bool& isPreferred) {
    char category = InvalidCategory;
    auto isConflict = false;
    isPreferred = false;

    for (const auto* candidate : candidates) {
        const auto argTypeId = getTypeId(candidate);
        const auto& argTypePtr = catalog.State->Types.FindPtr(argTypeId);
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

        std::function<ui32(const C*)> typeGetter = [i](const auto* candidate) {
            if constexpr (std::is_same_v<C, TProcDesc>) {
                return i < candidate->ArgTypes.size() ? candidate->ArgTypes[i] : candidate->VariadicType;
            } else {
                return candidate->ArgTypes[i];
            }
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
            ui32 argTypeId;
            if constexpr (std::is_same_v<C, TProcDesc>) {
                argTypeId = category.Position < candidate->ArgTypes.size() ? candidate->ArgTypes[category.Position] : candidate->VariadicType;
            } else {
                argTypeId = candidate->ArgTypes[category.Position];
            }

            const auto& argTypePtr = catalog.State->Types.FindPtr(argTypeId);
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

        std::function<ui32(const TOperDesc*)> typeGetter;
        if (i == 1) {
            typeGetter = [](const auto* candidate) {
                return candidate->RightType;
            };
        } else {
            typeGetter = [](const auto* candidate) {
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
                                   : (candidate->Kind == EOperKind::Binary) ? candidate->LeftType
                                                                            : candidate->RightType;

            const auto& argTypePtr = catalog.State->Types.FindPtr(argTypeId);
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
    const auto* castId = catalog.State->CastsByDir.FindPtr(std::make_pair(fromTypeId, toTypeId));
    if (!castId) {
        return false;
    }
    const auto* castPtr = catalog.State->Casts.FindPtr(*castId);
    Y_ENSURE(castPtr);

    return (castPtr->CoercionCode == ECoercionCode::Implicit);
}

} // namespace NPrivate

bool IsCoercible(ui32 fromTypeId, ui32 toTypeId, ECoercionCode coercionType) {
    const auto& catalog = TCatalog::Instance();

    return NPrivate::IsCoercible(fromTypeId, toTypeId, coercionType, catalog);
}

std::variant<const TProcDesc*, const TTypeDesc*> LookupProcWithCasts(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto lower = to_lower(name);
    auto procIdPtr = catalog.State->ProcByName.FindPtr(lower);
    if (!procIdPtr) {
        throw yexception() << "No such proc: " << name;
    }

    auto bestScore = NPrivate::NoFitScore;
    TVector<const TProcDesc*> candidates;

    for (const auto& id : *procIdPtr) {
        const auto& d = catalog.State->Procs.FindPtr(id);
        Y_ENSURE(d);

        if (!catalog.State->AllowAllFunctions && !catalog.State->AllowedProcs.contains(d->Name)) {
            throw yexception() << "No access to proc: " << d->Name;
        }

        if (NPrivate::IsExactMatch(d->ArgTypes, d->VariadicType, argTypeIds)) {
            // At most one exact match is possible, so look no further
            // https://www.postgresql.org/docs/14/typeconv-func.html, step 2
            catalog.ExportFunction(d->Name);
            return d;
        }

        // https://www.postgresql.org/docs/14/typeconv-func.html, steps 4.a, 4.c, 4.d
        auto score = NPrivate::CalcProcScore(d->ArgTypes, d->VariadicType, d->DefaultArgs.size(), argTypeIds, catalog);
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
        const auto typeIdPtr = catalog.State->TypeByName.FindPtr(to_lower(name));

        if (typeIdPtr) {
            const auto typePtr = catalog.State->Types.FindPtr(*typeIdPtr);
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
            catalog.ExportFunction(candidates[0]->Name);
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
        catalog.ExportFunction(candidates[0]->Name);
        return candidates[0];
    }

    // https://www.postgresql.org/docs/14/typeconv-func.html, step 4.f
    if (unknownsCount < argTypeIds.size()) {
        ui32 commonType = UnknownOid;

        for (const auto argType : argTypeIds) {
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
                    if (NPrivate::IsCoercible(commonType, i >= candidate->ArgTypes.size() ? candidate->VariadicType : candidate->ArgTypes[i], ECoercionCode::Implicit, catalog)) {
                        if (finalCandidate) {
                            NPrivate::ThrowProcAmbiguity(name, argTypeIds);
                        }
                        finalCandidate = candidate;
                    }
                }
            }

            if (finalCandidate) {
                catalog.ExportFunction(finalCandidate->Name);
                return finalCandidate;
            }
        }
    }
    NPrivate::ThrowProcNotFound(name, argTypeIds);
}

TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>& GetPosition, const TTypeDesc*& typeDesc, bool& castsNeeded) {
    Y_ENSURE(0 != typeIds.size());

    const auto& catalog = TCatalog::Instance();

    size_t unknownsCnt = (typeIds[0] == UnknownOid || typeIds[0] == InvalidOid) ? 1 : 0;
    castsNeeded = (unknownsCnt != 0);
    const TTypeDesc* commonType = nullptr;
    char commonCategory = 0;
    if (typeIds[0] != InvalidOid) {
        commonType = &LookupType(typeIds[0]);
        commonCategory = commonType->Category;
    }

    size_t i = 1;
    for (auto typeId = typeIds.cbegin() + 1; typeId != typeIds.cend(); ++typeId, ++i) {
        if (*typeId == UnknownOid || *typeId == InvalidOid) {
            ++unknownsCnt;
            castsNeeded = true;
            continue;
        }
        if (commonType && *typeId == commonType->TypeId) {
            continue;
        }
        const TTypeDesc& otherType = LookupType(*typeId);
        if (!commonType || commonType->TypeId == UnknownOid) {
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

TMaybe<TIssue> LookupCommonType(const TVector<ui32>& typeIds, const std::function<TPosition(size_t i)>& GetPosition, const TTypeDesc*& typeDesc) {
    bool _;
    return LookupCommonType(typeIds, GetPosition, typeDesc, _);
}

const TOperDesc& LookupOper(const TString& name, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();

    auto operIdPtr = catalog.State->OperatorsByName.FindPtr(to_lower(name));
    if (!operIdPtr) {
        throw yexception() << "No such operator: " << name;
    }

    EOperKind expectedOpKind;
    std::function<ui64(const TOperDesc*)> calcScore;

    switch (argTypeIds.size()) {
        case 2:
            expectedOpKind = EOperKind::Binary;
            calcScore = [&](const auto* d) {
                return NPrivate::CalcBinaryOperatorScore(*d, argTypeIds[0], argTypeIds[1], catalog);
            };
            break;

        case 1:
            expectedOpKind = EOperKind::LeftUnary;
            calcScore = [&](const auto* d) {
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
        const auto& oper = catalog.State->Operators.FindPtr(operId);
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

    auto TryResolveUnknownsBySpreadingType = [&](ui32 argTypeId, std::function<ui32(const TOperDesc*)> getArgType) {
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
        finalCandidate = TryResolveUnknownsBySpreadingType(argTypeIds[1], [&](const auto* oper) { return oper->LeftType; });
    } else if (argTypeIds[1] == UnknownOid) {
        finalCandidate = TryResolveUnknownsBySpreadingType(argTypeIds[0], [&](const auto* oper) { return oper->RightType; });
    }

    if (finalCandidate) {
        return *finalCandidate;
    }
    NPrivate::ThrowOperatorNotFound(name, argTypeIds);
}

const TOperDesc& LookupOper(ui32 operId, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    auto operPtr = catalog.State->Operators.FindPtr(operId);
    if (!operPtr) {
        throw yexception() << "No such oper: " << operId;
    }

    if (!ValidateOperArgs(*operPtr, argTypeIds, catalog.State->Types)) {
        throw yexception() << "Unable to find an overload for operator with oid " << operId << " with given argument types: "
                           << ArgTypesList(argTypeIds);
    }

    return *operPtr;
}

const TOperDesc& LookupOper(ui32 operId) {
    const auto& catalog = TCatalog::Instance();
    auto operPtr = catalog.State->Operators.FindPtr(operId);
    if (!operPtr) {
        throw yexception() << "No such oper: " << operId;
    }

    return *operPtr;
}

bool HasAggregation(const TString& name, EAggKind kind) {
    const auto& catalog = TCatalog::Instance();
    auto aggIdPtr = catalog.State->AggregationsByName.FindPtr(to_lower(name));
    if (!aggIdPtr) {
        return false;
    }

    for (const auto& id : *aggIdPtr) {
        const auto& d = catalog.State->Aggregations.FindPtr(id);
        Y_ENSURE(d);
        if (d->Kind == kind) {
            return true;
        }
    }

    return false;
}

bool ValidateAggregateArgs(const TAggregateDesc& d, const TVector<ui32>& argTypeIds) {
    const auto& catalog = TCatalog::Instance();
    return ValidateArgs(d.ArgTypes, argTypeIds, catalog.State->Types);
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
    auto aggIdPtr = catalog.State->AggregationsByName.FindPtr(to_lower(name));
    if (!aggIdPtr) {
        throw yexception() << "No such aggregate: " << name;
    }

    auto bestScore = NPrivate::NoFitScore;
    TVector<const TAggregateDesc*> candidates;

    for (const auto& id : *aggIdPtr) {
        const auto& d = catalog.State->Aggregations.FindPtr(id);
        Y_ENSURE(d);

        if (NPrivate::IsExactMatch(d->ArgTypes, 0, argTypeIds)) {
            // At most one exact match is possible, so look no further
            // https://www.postgresql.org/docs/14/typeconv-func.html, step 2
            return *d;
        }

        // https://www.postgresql.org/docs/14/typeconv-func.html, steps 4.a, 4.c, 4.d
        auto score = NPrivate::CalcProcScore(d->ArgTypes, 0, 0, argTypeIds, catalog);

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

        for (const auto argType : argTypeIds) {
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
    TStringBuf realName = name;
    TMaybe<ui32> aggId;
    TStringBuf left, right;
    if (realName.TrySplit('#', left, right)) {
        aggId = FromString<ui32>(right);
        realName = left;
    }

    const auto& catalog = TCatalog::Instance();
    auto aggIdPtr = catalog.State->AggregationsByName.FindPtr(to_lower(TString(realName)));
    if (!aggIdPtr) {
        throw yexception() << "No such aggregate: " << name;
    }

    for (const auto& id : *aggIdPtr) {
        const auto& d = catalog.State->Aggregations.FindPtr(id);
        Y_ENSURE(d);
        if (aggId && d->AggId != *aggId) {
            continue;
        }

        if (!ValidateAggregateArgs(*d, stateType, resultType)) {
            continue;
        }

        return *d;
    }

    throw yexception() << "Unable to find an overload for aggregate " << name << " with given state type: " << NPg::LookupType(stateType).Name << " and result type: " << NPg::LookupType(resultType).Name;
}

void EnumAggregation(std::function<void(ui32, const TAggregateDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& x : catalog.State->Aggregations) {
        f(x.first, x.second);
    }
}

bool HasOpClass(EOpClassMethod method, ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->OpClasses.contains(std::make_pair(method, typeId));
}

const TOpClassDesc* LookupDefaultOpClass(EOpClassMethod method, ui32 typeId) {
    const auto& catalog = TCatalog::Instance();
    auto lookupId = (typeId == VarcharOid ? TextOid : typeId);
    const auto opClassPtr = catalog.State->OpClasses.FindPtr(std::make_pair(method, lookupId));
    if (opClassPtr) {
        return opClassPtr;
    }

    throw yexception() << "No such opclass";

    // TODO: support binary coercible and preferred types as define in PG's GetDefaultOpClass()
}

bool HasAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType) {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->AmOps.contains(std::make_tuple(familyId, strategy, leftType, rightType));
}

const TAmOpDesc& LookupAmOp(ui32 familyId, ui32 strategy, ui32 leftType, ui32 rightType) {
    const auto& catalog = TCatalog::Instance();
    const auto amOpPtr = catalog.State->AmOps.FindPtr(std::make_tuple(familyId, strategy, leftType, rightType));
    if (!amOpPtr) {
        throw yexception() << "No such amop";
    }

    return *amOpPtr;
}

bool HasAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType) {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->AmProcs.contains(std::make_tuple(familyId, num, leftType, rightType));
}

const TAmProcDesc& LookupAmProc(ui32 familyId, ui32 num, ui32 leftType, ui32 rightType) {
    const auto& catalog = TCatalog::Instance();
    auto amProcPtr = catalog.State->AmProcs.FindPtr(std::make_tuple(familyId, num, leftType, rightType));
    if (!amProcPtr) {
        throw yexception() << "No such amproc";
    }

    return *amProcPtr;
}

bool HasConversion(const TString& from, const TString& to) {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->Conversions.contains(std::make_pair(from, to));
}

const TConversionDesc& LookupConversion(const TString& from, const TString& to) {
    const auto& catalog = TCatalog::Instance();
    auto convPtr = catalog.State->Conversions.FindPtr(std::make_pair(from, to));
    if (!convPtr) {
        throw yexception() << "No such conversion from " << from << " to " << to;
    }

    return *convPtr;
}

bool IsCompatibleTo(ui32 actualType, ui32 expectedType) {
    const auto& catalog = TCatalog::Instance();
    return IsCompatibleTo(actualType, expectedType, catalog.State->Types);
}

const TLanguageDesc& LookupLanguage(ui32 langId) {
    const auto& catalog = TCatalog::Instance();
    auto langPtr = catalog.State->Languages.FindPtr(langId);
    if (!langPtr) {
        throw yexception() << "No such lang: " << langId;
    }

    return *langPtr;
}

void EnumLanguages(std::function<void(ui32, const TLanguageDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (const auto& x : catalog.State->Languages) {
        f(x.first, x.second);
    }
}

const TVector<TTableInfo>& GetStaticTables() {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->AllStaticTables;
}

const THashMap<TTableInfoKey, TVector<TColumnInfo>>& GetStaticColumns() {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->StaticColumns;
}

const TTableInfo& LookupStaticTable(const TTableInfoKey& tableKey) {
    const auto& catalog = TCatalog::Instance();
    auto tablePtr = catalog.State->StaticTables.FindPtr(tableKey);
    if (!tablePtr) {
        throw yexception() << "No such table: " << tableKey.Schema << "." << tableKey.Name;
    }

    return *tablePtr;
}

const TVector<TMaybe<TString>>* ReadTable(
    const TTableInfoKey& tableKey,
    const TVector<TString>& columnNames,
    size_t* columnsRemap, // should have the same length as columnNames
    size_t& rowStep) {
    const auto& catalog = TCatalog::Instance();
    auto dataPtr = catalog.State->StaticTablesData.FindPtr(tableKey);
    if (!dataPtr) {
        throw yexception() << "Missing data for table "
                           << tableKey.Schema << "." << tableKey.Name;
    }

    const auto& allColumns = *catalog.State->StaticColumns.FindPtr(tableKey);
    THashMap<TString, size_t> columnsToIndex;
    for (size_t i = 0; i < allColumns.size(); ++i) {
        Y_ENSURE(columnsToIndex.emplace(allColumns[i].Name, i).second);
    }

    rowStep = allColumns.size();
    for (size_t i = 0; i < columnNames.size(); ++i) {
        auto indexPtr = columnsToIndex.FindPtr(columnNames[i]);
        if (!indexPtr) {
            throw yexception() << "Missing column " << columnNames[i] << " in table "
                               << tableKey.Schema << "." << tableKey.Name;
        }

        columnsRemap[i] = *indexPtr;
    }

    return dataPtr;
}

bool AreAllFunctionsAllowed() {
    const auto& catalog = TCatalog::Instance();
    return catalog.State->AllowAllFunctions;
}

void AllowFunction(const TString& name) {
    auto& catalog = TCatalog::MutableInstance();
    if (!catalog.State->AllowAllFunctions) {
        catalog.State->AllowedProcs.insert(name);
    }
}

struct TSqlLanguageParserHolder {
    std::unique_ptr<ISqlLanguageParser> Parser;
};

void SetSqlLanguageParser(std::unique_ptr<ISqlLanguageParser> parser) {
    Singleton<TSqlLanguageParserHolder>()->Parser = std::move(parser);
}

ISqlLanguageParser* GetSqlLanguageParser() {
    return Singleton<TSqlLanguageParserHolder>()->Parser.get();
}

void LoadSystemFunctions(ISystemFunctionsParser& parser) {
    YQL_PROFILE_FUNC(DEBUG);
    auto& catalog = TCatalog::MutableInstance();
    with_lock (catalog.ExtensionsGuard) {
        if (catalog.State->SystemFunctionInit) {
            return;
        }

        TString data;
        Y_ENSURE(NResource::FindExact("system_functions.sql", &data));
        TVector<TProcDesc> procs;
        parser.Parse(data, procs);
        for (const auto& p : procs) {
            auto procIdPtr = catalog.State->ProcByName.FindPtr(p.Name);
            Y_ENSURE(procIdPtr);
            TProcDesc* foundProc = nullptr;
            for (auto procId : *procIdPtr) {
                auto procPtr = catalog.State->Procs.FindPtr(procId);
                Y_ENSURE(procPtr);
                if (procPtr->ArgTypes == p.ArgTypes && procPtr->VariadicType == p.VariadicType) {
                    foundProc = procPtr;
                    break;
                }
            }

            Y_ENSURE(foundProc);
            foundProc->DefaultArgs = p.DefaultArgs;
            foundProc->Src = p.Src;
            foundProc->ExprNode = p.ExprNode;
        }

        catalog.State->SystemFunctionInit = true;
    }
}

void RegisterExtensions(const TVector<TExtensionDesc>& extensions, bool typesOnly,
                        IExtensionSqlParser& parser, IExtensionLoader* loader) {
    YQL_PROFILE_FUNC(DEBUG);
    if (extensions.size() > MaximumExtensionsCount) {
        throw yexception() << "Too many extensions: " << extensions.size();
    }

    auto& catalog = TCatalog::MutableInstance();
    with_lock (catalog.ExtensionsGuard) {
        Y_ENSURE(!catalog.ExtensionsInit);

        auto savedAllowAllFunctions = catalog.State->AllowAllFunctions;
        catalog.State->AllowAllFunctions = true;
        for (ui32 i = 0; i < extensions.size(); ++i) {
            auto e = extensions[i];
            e.TypesOnly = e.TypesOnly || typesOnly;
            if (e.Name.empty()) {
                throw yexception() << "Empty extension name";
            }

            if (!catalog.State->ExtensionsByName.insert(std::make_pair(e.Name, i + 1)).second) {
                throw yexception() << "Duplicated extension name: " << e.Name;
            }

            if (!catalog.State->ExtensionsByInstallName.insert(std::make_pair(e.InstallName, i + 1)).second) {
                throw yexception() << "Duplicated extension install name: " << e.InstallName;
            }

            if (e.LibraryMD5.empty() && !e.LibraryPath.empty()) {
                e.LibraryMD5 = MD5::File(e.LibraryPath);
            }

            catalog.State->Extensions.push_back(e);
            TVector<TString> sqls;
            for (const auto& p : e.SqlPaths) {
                TString sql = TFileInput(p).ReadAll();
                sqls.push_back(sql);
            }

            parser.Parse(i + 1, sqls, catalog);
            if (loader && !e.TypesOnly && !e.LibraryPath.empty()) {
                loader->Load(i + 1, e.Name, e.LibraryPath);
            }
        }

        catalog.State->AllowAllFunctions = savedAllowAllFunctions;
        catalog.ExtensionsInit = true;
    }
}

TString ExportExtensions(const TMaybe<TSet<ui32>>& filter) {
    auto& catalog = TCatalog::Instance();
    if (catalog.State->Extensions.empty()) {
        return TString();
    }

    NProto::TPgCatalog proto;
    for (ui32 i = 0; i < catalog.State->Extensions.size(); ++i) {
        const auto& ext = catalog.State->Extensions[i];
        const bool skip = filter && !filter->contains(i + 1);
        auto protoExt = proto.AddExtension();
        protoExt->SetName(ext.Name);
        protoExt->SetInstallName(ext.InstallName);
        protoExt->SetTypesOnly(skip);
        if (!skip && !ext.LibraryPath.empty()) {
            protoExt->SetLibraryPath(TFsPath(".") / TFsPath(ext.LibraryPath).GetName());
            protoExt->SetLibraryMD5(ext.LibraryMD5);
        }
    }

    TVector<ui32> extTypes;
    for (const auto& t : catalog.State->Types) {
        const auto& desc = t.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extTypes.push_back(t.first);
    }

    Sort(extTypes);
    for (const auto t : extTypes) {
        const auto& desc = *catalog.State->Types.FindPtr(t);
        auto protoType = proto.AddType();
        protoType->SetTypeId(desc.TypeId);
        protoType->SetName(desc.Name);
        protoType->SetExtensionIndex(desc.ExtensionIndex);
        protoType->SetCategory(desc.Category);
        protoType->SetTypeLen(desc.TypeLen);
        protoType->SetPassByValue(desc.PassByValue);
        protoType->SetTypeAlign(desc.TypeAlign);
        protoType->SetElementTypeId(desc.ElementTypeId);
        protoType->SetArrayTypeId(desc.ArrayTypeId);
        if (desc.InFuncId) {
            protoType->SetInFuncId(desc.InFuncId);
        }
        if (desc.OutFuncId) {
            protoType->SetOutFuncId(desc.OutFuncId);
        }
        if (desc.SendFuncId) {
            protoType->SetSendFuncId(desc.SendFuncId);
        }
        if (desc.ReceiveFuncId) {
            protoType->SetReceiveFuncId(desc.ReceiveFuncId);
        }
        if (desc.TypeModInFuncId) {
            protoType->SetTypeModInFuncId(desc.TypeModInFuncId);
        }
        if (desc.TypeModOutFuncId) {
            protoType->SetTypeModOutFuncId(desc.TypeModOutFuncId);
        }
        if (desc.TypeSubscriptFuncId) {
            protoType->SetTypeSubscriptFuncId(desc.TypeSubscriptFuncId);
        }
        if (desc.LessProcId) {
            protoType->SetLessProcId(desc.LessProcId);
        }
        if (desc.EqualProcId) {
            protoType->SetEqualProcId(desc.EqualProcId);
        }
        if (desc.CompareProcId) {
            protoType->SetCompareProcId(desc.CompareProcId);
        }
        if (desc.HashProcId) {
            protoType->SetHashProcId(desc.HashProcId);
        }
    }

    TVector<ui32> extProcs;
    for (const auto& p : catalog.State->Procs) {
        const auto& desc = p.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extProcs.push_back(p.first);
    }

    Sort(extProcs);
    for (const auto p : extProcs) {
        const auto& desc = *catalog.State->Procs.FindPtr(p);
        auto protoProc = proto.AddProc();
        protoProc->SetProcId(desc.ProcId);
        protoProc->SetName(desc.Name);
        protoProc->SetExtensionIndex(desc.ExtensionIndex);
        protoProc->SetSrc(desc.Src);
        for (const auto t : desc.ArgTypes) {
            protoProc->AddArgType(t);
        }

        for (const auto t : desc.OutputArgTypes) {
            protoProc->AddOutputArgType(t);
        }

        protoProc->SetVariadicType(desc.VariadicType);
        protoProc->SetVariadicArgType(desc.VariadicArgType);
        for (const auto& name : desc.InputArgNames) {
            protoProc->AddInputArgName(name);
        }

        for (const auto& name : desc.OutputArgNames) {
            protoProc->AddOutputArgName(name);
        }

        protoProc->SetVariadicArgName(desc.VariadicArgName);
        for (const auto& d : desc.DefaultArgs) {
            protoProc->AddDefaultArgNull(!d.Defined());
            protoProc->AddDefaultArgValue(d ? *d : "");
        }

        protoProc->SetIsStrict(desc.IsStrict);
        protoProc->SetLang(desc.Lang);
        protoProc->SetResultType(desc.ResultType);
        protoProc->SetReturnSet(desc.ReturnSet);
        protoProc->SetKind((ui32)desc.Kind);
    }

    TVector<TTableInfoKey> extTables;
    for (const auto& t : catalog.State->StaticTables) {
        if (!t.second.ExtensionIndex) {
            continue;
        }

        extTables.push_back(t.first);
    }

    Sort(extTables);
    for (const auto& key : extTables) {
        const auto& table = *catalog.State->StaticTables.FindPtr(key);
        auto protoTable = proto.AddTable();
        protoTable->SetOid(table.Oid);
        protoTable->SetSchema(table.Schema);
        protoTable->SetName(table.Name);
        protoTable->SetExtensionIndex(table.ExtensionIndex);
        const auto columnsPtr = catalog.State->StaticColumns.FindPtr(key);
        Y_ENSURE(columnsPtr);
        for (const auto& c : *columnsPtr) {
            protoTable->AddColumn(c.Name);
            protoTable->AddUdtType(c.UdtType);
        }

        const auto dataPtr = catalog.State->StaticTablesData.FindPtr(key);
        if (dataPtr) {
            for (const auto& v : *dataPtr) {
                if (v.Defined()) {
                    protoTable->AddDataNull(false);
                    protoTable->AddDataValue(*v);
                } else {
                    protoTable->AddDataNull(true);
                    protoTable->AddDataValue("");
                }
            }
        }
    }

    TVector<ui32> extCasts;
    for (const auto& c : catalog.State->Casts) {
        const auto& desc = c.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extCasts.push_back(c.first);
    }

    Sort(extCasts);
    for (const auto p : extCasts) {
        const auto& desc = *catalog.State->Casts.FindPtr(p);
        auto protoCast = proto.AddCast();
        protoCast->SetId(p);
        protoCast->SetSourceId(desc.SourceId);
        protoCast->SetTargetId(desc.TargetId);
        protoCast->SetExtensionIndex(desc.ExtensionIndex);
        protoCast->SetMethod((ui32)desc.Method);
        protoCast->SetFunctionId(desc.FunctionId);
        protoCast->SetCoercionCode((ui32)desc.CoercionCode);
    }

    TVector<ui32> extOpers;
    for (const auto& o : catalog.State->Operators) {
        const auto& desc = o.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extOpers.push_back(o.first);
    }

    Sort(extOpers);
    for (const auto o : extOpers) {
        const auto& desc = *catalog.State->Operators.FindPtr(o);
        auto protoOper = proto.AddOper();
        protoOper->SetOperId(o);
        protoOper->SetName(desc.Name);
        protoOper->SetExtensionIndex(desc.ExtensionIndex);
        protoOper->SetLeftType(desc.LeftType);
        protoOper->SetRightType(desc.RightType);
        protoOper->SetKind((ui32)desc.Kind);
        protoOper->SetProcId(desc.ProcId);
        protoOper->SetResultType(desc.ResultType);
        protoOper->SetComId(desc.ComId);
        protoOper->SetNegateId(desc.NegateId);
    }

    TVector<ui32> extAggs;
    for (const auto& a : catalog.State->Aggregations) {
        const auto& desc = a.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extAggs.push_back(a.first);
    }

    Sort(extAggs);
    for (const auto a : extAggs) {
        const auto& desc = *catalog.State->Aggregations.FindPtr(a);
        auto protoAggregation = proto.AddAggregation();
        protoAggregation->SetAggId(a);
        protoAggregation->SetName(desc.Name);
        protoAggregation->SetExtensionIndex(desc.ExtensionIndex);
        for (const auto argType : desc.ArgTypes) {
            protoAggregation->AddArgType(argType);
        }

        protoAggregation->SetKind((ui32)desc.Kind);
        protoAggregation->SetTransTypeId(desc.TransTypeId);
        protoAggregation->SetTransFuncId(desc.TransFuncId);
        protoAggregation->SetFinalFuncId(desc.FinalFuncId);
        protoAggregation->SetCombineFuncId(desc.CombineFuncId);
        protoAggregation->SetSerializeFuncId(desc.SerializeFuncId);
        protoAggregation->SetDeserializeFuncId(desc.DeserializeFuncId);
        protoAggregation->SetInitValue(desc.InitValue);
        protoAggregation->SetFinalExtra(desc.FinalExtra);
        protoAggregation->SetNumDirectArgs(desc.NumDirectArgs);
    }

    TVector<TString> extOpFamilies;
    for (const auto& f : catalog.State->OpFamilies) {
        const auto& desc = f.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extOpFamilies.push_back(f.first);
    }

    Sort(extOpFamilies);
    for (const auto& f : extOpFamilies) {
        const auto& desc = *catalog.State->OpFamilies.FindPtr(f);
        auto protoOpClassFamily = proto.AddOpClassFamily();
        protoOpClassFamily->SetFamilyId(desc.FamilyId);
        protoOpClassFamily->SetName(desc.Name);
        protoOpClassFamily->SetExtensionIndex(desc.ExtensionIndex);
    }

    TVector<std::pair<NPg::EOpClassMethod, ui32>> extOpClasses;
    for (const auto& c : catalog.State->OpClasses) {
        const auto& desc = c.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extOpClasses.push_back(c.first);
    }

    for (const auto& c : extOpClasses) {
        const auto& desc = *catalog.State->OpClasses.FindPtr(c);
        auto protoOpClass = proto.AddOpClass();
        protoOpClass->SetMethod((ui32)desc.Method);
        protoOpClass->SetTypeId(desc.TypeId);
        protoOpClass->SetExtensionIndex(desc.ExtensionIndex);
        protoOpClass->SetName(desc.Name);
        protoOpClass->SetFamilyId(desc.FamilyId);
    }

    TVector<std::tuple<ui32, ui32, ui32, ui32>> extAmOps;
    for (const auto& o : catalog.State->AmOps) {
        const auto& desc = o.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extAmOps.push_back(o.first);
    }

    for (const auto& o : extAmOps) {
        const auto& desc = *catalog.State->AmOps.FindPtr(o);
        auto protoAmOp = proto.AddAmOp();
        protoAmOp->SetFamilyId(desc.FamilyId);
        protoAmOp->SetStrategy(desc.Strategy);
        protoAmOp->SetLeftType(desc.LeftType);
        protoAmOp->SetRightType(desc.RightType);
        protoAmOp->SetOperId(desc.OperId);
        protoAmOp->SetExtensionIndex(desc.ExtensionIndex);
    }

    TVector<std::tuple<ui32, ui32, ui32, ui32>> extAmProcs;
    for (const auto& p : catalog.State->AmProcs) {
        const auto& desc = p.second;
        if (!desc.ExtensionIndex) {
            continue;
        }

        extAmProcs.push_back(p.first);
    }

    for (const auto& p : extAmProcs) {
        const auto& desc = *catalog.State->AmProcs.FindPtr(p);
        auto protoAmProc = proto.AddAmProc();
        protoAmProc->SetFamilyId(desc.FamilyId);
        protoAmProc->SetProcNum(desc.ProcNum);
        protoAmProc->SetLeftType(desc.LeftType);
        protoAmProc->SetRightType(desc.RightType);
        protoAmProc->SetProcId(desc.ProcId);
        protoAmProc->SetExtensionIndex(desc.ExtensionIndex);
    }

    return proto.SerializeAsString();
}

void ImportExtensions(const TString& exported, bool typesOnly, IExtensionLoader* loader) {
    auto& catalog = TCatalog::MutableInstance();
    with_lock (catalog.ExtensionsGuard) {
        Y_ENSURE(!catalog.ExtensionsInit);
        if (exported.empty()) {
            catalog.ExtensionsInit = true;
            return;
        }

        NProto::TPgCatalog proto;
        Y_ENSURE(proto.ParseFromString(exported));
        for (ui32 i = 0; i < proto.ExtensionSize(); ++i) {
            const auto& protoExt = proto.GetExtension(i);
            TExtensionDesc e;
            e.Name = protoExt.GetName();
            e.InstallName = protoExt.GetInstallName();
            e.TypesOnly = protoExt.GetTypesOnly();
            e.LibraryMD5 = protoExt.GetLibraryMD5();
            e.LibraryPath = protoExt.GetLibraryPath();
            catalog.State->Extensions.push_back(e);
            if (!catalog.State->ExtensionsByName.insert(std::make_pair(e.Name, i + 1)).second) {
                throw yexception() << "Duplicated extension name: " << e.Name;
            }

            if (!catalog.State->ExtensionsByInstallName.insert(std::make_pair(e.InstallName, i + 1)).second) {
                throw yexception() << "Duplicated extension install name: " << e.InstallName;
            }
        }

        for (const auto& protoType : proto.GetType()) {
            TTypeDesc desc;
            desc.TypeId = protoType.GetTypeId();
            desc.Name = protoType.GetName();
            desc.ExtensionIndex = protoType.GetExtensionIndex();
            desc.Category = protoType.GetCategory();
            desc.TypeLen = protoType.GetTypeLen();
            desc.PassByValue = protoType.GetPassByValue();
            desc.TypeAlign = protoType.GetTypeAlign();
            desc.ElementTypeId = protoType.GetElementTypeId();
            desc.ArrayTypeId = protoType.GetArrayTypeId();
            desc.InFuncId = protoType.GetInFuncId();
            desc.OutFuncId = protoType.GetOutFuncId();
            desc.SendFuncId = protoType.GetSendFuncId();
            desc.ReceiveFuncId = protoType.GetReceiveFuncId();
            desc.TypeModInFuncId = protoType.GetTypeModInFuncId();
            desc.TypeModOutFuncId = protoType.GetTypeModOutFuncId();
            desc.TypeSubscriptFuncId = protoType.GetTypeSubscriptFuncId();
            desc.LessProcId = protoType.GetLessProcId();
            desc.EqualProcId = protoType.GetEqualProcId();
            desc.CompareProcId = protoType.GetCompareProcId();
            desc.HashProcId = protoType.GetHashProcId();
            Y_ENSURE(catalog.State->Types.emplace(desc.TypeId, desc).second);
            Y_ENSURE(catalog.State->TypeByName.emplace(desc.Name, desc.TypeId).second);
        }

        for (const auto& protoProc : proto.GetProc()) {
            TProcDesc desc;
            desc.ProcId = protoProc.GetProcId();
            desc.Name = protoProc.GetName();
            desc.ExtensionIndex = protoProc.GetExtensionIndex();
            desc.Src = protoProc.GetSrc();
            desc.IsStrict = protoProc.GetIsStrict();
            desc.Lang = protoProc.GetLang();
            desc.ResultType = protoProc.GetResultType();
            desc.ReturnSet = protoProc.GetReturnSet();
            desc.Kind = (EProcKind)protoProc.GetKind();
            for (const auto t : protoProc.GetArgType()) {
                desc.ArgTypes.push_back(t);
            }
            for (const auto t : protoProc.GetOutputArgType()) {
                desc.OutputArgTypes.push_back(t);
            }
            desc.VariadicType = protoProc.GetVariadicType();
            desc.VariadicArgType = protoProc.GetVariadicArgType();
            for (const auto& name : protoProc.GetInputArgName()) {
                desc.InputArgNames.push_back(name);
            }
            for (const auto& name : protoProc.GetOutputArgName()) {
                desc.OutputArgNames.push_back(name);
            }
            desc.VariadicArgName = protoProc.GetVariadicArgName();
            Y_ENSURE(protoProc.DefaultArgNullSize() == protoProc.DefaultArgValueSize());
            for (ui32 i = 0; i < protoProc.DefaultArgNullSize(); ++i) {
                if (protoProc.GetDefaultArgNull(i)) {
                    desc.DefaultArgs.push_back(Nothing());
                } else {
                    desc.DefaultArgs.push_back(protoProc.GetDefaultArgValue(i));
                }
            }

            Y_ENSURE(catalog.State->Procs.emplace(desc.ProcId, desc).second);
            catalog.State->ProcByName[desc.Name].push_back(desc.ProcId);
        }

        for (const auto& protoTable : proto.GetTable()) {
            TTableInfo table;
            table.Oid = protoTable.GetOid();
            table.Schema = protoTable.GetSchema();
            table.Name = protoTable.GetName();
            table.Kind = ERelKind::Relation;
            table.ExtensionIndex = protoTable.GetExtensionIndex();
            catalog.State->AllStaticTables.push_back(table);
            TTableInfoKey key = table;
            Y_ENSURE(catalog.State->StaticTables.emplace(key, table).second);
            Y_ENSURE(protoTable.ColumnSize() > 0);
            Y_ENSURE(protoTable.ColumnSize() == protoTable.UdtTypeSize());
            for (ui32 i = 0; i < protoTable.ColumnSize(); ++i) {
                TColumnInfo columnInfo;
                columnInfo.Schema = table.Schema;
                columnInfo.TableName = table.Name;
                columnInfo.ExtensionIndex = table.ExtensionIndex;
                columnInfo.Name = protoTable.GetColumn(i);
                columnInfo.UdtType = protoTable.GetUdtType(i);
                catalog.State->AllStaticColumns.push_back(columnInfo);
                catalog.State->StaticColumns[key].push_back(columnInfo);
            }

            if (protoTable.DataValueSize() > 0) {
                Y_ENSURE(protoTable.DataValueSize() == protoTable.DataNullSize());
                auto& data = catalog.State->StaticTablesData[key];
                data.reserve(protoTable.DataValueSize());
                for (ui64 i = 0; i < protoTable.DataValueSize(); ++i) {
                    if (protoTable.GetDataNull(i)) {
                        data.push_back(Nothing());
                    } else {
                        data.push_back(protoTable.GetDataValue(i));
                    }
                }
            }
        }

        for (const auto& protoCast : proto.GetCast()) {
            auto id = protoCast.GetId();
            TCastDesc desc;
            desc.SourceId = protoCast.GetSourceId();
            desc.TargetId = protoCast.GetTargetId();
            desc.ExtensionIndex = protoCast.GetExtensionIndex();
            desc.Method = (ECastMethod)protoCast.GetMethod();
            desc.FunctionId = protoCast.GetFunctionId();
            desc.CoercionCode = (ECoercionCode)protoCast.GetCoercionCode();
            Y_ENSURE(catalog.State->Casts.emplace(id, desc).second);
            Y_ENSURE(catalog.State->CastsByDir.insert(std::make_pair(std::make_pair(desc.SourceId, desc.TargetId), id)).second);
        }

        for (const auto& protoOper : proto.GetOper()) {
            TOperDesc desc;
            desc.OperId = protoOper.GetOperId();
            desc.Name = protoOper.GetName();
            desc.ExtensionIndex = protoOper.GetExtensionIndex();
            desc.LeftType = protoOper.GetLeftType();
            desc.RightType = protoOper.GetRightType();
            desc.Kind = (EOperKind)protoOper.GetKind();
            desc.ProcId = protoOper.GetProcId();
            desc.ResultType = protoOper.GetResultType();
            desc.ComId = protoOper.GetComId();
            desc.NegateId = protoOper.GetNegateId();
            Y_ENSURE(catalog.State->Operators.emplace(desc.OperId, desc).second);
            catalog.State->OperatorsByName[desc.Name].push_back(desc.OperId);
        }

        for (const auto& protoAggregation : proto.GetAggregation()) {
            TAggregateDesc desc;
            desc.AggId = protoAggregation.GetAggId();
            desc.Name = protoAggregation.GetName();
            desc.ExtensionIndex = protoAggregation.GetExtensionIndex();
            for (const auto argType : protoAggregation.GetArgType()) {
                desc.ArgTypes.push_back(argType);
            }

            desc.Kind = (NPg::EAggKind)protoAggregation.GetKind();
            desc.TransTypeId = protoAggregation.GetTransTypeId();
            desc.TransFuncId = protoAggregation.GetTransFuncId();
            desc.FinalFuncId = protoAggregation.GetFinalFuncId();
            desc.CombineFuncId = protoAggregation.GetCombineFuncId();
            desc.SerializeFuncId = protoAggregation.GetSerializeFuncId();
            desc.DeserializeFuncId = protoAggregation.GetDeserializeFuncId();
            desc.InitValue = protoAggregation.GetInitValue();
            desc.FinalExtra = protoAggregation.GetFinalExtra();
            desc.NumDirectArgs = protoAggregation.GetNumDirectArgs();

            Y_ENSURE(catalog.State->Aggregations.emplace(desc.AggId, desc).second);
            catalog.State->AggregationsByName[desc.Name].push_back(desc.AggId);
        }

        THashMap<ui32, TString> opFamiliesByOid;
        for (const auto& protoOpClassFamily : proto.GetOpClassFamily()) {
            TOpFamilyDesc desc;
            desc.FamilyId = protoOpClassFamily.GetFamilyId();
            desc.Name = protoOpClassFamily.GetName();
            desc.ExtensionIndex = protoOpClassFamily.GetExtensionIndex();
            Y_ENSURE(catalog.State->OpFamilies.emplace(desc.Name, desc).second);
            Y_ENSURE(opFamiliesByOid.emplace(desc.FamilyId, desc.Name).second);
        }

        for (const auto& protoOpClass : proto.GetOpClass()) {
            TOpClassDesc desc;
            desc.Method = (EOpClassMethod)protoOpClass.GetMethod();
            desc.TypeId = protoOpClass.GetTypeId();
            desc.ExtensionIndex = protoOpClass.GetExtensionIndex();
            desc.FamilyId = protoOpClass.GetFamilyId();
            desc.Name = protoOpClass.GetName();
            desc.Family = *opFamiliesByOid.FindPtr(desc.FamilyId);
            Y_ENSURE(catalog.State->OpClasses.emplace(std::make_pair(desc.Method, desc.TypeId), desc).second);
        }

        for (const auto& protoAmOp : proto.GetAmOp()) {
            TAmOpDesc desc;
            desc.FamilyId = protoAmOp.GetFamilyId();
            desc.Strategy = protoAmOp.GetStrategy();
            desc.LeftType = protoAmOp.GetLeftType();
            desc.RightType = protoAmOp.GetRightType();
            desc.OperId = protoAmOp.GetOperId();
            desc.ExtensionIndex = protoAmOp.GetExtensionIndex();
            desc.Family = *opFamiliesByOid.FindPtr(desc.FamilyId);
            Y_ENSURE(catalog.State->AmOps.emplace(std::make_tuple(desc.FamilyId, desc.Strategy, desc.LeftType, desc.RightType), desc).second);
        }

        for (const auto& protoAmProc : proto.GetAmProc()) {
            TAmProcDesc desc;
            desc.FamilyId = protoAmProc.GetFamilyId();
            desc.ProcNum = protoAmProc.GetProcNum();
            desc.LeftType = protoAmProc.GetLeftType();
            desc.RightType = protoAmProc.GetRightType();
            desc.ProcId = protoAmProc.GetProcId();
            desc.ExtensionIndex = protoAmProc.GetExtensionIndex();
            desc.Family = *opFamiliesByOid.FindPtr(desc.FamilyId);
            Y_ENSURE(catalog.State->AmProcs.emplace(std::make_tuple(desc.FamilyId, desc.ProcNum, desc.LeftType, desc.RightType), desc).second);
        }

        if (!typesOnly && loader) {
            for (ui32 extensionIndex = 1; extensionIndex <= catalog.State->Extensions.size(); ++extensionIndex) {
                const auto& e = catalog.State->Extensions[extensionIndex - 1];
                if (!e.TypesOnly) {
                    loader->Load(extensionIndex, e.Name, e.LibraryPath);
                }
            }
        }

        catalog.State->AllowAllFunctions = true;
        catalog.State->AllowedProcs.clear();
        catalog.ExtensionsInit = true;
    }
}

void ClearExtensions() {
    auto& catalog = TCatalog::MutableInstance();
    with_lock (catalog.ExtensionsGuard) {
        if (!catalog.ExtensionsInit) {
            return;
        }

        catalog.Init();
        catalog.ExtensionsInit = false;
    }
}

void EnumExtensions(std::function<void(ui32, const TExtensionDesc&)> f) {
    const auto& catalog = TCatalog::Instance();
    for (ui32 i = 0; i < catalog.State->Extensions.size(); ++i) {
        f(i + 1, catalog.State->Extensions[i]);
    }
}

const TExtensionDesc& LookupExtension(ui32 extIndex) {
    const auto& catalog = TCatalog::Instance();
    Y_ENSURE(extIndex > 0 && extIndex <= catalog.State->Extensions.size());
    return catalog.State->Extensions[extIndex - 1];
}

ui32 LookupExtensionByName(const TString& name) {
    const auto& catalog = TCatalog::Instance();
    auto indexPtr = catalog.State->ExtensionsByName.FindPtr(name);
    if (!indexPtr) {
        throw yexception() << "Unknown extension name: " << name;
    }

    return *indexPtr;
}

ui32 LookupExtensionByInstallName(const TString& installName) {
    const auto& catalog = TCatalog::Instance();
    auto indexPtr = catalog.State->ExtensionsByInstallName.FindPtr(installName);
    if (!indexPtr) {
        throw yexception() << "Unknown extension install name: " << installName;
    }

    return *indexPtr;
}

} // namespace NYql::NPg
