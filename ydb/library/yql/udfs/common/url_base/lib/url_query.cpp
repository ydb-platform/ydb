#include "url_query.h"

#include <ydb/library/yql/public/udf/udf_type_printer.h>

#include <util/string/split.h>

#include <library/cpp/string_utils/quote/quote.h>

namespace NUrlUdf {
    void TQueryStringParse::MakeSignature(IFunctionTypeInfoBuilder& builder,
                                          const TType* retType)
    {
        builder.Returns(retType).OptionalArgs(4);
        auto args = builder.Args();
        args->Add<TAutoMap<TQueryStr>>();
        args->Add<TKeepBlankValuesNArg>();
        args->Add<TStrictNArg>();
        args->Add<TMaxFieldsNArg>();
        args->Add<TSeparatorNArg>().Done();
    }

    std::vector<std::pair<TString, TString>>
    TQueryStringParse::RunImpl(const TUnboxedValuePod* args) const {
        const std::string_view query(args[0].AsStringRef());
        if (query.empty())
            return {};
        const bool keepBlankValues = args[1].GetOrDefault(false);
        const bool strict = args[2].GetOrDefault(true);
        const ui32 maxFieldCnt = args[3].GetOrDefault(Max<ui32>());
        const std::string_view sep(args[4] ? args[4].AsStringRef() : "&");

        std::vector<TStringBuf> parts;
        StringSplitter(query).SplitByString(sep).Collect(&parts);
        if (parts.size() > maxFieldCnt) {
            UdfTerminate((TStringBuilder() << Pos_ << "Max number of fields (" << maxFieldCnt
                                           << ") exceeded: got " << parts.size()).data());
        }

        std::vector<std::pair<TString, TString>> pairs;
        for (const TStringBuf& part: parts) {
            if (part.empty() && !strict) {
                continue;
            }
            TVector<TString> nvPair = StringSplitter(part).Split('=').Limit(2);
            if (nvPair.size() != 2) {
                if (strict) {
                    UdfTerminate((TStringBuilder() << Pos_ << "Bad query field: \""
                                                   << nvPair[0] << "\"").data());
                }
                if (keepBlankValues) {
                    nvPair.emplace_back("");
                } else {
                    continue;
                }
            }
            if (!nvPair[1].empty() || keepBlankValues) {
                CGIUnescape(nvPair[0]);
                CGIUnescape(nvPair[1]);
                pairs.emplace_back(nvPair[0], nvPair[1]);
            }
        }
        return pairs;
    }

    bool TQueryStringToList::DeclareSignature(const TStringRef& name,
                                              TType*,
                                              IFunctionTypeInfoBuilder& builder,
                                              bool typesOnly) {
        if (Name() == name) {
            MakeSignature(builder, GetListType(builder));
            if (!typesOnly) {
                builder.Implementation(new TQueryStringToList(builder.GetSourcePosition()));
            }
            return true;
        }
        return false;
    }

    TUnboxedValue TQueryStringToList::Run(const IValueBuilder* valueBuilder,
                                          const TUnboxedValuePod* args) const {
        const auto pairs = RunImpl(args);
        std::vector<TUnboxedValue> ret;
        for (const auto& nvPair : pairs) {
            TUnboxedValue* pair = nullptr;
            auto item = valueBuilder->NewArray(2U, pair);
            pair[0] = valueBuilder->NewString(nvPair.first);
            pair[1] = valueBuilder->NewString(nvPair.second);
            ret.push_back(item);
        }
        return valueBuilder->NewList(ret.data(), ret.size());
    }

    bool TQueryStringToDict::DeclareSignature(const TStringRef& name,
                                              TType*,
                                              IFunctionTypeInfoBuilder& builder,
                                              bool typesOnly) {
        if (Name() == name) {
            auto dictType = GetDictType(builder);
            MakeSignature(builder, dictType);
            if (!typesOnly) {
                builder.Implementation(new TQueryStringToDict(dictType,
                                                              builder.GetSourcePosition()));
            }
            return true;
        }
        return false;
    }

    TUnboxedValue TQueryStringToDict::Run(const IValueBuilder* valueBuilder,
                                          const TUnboxedValuePod* args) const {
        const auto pairs = RunImpl(args);
        auto ret = valueBuilder->NewDict(DictType_, TDictFlags::Hashed | TDictFlags::Multi);
        for (const auto& nvPair : pairs) {
            ret->Add(valueBuilder->NewString(nvPair.first),
                     valueBuilder->NewString(nvPair.second));
        }
        return ret->Build();
    }

    TUnboxedValue TBuildQueryString::Run(const IValueBuilder* valueBuilder,
                                         const TUnboxedValuePod* args) const {
        const std::string_view sep(args[1] ? args[1].AsStringRef() : "&");
        TStringBuilder ret;

        switch(FirstArgTypeId_) {
        case EFirstArgTypeId::Dict: {
            TUnboxedValue key, value;
            const auto dictIt = args[0].GetDictIterator();
            ui64 wasItem = 0;
            while (dictIt.NextPair(key, value)) {
                TString keyEscaped = CGIEscapeRet(key.AsStringRef());
                const auto listIt = value.GetListIterator();
                TUnboxedValue item;
                while (listIt.Next(item)) {
                    if (wasItem++)
                        ret << sep;
                    if (item) {
                        ret << keyEscaped << '=' << CGIEscapeRet(item.AsStringRef());
                    } else {
                        ret << keyEscaped << '=';
                    }
                }
            }
            break;
        }
        case EFirstArgTypeId::FlattenDict: {
            TUnboxedValue key, value;
            const auto dictIt = args[0].GetDictIterator();
            ui64 wasKey = 0;
            while (dictIt.NextPair(key, value)) {
                if (wasKey++)
                    ret << sep;
                if (value) {
                    ret << CGIEscapeRet(key.AsStringRef()) << '='
                        << CGIEscapeRet(value.AsStringRef());
                } else {
                    ret << CGIEscapeRet(key.AsStringRef()) << '=';
                }
            }
            break;
        }
        case EFirstArgTypeId::List: {
            ui64 wasItem = 0;
            TUnboxedValue item;
            const auto listIt = args[0].GetListIterator();
            while (listIt.Next(item)) {
                if (wasItem++)
                    ret << sep;
                TUnboxedValue key = item.GetElement(0), val = item.GetElement(1);
                if (val) {
                    ret << CGIEscapeRet(key.AsStringRef()) << '='
                        << CGIEscapeRet(val.AsStringRef());
                } else {
                    ret << CGIEscapeRet(key.AsStringRef()) << '=';
                }
            }
            break;
        }
        default:
            Y_ABORT("Current first parameter type is not yet implemented");
        }
        return valueBuilder->NewString(ret);
    }

    bool TBuildQueryString::DeclareSignature(const TStringRef& name,
                                             TType* userType,
                                             IFunctionTypeInfoBuilder& builder,
                                             bool typesOnly) {
        if (Name() == name) {
            if (!userType) {
                builder.SetError("Missing user type");
                return true;
            }
            builder.UserType(userType);
            const auto typeHelper = builder.TypeInfoHelper();
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || !userTypeInspector.GetElementsCount()) {
                builder.SetError("User type is not tuple");
                return true;
            }
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper,
                                                               userTypeInspector.GetElementType(0));
            if (!argsTypeInspector || !argsTypeInspector.GetElementsCount()) {
                builder.SetError("Please provide at least one argument");
                return true;
            }
            const auto firstArgType = argsTypeInspector.GetElementType(0);
            EFirstArgTypeId firstArgTypeId = EFirstArgTypeId::None;

            if (typeHelper->IsSameType(GetDictType(builder), firstArgType) ||
                typeHelper->IsSameType(GetDictType(builder, true), firstArgType)) {
                firstArgTypeId = EFirstArgTypeId::Dict;
            } else if (typeHelper->IsSameType(GetListType(builder), firstArgType) ||
                       typeHelper->IsSameType(GetListType(builder, true), firstArgType) ||
                       typeHelper->GetTypeKind(firstArgType) == ETypeKind::EmptyList)
            {
                firstArgTypeId = EFirstArgTypeId::List;
            } else if (typeHelper->IsSameType(GetFlattenDictType(builder), firstArgType) ||
                       typeHelper->IsSameType(GetFlattenDictType(builder, true), firstArgType) ||
                       typeHelper->GetTypeKind(firstArgType) == ETypeKind::EmptyDict)
            {
                firstArgTypeId = EFirstArgTypeId::FlattenDict;
            }
            if (firstArgTypeId != EFirstArgTypeId::None) {
                builder.Returns<TQueryStr>().OptionalArgs(1);
                auto args = builder.Args();
                args->Add(firstArgType).Flags(ICallablePayload::TArgumentFlags::AutoMap);
                args->Add<TSeparatorNArg>().Done();
                if (!typesOnly) {
                    builder.Implementation(new TBuildQueryString(builder.GetSourcePosition(),
                                                                 firstArgTypeId));
                }
            } else {
                TStringBuilder sb;
                sb << "Unsupported first argument type: ";
                TTypePrinter(*typeHelper, firstArgType).Out(sb.Out);
                builder.SetError(sb);
            }
            return true;
        }
        return false;
    }
}
