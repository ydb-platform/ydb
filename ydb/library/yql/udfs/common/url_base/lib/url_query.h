#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>

namespace NUrlUdf {
    using namespace NYql::NUdf;

    struct TQueryStringConv : public TBoxedValue {
    protected:
        static constexpr char Separator[] = "Separator";

        using TQueryStr = char*;
        using TSeparatorNArg = TNamedArg<TQueryStr, Separator>;

        static inline TType* GetListType(const IFunctionTypeInfoBuilder& builder,
                                         bool optional = false)
        {
            auto tupleType = optional ?
                             builder.Tuple()->Add<TQueryStr>().Add(builder.Optional()->Item<TQueryStr>().Build()).Build()
                           : builder.Tuple()->Add<TQueryStr>().Add<TQueryStr>().Build();
            return builder.List()->Item(tupleType).Build();
        }

        static inline TType* GetDictType(const IFunctionTypeInfoBuilder& builder,
                                         bool optional = false)
        {
            auto listType = optional ?
                            builder.List()->Item(builder.Optional()->Item<TQueryStr>().Build()).Build()
                          : builder.List()->Item<TQueryStr>().Build();
            return builder.Dict()->Key<TQueryStr>().Value(listType).Build();
        }

        static inline TType* GetFlattenDictType(const IFunctionTypeInfoBuilder& builder,
                                                bool optional = false)
        {
            return optional ?
                    builder.Dict()->Key<TQueryStr>().Value(builder.Optional()->Item<TQueryStr>().Build()).Build()
                  : builder.Dict()->Key<TQueryStr>().Value<TQueryStr>().Build();
        }
    };

    struct TQueryStringParse: public TQueryStringConv {
        explicit TQueryStringParse(TSourcePosition&& pos) : Pos_(std::move(pos)) {}

    protected:
        static constexpr char KeepBlankValues[] = "KeepBlankValues";
        static constexpr char Strict[] = "Strict";
        static constexpr char MaxFields[] = "MaxFields";

        using TKeepBlankValuesNArg = TNamedArg<bool, KeepBlankValues>;
        using TStrictNArg = TNamedArg<bool, Strict>;
        using TMaxFieldsNArg = TNamedArg<ui32, MaxFields>;

        static void MakeSignature(IFunctionTypeInfoBuilder& builder, const TType* retType);

        std::vector<std::pair<TString, TString>>
        RunImpl(const TUnboxedValuePod* args) const;

    private:
        TSourcePosition Pos_;
    };

    struct TQueryStringToList : public TQueryStringParse {
        explicit TQueryStringToList(TSourcePosition&& pos)
            : TQueryStringParse(std::forward<TSourcePosition>(pos)) {}

        static const TStringRef& Name() {
            static const auto name = TStringRef::Of("QueryStringToList");
            return name;
        }

        static bool DeclareSignature(const TStringRef& name,
                                     TType*,
                                     IFunctionTypeInfoBuilder& builder,
                                     bool typesOnly);

        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override;
    };

    struct TQueryStringToDict : public TQueryStringParse {
        explicit TQueryStringToDict(TType* dictType, TSourcePosition&& pos)
            : TQueryStringParse(std::move(pos))
            , DictType_(dictType)
            {}

        static const TStringRef& Name() {
            static const auto name = TStringRef::Of("QueryStringToDict");
            return name;
        }

        static bool DeclareSignature(const TStringRef& name,
                                     TType*,
                                     IFunctionTypeInfoBuilder& builder,
                                     bool typesOnly);

        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override;

    private:
        TType* DictType_;
    };

    class TBuildQueryString : public TQueryStringConv {
        TSourcePosition Pos_;
        enum class EFirstArgTypeId {
            None,
            Dict,
            FlattenDict,
            List,
        } FirstArgTypeId_;

    public:
        typedef bool TTypeAwareMarker;

        explicit TBuildQueryString(TSourcePosition&& pos, EFirstArgTypeId firstArgTypeId)
            : Pos_(std::move(pos))
            , FirstArgTypeId_(firstArgTypeId)
            {}

        static const TStringRef& Name() {
            static const auto name = TStringRef::Of("BuildQueryString");
            return name;
        }

        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override;

        static bool DeclareSignature(const TStringRef& name,
                                     TType* userType,
                                     IFunctionTypeInfoBuilder& builder,
                                     bool typesOnly);
    };
}
