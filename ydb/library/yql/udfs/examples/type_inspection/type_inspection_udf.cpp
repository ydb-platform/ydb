#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <util/generic/yexception.h>


using namespace NKikimr;
using namespace NUdf;

namespace {

//////////////////////////////////////////////////////////////////////////////
// TZip
//////////////////////////////////////////////////////////////////////////////
class TZip: public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("Zip");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        const auto it1 = args[0].GetListIterator();
        const auto it2 = args[1].GetListIterator();

        std::vector<TUnboxedValue> list;
        if (args[0].HasFastListLength() && args[1].HasFastListLength())
            list.reserve(std::min(args[0].GetListLength(), args[1].GetListLength()));
        for (TUnboxedValue one, two, *items = nullptr; it1.Next(one) && it2.Next(two);) {
            auto tuple = valueBuilder->NewArray(2U, items);
            items[0] = std::move(one);
            items[1] = std::move(two);
            list.emplace_back(std::move(tuple));
        }

        return valueBuilder->NewList(list.data(), list.size());
    }
};

//////////////////////////////////////////////////////////////////////////////
// TFold
//////////////////////////////////////////////////////////////////////////////
class TFold : public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("Fold");
        return name;
    }

private:
    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const override
    {
        const auto it = args[0].GetListIterator();
        TUnboxedValue state = TUnboxedValuePod(args[1]);
        auto func = args[2];
        for (TUnboxedValue item; it.Next(item);) {
            const TUnboxedValue args[2] = {std::move(item), std::move(state)};
            state = func.Run(valueBuilder, args);
        }

        return state;
    }
};

//////////////////////////////////////////////////////////////////////////////
// TInterleave
//////////////////////////////////////////////////////////////////////////////
class TInterleave : public TBoxedValue
{
public:
    class TValue : public TBoxedValue {
    public:
        TValue(const IValueBuilder* valueBuilder, const TUnboxedValuePod& left, const TUnboxedValuePod& right)
            : ValueBuilder_(valueBuilder)
        {
            Streams_[0] = TUnboxedValuePod(left);
            Streams_[1] = TUnboxedValuePod(right);
        }

    private:
        EFetchStatus Fetch(TUnboxedValue& value) override {
            auto status1 = Streams_[LastFetchIndex_].Fetch(value);
            if (status1 == EFetchStatus::Ok) {
                value = ValueBuilder_->NewVariant(LastFetchIndex_, std::move(value));
                LastFetchIndex_ ^= 1;
                return status1;
            }

            // either yield or finish
            LastFetchIndex_ ^= 1;
            auto status2 = Streams_[LastFetchIndex_].Fetch(value);
            if (status2 == EFetchStatus::Ok) {
                value = ValueBuilder_->NewVariant(LastFetchIndex_, std::move(value));
                LastFetchIndex_ ^= 1;
            }

            return status2;
        }

        const IValueBuilder* ValueBuilder_;
        TUnboxedValue Streams_[2];
        ui32 LastFetchIndex_ = 0;
    };

    static TStringRef Name() {
        static auto name = TStringRef::Of("Interleave");
        return name;
    }

private:
    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const override
    {
        return TUnboxedValuePod(new TValue(valueBuilder, args[0], args[1]));
    }
};

//////////////////////////////////////////////////////////////////////////////
// TTypeInspectionModule
//////////////////////////////////////////////////////////////////////////////
class TTypeInspectionModule: public IUdfModule
{
public:
    TStringRef Name() const {
        return TStringRef::Of("TypeInspection");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TZip::Name())->SetTypeAwareness();
        sink.Add(TFold::Name())->SetTypeAwareness();
        sink.Add(TInterleave::Name())->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final
    {
        try {
            Y_UNUSED(typeConfig);

            bool typesOnly = (flags & TFlags::TypesOnly);

            if (TZip::Name() == name) {
                // function signature:
                //      (List<T1>, List<T2>)->List<Tuple<T1, T2>>
                // user type: Tuple<Tuple<List<T1>, List<T2>>, Struct<>, Tuple<>>
                // run config: void

                auto typeHelper = builder.TypeInfoHelper();
                auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
                auto argsTypeTuple = userTypeInspector.GetElementType(0);
                auto tuple = TTupleTypeInspector(*typeHelper, argsTypeTuple);
                bool isOk = false;
                if (tuple && tuple.GetElementsCount() == 2) {
                    auto type1 = TListTypeInspector(*typeHelper, tuple.GetElementType(0));
                    auto type2 = TListTypeInspector(*typeHelper, tuple.GetElementType(1));
                    if (type1 && type2) {
                        auto zipItem = builder.Tuple(2)->Add(type1.GetItemType()).Add(type2.GetItemType()).Build();
                        builder.UserType(userType).Returns(builder.List()->Item(zipItem).Build());

                        auto argsBuilder = builder.Args(2);
                        argsBuilder->Add(tuple.GetElementType(0));
                        argsBuilder->Add(tuple.GetElementType(1));
                        argsBuilder->Done();
                        isOk = true;
                    }
                }

                if (!isOk) {
                    builder.SetError("Expected user type is: Tuple<List<T1>, List<T2>>");
                    return;
                }

                if (!typesOnly) {
                    builder.Implementation(new TZip);
                }
            }

            if (TFold::Name() == name) {
                // function signature:
                //      (List<T1>, T2, Callable<(T1, T2)->T2>)->T2
                // user type: Tuple<Tuple<List<T1>, T2, Callable<(T1, T2)->T2>>, Struct<>, Tuple<>>
                // run config: void

                auto typeHelper = builder.TypeInfoHelper();
                auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
                auto argsTypeTuple = userTypeInspector.GetElementType(0);
                auto tuple = TTupleTypeInspector(*typeHelper, argsTypeTuple);
                bool isOk = false;
                if (tuple && tuple.GetElementsCount() == 3) {
                    auto type1 = TListTypeInspector(*typeHelper, tuple.GetElementType(0));
                    auto stateType = tuple.GetElementType(1);
                    auto type3 = TCallableTypeInspector(*typeHelper, tuple.GetElementType(2));
                    if (type1 && type3) {
                        auto itemType = type1.GetItemType();
                        if (type3.GetArgsCount() == 2) {
                            auto callableArg1 = type3.GetArgType(0);
                            auto callableArg2 = type3.GetArgType(1);
                            auto callableRet = type3.GetReturnType();
                            if (typeHelper->IsSameType(callableArg1, itemType) &&
                                typeHelper->IsSameType(callableArg2, stateType) &&
                                typeHelper->IsSameType(callableRet, stateType)) {
                                auto argsBuilder = builder.Args(3);
                                argsBuilder->Add(tuple.GetElementType(0));
                                argsBuilder->Add(tuple.GetElementType(1));
                                argsBuilder->Add(tuple.GetElementType(2));
                                argsBuilder->Done();

                                builder.UserType(userType).Returns(tuple.GetElementType(1));
                                isOk = true;
                            }
                        }
                    }
                }

                if (!isOk) {
                    builder.SetError("Expected user type is: Tuple<List<T1>, T2, Callable<(T1, T2)->T2>>");
                    return;
                }

                if (!typesOnly) {
                    builder.Implementation(new TFold);
                }
            }

            if (TInterleave::Name() == name) {
                // function signature:
                //      (Stream<T1>, Stream<T2>)->Stream<Variant<T1, T2>>
                // user type: Tuple<Tuple<Stream<T1>, Stream<T2>>, Struct<>, Tuple<>>
                // run config: void

                auto typeHelper = builder.TypeInfoHelper();
                auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
                auto argsTypeTuple = userTypeInspector.GetElementType(0);
                auto tuple = TTupleTypeInspector(*typeHelper, argsTypeTuple);
                bool isOk = false;
                if (tuple && tuple.GetElementsCount() == 2) {
                    auto type1 = TStreamTypeInspector(*typeHelper, tuple.GetElementType(0));
                    auto type2 = TStreamTypeInspector(*typeHelper, tuple.GetElementType(1));
                    if (type1 && type2) {
                        auto tupleType = builder.Tuple(2)->Add(type1.GetItemType()).Add(type2.GetItemType()).Build();
                        auto retItem = builder.Variant()->Over(tupleType).Build();
                        builder.UserType(userType).Returns(builder.Stream()->Item(retItem).Build());

                        auto argsBuilder = builder.Args(2);
                        argsBuilder->Add(tuple.GetElementType(0));
                        argsBuilder->Add(tuple.GetElementType(1));
                        argsBuilder->Done();
                        isOk = true;
                    }
                }

                if (!isOk) {
                    builder.SetError("Expected user type is: Tuple<Stream<T1>, Stream<T2>>");
                    return;
                }

                if (!typesOnly) {
                    builder.Implementation(new TInterleave());
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TTypeInspectionModule)
