#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <util/generic/yexception.h>
#include <vector>
#include <array>

using namespace NKikimr;
using namespace NUdf;

namespace {

//////////////////////////////////////////////////////////////////////////////
// TNumbersList
//////////////////////////////////////////////////////////////////////////////
class TNumbers: public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("Numbers");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        const auto appendPrepend = args[0].AsStringRef();
        const auto count = args[1].Get<ui32>();
        std::vector<TUnboxedValue> list(count);
        ui32 i = 0U;
        if (TStringRef::Of("Append") == appendPrepend) {
            for (auto it = list.begin(); list.end() != it; ++it) {
                *it = TUnboxedValuePod(i++);
            }
        }
        else if (TStringRef::Of("Prepend") == appendPrepend) {
            for (auto it = list.rbegin(); list.rend() != it; ++it) {
                *it = TUnboxedValuePod(i++);
            }
        }

        return valueBuilder->NewList(list.data(), list.size());
    }
};

//////////////////////////////////////////////////////////////////////////////
// TExtend
//////////////////////////////////////////////////////////////////////////////
class TExtend: public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("Extend");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        std::array<TUnboxedValue, 2U> list = {{TUnboxedValuePod(args[0]), TUnboxedValuePod(args[1])}};
        return valueBuilder->NewList(list.data(), list.size());
    }
};

//////////////////////////////////////////////////////////////////////////////
// TListsModule
//////////////////////////////////////////////////////////////////////////////
class TListsModule: public IUdfModule
{
public:
    TStringRef Name() const {
        return TStringRef::Of("Lists");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TNumbers::Name());
        sink.Add(TExtend::Name());
    }

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final
    {
        try {
            Y_UNUSED(userType);
            Y_UNUSED(typeConfig);

            bool typesOnly = (flags & TFlags::TypesOnly);

            if (TNumbers::Name() == name) {
                // function signature:
                //    List<ui32> Numbers(String, ui32)
                // runConfig: void
                builder.SimpleSignature<TListType<ui32>(char*, ui32)>();

                if (!typesOnly) {
                    builder.Implementation(new TNumbers);
                }
            }
            else if (TExtend::Name() == name) {
                // function signature:
                //    List<ui32> Numbers(List<ui32>, List<ui32>)
                // runConfig: void
                auto listType = builder.List()->Item<ui32>().Build();
                builder.Returns(listType)
                        .Args()->Add(listType).Add(listType).Done();

                if (!typesOnly) {
                    builder.Implementation(new TExtend);
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TListsModule)
