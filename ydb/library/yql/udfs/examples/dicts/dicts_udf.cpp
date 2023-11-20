#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <util/generic/yexception.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

//////////////////////////////////////////////////////////////////////////////
// TStrToInt
//////////////////////////////////////////////////////////////////////////////
class TStrToInt: public TBoxedValue
{
public:
    explicit TStrToInt(TType* dictType)
        : DictType_(dictType)
    {
    }

    static TStringRef Name() {
        static auto name = TStringRef::Of("StrToInt");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        auto kind = args[0].AsStringRef();

        ui32 flags = 0;
        if (TStringRef::Of("Hashed") == kind) {
            flags |= TDictFlags::Hashed;
        } else if (TStringRef::Of("Sorted") == kind) {
            flags |= TDictFlags::Sorted;
        }

        return valueBuilder->NewDict(DictType_, flags)->
             Add(valueBuilder->NewString("zero"), TUnboxedValuePod((ui32) 0))
            .Add(valueBuilder->NewString("one"), TUnboxedValuePod((ui32) 1))
            .Add(valueBuilder->NewString("two"), TUnboxedValuePod((ui32) 2))
            .Add(valueBuilder->NewString("three"), TUnboxedValuePod((ui32) 3))
            .Add(valueBuilder->NewString("four"), TUnboxedValuePod((ui32) 4))
            .Add(valueBuilder->NewString("five"), TUnboxedValuePod((ui32) 5))
            .Add(valueBuilder->NewString("six"), TUnboxedValuePod((ui32) 6))
            .Add(valueBuilder->NewString("seven"), TUnboxedValuePod((ui32) 7))
            .Add(valueBuilder->NewString("eight"), TUnboxedValuePod((ui32) 8))
            .Add(valueBuilder->NewString("nine"), TUnboxedValuePod((ui32) 9))
            .Build();
    }

    TType* DictType_;
};

//////////////////////////////////////////////////////////////////////////////
// TDictsModule
//////////////////////////////////////////////////////////////////////////////
class TDictsModule: public IUdfModule
{
public:
    TStringRef Name() const {
        return TStringRef::Of("Dicts");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TStrToInt::Name());
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

            TType* dictType = builder.Dict()->Key<char*>().Value<ui32>().Build();

            if (TStrToInt::Name() == name) {
                // function signature:
                //    Dict<String, ui32> New(String)
                // runConfig: void
                builder.Returns(dictType).Args()->Add<char*>().Done();

                if (!typesOnly) {
                    builder.Implementation(new TStrToInt(dictType));
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TDictsModule)
