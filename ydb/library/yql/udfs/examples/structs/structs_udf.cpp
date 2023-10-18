#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

struct TPersonInfo
{
    ui32 FirstName = 0;
    ui32 LastName = 0;
    ui32 Age = 0;
    bool RemapKSV = false;
    ui32 Key = 0;
    ui32 Subkey = 0;
    ui32 Value = 0;
    static constexpr ui32 FieldsCount = 3U;
};

//////////////////////////////////////////////////////////////////////////////
// TPersonMember
//////////////////////////////////////////////////////////////////////////////
class TPersonMember: public TBoxedValue
{
public:
    explicit TPersonMember(ui32 memberIndex)
        : MemberIndex_(memberIndex)
    {
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        Y_UNUSED(valueBuilder);
        return args[0].GetElement(MemberIndex_);
    }

    const ui32 MemberIndex_;
};

//////////////////////////////////////////////////////////////////////////////
// TNewPerson
//////////////////////////////////////////////////////////////////////////////
class TNewPerson: public TBoxedValue
{
public:
    explicit TNewPerson(const TPersonInfo& personIndexes)
        : Info_(personIndexes)
    {
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        TUnboxedValue name, surname, age;
        if (Info_.RemapKSV) {
            name = args->GetElement(Info_.Key);
            surname = args->GetElement(Info_.Subkey);
            const auto ageStr = args->GetElement(Info_.Value);
            const ui32 ageNum = FromString(ageStr.AsStringRef().Data(), ageStr.AsStringRef().Size());
            age = TUnboxedValuePod(ageNum);
        } else {
            name = TUnboxedValuePod(args[0]);
            surname = TUnboxedValuePod(args[1]);
            age = TUnboxedValuePod(args[2]);
        }
        TUnboxedValue* items = nullptr;
        auto result = valueBuilder->NewArray(Info_.FieldsCount, items);
        items[Info_.FirstName] = std::move(name);
        items[Info_.LastName] = std::move(surname);
        items[Info_.Age] = std::move(age);
        return result;
    }

    const TPersonInfo Info_;
};

//////////////////////////////////////////////////////////////////////////////
// TPersonModule
//////////////////////////////////////////////////////////////////////////////
class TPersonModule: public IUdfModule
{
public:
    TStringRef Name() const {
        return TStringRef::Of("Person");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TStringRef::Of("FirstName"));
        sink.Add(TStringRef::Of("LastName"));
        sink.Add(TStringRef::Of("Age"));
        sink.Add(TStringRef::Of("New"))->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final
    {
        Y_UNUSED(userType);
        try {
            bool typesOnly = (flags & TFlags::TypesOnly);

            TPersonInfo personInfo;
            auto personType = builder.Struct(personInfo.FieldsCount)->
                    AddField<char*>("FirstName", &personInfo.FirstName)
                    .AddField<char*>("LastName", &personInfo.LastName)
                    .AddField<ui32>("Age", &personInfo.Age)
                    .Build();

            if (TStringRef::Of("FirstName") == name) {
                // function signature: String FirstName(PersonStruct p)
                // runConfig: void
                builder.Returns<char*>().Args()->Add(personType).Done();

                if (!typesOnly) {
                    builder.Implementation(new TPersonMember(personInfo.FirstName));
                }
            }
            else if (TStringRef::Of("LastName") == name) {
                // function signature: String LastName(PersonStruct p)
                // runConfig: void
                builder.Returns<char*>().Args()->Add(personType).Done();

                if (!typesOnly) {
                    builder.Implementation(new TPersonMember(personInfo.LastName));
                }
            }
            else if (TStringRef::Of("Age") == name) {
                // function signature: ui32 Age(PersonStruct p)
                // runConfig: void
                builder.Returns<ui32>().Args()->Add(personType).Done();

                if (!typesOnly) {
                    builder.Implementation(new TPersonMember(personInfo.Age));
                }
            }
            else if (TStringRef::Of("New") == name) {
                // function signature:
                //    PersonStruct New(String firstName, String lastName, ui32 age)
                // runConfig: void
                builder.Returns(personType);
                if (TStringRef::Of("RemapKSV") == typeConfig) {
                    personInfo.RemapKSV = true;
                    auto inputType = builder.Struct(personInfo.FieldsCount)->
                        AddField<char*>("key", &personInfo.Key)
                        .AddField<char*>("subkey", &personInfo.Subkey)
                        .AddField<char*>("value", &personInfo.Value)
                        .Build();
                    builder.Args()->Add(inputType);
                } else {
                    builder.Args()->Add<char*>().Add<char*>().Add<ui32>();
                }
                builder.RunConfig<void>(); // this is optional because default runConfigType is Void

                if (!typesOnly) {
                    builder.Implementation(new TNewPerson(personInfo));
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TPersonModule)
