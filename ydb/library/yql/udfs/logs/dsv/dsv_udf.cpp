#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include <util/generic/yexception.h>
#include <library/cpp/deprecated/split/split_iterator.h>
#include <util/string/vector.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

struct TKsvIndexes
{
    ui32 key;
    ui32 subkey;
    ui32 value;
};

struct TResultIndexes
{
    TType* DictType;

    ui32 key;
    ui32 subkey;
    ui32 dict;
    static constexpr ui32 FieldsCount = 3U;
};

void ParseDsv(const TUnboxedValuePod& value,
              const std::string_view& separator,
              const IValueBuilder* valueBuilder,
              IDictValueBuilder* builder) {
    const std::string_view input(value.AsStringRef());
    const std::vector<std::string_view> parts = StringSplitter(input).SplitByString(separator);
    for (const auto& part : parts) {
        const auto pos = part.find('=');
        if (std::string_view::npos != pos) {
            const auto from = std::distance(input.begin(), part.begin());
            builder->Add(
                valueBuilder->SubString(value, from, pos),
                valueBuilder->SubString(value, from + pos + 1U, part.length() - pos - 1U)
            );
        }
    }
}

class TDsvReadRecord: public TBoxedValue
{
public:
    class TFactory : public TBoxedValue {
    public:
        TFactory(const TResultIndexes& fieldIndexes,
                 const TKsvIndexes& ksvIndexes)
        : ResultIndexes_(fieldIndexes)
        , KsvIndexes_(ksvIndexes)
        {
        }
    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try
        {
            const auto optRunConfig = args[0];
            TUnboxedValue separator;
            if (optRunConfig && !optRunConfig.AsStringRef().Empty()) {
                separator = optRunConfig;
            } else {
                separator = valueBuilder->NewString("\t");
            }

            return TUnboxedValuePod(new TDsvReadRecord(separator, ResultIndexes_, KsvIndexes_));
        }
        catch (const std::exception& e) {
            UdfTerminate(e.what());
        }

        const TResultIndexes ResultIndexes_;
        const TKsvIndexes KsvIndexes_;
    };

    explicit TDsvReadRecord(const TUnboxedValue& separator,
                            const TResultIndexes& fieldIndexes,
                            const TKsvIndexes& ksvIndexes)
        : Separator_(std::move(separator))
        , ResultIndexes_(fieldIndexes)
        , KsvIndexes_(ksvIndexes)
    {
    }
private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try
    {
        auto keyData = args[0].GetElement(KsvIndexes_.key);
        auto subkeyData = args[0].GetElement(KsvIndexes_.subkey);
        auto valueData = args[0].GetElement(KsvIndexes_.value);


        auto dict = valueBuilder->NewDict(ResultIndexes_.DictType, 0);

        ParseDsv(valueData, Separator_.AsStringRef(), valueBuilder, dict.Get());

        TUnboxedValue* items = nullptr;
        const auto result = valueBuilder->NewArray(ResultIndexes_.FieldsCount, items);
        items[ResultIndexes_.key] = keyData;
        items[ResultIndexes_.subkey] = subkeyData;
        items[ResultIndexes_.dict] = dict->Build();
        return result;
    }
    catch (const std::exception& e) {
        UdfTerminate(e.what());
    }

    const TUnboxedValue Separator_;
    const TResultIndexes ResultIndexes_;
    const TKsvIndexes KsvIndexes_;
};

class TDsvParse: public TBoxedValue
{
public:
    explicit TDsvParse(TType* dictType)
        : DictType(dictType)
    {}
private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try
    {
        const std::string_view separator = args[1] ?
             std::string_view(args[1].AsStringRef()):
             std::string_view("\t");

        auto dict = valueBuilder->NewDict(DictType, 0);
        ParseDsv(args[0], separator, valueBuilder, dict.Get());
        return dict->Build();
    }
    catch (const std::exception& e) {
        UdfTerminate(e.what());
    }

    const TType* DictType;
};

#define TYPE_TO_STRING(type) \
case TDataType<type>::Id: part += ToString(member.Get<type>()); break;

class TDsvSerialize: public TBoxedValue
{
public:
    explicit TDsvSerialize(const TVector<TDataTypeId>& typeIds, TStructTypeInspector* structInspector)
        : TypeIds(typeIds)
        , StructInspector(structInspector)
    {}

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final try
    {
        TVector<TString> result;
        if (const ui32 structSize = StructInspector->GetMembersCount()) {
            result.reserve(structSize);
            for (ui32 i = 0; i < structSize; ++i) {
                auto part = TString(StructInspector->GetMemberName(i));
                part += '=';
                const TUnboxedValue& member = args[0].GetElement(i);
                switch (TypeIds[i]) {
                    TYPE_TO_STRING(i32)
                    TYPE_TO_STRING(ui32)
                    TYPE_TO_STRING(i64)
                    TYPE_TO_STRING(ui64)
                    TYPE_TO_STRING(ui8)
                    TYPE_TO_STRING(bool)
                    TYPE_TO_STRING(double)
                    TYPE_TO_STRING(float)
                    default:
                        part += member.AsStringRef();
                        break;

                }
                result.emplace_back(std::move(part));
            }
        }
        return valueBuilder->NewString(JoinStrings(result, "\t"));
    }
    catch (const std::exception& e) {
        UdfTerminate(e.what());
    }

    const TVector<TDataTypeId> TypeIds;
    THolder<TStructTypeInspector> StructInspector;
};

class TDsvModule: public IUdfModule
{
public:
    TStringRef Name() const {
        return TStringRef::Of("Dsv");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TStringRef::Of("ReadRecord"));
        sink.Add(TStringRef::Of("Parse"));
        sink.Add(TStringRef::Of("Serialize"))->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final try
    {
        Y_UNUSED(typeConfig);

        bool typesOnly = (flags & TFlags::TypesOnly);

        if (TStringRef::Of("ReadRecord") == name) {
            TKsvIndexes ksvIndexes;
            auto recordType = builder.Struct(3U)->
                    AddField<char*>("key", &ksvIndexes.key)
                    .AddField<char*>("subkey", &ksvIndexes.subkey)
                    .AddField<char*>("value", &ksvIndexes.value)
                    .Build();

            TResultIndexes resultIndexes;
            resultIndexes.DictType = builder.Dict()->Key<char*>().Value<char*>().Build();
            const auto structType = builder.Struct(resultIndexes.FieldsCount)
                    ->AddField<char*>("key", &resultIndexes.key)
                    .AddField<char*>("subkey", &resultIndexes.subkey)
                    .AddField("dict", resultIndexes.DictType, &resultIndexes.dict)
                    .Build();

            builder.Returns(structType)
                    .Args()->Add(recordType).Done()
                    .RunConfig<TOptional<char*>>();

            if (!typesOnly) {
                builder.Implementation(new TDsvReadRecord::TFactory(
                        resultIndexes, ksvIndexes));
            }
            builder.IsStrict();
        } else if (TStringRef::Of("Parse") == name) {
            auto optionalStringType = builder.Optional()->Item<char*>().Build();
            auto dictType = builder.Dict()->Key<char*>().Value<char*>().Build();

            builder.Returns(dictType)
                    .Args()->Add<char*>().Flags(ICallablePayload::TArgumentFlags::AutoMap).Add(optionalStringType).Done()
                    .OptionalArgs(1);

            if (!typesOnly) {
                builder.Implementation(new TDsvParse(dictType));
            }
            builder.IsStrict();
        } else if (TStringRef::Of("Serialize") == name) {
            auto typeHelper = builder.TypeInfoHelper();
            auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
                builder.SetError("Expected user type");
                return;
            }
            auto argsTypeTuple = userTypeInspector.GetElementType(0);
            auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!(argsTypeInspector && argsTypeInspector.GetElementsCount() == 1)) {
                builder.SetError("Only one argument is expected " + ToString(argsTypeInspector.GetElementsCount()));
                return;
            }

            TVector<TDataTypeId> typeIds;
            const auto structType = argsTypeInspector.GetElementType(0);
            THolder<TStructTypeInspector> structInspector(new TStructTypeInspector(*typeHelper, structType));
            if (structInspector) {
                ui32 memberCount = structInspector->GetMembersCount();
                typeIds.reserve(memberCount);

                if (memberCount) {
                    for (ui32 i = 0; i < memberCount; ++i) {
                        const TString memberName(structInspector->GetMemberName(i));
                        const auto memberType = structInspector->GetMemberType(i);
                        auto memberInspector = TDataTypeInspector(*typeHelper, memberType);
                        if (!memberInspector) {
                            builder.SetError("Only DataType members are supported at the moment, failed at " + memberName);
                            return;
                        }
                        typeIds.push_back(memberInspector.GetTypeId());
                    }
                } else {
                    builder.SetError("Zero members in input Struct");
                    return;
                }
            } else {
                builder.SetError("Only Structs are supported at the moment");
                return;
            }

            builder.UserType(userType).Returns<char*>().Args()->Add(structType).Done();

            if (!typesOnly) {
                builder.Implementation(new TDsvSerialize(typeIds, structInspector.Release()));
            }
            builder.IsStrict();

        }
    } catch (const std::exception& e) {
        builder.SetError(CurrentExceptionMessage());
    }
};

} // namespace

REGISTER_MODULES(TDsvModule)
