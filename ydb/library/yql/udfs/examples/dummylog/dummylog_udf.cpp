#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <util/generic/yexception.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

struct TRecordInfo
{
    ui32 Key;
    ui32 Subkey;
    ui32 Value;
    static constexpr ui32 FieldsCount = 3U;
};


//////////////////////////////////////////////////////////////////////////////
// TDummyLog
//////////////////////////////////////////////////////////////////////////////
class TDummyLog: public TBoxedValue
{
public:
    explicit TDummyLog(
            const TRecordInfo& fieldIndexes)
        : RecordInfo_(fieldIndexes)
    {
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        auto keyData = args[0].GetElement(RecordInfo_.Key);
        auto subkeyData = args[0].GetElement(RecordInfo_.Subkey);
        auto valueData = args[0].GetElement(RecordInfo_.Value);

        TString key = TString("Key: ") + keyData.AsStringRef();
        TString subkey = TString("Subkey: ") + subkeyData.AsStringRef();
        TString value = TString("Value: ") + valueData.AsStringRef();

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(RecordInfo_.FieldsCount, items);
        items[RecordInfo_.Key] = valueBuilder->NewString(key);
        items[RecordInfo_.Subkey] = valueBuilder->NewString(subkey);
        items[RecordInfo_.Value] = valueBuilder->NewString(value);
        return res;
    }

    const TRecordInfo RecordInfo_;
};

class TDummyLog2 : public TBoxedValue
{
public:
    class TFactory : public TBoxedValue {
    public:
        TFactory(const TRecordInfo& inputInfo, const TRecordInfo& outputInfo)
            : InputInfo_(inputInfo)
            , OutputInfo_(outputInfo)
        {}

private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
        {
            Y_UNUSED(valueBuilder);
            return TUnboxedValuePod(new TDummyLog2(args[0], InputInfo_, OutputInfo_));
        }

        const TRecordInfo InputInfo_;
        const TRecordInfo OutputInfo_;
    };

    explicit TDummyLog2(
        const TUnboxedValuePod& runConfig,
        const TRecordInfo& inputInfo,
        const TRecordInfo& outputInfo
    )
        : Prefix_(runConfig.AsStringRef())
        , InputInfo_(inputInfo)
        , OutputInfo_(outputInfo)
    {
    }

private:
    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const override
    {
        auto keyData = args[0].GetElement(InputInfo_.Key);
        auto valueData = args[0].GetElement(InputInfo_.Value);

        TString key = Prefix_ + TString("Key: ") + keyData.AsStringRef();
        TString value = Prefix_ + TString("Value: ") + valueData.AsStringRef();

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(2U, items);
        items[OutputInfo_.Key] = valueBuilder->NewString(key);
        items[OutputInfo_.Value] = valueBuilder->NewString(value);
        return res;
    }

    const TString Prefix_;
    const TRecordInfo InputInfo_;
    const TRecordInfo OutputInfo_;
};

//////////////////////////////////////////////////////////////////////////////
// TDummyLogModule
//////////////////////////////////////////////////////////////////////////////
class TDummyLogModule: public IUdfModule
{
public:
    TStringRef Name() const {
        return TStringRef::Of("DummyLog");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TStringRef::Of("ReadRecord"));
        sink.Add(TStringRef::Of("ReadRecord2"))->SetTypeAwareness();
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

            if (TStringRef::Of("ReadRecord") == name) {
                TRecordInfo recordInfo;
                auto recordType = builder.Struct(recordInfo.FieldsCount)->
                        AddField<char*>("key", &recordInfo.Key)
                        .AddField<char*>("subkey", &recordInfo.Subkey)
                        .AddField<char*>("value", &recordInfo.Value)
                        .Build();

                builder.Returns(recordType).Args()->Add(recordType).Done();

                if (!typesOnly) {
                    builder.Implementation(new TDummyLog(recordInfo));
                }
            }

            if (TStringRef::Of("ReadRecord2") == name) {
                if (TStringBuf(typeConfig) != TStringBuf("AAA")) {
                    builder.SetError(TStringRef::Of("Only AAA is valid type config"));
                }
                TRecordInfo inputInfo;
                auto inputType = builder.Struct(inputInfo.FieldsCount)->
                    AddField<char*>("key", &inputInfo.Key)
                    .AddField<char*>("subkey", &inputInfo.Subkey)
                    .AddField<char*>("value", &inputInfo.Value)
                    .Build();

                TRecordInfo outputInfo;
                auto outputType = builder.Struct(2U)->
                    AddField<char*>("key", &outputInfo.Key)
                    .AddField<char*>("value", &outputInfo.Value)
                    .Build();


                builder.Returns(outputType).Args()->Add(inputType).Done();
                builder.RunConfig<char*>();

                if (!typesOnly) {
                    builder.Implementation(new TDummyLog2::TFactory(inputInfo, outputInfo));
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TDummyLogModule)
