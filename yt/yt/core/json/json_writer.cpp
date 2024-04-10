#include "json_writer.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/core/misc/utf8_decoder.h>

#include <contrib/libs/yajl/api/yajl_gen.h>

#include <cmath>
#include <iostream>

namespace NYT::NJson {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TJsonWriter
    : public IJsonWriter
{
public:
    TJsonWriter(IOutputStream* output, bool isPretty);
    virtual ~TJsonWriter() override;

    void Flush() override;
    void OnStringScalar(TStringBuf value) override;

    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;

    void OnBeginList() override;

    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;

    void OnKeyedItem(TStringBuf name) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;

    void OnEndAttributes() override;
    void OnRaw(TStringBuf yson, EYsonType type) override;

    void StartNextValue() override;

    ui64 GetWrittenByteCount() const override;

private:
    void GenerateString(TStringBuf value);
    TStringBuf GetBuffer() const;

private:
    yajl_gen Handle;
    IOutputStream* Output;
    ui64 WrittenToOutputByteCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENanInfinityMode,
    (NotSupported)
    (WriteInfinitiesUnquoted)
    (WriteAllQuouted)
);

class TJsonConsumer
    : public TYsonConsumerBase
    , public IJsonConsumer
{
public:
    TJsonConsumer(
        IJsonWriter* jsonWriter,
        EYsonType type,
        TJsonFormatConfigPtr config);

    TJsonConsumer(
        std::unique_ptr<IJsonWriter> jsonWriter,
        EYsonType type,
        TJsonFormatConfigPtr config);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;

    void OnEntity() override;

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

    void SetAnnotateWithTypesParameter(bool value) override;

    void OnStringScalarWeightLimited(TStringBuf value, std::optional<i64> weightLimit) override;
    void OnNodeWeightLimited(TStringBuf yson, std::optional<i64> weightLimit) override;

    void Flush() override;

private:
    void WriteStringScalar(TStringBuf value);
    void WriteStringScalarWithAttributes(TStringBuf value, TStringBuf type, bool incomplete);

    void EnterNode();
    void LeaveNode();
    bool IsWriteAllowed();

private:
    IJsonWriter* const JsonWriter;
    std::unique_ptr<IJsonWriter> JsonWriterHolder_;

    const EYsonType Type;
    const TJsonFormatConfigPtr Config;
    ENanInfinityMode NanInfinityMode_;

    TUtf8Transcoder Utf8Transcoder;

    std::vector<bool> HasUnfoldedStructureStack;
    int InAttributesBalance = 0;
    bool HasAttributes = false;
    int Depth = 0;
    bool CheckLimit = true;
};

////////////////////////////////////////////////////////////////////////////////

static void CheckYajlCode(int yajlCode)
{
    if (yajlCode == yajl_gen_status_ok) {
        return;
    }

    TString errorMessage;
    switch (yajlCode)
    {
        case yajl_gen_keys_must_be_strings:
            errorMessage = "JSON key must be a string";
            break;
        case yajl_max_depth_exceeded:
            errorMessage = Format("JSON maximal depth exceeded %v", YAJL_MAX_DEPTH);
            break;
        case yajl_gen_in_error_state:
            errorMessage = "JSON: a generator function (yajl_gen_XXX) was called while in an error state";
            break;
        case yajl_gen_generation_complete:
            errorMessage = "Attempt to alter already completed JSON document";
            break;
        case yajl_gen_invalid_number:
            errorMessage = "Invalid floating point value in JSON";
            break;
        case yajl_gen_invalid_string:
            errorMessage = "Invalid UTF-8 string in JSON";
            break;
        default:
            errorMessage = Format("Yajl writer failed with code %v", yajlCode);
    }
    THROW_ERROR_EXCEPTION(errorMessage);
}

TJsonWriter::TJsonWriter(IOutputStream* output, bool isPretty)
    : Output(output)
{
    Handle = yajl_gen_alloc(nullptr);
    yajl_gen_config(Handle, yajl_gen_beautify, isPretty ? 1 : 0);
    yajl_gen_config(Handle, yajl_gen_skip_final_newline, 0);

    yajl_gen_config(Handle, yajl_gen_support_infinity, 1);

    yajl_gen_config(Handle, yajl_gen_disable_yandex_double_format, 1);
    yajl_gen_config(Handle, yajl_gen_validate_utf8, 1);
}

TJsonWriter::~TJsonWriter()
{
    yajl_gen_free(Handle);
}

void TJsonWriter::GenerateString(TStringBuf value)
{
    CheckYajlCode(yajl_gen_string(Handle, (const unsigned char*) value.data(), value.size()));
}

TStringBuf TJsonWriter::GetBuffer() const
{
    size_t len = 0;
    const unsigned char* buf = nullptr;
    CheckYajlCode(yajl_gen_get_buf(Handle, &buf, &len));
    return TStringBuf(static_cast<const char*>(static_cast<const void*>(buf)), len);
}

void TJsonWriter::Flush()
{
    auto buf = GetBuffer();
    Output->Write(buf);
    WrittenToOutputByteCount_ += buf.Size();
    yajl_gen_clear(Handle);
}

void TJsonWriter::StartNextValue()
{
    Flush();
    yajl_gen_reset(Handle, nullptr);
    Output->Write('\n');
}

void TJsonWriter::OnBeginMap()
{
    CheckYajlCode(yajl_gen_map_open(Handle));
}

void TJsonWriter::OnKeyedItem(TStringBuf name)
{
    GenerateString(name);
}

void TJsonWriter::OnEndMap()
{
    CheckYajlCode(yajl_gen_map_close(Handle));
}

void TJsonWriter::OnBeginList()
{
    CheckYajlCode(yajl_gen_array_open(Handle));
}

void TJsonWriter::OnListItem()
{ }

void TJsonWriter::OnEndList()
{
    CheckYajlCode(yajl_gen_array_close(Handle));
}

void TJsonWriter::OnStringScalar(TStringBuf value)
{
    GenerateString(value);
}

void TJsonWriter::OnEntity()
{
    CheckYajlCode(yajl_gen_null(Handle));
}

void TJsonWriter::OnDoubleScalar(double value)
{
    CheckYajlCode(yajl_gen_double(Handle, value));
}

void TJsonWriter::OnInt64Scalar(i64 value)
{
    CheckYajlCode(yajl_gen_integer(Handle, value));
}

void TJsonWriter::OnUint64Scalar(ui64 value)
{
    CheckYajlCode(yajl_gen_uinteger(Handle, value));
}

void TJsonWriter::OnBooleanScalar(bool value)
{
    CheckYajlCode(yajl_gen_bool(Handle, value ? 1 : 0));
}

void TJsonWriter::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("TJsonWriter does not support attributes");
}

void TJsonWriter::OnEndAttributes()
{
    THROW_ERROR_EXCEPTION("TJsonWriter does not support attributes");
}

void TJsonWriter::OnRaw(TStringBuf /*yson*/, NYT::NYson::EYsonType /*type*/)
{
    THROW_ERROR_EXCEPTION("TJsonWriter does not support OnRaw()");
}

ui64 TJsonWriter::GetWrittenByteCount() const
{
    return GetBuffer().Size() + WrittenToOutputByteCount_;
}

////////////////////////////////////////////////////////////////////////////////

TJsonConsumer::TJsonConsumer(
    IJsonWriter* jsonWriter,
    EYsonType type,
    TJsonFormatConfigPtr config)
    : JsonWriter(jsonWriter)
    , Type(type)
    , Config(std::move(config))
    , Utf8Transcoder(Config->EncodeUtf8)
{
    if (Type == EYsonType::MapFragment) {
        THROW_ERROR_EXCEPTION("Map fragments are not supported by JSON");
    }

    NanInfinityMode_ = ENanInfinityMode::NotSupported;
    if (Config->SupportInfinity) {
        NanInfinityMode_ = ENanInfinityMode::WriteInfinitiesUnquoted;
    } else if (Config->StringifyNanAndInfinity) {
        NanInfinityMode_ = ENanInfinityMode::WriteAllQuouted;
    }
}

TJsonConsumer::TJsonConsumer(
    std::unique_ptr<IJsonWriter> jsonWriter,
    EYsonType type,
    TJsonFormatConfigPtr config)
    : TJsonConsumer(jsonWriter.get(), type, std::move(config))
{
    JsonWriterHolder_ = std::move(jsonWriter);
}

void TJsonConsumer::EnterNode()
{
    if (Config->AttributesMode == EJsonAttributesMode::Never) {
        HasAttributes = false;
    } else if (Config->AttributesMode == EJsonAttributesMode::OnDemand) {
        // Do nothing
    } else if (Config->AttributesMode == EJsonAttributesMode::Always) {
        if (!HasAttributes) {
            JsonWriter->OnBeginMap();
            JsonWriter->OnKeyedItem(TStringBuf("$attributes"));
            JsonWriter->OnBeginMap();
            JsonWriter->OnEndMap();
            HasAttributes = true;
        }
    }
    HasUnfoldedStructureStack.push_back(HasAttributes);

    if (HasAttributes) {
        JsonWriter->OnKeyedItem(TStringBuf("$value"));
        HasAttributes = false;
    }

    Depth += 1;
}

void TJsonConsumer::LeaveNode()
{
    YT_VERIFY(!HasUnfoldedStructureStack.empty());
    if (HasUnfoldedStructureStack.back()) {
        // Close map of the {$attributes, $value}
        JsonWriter->OnEndMap();
    }
    HasUnfoldedStructureStack.pop_back();

    Depth -= 1;

    if (Depth == 0 && Type == EYsonType::ListFragment && InAttributesBalance == 0) {
        JsonWriter->StartNextValue();
    }
}

bool TJsonConsumer::IsWriteAllowed()
{
    if (Config->AttributesMode == EJsonAttributesMode::Never) {
        return InAttributesBalance == 0;
    }
    return true;
}

void TJsonConsumer::OnStringScalar(TStringBuf value)
{
    TStringBuf writeValue = value;
    bool incomplete = false;
    if (Config->AttributesMode != EJsonAttributesMode::Never) {
        if (CheckLimit && Config->StringLengthLimit && std::ssize(value) > *Config->StringLengthLimit) {
            writeValue = value.substr(0, *Config->StringLengthLimit);
            incomplete = true;
        }
    }

    WriteStringScalarWithAttributes(writeValue, TStringBuf("string"), incomplete);
}

void TJsonConsumer::OnInt64Scalar(i64 value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->OnBeginMap();
                HasAttributes = true;
            }
            JsonWriter->OnKeyedItem(TStringBuf("$type"));
            JsonWriter->OnStringScalar(TStringBuf("int64"));
        }
        EnterNode();
        if (Config->Stringify) {
            WriteStringScalar(::ToString(value));
        } else {
            JsonWriter->OnInt64Scalar(value);
        }
        LeaveNode();
    }
}

void TJsonConsumer::OnUint64Scalar(ui64 value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->OnBeginMap();
                HasAttributes = true;
            }
            JsonWriter->OnKeyedItem(TStringBuf("$type"));
            JsonWriter->OnStringScalar(TStringBuf("uint64"));
        }
        EnterNode();
        if (Config->Stringify) {
            WriteStringScalar(::ToString(value));
        } else {
            JsonWriter->OnUint64Scalar(value);
        }
        LeaveNode();

    }
}

void TJsonConsumer::OnDoubleScalar(double value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->OnBeginMap();
                HasAttributes = true;
            }
            JsonWriter->OnKeyedItem(TStringBuf("$type"));
            JsonWriter->OnStringScalar(TStringBuf("double"));
        }
        EnterNode();
        if (Config->Stringify) {
            char buf[256];
            auto str = TStringBuf(buf, FloatToString(value, buf, sizeof(buf)));
            WriteStringScalar(str);
        } else {
            switch (NanInfinityMode_) {
                case ENanInfinityMode::WriteAllQuouted:
                    if (std::isnan(value)) {
                        JsonWriter->OnStringScalar(TStringBuf("nan"));
                    } else if (std::isinf(value)) {
                        if (value < 0) {
                            JsonWriter->OnStringScalar(TStringBuf("-inf"));
                        } else {
                            JsonWriter->OnStringScalar(TStringBuf("inf"));
                        }
                    } else {
                        JsonWriter->OnDoubleScalar(value);
                    }
                    break;
                case ENanInfinityMode::WriteInfinitiesUnquoted:
                    if (std::isnan(value)) {
                        THROW_ERROR_EXCEPTION(
                            "Unexpected NaN encountered during JSON writing; "
                            "consider \"stringify_nan_and_infinity\" config option");
                    }
                    JsonWriter->OnDoubleScalar(value);
                    break;
                case ENanInfinityMode::NotSupported:
                    if (std::isnan(value) || std::isinf(value)) {
                        THROW_ERROR_EXCEPTION(
                            "Unexpected NaN or infinity encountered during JSON writing; "
                            "consider using either \"support_infinity\" or \"stringify_nan_and_infinity\" config options");
                    }
                    JsonWriter->OnDoubleScalar(value);
                    break;
            }
        }
        LeaveNode();
    }
}

void TJsonConsumer::OnBooleanScalar(bool value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->OnBeginMap();
                HasAttributes = true;
            }
            JsonWriter->OnKeyedItem(TStringBuf("$type"));
            JsonWriter->OnStringScalar(TStringBuf("boolean"));
        }
        EnterNode();
        if (Config->Stringify) {
            WriteStringScalar(FormatBool(value));
        } else {
            JsonWriter->OnBooleanScalar(value);
        }
        LeaveNode();
    }
}

void TJsonConsumer::OnEntity()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->OnEntity();
        LeaveNode();
    }
}

void TJsonConsumer::OnBeginList()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->OnBeginList();
    }
}

void TJsonConsumer::OnListItem()
{
    if (IsWriteAllowed()) {
        JsonWriter->OnListItem();
    }
}

void TJsonConsumer::OnEndList()
{
    if (IsWriteAllowed()) {
        JsonWriter->OnEndList();
        LeaveNode();
    }
}

void TJsonConsumer::OnBeginMap()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->OnBeginMap();
    }
}

void TJsonConsumer::OnKeyedItem(TStringBuf name)
{
    if (IsWriteAllowed()) {
        if (IsSpecialJsonKey(name)) {
            JsonWriter->OnKeyedItem(Utf8Transcoder.Encode(TString("$") + name));
        } else {
            JsonWriter->OnKeyedItem(Utf8Transcoder.Encode(name));
        }
    }
}

void TJsonConsumer::OnEndMap()
{
    if (IsWriteAllowed()) {
        JsonWriter->OnEndMap();
        LeaveNode();
    }
}

void TJsonConsumer::OnBeginAttributes()
{
    InAttributesBalance += 1;
    if (Config->AttributesMode != EJsonAttributesMode::Never) {
        JsonWriter->OnBeginMap();
        JsonWriter->OnKeyedItem(TStringBuf("$attributes"));
        JsonWriter->OnBeginMap();
    }
}

void TJsonConsumer::OnEndAttributes()
{
    InAttributesBalance -= 1;
    if (Config->AttributesMode != EJsonAttributesMode::Never) {
        JsonWriter->OnEndMap();
        HasAttributes = true;
    }
}

void TJsonConsumer::Flush()
{
    JsonWriter->Flush();
}

void TJsonConsumer::WriteStringScalar(TStringBuf value)
{
    JsonWriter->OnStringScalar(Utf8Transcoder.Encode(value));
}

void TJsonConsumer::WriteStringScalarWithAttributes(
    TStringBuf value,
    TStringBuf type,
    bool incomplete)
{
    if (IsWriteAllowed()) {
        if (Config->AttributesMode != EJsonAttributesMode::Never) {
            if (incomplete) {
                if (!HasAttributes) {
                    JsonWriter->OnBeginMap();
                    HasAttributes = true;
                }

                JsonWriter->OnKeyedItem(TStringBuf("$incomplete"));
                JsonWriter->OnBooleanScalar(true);
            }

            if (Config->AnnotateWithTypes) {
                if (!HasAttributes) {
                    JsonWriter->OnBeginMap();
                    HasAttributes = true;
                }

                JsonWriter->OnKeyedItem(TStringBuf("$type"));
                JsonWriter->OnStringScalar(type);
            }
        }

        EnterNode();
        WriteStringScalar(value);
        LeaveNode();
    }
}

void TJsonConsumer::SetAnnotateWithTypesParameter(bool value)
{
    Config->AnnotateWithTypes = value;
}

void TJsonConsumer::OnStringScalarWeightLimited(TStringBuf value, std::optional<i64> weightLimit)
{
    TStringBuf writeValue = value;
    bool incomplete = false;
    if (CheckLimit && weightLimit && std::ssize(value) > *weightLimit) {
        writeValue = value.substr(0, *weightLimit);
        incomplete = true;
    }

    WriteStringScalarWithAttributes(writeValue, TStringBuf("string"), incomplete);
}

void TJsonConsumer::OnNodeWeightLimited(TStringBuf yson, std::optional<i64> weightLimit)
{
    if (CheckLimit && weightLimit && std::ssize(yson) > *weightLimit) {
        WriteStringScalarWithAttributes({}, TStringBuf("any"), true);
        return;
    }

    OnRaw(yson, EYsonType::Node);
}

std::unique_ptr<IJsonConsumer> CreateJsonConsumer(
    IOutputStream* output,
    EYsonType type,
    TJsonFormatConfigPtr config)
{
    auto jsonWriter = CreateJsonWriter(
        output,
        /*pretty*/ config->Format == EJsonFormat::Pretty);
    return std::make_unique<TJsonConsumer>(std::move(jsonWriter), type, std::move(config));
}

std::unique_ptr<IJsonConsumer> CreateJsonConsumer(
    IJsonWriter* jsonWriter,
    EYsonType type,
    TJsonFormatConfigPtr config)
{
    return std::make_unique<TJsonConsumer>(jsonWriter, type, std::move(config));
}

std::unique_ptr<IJsonWriter> CreateJsonWriter(
    IOutputStream* output,
    bool pretty)
{
    return std::make_unique<TJsonWriter>(output, pretty);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJson
