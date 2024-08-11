#include "json_writer.h"

#include "helpers.h"

#include <yt/yt/core/misc/utf8_decoder.h>

#include <contrib/libs/yajl/api/yajl_gen.h>

#include <cmath>

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
    yajl_gen Handle_;
    IOutputStream* Output_;
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
    IJsonWriter* const JsonWriter_;
    std::unique_ptr<IJsonWriter> JsonWriterHolder_;

    const EYsonType Type_;
    const TJsonFormatConfigPtr Config_;
    ENanInfinityMode NanInfinityMode_;

    TUtf8Transcoder Utf8Transcoder_;

    std::vector<bool> HasUnfoldedStructureStack_;
    int InAttributesBalance_ = 0;
    bool HasAttributes_ = false;
    int Depth_ = 0;
    bool CheckLimit_ = true;
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
    : Output_(output)
{
    Handle_ = yajl_gen_alloc(nullptr);
    yajl_gen_config(Handle_, yajl_gen_beautify, isPretty ? 1 : 0);
    yajl_gen_config(Handle_, yajl_gen_skip_final_newline, 0);

    yajl_gen_config(Handle_, yajl_gen_support_infinity, 1);

    yajl_gen_config(Handle_, yajl_gen_disable_yandex_double_format, 1);
    yajl_gen_config(Handle_, yajl_gen_validate_utf8, 1);
}

TJsonWriter::~TJsonWriter()
{
    yajl_gen_free(Handle_);
}

void TJsonWriter::GenerateString(TStringBuf value)
{
    CheckYajlCode(yajl_gen_string(Handle_, (const unsigned char*) value.data(), value.size()));
}

TStringBuf TJsonWriter::GetBuffer() const
{
    size_t len = 0;
    const unsigned char* buf = nullptr;
    CheckYajlCode(yajl_gen_get_buf(Handle_, &buf, &len));
    return TStringBuf(static_cast<const char*>(static_cast<const void*>(buf)), len);
}

void TJsonWriter::Flush()
{
    auto buf = GetBuffer();
    Output_->Write(buf);
    WrittenToOutputByteCount_ += buf.Size();
    yajl_gen_clear(Handle_);
}

void TJsonWriter::StartNextValue()
{
    Flush();
    yajl_gen_reset(Handle_, nullptr);
    Output_->Write('\n');
}

void TJsonWriter::OnBeginMap()
{
    CheckYajlCode(yajl_gen_map_open(Handle_));
}

void TJsonWriter::OnKeyedItem(TStringBuf name)
{
    GenerateString(name);
}

void TJsonWriter::OnEndMap()
{
    CheckYajlCode(yajl_gen_map_close(Handle_));
}

void TJsonWriter::OnBeginList()
{
    CheckYajlCode(yajl_gen_array_open(Handle_));
}

void TJsonWriter::OnListItem()
{ }

void TJsonWriter::OnEndList()
{
    CheckYajlCode(yajl_gen_array_close(Handle_));
}

void TJsonWriter::OnStringScalar(TStringBuf value)
{
    GenerateString(value);
}

void TJsonWriter::OnEntity()
{
    CheckYajlCode(yajl_gen_null(Handle_));
}

void TJsonWriter::OnDoubleScalar(double value)
{
    CheckYajlCode(yajl_gen_double(Handle_, value));
}

void TJsonWriter::OnInt64Scalar(i64 value)
{
    CheckYajlCode(yajl_gen_integer(Handle_, value));
}

void TJsonWriter::OnUint64Scalar(ui64 value)
{
    CheckYajlCode(yajl_gen_uinteger(Handle_, value));
}

void TJsonWriter::OnBooleanScalar(bool value)
{
    CheckYajlCode(yajl_gen_bool(Handle_, value ? 1 : 0));
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
    : JsonWriter_(jsonWriter)
    , Type_(type)
    , Config_(std::move(config))
    , Utf8Transcoder_(Config_->EncodeUtf8)
{
    if (Type_ == EYsonType::MapFragment) {
        THROW_ERROR_EXCEPTION("Map fragments are not supported by JSON");
    }

    NanInfinityMode_ = ENanInfinityMode::NotSupported;
    if (Config_->SupportInfinity) {
        NanInfinityMode_ = ENanInfinityMode::WriteInfinitiesUnquoted;
    } else if (Config_->StringifyNanAndInfinity) {
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
    if (Config_->AttributesMode == EJsonAttributesMode::Never) {
        HasAttributes_ = false;
    } else if (Config_->AttributesMode == EJsonAttributesMode::OnDemand) {
        // Do nothing
    } else if (Config_->AttributesMode == EJsonAttributesMode::Always) {
        if (!HasAttributes_) {
            JsonWriter_->OnBeginMap();
            JsonWriter_->OnKeyedItem(TStringBuf("$attributes"));
            JsonWriter_->OnBeginMap();
            JsonWriter_->OnEndMap();
            HasAttributes_ = true;
        }
    }
    HasUnfoldedStructureStack_.push_back(HasAttributes_);

    if (HasAttributes_) {
        JsonWriter_->OnKeyedItem(TStringBuf("$value"));
        HasAttributes_ = false;
    }

    Depth_ += 1;
}

void TJsonConsumer::LeaveNode()
{
    YT_VERIFY(!HasUnfoldedStructureStack_.empty());
    if (HasUnfoldedStructureStack_.back()) {
        // Close map of the {$attributes, $value}
        JsonWriter_->OnEndMap();
    }
    HasUnfoldedStructureStack_.pop_back();

    Depth_ -= 1;

    if (Depth_ == 0 && Type_ == EYsonType::ListFragment && InAttributesBalance_ == 0) {
        JsonWriter_->StartNextValue();
    }
}

bool TJsonConsumer::IsWriteAllowed()
{
    if (Config_->AttributesMode == EJsonAttributesMode::Never) {
        return InAttributesBalance_ == 0;
    }
    return true;
}

void TJsonConsumer::OnStringScalar(TStringBuf value)
{
    TStringBuf writeValue = value;
    bool incomplete = false;
    if (Config_->AttributesMode != EJsonAttributesMode::Never) {
        if (CheckLimit_ && Config_->StringLengthLimit && std::ssize(value) > *Config_->StringLengthLimit) {
            writeValue = value.substr(0, *Config_->StringLengthLimit);
            incomplete = true;
        }
    }

    WriteStringScalarWithAttributes(writeValue, TStringBuf("string"), incomplete);
}

void TJsonConsumer::OnInt64Scalar(i64 value)
{
    if (IsWriteAllowed()) {
        if (Config_->AnnotateWithTypes && Config_->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes_) {
                JsonWriter_->OnBeginMap();
                HasAttributes_ = true;
            }
            JsonWriter_->OnKeyedItem(TStringBuf("$type"));
            JsonWriter_->OnStringScalar(TStringBuf("int64"));
        }
        EnterNode();
        if (Config_->Stringify) {
            WriteStringScalar(::ToString(value));
        } else {
            JsonWriter_->OnInt64Scalar(value);
        }
        LeaveNode();
    }
}

void TJsonConsumer::OnUint64Scalar(ui64 value)
{
    if (IsWriteAllowed()) {
        if (Config_->AnnotateWithTypes && Config_->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes_) {
                JsonWriter_->OnBeginMap();
                HasAttributes_ = true;
            }
            JsonWriter_->OnKeyedItem(TStringBuf("$type"));
            JsonWriter_->OnStringScalar(TStringBuf("uint64"));
        }
        EnterNode();
        if (Config_->Stringify) {
            WriteStringScalar(::ToString(value));
        } else {
            JsonWriter_->OnUint64Scalar(value);
        }
        LeaveNode();

    }
}

void TJsonConsumer::OnDoubleScalar(double value)
{
    if (IsWriteAllowed()) {
        if (Config_->AnnotateWithTypes && Config_->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes_) {
                JsonWriter_->OnBeginMap();
                HasAttributes_ = true;
            }
            JsonWriter_->OnKeyedItem(TStringBuf("$type"));
            JsonWriter_->OnStringScalar(TStringBuf("double"));
        }
        EnterNode();
        if (Config_->Stringify) {
            char buf[256];
            auto str = TStringBuf(buf, FloatToString(value, buf, sizeof(buf)));
            WriteStringScalar(str);
        } else {
            switch (NanInfinityMode_) {
                case ENanInfinityMode::WriteAllQuouted:
                    if (std::isnan(value)) {
                        JsonWriter_->OnStringScalar(TStringBuf("nan"));
                    } else if (std::isinf(value)) {
                        if (value < 0) {
                            JsonWriter_->OnStringScalar(TStringBuf("-inf"));
                        } else {
                            JsonWriter_->OnStringScalar(TStringBuf("inf"));
                        }
                    } else {
                        JsonWriter_->OnDoubleScalar(value);
                    }
                    break;
                case ENanInfinityMode::WriteInfinitiesUnquoted:
                    if (std::isnan(value)) {
                        THROW_ERROR_EXCEPTION(
                            "Unexpected NaN encountered during JSON writing; "
                            "consider \"stringify_nan_and_infinity\" config option");
                    }
                    JsonWriter_->OnDoubleScalar(value);
                    break;
                case ENanInfinityMode::NotSupported:
                    if (std::isnan(value) || std::isinf(value)) {
                        THROW_ERROR_EXCEPTION(
                            "Unexpected NaN or infinity encountered during JSON writing; "
                            "consider using either \"support_infinity\" or \"stringify_nan_and_infinity\" config options");
                    }
                    JsonWriter_->OnDoubleScalar(value);
                    break;
            }
        }
        LeaveNode();
    }
}

void TJsonConsumer::OnBooleanScalar(bool value)
{
    if (IsWriteAllowed()) {
        if (Config_->AnnotateWithTypes && Config_->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes_) {
                JsonWriter_->OnBeginMap();
                HasAttributes_ = true;
            }
            JsonWriter_->OnKeyedItem(TStringBuf("$type"));
            JsonWriter_->OnStringScalar(TStringBuf("boolean"));
        }
        EnterNode();
        if (Config_->Stringify) {
            WriteStringScalar(FormatBool(value));
        } else {
            JsonWriter_->OnBooleanScalar(value);
        }
        LeaveNode();
    }
}

void TJsonConsumer::OnEntity()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter_->OnEntity();
        LeaveNode();
    }
}

void TJsonConsumer::OnBeginList()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter_->OnBeginList();
    }
}

void TJsonConsumer::OnListItem()
{
    if (IsWriteAllowed()) {
        JsonWriter_->OnListItem();
    }
}

void TJsonConsumer::OnEndList()
{
    if (IsWriteAllowed()) {
        JsonWriter_->OnEndList();
        LeaveNode();
    }
}

void TJsonConsumer::OnBeginMap()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter_->OnBeginMap();
    }
}

void TJsonConsumer::OnKeyedItem(TStringBuf name)
{
    if (IsWriteAllowed()) {
        if (IsSpecialJsonKey(name)) {
            JsonWriter_->OnKeyedItem(Utf8Transcoder_.Encode(TString("$") + name));
        } else {
            JsonWriter_->OnKeyedItem(Utf8Transcoder_.Encode(name));
        }
    }
}

void TJsonConsumer::OnEndMap()
{
    if (IsWriteAllowed()) {
        JsonWriter_->OnEndMap();
        LeaveNode();
    }
}

void TJsonConsumer::OnBeginAttributes()
{
    InAttributesBalance_ += 1;
    if (Config_->AttributesMode != EJsonAttributesMode::Never) {
        JsonWriter_->OnBeginMap();
        JsonWriter_->OnKeyedItem(TStringBuf("$attributes"));
        JsonWriter_->OnBeginMap();
    }
}

void TJsonConsumer::OnEndAttributes()
{
    InAttributesBalance_ -= 1;
    if (Config_->AttributesMode != EJsonAttributesMode::Never) {
        JsonWriter_->OnEndMap();
        HasAttributes_ = true;
    }
}

void TJsonConsumer::Flush()
{
    JsonWriter_->Flush();
}

void TJsonConsumer::WriteStringScalar(TStringBuf value)
{
    JsonWriter_->OnStringScalar(Utf8Transcoder_.Encode(value));
}

void TJsonConsumer::WriteStringScalarWithAttributes(
    TStringBuf value,
    TStringBuf type,
    bool incomplete)
{
    if (IsWriteAllowed()) {
        if (Config_->AttributesMode != EJsonAttributesMode::Never) {
            if (incomplete) {
                if (!HasAttributes_) {
                    JsonWriter_->OnBeginMap();
                    HasAttributes_ = true;
                }

                JsonWriter_->OnKeyedItem(TStringBuf("$incomplete"));
                JsonWriter_->OnBooleanScalar(true);
            }

            if (Config_->AnnotateWithTypes) {
                if (!HasAttributes_) {
                    JsonWriter_->OnBeginMap();
                    HasAttributes_ = true;
                }

                JsonWriter_->OnKeyedItem(TStringBuf("$type"));
                JsonWriter_->OnStringScalar(type);
            }
        }

        EnterNode();
        WriteStringScalar(value);
        LeaveNode();
    }
}

void TJsonConsumer::SetAnnotateWithTypesParameter(bool value)
{
    Config_->AnnotateWithTypes = value;
}

void TJsonConsumer::OnStringScalarWeightLimited(TStringBuf value, std::optional<i64> weightLimit)
{
    TStringBuf writeValue = value;
    bool incomplete = false;
    if (CheckLimit_ && weightLimit && std::ssize(value) > *weightLimit) {
        writeValue = value.substr(0, *weightLimit);
        incomplete = true;
    }

    WriteStringScalarWithAttributes(writeValue, TStringBuf("string"), incomplete);
}

void TJsonConsumer::OnNodeWeightLimited(TStringBuf yson, std::optional<i64> weightLimit)
{
    if (CheckLimit_ && weightLimit && std::ssize(yson) > *weightLimit) {
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
