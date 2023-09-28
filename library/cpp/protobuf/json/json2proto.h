#pragma once

#include "string_transform.h"
#include "name_generator.h"
#include "unknown_fields_collector.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/generic/ptr.h>
#include <util/stream/input.h>
#include <util/stream/str.h>
#include <util/stream/mem.h>

namespace google {
    namespace protobuf {
        class Message;
    }
}

namespace NProtobufJson {
    struct TJson2ProtoConfig {
        using TSelf = TJson2ProtoConfig;
        using TValueVectorizer = std::function<NJson::TJsonValue::TArray(const NJson::TJsonValue& jsonValue)>;

        enum FldNameMode {
            FieldNameOriginalCase = 0, // default
            FieldNameLowerCase,
            FieldNameUpperCase,
            FieldNameCamelCase,
            FieldNameSnakeCase,     // ABC -> a_b_c,    UserID -> user_i_d
            FieldNameSnakeCaseDense // ABC -> abc,      UserID -> user_id
        };

        enum EnumValueMode {
            EnumCaseSensetive = 0, // default
            EnumCaseInsensetive,
            EnumSnakeCaseInsensitive
        };

        TSelf& SetFieldNameMode(FldNameMode mode) {
            Y_ENSURE(mode == FieldNameOriginalCase || !UseJsonName, "FieldNameMode and UseJsonName are mutually exclusive");
            FieldNameMode = mode;
            return *this;
        }

        TSelf& SetUseJsonName(bool jsonName) {
            Y_ENSURE(!jsonName || FieldNameMode == FieldNameOriginalCase, "FieldNameMode and UseJsonName are mutually exclusive");
            UseJsonName = jsonName;
            return *this;
        }

        TSelf& SetUseJsonEnumValue(bool jsonEnumValue) {
            Y_ENSURE(!jsonEnumValue || EnumValueMode == EnumCaseSensetive, "EnumValueMode and UseJsonEnumValue are mutually exclusive");
            UseJsonEnumValue = jsonEnumValue;
            return *this;
        }

        TSelf& AddStringTransform(TStringTransformPtr transform) {
            StringTransforms.push_back(transform);
            return *this;
        }

        TSelf& SetCastFromString(bool cast) {
            CastFromString = cast;
            return *this;
        }

        TSelf& SetDoNotCastEmptyStrings(bool cast) {
            DoNotCastEmptyStrings = cast;
            return *this;
        }

        TSelf& SetCastRobust(bool cast) {
            CastRobust = cast;
            return *this;
        }

        TSelf& SetMapAsObject(bool mapAsObject) {
            MapAsObject = mapAsObject;
            return *this;
        }

        TSelf& SetReplaceRepeatedFields(bool replaceRepeatedFields) {
            ReplaceRepeatedFields = replaceRepeatedFields;
            return *this;
        }

        TSelf& SetNameGenerator(TNameGenerator callback) {
            NameGenerator = callback;
            return *this;
        }

        TSelf& SetEnumValueMode(EnumValueMode enumValueMode) {
            Y_ENSURE(!UseJsonEnumValue || enumValueMode == EnumCaseSensetive, "EnumValueMode and UseJsonEnumValue are mutually exclusive");
            EnumValueMode = enumValueMode;
            return *this;
        }

        TSelf& SetVectorizeScalars(bool vectorizeScalars) {
            VectorizeScalars = vectorizeScalars;
            return *this;
        }

        TSelf& SetAllowComments(bool value) {
            AllowComments = value;
            return *this;
        }

        TSelf& SetAllowUnknownFields(bool value) {
            AllowUnknownFields = value;
            return *this;
        }

        TSelf& SetAllowString2TimeConversion(bool value) {
            AllowString2TimeConversion = value;
            return *this;
        }

        TSelf& SetUnknownFieldsCollector(TSimpleSharedPtr<IUnknownFieldsCollector> value) {
            UnknownFieldsCollector = std::move(value);
            return *this;
        }

        FldNameMode FieldNameMode = FieldNameOriginalCase;
        bool AllowUnknownFields = true;

        /// Use 'json_name' protobuf option for field name, mutually exclusive
        /// with FieldNameMode.
        bool UseJsonName = false;

        /// Use 'json_enum_value' protobuf option for enum value, mutually exclusive
        /// with EnumValueMode
        bool UseJsonEnumValue = false;

        /// Transforms will be applied only to string values (== protobuf fields of string / bytes type).
        TVector<TStringTransformPtr> StringTransforms;

        /// Cast string json values to protobuf field type
        bool CastFromString = false;
        /// Skip empty strings, instead casting from string into scalar types.
        /// I.e. empty string like default value for scalar types.
        bool DoNotCastEmptyStrings = false;
        /// Cast all json values to protobuf field types
        bool CastRobust = false;

        /// Consider map to be an object, otherwise consider it to be an array of key/value objects
        bool MapAsObject = false;

        /// Throw exception if there is no required fields in json object.
        bool CheckRequiredFields = true;

        /// Replace repeated fields content during merging
        bool ReplaceRepeatedFields = false;

        /// Custom field names generator.
        TNameGenerator NameGenerator = {};

        /// Enum value parsing mode.
        EnumValueMode EnumValueMode = EnumCaseSensetive;

        /// Append scalars to repeated fields
        bool VectorizeScalars = false;

        /// Custom spliter non array value to repeated fields.
        TValueVectorizer ValueVectorizer;

        /// Allow js-style comments (both // and /**/)
        bool AllowComments = false;

        /// Allow nonstandard conversions, e.g. google.protobuf.Duration from String
        bool AllowString2TimeConversion = false;

        /// Stores information about unknown fields
        TSimpleSharedPtr<IUnknownFieldsCollector> UnknownFieldsCollector = nullptr;
    };

    /// @throw yexception
    void MergeJson2Proto(const NJson::TJsonValue& json, google::protobuf::Message& proto,
                    const TJson2ProtoConfig& config = TJson2ProtoConfig());

    /// @throw yexception
    void MergeJson2Proto(const TStringBuf& json, google::protobuf::Message& proto,
                    const TJson2ProtoConfig& config = TJson2ProtoConfig());

    /// @throw yexception
    inline void MergeJson2Proto(const TString& json, google::protobuf::Message& proto,
                           const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        MergeJson2Proto(TStringBuf(json), proto, config);
    }

    /// @throw yexception
    void Json2Proto(const NJson::TJsonValue& json, google::protobuf::Message& proto,
                    const TJson2ProtoConfig& config = TJson2ProtoConfig());

    /// @throw yexception
    void Json2Proto(const TStringBuf& json, google::protobuf::Message& proto,
                    const TJson2ProtoConfig& config = TJson2ProtoConfig());

    /// @throw yexception
    inline void Json2Proto(const TString& json, google::protobuf::Message& proto,
                           const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        Json2Proto(TStringBuf(json), proto, config);
    }

    /// @throw yexception
    inline void Json2Proto(IInputStream& in, google::protobuf::Message& proto,
                           const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        Json2Proto(TStringBuf(in.ReadAll()), proto, config);
    }

    /// @throw yexception
    template <typename T>
    T Json2Proto(IInputStream& in, const NJson::TJsonReaderConfig& readerConfig,
                 const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        NJson::TJsonValue jsonValue;
        NJson::ReadJsonTree(&in, &readerConfig, &jsonValue, true);
        T protoValue;
        Json2Proto(jsonValue, protoValue, config);
        return protoValue;
    }

    /// @throw yexception
    template <typename T>
    T Json2Proto(IInputStream& in, const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        // NOTE: TJson2ProtoConfig.AllowComments=true doesn't work, when using TJsonReaderConfig
        NJson::TJsonReaderConfig readerConfig;
        readerConfig.DontValidateUtf8 = true;
        return Json2Proto<T>(in, readerConfig, config);
    }

    /// @throw yexception
    template <typename T>
    T Json2Proto(const TString& value, const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        return Json2Proto<T>(TStringBuf(value), config);
    }

    /// @throw yexception
    template <typename T>
    T Json2Proto(const TStringBuf& value, const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        T protoValue;
        Json2Proto(value, protoValue, config);
        return protoValue;
    }

    /// @throw yexception
    template <typename T>
    T Json2Proto(const char* ptr, const TJson2ProtoConfig& config = TJson2ProtoConfig()) {
        return Json2Proto<T>(TStringBuf(ptr), config);
    }

}
