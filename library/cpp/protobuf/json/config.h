#pragma once

#include "string_transform.h"
#include "name_generator.h"

#include <util/generic/vector.h>
#include <util/generic/yexception.h> 

#include <functional>

namespace NProtobufJson {
    struct TProto2JsonConfig {
        using TSelf = TProto2JsonConfig;

        bool FormatOutput = false;

        enum MissingKeyMode {
            // Skip missing keys
            MissingKeySkip = 0,
            // Fill missing keys with json null value.
            MissingKeyNull,
            // Use default value in any case.
            // If default value is not explicitly defined, use default type value:
            //     i.e. 0 for integers, "" for strings
            // For repeated keys, means []
            MissingKeyDefault,
            // Use default value if it is explicitly specified for optional fields.
            // Skip if no explicitly defined default value for optional fields.
            // Throw exception if required field is empty.
            // For repeated keys, same as MissingKeySkip
            MissingKeyExplicitDefaultThrowRequired
        };
        MissingKeyMode MissingSingleKeyMode = MissingKeySkip;
        MissingKeyMode MissingRepeatedKeyMode = MissingKeySkip;

        /// Add null value for missing fields (false by default).
        bool AddMissingFields = false;

        enum EnumValueMode {
            EnumNumber = 0, // default
            EnumName,
            EnumFullName,
            EnumNameLowerCase,
            EnumFullNameLowerCase,
        };
        EnumValueMode EnumMode = EnumNumber;

        enum FldNameMode {
            FieldNameOriginalCase = 0, // default
            FieldNameLowerCase,
            FieldNameUpperCase,
            FieldNameCamelCase,
            FieldNameSnakeCase,     // ABC -> a_b_c,    UserID -> user_i_d
            FieldNameSnakeCaseDense // ABC -> abc,      UserID -> user_id
        };
        FldNameMode FieldNameMode = FieldNameOriginalCase;

        enum ExtFldNameMode {
            ExtFldNameFull = 0, // default, field.full_name()
            ExtFldNameShort // field.name()
        };
        ExtFldNameMode ExtensionFieldNameMode = ExtFldNameFull;

        /// Use 'json_name' protobuf option for field name, mutually exclusive
        /// with FieldNameMode.
        bool UseJsonName = false;

        /// Transforms will be applied only to string values (== protobuf fields of string / bytes type).
        /// yajl_encode_string will be used if no transforms are specified.
        TVector<TStringTransformPtr> StringTransforms;
 
        /// Print map as object, otherwise print it as array of key/value objects
        bool MapAsObject = false;

        /// Stringify long integers which are not exactly representable by float or double values
        enum EStringifyLongNumbersMode {
            StringifyLongNumbersNever = 0, // default
            StringifyLongNumbersForFloat,
            StringifyLongNumbersForDouble,
        };
        EStringifyLongNumbersMode StringifyLongNumbers = StringifyLongNumbersNever;

        /// Custom field names generator.
        TNameGenerator NameGenerator = {};

        /// Custom enum values generator.
        TEnumValueGenerator EnumValueGenerator = {};

        bool WriteNanAsString = false;

        TSelf& SetFormatOutput(bool format) {
            FormatOutput = format;
            return *this;
        }

        TSelf& SetMissingSingleKeyMode(MissingKeyMode mode) {
            MissingSingleKeyMode = mode;
            return *this;
        }

        TSelf& SetMissingRepeatedKeyMode(MissingKeyMode mode) {
            MissingRepeatedKeyMode = mode;
            return *this;
        }

        TSelf& SetAddMissingFields(bool add) {
            AddMissingFields = add;
            return *this;
        }

        TSelf& SetEnumMode(EnumValueMode mode) {
            EnumMode = mode;
            return *this;
        }

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

        TSelf& SetExtensionFieldNameMode(ExtFldNameMode mode) {
            ExtensionFieldNameMode = mode;
            return *this;
        }

        TSelf& AddStringTransform(TStringTransformPtr transform) {
            StringTransforms.push_back(transform);
            return *this;
        }
 
        TSelf& SetMapAsObject(bool value) {
            MapAsObject = value;
            return *this;
        }

        TSelf& SetStringifyLongNumbers(EStringifyLongNumbersMode stringify) {
            StringifyLongNumbers = stringify;
            return *this;
        }

        TSelf& SetNameGenerator(TNameGenerator callback) {
            NameGenerator = callback;
            return *this;
        }

        TSelf& SetEnumValueGenerator(TEnumValueGenerator callback) {
            EnumValueGenerator = callback;
            return *this;
        }

        TSelf& SetWriteNanAsString(bool value) {
            WriteNanAsString = value;
            return *this;
        }
    };

}
