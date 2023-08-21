#pragma once

#include "public.h"

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TControlAttributesConfig
    : public NTableClient::TChunkReaderOptions
{
public:
    bool EnableKeySwitch;

    bool EnableEndOfStream;

    REGISTER_YSON_STRUCT(TControlAttributesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControlAttributesConfig)

////////////////////////////////////////////////////////////////////////////////

class TYsonFormatConfig
    : public NTableClient::TTypeConversionConfig
{
public:
    NYson::EYsonFormat Format;
    EComplexTypeMode ComplexTypeMode;
    EDictMode StringKeyedDictMode;
    EDecimalMode DecimalMode;
    ETimeMode TimeMode;
    EUuidMode UuidMode;

    //! Only works for tabular data.
    bool SkipNullValues;

    REGISTER_YSON_STRUCT(TYsonFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////
// Readers for Yamr and Dsv share lots of methods and functionality                          //
// and dependency diagram has the following shape:                                           //
//                                                                                           //
//                    TTableFormatConfigBase --------------------------.                     //
//                      /                 \                             \                    //
//                     /                   \                             \                   //
//       TYamrFormatConfigBase        TDsvFormatConfigBase                \                  //
//            /        \                   /            \                  \                 //
//           /          \                 /              \                  \                //
// TYamrFormatConfig   TYamredDsvFormatConfig   TDsvFormatConfig  TSchemafulDsvFormatConfig  //
//                                                                                           //
// All fields are declared in Base classes, all parameters are                               //
// registered in derived classes.                                                            //

class TTableFormatConfigBase
    : public NTableClient::TTypeConversionConfig
{
public:
    char RecordSeparator;
    char FieldSeparator;

    // Escaping rules (EscapingSymbol is '\\')
    //  * '\0' ---> "\0"
    //  * '\n' ---> "\n"
    //  * '\t' ---> "\t"
    //  * 'X'  ---> "\X" if X not in ['\0', '\n', '\t']
    bool EnableEscaping;
    char EscapingSymbol;

    bool EnableTableIndex;

    REGISTER_YSON_STRUCT(TTableFormatConfigBase);

    static void Register(TRegistrar )
    { }
};

DEFINE_REFCOUNTED_TYPE(TTableFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TYamrFormatConfigBase
    : public virtual TTableFormatConfigBase
{
public:
    bool HasSubkey;
    bool Lenval;
    bool EnableEom;

    REGISTER_YSON_STRUCT(TYamrFormatConfigBase);

    static void Register(TRegistrar )
    { }
};

DEFINE_REFCOUNTED_TYPE(TYamrFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfigBase
    : public virtual TTableFormatConfigBase
{
public:
    char KeyValueSeparator;

    // Only supported for tabular data
    std::optional<TString> LinePrefix;

    REGISTER_YSON_STRUCT(TDsvFormatConfigBase);

    static void Register(TRegistrar )
    { }
};

DEFINE_REFCOUNTED_TYPE(TDsvFormatConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TYamrFormatConfig
    : public TYamrFormatConfigBase
{
public:
    TString Key;
    TString Subkey;
    TString Value;

    REGISTER_YSON_STRUCT(TYamrFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYamrFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TDsvFormatConfig
    : public TDsvFormatConfigBase
{
public:

    TString TableIndexColumn;
    bool SkipUnsupportedTypes = false;

    REGISTER_YSON_STRUCT(TDsvFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDsvFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TYamredDsvFormatConfig
    : public TYamrFormatConfigBase
    , public TDsvFormatConfigBase
{
public:
    char YamrKeysSeparator;

    std::vector<TString> KeyColumnNames;
    std::vector<TString> SubkeyColumnNames;

    bool SkipUnsupportedTypesInValue = false;

    REGISTER_YSON_STRUCT(TYamredDsvFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYamredDsvFormatConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMissingSchemafulDsvValueMode,
    (SkipRow)
    (Fail)
    (PrintSentinel)
);

class TSchemafulDsvFormatConfig
    : public TTableFormatConfigBase
{
public:
    std::optional<std::vector<TString>> Columns;

    EMissingSchemafulDsvValueMode MissingValueMode;
    TString MissingValueSentinel;

    std::optional<bool> EnableColumnNamesHeader;

    const std::vector<TString>& GetColumnsOrThrow() const;

    REGISTER_YSON_STRUCT(TSchemafulDsvFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchemafulDsvFormatConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProtobufType,
    (Double)
    (Float)

    (Int64)
    (Uint64)
    (Sint64)
    (Fixed64)
    (Sfixed64)

    (Int32)
    (Uint32)
    (Sint32)
    (Fixed32)
    (Sfixed32)

    (Bool)
    (String)
    (Bytes)

    (EnumInt)
    (EnumString)

    // Same as 'bytes'.
    (Message)

    // Protobuf type must be message.
    // It corresponds to struct type.
    (StructuredMessage)

    // Protobuf type must be message.
    // It corresponds to a set of table columns.
    (EmbeddedMessage)

    // Corresponds to variant struct type.
    (Oneof)

    // Protobuf type must be string.
    // Maps to any scalar type (not necessarily "any" type) in table row.
    (Any)

    // Protobuf type must be string containing valid YSON map.
    // Each entry (|key|, |value|) of this map will correspond
    // a separate |TUnversionedValue| under name |key|.
    // NOTE: Not allowed inside complex types.
    (OtherColumns)
);

DEFINE_ENUM(EProtobufEnumWritingMode,
    (CheckValues)
    (SkipUnknownValues)
);

class TProtobufTypeConfig
    : public NYTree::TYsonStruct
{
public:
    EProtobufType ProtoType;
    std::vector<TProtobufColumnConfigPtr> Fields;
    std::optional<TString> EnumerationName;

    REGISTER_YSON_STRUCT(TProtobufTypeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProtobufTypeConfig)

class TProtobufColumnConfig
    : public NYTree::TYsonStruct
{
public:
    TString Name;
    std::optional<ui64> FieldNumber;
    bool Repeated;
    bool Packed;

    TProtobufTypeConfigPtr Type;

    std::optional<EProtobufType> ProtoType;
    std::vector<TProtobufColumnConfigPtr> Fields;
    std::optional<TString> EnumerationName;
    EProtobufEnumWritingMode EnumWritingMode;

    REGISTER_YSON_STRUCT(TProtobufColumnConfig);

    static void Register(TRegistrar registrar);
public:
    void CustomPostprocess();
};

DEFINE_REFCOUNTED_TYPE(TProtobufColumnConfig)

////////////////////////////////////////////////////////////////////////////////

class TProtobufTableConfig
    : public NYTree::TYsonStruct
{
public:
    std::vector<TProtobufColumnConfigPtr> Columns;

    REGISTER_YSON_STRUCT(TProtobufTableConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProtobufTableConfig)

DEFINE_ENUM(ENestedMessagesMode,
    (Protobuf)
    (Yson)
);

class TProtobufFormatConfig
    : public NYTree::TYsonStruct
{
public:
    TString FileDescriptorSet; // deprecated
    std::vector<int> FileIndices; // deprecated
    std::vector<int> MessageIndices; // deprecated
    bool EnumsAsStrings; // deprecated
    ENestedMessagesMode NestedMessagesMode; // deprecated

    std::vector<TProtobufTableConfigPtr> Tables;
    NYTree::IMapNodePtr Enumerations;

    std::optional<TString> FileDescriptorSetText;
    std::vector<TString> TypeNames;

    EComplexTypeMode ComplexTypeMode;
    EDecimalMode DecimalMode;
    ETimeMode TimeMode;
    EUuidMode UuidMode;

    REGISTER_YSON_STRUCT(TProtobufFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProtobufFormatConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWebJsonValueFormat,
    // Values are stringified and (de-)serialized together with their types in form
    // |"column_name": {"$type": "double", "$value": "3.141592"}|.
    // Strings with length exceeding |FieldWeightLimit| are truncated and meta-attribute
    // "$incomplete" with value |true| is added to the representation, e.g.
    // |"column_name": {"$type": "string", "$incomplete": true, "$value": "Some very long st"}|
    (Schemaless)

    // Values are stringified and (de-)serialized in form
    // |<column_name>: [ <value>,  <stringified-type-index>]|, e.g. |"column_name": ["3.141592", "3"]|.
    // Type indices point to type registry stored under "yql_type_registry" key.
    // Non-UTF-8 strings are Base64-encoded and enclosed in a map with "b64" and "val" keys:
    //    | "column_name": {"val": "aqw==", "b64": true} |.
    // Strings and lists can be truncated, in which case they are enclosed in a map with "inc" and "val" keys:
    //    | "column_name": {"val": ["12", "13"], "inc": true} |
    // Wrapping in an additional map can occur on any depth.
    // Both "inc" and "b64" keys may appear in such maps.
    (Yql)
);

class TWebJsonFormatConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxSelectedColumnCount;
    int FieldWeightLimit;
    int StringWeightLimit;
    int MaxAllColumnNamesCount;
    std::optional<std::vector<TString>> ColumnNames;
    EWebJsonValueFormat ValueFormat;

    // Intentionally do not reveal following options to user.
    bool SkipSystemColumns = true;

    REGISTER_YSON_STRUCT(TWebJsonFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWebJsonFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TSkiffFormatConfig
    : public NYTree::TYsonStruct
{
public:
    NYTree::IMapNodePtr SkiffSchemaRegistry;
    NYTree::IListNodePtr TableSkiffSchemas;

    // This is temporary configuration until we support schema on mapreduce operations fully.
    std::optional<NTableClient::TTableSchema> OverrideIntermediateTableSchema;

    REGISTER_YSON_STRUCT(TSkiffFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSkiffFormatConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
