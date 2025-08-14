#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((InvalidFormat) (2800))
);

DEFINE_ENUM(EComplexTypeMode,
    (Positional)
    (Named)
);

DEFINE_ENUM(EDictMode,
    (Positional)
    (Named)
);

DEFINE_ENUM(EDecimalMode,
    (Text)
    (Binary)
);

DEFINE_ENUM(ETimeMode,
    (Text)
    (Binary)
);

DEFINE_ENUM(EUuidMode,
    (TextYql)
    (TextYt)
    (Binary)
);

//! Type of data that can be read or written by a driver command.
DEFINE_ENUM(EDataType,
    (Null)
    (Binary)
    (Structured)
    (Tabular)
);

DEFINE_ENUM(EFormatType,
    (Null)
    (Yson)
    (Json)
    (Dsv)
    (Yamr)
    (YamredDsv)
    (SchemafulDsv)
    (Protobuf)
    (WebJson)
    (Skiff)
    (Arrow)
    (Yaml)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TYsonFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TTableFormatConfigBase)
DECLARE_REFCOUNTED_STRUCT(TYamrFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TYamrFormatConfigBase)
DECLARE_REFCOUNTED_STRUCT(TDsvFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TDsvFormatConfigBase)
DECLARE_REFCOUNTED_STRUCT(TYamredDsvFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TSchemafulDsvFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TProtobufTypeConfig)
DECLARE_REFCOUNTED_STRUCT(TProtobufColumnConfig)
DECLARE_REFCOUNTED_STRUCT(TProtobufTableConfig)
DECLARE_REFCOUNTED_STRUCT(TProtobufFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TWebJsonFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TSkiffFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TYamlFormatConfig)
DECLARE_REFCOUNTED_STRUCT(TArrowFormatConfig)

DECLARE_REFCOUNTED_STRUCT(IYamrConsumer)

DECLARE_REFCOUNTED_STRUCT(ISchemalessFormatWriter)
DECLARE_REFCOUNTED_STRUCT(IFormatFactory)

DECLARE_REFCOUNTED_STRUCT(TControlAttributesConfig)

struct IParser;

class TEscapeTable;

class TFormat;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, FormatsLogger, "Formats");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
