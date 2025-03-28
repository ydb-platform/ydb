#include "name_service.h"

namespace NSQLComplete {

    // TODO(YQL-19747): Use some name registry
    NameSet MakeDefaultNameSet() {
        return {
            .Types = {
                "Bool",
                "Int8",
                "Uint8",
                "Int16",
                "Uint16",
                "Int32",
                "Uint32",
                "Int64",
                "Uint64",
                "Float",
                "Double",
                "String",
                "Utf8",
                "Yson",
                "Json",
                "Uuid",
                "JsonDocument",
                "Date",
                "Datetime",
                "Timestamp",
                "Interval",
                "TzDate",
                "TzDatetime",
                "TzTimestamp",
                "Date32",
                "Datetime64",
                "Timestamp64",
                "Interval64",
                "TzDate32",
                "TzDatetime64",
                "TzTimestamp64",
                "Decimal",
                "DyNumber",
            },
        };
    }

} // namespace NSQLComplete
