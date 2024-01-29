#include "core_ydbc.h"
#include "core_ydbc_impl.h"

#include <ydb/core/util/wildcard.h>
#include <ydb/library/actors/http/http_cache.h>

#include <util/string/split.h>
#include <util/string/join.h>

TString SnakeToCamelCaseProtoConverter(const google::protobuf::FieldDescriptor& field) {
    return SnakeToCamelCase(field.name());
}

TString SnakeToCCamelCaseProtoConverter(const google::protobuf::FieldDescriptor& field) {
    return SnakeToCCamelCase(field.name());
}


TString SnakeToCamelCase(TString name) {
    name[0] = tolower(name[0]);
    size_t max = name.size() - 1;
    for (size_t i = 1; i < max;) {
        if (name[i] == '_') {
            name[i] = toupper(name[i + 1]);
            name.erase(i + 1, 1);
            max--;
        } else {
            ++i;
        }
    }
    return name;
}

TString SnakeToCCamelCase(TString name) {
    name[0] = toupper(name[0]);
    size_t max = name.size() - 1;
    for (size_t i = 1; i < max;) {
        if (name[i] == '_') {
            name[i] = toupper(name[i + 1]);
            name.erase(i + 1, 1);
            max--;
        } else {
            ++i;
        }
    }
    return name;
}


TString CamelToSnakeCase(TString name) {
    size_t max = name.size() - 1;
    name[0] = tolower(name[0]);
    for (size_t i = 1; i < max;) {
        if (isupper(name[i])) {
            name.insert(i, "_");
            ++i;
            name[i] = tolower(name[i]);
            ++max;
        } else {
            ++i;
        }
    }
    return name;
}

namespace {
    TJsonSettings ComposeDefaultJsonSettings() {
        TJsonSettings result;
        result.UI64AsString = false;
        result.EnumAsNumbers = false;
        result.EmptyRepeated = false;
        result.NaN = "null";
        result.NameGenerator = SnakeToCamelCaseProtoConverter;
        return result;
    }
}

TJsonSettings NMVP::THandlerActorYdbc::JsonSettings = ComposeDefaultJsonSettings();
NProtobufJson::TJson2ProtoConfig NMVP::THandlerActorYdbc::Json2ProtoConfig = NProtobufJson::TJson2ProtoConfig()
        .SetNameGenerator(SnakeToCamelCaseProtoConverter)
        .SetMapAsObject(true)
        .SetAllowString2TimeConversion(true);

NProtobufJson::TJson2ProtoConfig NMVP::THandlerActorYdbc::Json2ProtoConfig2 = NProtobufJson::TJson2ProtoConfig()
        .SetNameGenerator(SnakeToCCamelCaseProtoConverter)
        .SetMapAsObject(true)
        .SetAllowString2TimeConversion(true);


NProtobufJson::TProto2JsonConfig NMVP::THandlerActorYdbc::Proto2JsonConfig = NProtobufJson::TProto2JsonConfig()
        .SetNameGenerator(SnakeToCamelCaseProtoConverter)
        .SetMapAsObject(true)
        .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumValueMode::EnumName);

TString TrimAtAs(const TString& owner) {
    return owner.substr(0, owner.find("@"));
}

/*
TString ConvertName(const TString& name, const TString& from, const TString& to) {
    TVector<TString> parts;
    Split(name, from, parts);
    return JoinSeq(to, parts);
}
*/

TString EscapeStreamName(const TString& name) {
    return name;
    // return ConvertName(name, "/", "~");
}

TString UnescapeStreamName(const TString& name) {
    return name;
    // return ConvertName(name, "~", "/");
}
