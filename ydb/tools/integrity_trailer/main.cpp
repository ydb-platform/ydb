#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <util/string/cast.h>
#include <fstream>
#include <sstream>
#include "ydb/core/scheme/scheme_tablecell.h"
#include "ydb/library/dynumber/dynumber.h"
#include "ydb/library/uuid/uuid.h"

using namespace NKikimr;

#define IF_HASH(typeName) \
    else if (type == #typeName) { \
        resolved = NScheme::NTypeIds::typeName; \
    }

NScheme::TTypeId resolveOption(std::string type) {
    NScheme::TTypeId resolved = NScheme::NTypeIds::String;

    if (type == "Bool") {
        resolved = NScheme::NTypeIds::Bool;
    }
    IF_HASH(Int8)
    IF_HASH(Uint8)
    IF_HASH(Int16)
    IF_HASH(Uint16)
    IF_HASH(Int32)
    IF_HASH(Uint32)
    IF_HASH(Int64)
    IF_HASH(Uint64)
    IF_HASH(Double)
    IF_HASH(Float)
    IF_HASH(String)
    IF_HASH(Utf8)
    IF_HASH(Yson)
    IF_HASH(Json)
    IF_HASH(Uuid)
    IF_HASH(Date)
    IF_HASH(Datetime)
    IF_HASH(Timestamp)
    IF_HASH(Interval)
    IF_HASH(Decimal)
    IF_HASH(DyNumber)
    IF_HASH(JsonDocument)
    IF_HASH(Date32)
    IF_HASH(Datetime64)
    IF_HASH(Timestamp64)
    IF_HASH(Interval64)

    return resolved;   
}

#define EXTRACT_VAL(cellType, protoType, cppType) \
    case NScheme::NTypeIds::cellType : { \
            cppType v = FromString<cppType>(val); \
            c = TCell((const char*)&v, sizeof(v)); \
            break; \
        }

void celler(std::string type, std::string val) {
    NScheme::TTypeId typeId = resolveOption(type);
    TCell c;

    switch (typeId) {
    EXTRACT_VAL(Bool, bool, ui8);
    EXTRACT_VAL(Int8, int32, i8);
    EXTRACT_VAL(Uint8, uint32, ui8);
    EXTRACT_VAL(Int16, int32, i16);
    EXTRACT_VAL(Uint16, uint32, ui16);
    EXTRACT_VAL(Int32, int32, i32);
    EXTRACT_VAL(Uint32, uint32, ui32);
    EXTRACT_VAL(Int64, int64, i64);
    EXTRACT_VAL(Uint64, uint64, ui64);
    EXTRACT_VAL(Float, float, float);
    EXTRACT_VAL(Double, double, double);
    EXTRACT_VAL(Date, uint32, ui16);
    EXTRACT_VAL(Datetime, uint32, ui32);
    EXTRACT_VAL(Timestamp, uint64, ui64);
    EXTRACT_VAL(Interval, int64, i64);
    EXTRACT_VAL(Date32, int32, i32);
    EXTRACT_VAL(Datetime64, int64, i64);
    EXTRACT_VAL(Timestamp64, int64, i64);
    EXTRACT_VAL(Interval64, int64, i64);
    case NScheme::NTypeIds::Json :
    case NScheme::NTypeIds::Utf8 : {
        c = TCell(val.data(), val.size());
        break;
    }
    case NScheme::NTypeIds::DyNumber : {
        const auto dyNumber = NDyNumber::ParseDyNumberString(val);
        if (!dyNumber.Defined()) {
            Cerr << "Invalid DyNumber string representation";
            return;
        }
        c = TCell(dyNumber->data(), dyNumber->size());
        break;
    }
    case NScheme::NTypeIds::Yson :
    case NScheme::NTypeIds::String : {
            c = TCell(val.data(), val.size());
            break;
        }
    case NScheme::NTypeIds::Decimal :
    case NScheme::NTypeIds::Uuid : {
        char uuid[16];
        NUuid::ParseUuidToArray(TString("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), reinterpret_cast<ui16*>(uuid), false);
        c = TCell(uuid, sizeof(uuid));
        break;
    }
    default:
        Cerr << "Unexpected type " << type;
        return;
    };
}

int main() {
    std::stringstream buffer;

    std::string path = "/home/ee8jsmrbc5t8l9r7duiv/tmp/scheme.json";
    std::ifstream fileStream(path);
    buffer << fileStream.rdbuf();
    
    std::string json = buffer.str();

    NJson::TJsonValue jsonValue;

    NJson::ReadJsonTree(json, &jsonValue);

    auto &pkField = jsonValue["primary_key"];

    if (!pkField.IsArray()) {
        Cerr << "Scheme parsing error, primary key is not an array";
        return 0;
    }

    std::vector<std::string> pk;

    if (pkField.IsArray()) {
        auto &pkArray = pkField.GetArray();

        for (size_t i = 0; i < pkArray.size(); ++i) {
            if (!pkArray[i].IsString()) {
                Cerr << "Scheme parsing error, primary key array element is not a string";
                return 0;
            }
            pk.push_back(pkArray[i].GetString());
        }
    }

    auto &columnsField = jsonValue["columns"];

    if (!columnsField.IsArray()) {
        Cerr << "Scheme parsing error, columns is not an array";
        return 0;
    }

    auto &columnsArray = columnsField.GetArray();

    std::map<std::string, std::string> colToType;

    for (size_t i = 0; i < columnsArray.size(); ++i) {
        auto &column = columnsArray[i];

        if (!column.IsMap()) {
            Cerr << "Scheme parsing error, column is not an object";
            return 0;
        }

        auto &nameField = column["name"].GetString();
        auto &typeField = column["type"];

        std::string typeId = "";

        if (typeField.Has("type_id")) {
            typeId = typeField["type_id"].GetString();
        } else if (typeField.Has("optional_type")) {
            typeId = typeField["optional_type"]["item"]["type_id"].GetString();
        }

        colToType[nameField] = typeId;
    }

    Cout << pk.size() << Endl;

    Cout << colToType.size() << Endl;

    return 0;
}