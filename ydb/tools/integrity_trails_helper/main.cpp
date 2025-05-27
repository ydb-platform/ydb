#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <util/string/cast.h>
#include <fstream>
#include <sstream>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#define USE_CURRENT_UDF_ABI_VERSION true
#include <ydb/core/tx/datashard/datashard_integrity_trails.h>
#include <yql/essentials/types/dynumber/dynumber.h>
#include <yql/essentials/core/sql_types/simple_types.h>

using namespace NKikimr;

#define IF_TYPE(typeName) \
    else if (type == #typeName) { \
        resolved = NScheme::NTypeIds::typeName; \
    }

std::optional<NScheme::TTypeId> ResolveType(std::string typeAlias) {
    auto type = NYql::LookupSimpleTypeBySqlAlias(typeAlias, true);

    if (!type) {
        return {};
    }

    std::optional<NScheme::TTypeId> resolved = {};

    if (false) {}
    IF_TYPE(Bool)
    IF_TYPE(Int8)
    IF_TYPE(Uint8)
    IF_TYPE(Int16)
    IF_TYPE(Uint16)
    IF_TYPE(Int32)
    IF_TYPE(Uint32)
    IF_TYPE(Int64)
    IF_TYPE(Uint64)
    IF_TYPE(Double)
    IF_TYPE(Float)
    IF_TYPE(String)
    IF_TYPE(Utf8)
    IF_TYPE(Yson)
    IF_TYPE(Json)
    IF_TYPE(Uuid)
    IF_TYPE(Date)
    IF_TYPE(Datetime)
    IF_TYPE(Timestamp)
    IF_TYPE(Interval)
    IF_TYPE(Decimal)
    IF_TYPE(DyNumber)
    IF_TYPE(JsonDocument)
    IF_TYPE(Date32)
    IF_TYPE(Datetime64)
    IF_TYPE(Timestamp64)
    IF_TYPE(Interval64)

    return resolved;   
}

#define EXTRACT_VAL(cellType, protoType, cppType) \
    case NScheme::NTypeIds::cellType : { \
            cppType v = FromString<cppType>(val); \
            cell = TCell((const char*)&v, sizeof(v)); \
            break; \
        }

std::optional<TCell> ParseCell(std::string type, std::string val) {
    auto typeId = ResolveType(type);

    std::optional<TCell> cell = {};

    if (!typeId) {
        return {};
    }

    switch (*typeId) {
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
        cell = TCell(val.data(), val.size());
        break;
    }
    case NScheme::NTypeIds::DyNumber : {
        const auto dyNumber = NDyNumber::ParseDyNumberString(val);
        if (!dyNumber.Defined()) {
            return {};
        }
        cell = TCell(dyNumber->data(), dyNumber->size());
        break;
    }
    case NScheme::NTypeIds::Yson :
    case NScheme::NTypeIds::String : {
            cell = TCell(val.data(), val.size());
            break;
        }
    case NScheme::NTypeIds::Decimal :
    case NScheme::NTypeIds::Uuid : {
        char uuid[16];
        cell = TCell(uuid, sizeof(uuid));
        break;
    }
    default:
        return {};
    };

    return cell;
}

std::vector<std::string> ReadPK(NJson::TJsonValue& jsonValue) {
    auto &pkField = jsonValue["primary_key"];

    if (!pkField.IsArray()) {
        Cerr << "Scheme parsing error, primary_key is not an array" << Endl;
        return {};
    }

    std::vector<std::string> pk;

    auto &pkArray = pkField.GetArray();

    for (size_t i = 0; i < pkArray.size(); ++i) {
        if (!pkArray[i].IsString()) {
            Cerr << "Scheme parsing error, primary key array element is not a string" << Endl;
            return {};
        }
        pk.push_back(pkArray[i].GetString());
    }

    return pk;
}

std::map<std::string, std::string> ReadColumnMapping(NJson::TJsonValue& jsonValue) {
    auto &columnsField = jsonValue["columns"];

    if (!columnsField.IsArray()) {
        Cerr << "Scheme parsing error, columns is not an array" << Endl;
        return {};
    }

    auto &columnsArray = columnsField.GetArray();

    std::map<std::string, std::string> colToType;

    for (size_t i = 0; i < columnsArray.size(); ++i) {
        auto &column = columnsArray[i];

        if (!column.IsMap()) {
            Cerr << "Scheme parsing error, column is not an object" << Endl;
            return {};
        }

        auto &nameField = column["name"].GetString();
        auto &typeField = column["type"];

        std::string typeId = "";

        if (typeField.Has("type_id")) {
            typeId = typeField["type_id"].GetString();
        } else if (typeField.Has("optional_type")) {
            typeId = typeField["optional_type"]["item"]["type_id"].GetString();
        }

        if (typeId.empty()) {
            Cerr << "Scheme parsing error, type_id is not found" << Endl;
            return {};
        }

        colToType[nameField] = typeId;
    }

    return colToType;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        Cerr << "Usage: path-to-scheme.json key-column1-value ... key-columnN-value" << Endl;
        return 1;
    }

    std::string path = argv[1];

    std::vector<std::string> values;

    for (int i = 2; i < argc; ++i) {
        values.push_back(argv[i]);
    }

    std::stringstream buffer;

    std::ifstream fileStream(path);
    buffer << fileStream.rdbuf();
    
    std::string json = buffer.str();

    NJson::TJsonValue jsonValue;

    if (!NJson::ReadJsonTree(json, &jsonValue)) {
        Cerr << "Failed to parse JSON" << Endl;
        return 1;
    }

    std::vector<std::string> pk = ReadPK(jsonValue);

    if (pk.empty()) {
        Cerr << "Primary key is empty" << Endl;
        return 1;
    }

    std::map<std::string, std::string> colToType = ReadColumnMapping(jsonValue);
    
    if (colToType.empty()) {
        Cerr << "Column mapping is empty" << Endl;
        return 1;
    }

    if (values.size() != pk.size()) {
        Cerr << "Key's columns count doesn't match scheme" << Endl;

        return 1;
    }

    TVector<TCell> arr(pk.size());

    for (size_t i = 0; i < values.size(); ++i) {
        auto col = pk[i];
        auto type = colToType[col];
        auto cell = ParseCell(colToType[pk[i]], values[i]);

        if (!cell) {
            Cerr << "Unexpected type " << type << " of column " << col << Endl;

            return 1;
        }

        arr[i] = *cell;
    }

    TSerializedCellVec vec(arr);

    Cout << "Obfuscated key: " << Endl;

    TStringStream output;

    NDataIntegrity::WriteTablePoint(vec.GetCells(), output);

    Cout << output.Str() << Endl;

    return 0;
}
