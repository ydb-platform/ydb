#include "parser.h"

#include <library/cpp/json/json_reader.h>

#include <util/string/cast.h>

namespace NTvmAuth::NRoles {
    static void GetRequiredValue(const NJson::TJsonValue& doc,
                                 TStringBuf key,
                                 NJson::TJsonValue& obj) {
        Y_ENSURE(doc.GetValue(key, &obj), "Missing '" << key << "'");
    }

    static ui64 GetRequiredUInt(const NJson::TJsonValue& doc,
                                TStringBuf key) {
        NJson::TJsonValue obj;
        GetRequiredValue(doc, key, obj);
        Y_ENSURE(obj.IsUInteger(), "key '" << key << "' must be uint");
        return obj.GetUInteger();
    }

    static bool GetOptionalMap(const NJson::TJsonValue& doc,
                               TStringBuf key,
                               NJson::TJsonValue& obj) {
        if (!doc.GetValue(key, &obj)) {
            return false;
        }

        Y_ENSURE(obj.IsMap(), "'" << key << "' must be object");
        return true;
    }

    TRolesPtr TParser::Parse(TRawPtr decodedBlob) {
        try {
            return ParseImpl(decodedBlob);
        } catch (const std::exception& e) {
            throw yexception() << "Failed to parse roles from tirole: " << e.what()
                               << ". '" << *decodedBlob << "'";
        }
    }

    TRolesPtr TParser::ParseImpl(TRawPtr decodedBlob) {
        NJson::TJsonValue doc;
        Y_ENSURE(NJson::ReadJsonTree(*decodedBlob, &doc), "Invalid json");
        Y_ENSURE(doc.IsMap(), "Json must be object");

        TRoles::TTvmConsumers tvm = GetConsumers<TTvmId>(doc, "tvm");
        TRoles::TUserConsumers user = GetConsumers<TUid>(doc, "user");

        // fetch it last to provide more correct apply instant
        TRoles::TMeta meta = GetMeta(doc);

        return std::make_shared<TRoles>(
            std::move(meta),
            std::move(tvm),
            std::move(user),
            std::move(decodedBlob));
    }

    TRoles::TMeta TParser::GetMeta(const NJson::TJsonValue& doc) {
        TRoles::TMeta res;

        NJson::TJsonValue obj;
        GetRequiredValue(doc, "revision", obj);
        if (obj.IsString()) {
            res.Revision = obj.GetString();
        } else if (obj.IsUInteger()) {
            res.Revision = ToString(obj.GetUInteger());
        } else {
            ythrow yexception() << "'revision' has unexpected type: " << obj.GetType();
        }

        res.BornTime = TInstant::Seconds(GetRequiredUInt(doc, "born_date"));

        return res;
    }

    template <typename Id>
    THashMap<Id, TConsumerRolesPtr> TParser::GetConsumers(const NJson::TJsonValue& doc,
                                                          TStringBuf type) {
        THashMap<Id, TConsumerRolesPtr> res;

        NJson::TJsonValue obj;
        if (!GetOptionalMap(doc, type, obj)) {
            return res;
        }

        for (const auto& [key, value] : obj.GetMap()) {
            Y_ENSURE(value.IsMap(),
                     "roles for consumer must be map: '" << key << "' is " << value.GetType());

            Id id = 0;
            Y_ENSURE(TryIntFromString<10>(key, id),
                     "id must be valid positive number of proper size for "
                         << type << ". got '"
                         << key << "'");

            Y_ENSURE(res.emplace(id, GetConsumer(value, key)).second,
                     "consumer duplicate detected: '" << key << "' for " << type);
        }

        return res;
    }

    TConsumerRolesPtr TParser::GetConsumer(const NJson::TJsonValue& obj, TStringBuf consumer) {
        THashMap<TString, TEntitiesPtr> entities;

        for (const auto& [key, value] : obj.GetMap()) {
            Y_ENSURE(value.IsArray(),
                     "entities for roles must be array: '" << key << "' is " << value.GetType());

            entities.emplace(key, GetEntities(value, consumer, key));
        }

        return std::make_shared<TConsumerRoles>(std::move(entities));
    }

    TEntitiesPtr TParser::GetEntities(const NJson::TJsonValue& obj,
                                      TStringBuf consumer,
                                      TStringBuf role) {
        std::vector<TEntityPtr> entities;
        entities.reserve(obj.GetArray().size());

        for (const NJson::TJsonValue& e : obj.GetArray()) {
            Y_ENSURE(e.IsMap(),
                     "role entity for role must be map: consumer '"
                         << consumer << "' with role '" << role << "' has " << e.GetType());

            entities.push_back(GetEntity(e, consumer, role));
        }

        return std::make_shared<TEntities>(TEntities(entities));
    }

    TEntityPtr TParser::GetEntity(const NJson::TJsonValue& obj, TStringBuf consumer, TStringBuf role) {
        TEntityPtr res = std::make_shared<TEntity>();

        for (const auto& [key, value] : obj.GetMap()) {
            Y_ENSURE(value.IsString(),
                     "entity is map (str->str), got value "
                         << value.GetType() << ". consumer '"
                         << consumer << "' with role '" << role << "'");

            res->emplace(key, value.GetString());
        }

        return res;
    }
}
