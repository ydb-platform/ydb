#include "schema_json.h"

#include "schema.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/scope.h>
#include <util/string/join.h>

namespace NSQLComplete {

    namespace {

        TString PathString(const TVector<TString>& path) {
            return "/" + JoinSeq("/", path);
        }

        const NJson::TJsonValue& Expect(
            const TVector<TString>& path,
            const NJson::TJsonValue::TMapType& json,
            TStringBuf key) {
            if (!json.contains(key)) {
                ythrow yexception() << "At " << PathString(path) << " expected " << key;
            }
            return json.at(key);
        }

        const NJson::TJsonValue::TMapType& ExpectMap(
            const TVector<TString>& path,
            const NJson::TJsonValue& json) {
            if (!json.IsMap()) {
                ythrow yexception() << "At " << PathString(path) << " expected Map";
            }
            return json.GetMapSafe();
        }

        const NJson::TJsonValue::TMapType& ExpectMap(
            const TVector<TString>& path,
            const NJson::TJsonValue::TMapType& json,
            TStringBuf key) {
            Expect(path, json, key);
            return ExpectMap(path, json.at(key));
        }

        TString Parse(
            const TString& cluster,
            const NJson::TJsonValue::TMapType& json,
            TSchemaData& data,
            TVector<TString>& path);

        void ParseFolder(
            const TString& cluster,
            const NJson::TJsonValue::TMapType& json,
            TSchemaData& data,
            TVector<TString>& path) {
            for (const auto& [name, entry] : ExpectMap(path, json, "entries")) {
                path.emplace_back(name);
                TString type = Parse(cluster, ExpectMap(path, entry), data, path);
                path.pop_back();

                TStringBuf suffix = (!path.empty()) ? "/" : "";
                data.Folders[cluster][PathString(path) + suffix].emplace_back(type, name);
            }
        }

        void ParseTable(
            const TString& cluster,
            const NJson::TJsonValue::TMapType& json,
            TSchemaData& data,
            TVector<TString>& path) {
            for (const auto& [name, _] : ExpectMap(path, json, "columns")) {
                data.Tables[cluster][PathString(path)].Columns.emplace_back(name);
            }
        }

        TString Parse(
            const TString& cluster,
            const NJson::TJsonValue::TMapType& json,
            TSchemaData& data,
            TVector<TString>& path) {
            auto type = Expect(path, json, "type");

            if (type == TFolderEntry::Folder) {
                ParseFolder(cluster, json, data, path);
                return TFolderEntry::Folder;
            }

            if (type == TFolderEntry::Table) {
                ParseTable(cluster, json, data, path);
                return TFolderEntry::Table;
            }

            if (!type.IsString()) {
                ythrow yexception() << "Unexpected type: " << type;
            }

            return type.GetStringSafe();
        }

    } // namespace

    ISimpleSchema::TPtr MakeStaticSimpleSchema(const NJson::TJsonMap& json) {
        TSchemaData data;

        for (const auto& [cluster, tree] : json.GetMap()) {
            TVector<TString> path;
            ParseFolder(cluster, tree.GetMapSafe(), data, path);
        }

        return MakeStaticSimpleSchema(std::move(data));
    }

} // namespace NSQLComplete
