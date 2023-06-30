#include "yql_yt_qb2.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/join.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NYql {

namespace NNative {

void ProcessTableQB2Premapper(const NYT::TNode& remapper,
    const TString& tableName,
    NYT::TRichYPath& tablePath,
    size_t tableNum,
    TRemapperMap& remapperMap,
    TSet<TString>& remapperAllFiles,
    bool& useSkiff)
{
    THashMap<TString, TVector<TString>> requiredColumnsForField;
    THashMap<TString, TVector<TString>> requiredFilesForField;
    THashSet<TString> passtroughFields;
    bool allPasstrough = true;
    for (const auto& field: remapper["fields"].AsMap()) {
        auto& requiredColumns = requiredColumnsForField[field.first];
        for (const auto& column: field.second["required_columns"].AsList()) {
            requiredColumns.push_back(column.AsString());
        }

        for (const auto& file: field.second["required_dict_paths"].AsList()) {
            requiredFilesForField[field.first].push_back(file.AsString());
        }

        bool passthrough = false;
        if (field.second.HasKey("passthrough")) {
            passthrough = field.second["passthrough"].AsBool();
            // check real field name
            if (passthrough && (requiredColumns.size() != 1 || requiredColumns.front() != field.first)) {
                passthrough = false;
            }
        }

        if (passthrough) {
            passtroughFields.insert(field.first);
        }
    }

    TVector<TString> remappedFields;
    if (tablePath.Columns_) {
        TSet<TString> requiredColumns;
        for (const auto& field : tablePath.Columns_->Parts_) {
            allPasstrough = allPasstrough && passtroughFields.contains(field);
            remappedFields.push_back(field);
            auto columns = requiredColumnsForField.FindPtr(field);
            YQL_ENSURE(columns, "Unknown column name in remapper specification: " << field << ", table: " << tableName);
            requiredColumns.insert(columns->begin(), columns->end());
        }

        if (!allPasstrough) {
            for (const auto& field : tablePath.Columns_->Parts_) {
                auto files = requiredFilesForField.FindPtr(field);
                if (files) {
                    remapperAllFiles.insert(files->begin(), files->end());
                }
            }
            tablePath.Columns(TVector<TString>(requiredColumns.begin(), requiredColumns.end()));
        }
    }
    else {
        // add all fields
        for (const auto& field: remapper["fields"].AsMap()) {
            allPasstrough = allPasstrough && passtroughFields.contains(field.first);
            remappedFields.push_back(field.first);
        }

        if (!allPasstrough) {
            for (const auto& x : requiredFilesForField) {
                remapperAllFiles.insert(x.second.begin(), x.second.end());
            }
        }
    }

    if (!allPasstrough) {
        remapperAllFiles.insert(remapper["binary_path"].AsString());

        TRemapperKey key = std::make_tuple(remapper["command_prefix"].AsString(), remapper["log_name"].AsString(), JoinSeq(",", remappedFields));
        remapperMap[key].push_back(tableNum);

        if (remapper.HasKey("skiff")) {
            const auto& skiffMap = remapper["skiff"];
            useSkiff = skiffMap.HasKey("output") && skiffMap["output"].AsBool();
        } else {
            useSkiff = false;
        }
    }

}

TString GetQB2PremapperPrefix(const TRemapperMap& remapperMap, bool useSkiff) {
    TString remapperPrefix;
    TString prevPremapperBinary;
    for (const auto& x : remapperMap) {
        if (remapperPrefix.empty()) {
            remapperPrefix.append("set -o pipefail; ");
        }

        const auto& premapperBinary = std::get<0>(x.first);
        const auto& logName = std::get<1>(x.first);
        const auto& fields = std::get<2>(x.first);
        if (premapperBinary != prevPremapperBinary) {
            if (!prevPremapperBinary.empty()) {
                remapperPrefix.append(" | ");
            }

            prevPremapperBinary = premapperBinary;
            remapperPrefix.append(premapperBinary);
        }

        if (useSkiff) {
            remapperPrefix.append(" --output-format skiff");
        }

        remapperPrefix.append(" -l ").append(logName);
        remapperPrefix.append(" -f ").append(fields);
        remapperPrefix.append(" -t ").append(JoinSeq(",", x.second));
    }

    if (!remapperPrefix.empty()) {
        remapperPrefix.append(" | ");
    }
    return remapperPrefix;
}

void UpdateQB2PremapperUseSkiff(const TRemapperMap& remapperMap, bool& useSkiff) {
    THashSet<TString> remapperBinaries;

    for (const auto& pair : remapperMap) {
        remapperBinaries.insert(std::get<0>(pair.first));
    }

    // explicitly disable qb2 premapper's skiff mode
    // as it won't work with pipeline from GetQB2PremapperPrefix
    if (remapperBinaries.size() > 1) {
        useSkiff = false;
    }
}

} // NNative

} // NYql
