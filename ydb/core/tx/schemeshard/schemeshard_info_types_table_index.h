#pragma once

#include "schemeshard_info_types_base.h"

#include <string_view>
#include <variant>

namespace NKikimr {
namespace NSchemeShard {

struct TTableIndexInfo : public TSimpleRefCount<TTableIndexInfo> {
    using TPtr = TIntrusivePtr<TTableIndexInfo>;
    using EType = NKikimrSchemeOp::EIndexType;
    using EState = NKikimrSchemeOp::EIndexState;

    TTableIndexInfo(ui64 version, EType type, EState state, std::string_view description)
        : AlterVersion(version)
        , Type(type)
        , State(state)
    {
        switch (type) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            case NKikimrSchemeOp::EIndexTypeLocalMinMax:
            case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
                // no specialized index description
                Y_ASSERT(description.empty());
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalJson:
            case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
                // JSON indexes carry a fulltext description only when rowid mode (__ydb_row_id as doc_id)
                // is enabled (the serialized description is then non-empty); legacy JSON indexes have none.
                if (!description.empty()) {
                    auto success = SpecializedIndexDescription
                        .emplace<NKikimrSchemeOp::TFulltextIndexDescription>()
                        .ParseFromString(description);
                    Y_ENSURE(success, description);
                }
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TFulltextIndexDescription>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TBloomFilter>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter: {
                auto success = SpecializedIndexDescription
                    .emplace<NKikimrSchemeOp::TBloomNGrammFilter>()
                    .ParseFromString(description);
                Y_ENSURE(success, description);
                break;
            }
            case NKikimrSchemeOp::EIndexTypeInvalid:
                break;
        }
    }

    TTableIndexInfo(const TTableIndexInfo&) = default;

    TPtr CreateNextVersion() {
        this->AlterData = this->GetNextVersion();
        return this->AlterData;
    }

    TPtr GetNextVersion() const {
        Y_ENSURE(AlterData == nullptr);
        TPtr result = new TTableIndexInfo(*this);
        ++result->AlterVersion;
        return result;
    }

    TString SerializeDescription() const {
        return std::visit([]<typename T>(const T& v) {
            if constexpr (std::is_same_v<std::monostate, T>) {
                return TString{};
            } else if constexpr (std::is_same_v<NKikimrSchemeOp::TBloomNGrammFilter, T>) {
                TString str;
                Y_ENSURE(v.SerializeToString(&str));
                return str;
            } else {
                TString str{v.SerializeAsString()};
                Y_ENSURE(!str.empty());
                return str;
            }
        }, SpecializedIndexDescription);
    }

    static TPtr NotExistedYet(EType type) {
        return new TTableIndexInfo(0, type, EState::EIndexStateInvalid, {});
    }

    static bool IsLocalIndex(EType type) {
        return type == NKikimrSchemeOp::EIndexTypeLocalBloomFilter
            || type == NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter
            || type == NKikimrSchemeOp::EIndexTypeLocalMinMax
            || type == NKikimrSchemeOp::EIndexTypeLocalCountMinSketch;
    }

    static TPtr Create(const NKikimrSchemeOp::TIndexCreationConfig& config, TString& errMsg) {
        if (!config.KeyColumnNamesSize() && !IsLocalIndex(config.GetType())) {
            errMsg += TStringBuilder() << "no key columns in index creation config";
            return nullptr;
        }

        TPtr result = NotExistedYet(config.GetType());

        TPtr alterData = result->CreateNextVersion();
        alterData->IndexKeys.assign(config.GetKeyColumnNames().begin(), config.GetKeyColumnNames().end());
        if (!IsLocalIndex(config.GetType())) {
            Y_ENSURE(!alterData->IndexKeys.empty());
        }
        alterData->IndexDataColumns.assign(config.GetDataColumnNames().begin(), config.GetDataColumnNames().end());

        alterData->State = config.HasState() ? config.GetState() : EState::EIndexStateReady;

        switch (GetIndexType(config)) {
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
            case NKikimrSchemeOp::EIndexTypeLocalMinMax:
            case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
                // no specialized index description
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalJson:
            case NKikimrSchemeOp::EIndexTypeGlobalJsonCompact:
                // JSON indexes carry a fulltext description only when rowid mode (__ydb_row_id as doc_id)
                // is enabled; otherwise there is no specialized index description.
                if (config.HasFulltextIndexDescription()) {
                    alterData->SpecializedIndexDescription = config.GetFulltextIndexDescription();
                }
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
                alterData->SpecializedIndexDescription = config.GetVectorIndexKmeansTreeDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact:
            case NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance:
                alterData->SpecializedIndexDescription = config.GetFulltextIndexDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
                alterData->SpecializedIndexDescription = config.GetBloomFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
                alterData->SpecializedIndexDescription = config.GetBloomNGrammFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeInvalid:
                errMsg += InvalidIndexType(config.GetType());
                return nullptr;
        }

        return result;
    }

    static TPtr Create(const NKikimrSchemeOp::TIndexAlteringConfig& config, TString& errMsg) {
        if (!config.HasType() || !IsLocalIndex(config.GetType())) {
            errMsg += TStringBuilder() << "TIndexAlteringConfig requires a valid local index type to be set";
            return nullptr;
        }

        if (!config.KeyColumnNamesSize() && !IsLocalIndex(config.GetType())) {
            errMsg += TStringBuilder() << "no key columns in index altering config";
            return nullptr;
        }

        TPtr result = NotExistedYet(config.GetType());

        TPtr alterData = result->CreateNextVersion();
        alterData->IndexKeys.assign(config.GetKeyColumnNames().begin(), config.GetKeyColumnNames().end());
        if (!IsLocalIndex(config.GetType())) {
            Y_ENSURE(!alterData->IndexKeys.empty());
        }

        alterData->State = config.HasState() ? config.GetState() : EState::EIndexStateReady;

        switch (GetIndexType(config)) {
            case NKikimrSchemeOp::EIndexTypeLocalBloomFilter:
                alterData->SpecializedIndexDescription = config.GetBloomFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter:
                alterData->SpecializedIndexDescription = config.GetBloomNGrammFilterDescription();
                break;
            case NKikimrSchemeOp::EIndexTypeLocalMinMax:
            case NKikimrSchemeOp::EIndexTypeLocalCountMinSketch:
                alterData->SpecializedIndexDescription = std::monostate{};
                break;
            default:
                errMsg += TStringBuilder() << "TIndexAlteringConfig only supports local index types, got: " << NKikimrSchemeOp::EIndexType_Name(config.GetType());
                return nullptr;
        }

        return result;
    }

    ui64 AlterVersion = 1;
    EType Type;
    EState State;

    TVector<TString> IndexKeys;
    TVector<TString> IndexDataColumns;

    TTableIndexInfo::TPtr AlterData = nullptr;

    std::variant<std::monostate,
        NKikimrSchemeOp::TVectorIndexKmeansTreeDescription,
        NKikimrSchemeOp::TFulltextIndexDescription,
        NKikimrSchemeOp::TBloomFilter,
        NKikimrSchemeOp::TBloomNGrammFilter> SpecializedIndexDescription;
};

}
}
