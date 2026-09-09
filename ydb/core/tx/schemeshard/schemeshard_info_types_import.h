#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/protos/index_builder.pb.h>

#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/regex/pcre/regexp.h>

namespace NKikimr {
namespace NSchemeShard {

struct TImportInfo: public TSimpleRefCount<TImportInfo> {
    using TPtr = TIntrusivePtr<TImportInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        Waiting = 1,
        GetScheme = 2,
        CreateSchemeObject = 3,
        Transferring = 4,
        BuildIndexes = 5,
        CreateChangefeed = 6,
        DownloadExportMetadata = 7,
        Done = 240,
        Cancellation = 250,
        Cancelled = 251,
    };

    enum class EKind: ui8 {
        S3 = 0,
        FS = 1,
    };

    struct TItem {
        enum class ESubState: ui8 {
            AllocateTxId = 0,
            Proposed,
            Subscribed,
        };

        enum class EChangefeedState: ui8 {
            CreateChangefeed = 0,
            CreateConsumers,
        };

        TString DstPathName;
        TPathId DstPathId;
        TString SrcPrefix;
        TString SrcPath; // Src path from schema mapping
        TMaybe<Ydb::Table::CreateTableRequest> Table;
        TMaybe<Ydb::Topic::CreateTopicRequest> Topic;
        TMaybe<Ydb::Table::DescribeSystemViewResult> SysView;
        TString CreationQuery;
        TMaybe<NKikimrSchemeOp::TModifyScheme> PreparedCreationQuery;
        TMaybeFail<Ydb::Scheme::ModifyPermissionsRequest> Permissions;
        NBackup::TMetadata Metadata;
        TVector<std::pair<NBackup::TIndexMetadata, Ydb::Table::CreateTableRequest>> MaterializedIndexes;
        NKikimrSchemeOp::TImportTableChangefeeds Changefeeds;

        EState State = EState::GetScheme;
        ESubState SubState = ESubState::AllocateTxId;
        EChangefeedState ChangefeedState = EChangefeedState::CreateChangefeed;
        TTxId WaitTxId = InvalidTxId;
        TActorId SchemeGetter;
        TActorId SchemeQueryExecutor;
        int NextIndexIdx = 0;
        int NextChangefeedIdx = 0;
        TString Issue;
        TPathId StreamImplPathId;
        TMaybe<NBackup::TEncryptionIV> ExportItemIV;

        ui32 ParentIdx = Max<ui32>();
        TVector<ui32> ChildItems;

        TItem() = default;

        explicit TItem(const TString& dstPathName)
            : DstPathName(dstPathName)
        {
        }

        explicit TItem(const TString& dstPathName, const TPathId& dstPathId)
            : DstPathName(dstPathName)
            , DstPathId(dstPathId)
        {
        }

        TString ToString(ui32 idx) const;

        static bool IsDone(const TItem& item);
    };

    ui64 Id;  // TxId from the original TEvCreateImportRequest
    TString Uid;
    EKind Kind;
    TString SettingsSerialized;
    std::variant<Ydb::Import::ImportFromS3Settings,
                 Ydb::Import::ImportFromFsSettings> Settings;
    TPathId DomainPathId;
    TMaybe<TString> UserSID;
    TString PeerName;  // required for making audit log records
    TString SanitizedToken;  // required for making audit log records
    TMaybe<NBackup::TEncryptionIV> ExportIV;
    TMaybe<NBackup::TSchemaMapping> SchemaMapping;
    TActorId SchemaMappingGetter;

    EState State = EState::Invalid;
    TString Issue;
    TVector<TItem> Items;
    int WaitingSchemeObjects = 0;

    TSet<TActorId> Subscribers;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    TMaybe<std::vector<TRegExMatch>> ExcludeRegexps;

private:
    template <typename TSettingsPB>
    static TString SerializeSettings(const TSettingsPB& settings) {
        TString serialized;
        Y_ABORT_UNLESS(settings.SerializeToString(&serialized));
        return serialized;
    }

    template <typename TFunc>
    auto Visit(TFunc&& func) const {
        return VisitSettings(Settings, std::forward<TFunc>(func));
    }

public:

    TString GetItemSrcPrefix(size_t i) const {
        if (i < Items.size() && Items[i].SrcPrefix) {
            return Items[i].SrcPrefix;
        }

        // Backward compatibility.
        // But there can be no paths in settings at all.
        return Visit([i](const auto& settings) -> TString {
            return GetItemSource(settings, i);
        });
    }

    const Ydb::Import::ImportFromS3Settings& GetS3Settings() const {
        Y_ABORT_UNLESS(Kind == EKind::S3);
        return std::get<Ydb::Import::ImportFromS3Settings>(Settings);
    }

    const Ydb::Import::ImportFromFsSettings& GetFsSettings() const {
        Y_ABORT_UNLESS(Kind == EKind::FS);
        return std::get<Ydb::Import::ImportFromFsSettings>(Settings);
    }

    TString GetSource() const {
        if (Kind == EKind::S3) {
            return GetS3Settings().source_prefix();
        } else if (Kind == EKind::FS) {
            return GetFsSettings().base_path();
        }
        Y_ABORT("Unknown import kind");
        return {};
    }

    // Getters for common settings fields
    bool GetNoAcl() const {
        return Visit([](const auto& settings) {
            return settings.no_acl();
        });
    }

    TString GetDestinationPath() const {
        return Visit([](const auto& settings) {
            return settings.destination_path();
        });
    }

    bool GetEncryptedBackup() const {
        return Visit([](const auto& settings) {
            return settings.has_encryption_settings();
        });
    }

    bool GetSkipChecksumValidation() const {
        return Visit([](const auto& settings) {
            return settings.skip_checksum_validation();
        });
    }

    Ydb::Import::ImportFromS3Settings::IndexPopulationMode GetIndexPopulationMode() const {
        return Visit([](const auto& settings) {
            return settings.index_population_mode();
        });
    }

    bool CompileExcludeRegexps(TString& errorDescription);

    bool IsExcludedFromImport(const TString& path) const;

    explicit TImportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TString& serializedSettings,
            const TPathId domainPathId,
            const TString& peerName)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , SettingsSerialized(serializedSettings)
        , DomainPathId(domainPathId)
        , PeerName(peerName)
    {
        switch (kind) {
        case EKind::S3: {
            Settings = ParseSettings<Ydb::Import::ImportFromS3Settings>(serializedSettings);
            break;
        }
        case EKind::FS: {
            Settings = ParseSettings<Ydb::Import::ImportFromFsSettings>(serializedSettings);
            break;
        }
        default:
            Y_ABORT("Unknown import kind");
        }
    }

    template <typename TSettingsPB>
    explicit TImportInfo(
            const ui64 id,
            const TString& uid,
            const EKind kind,
            const TSettingsPB& settingsPb,
            const TPathId domainPathId,
            const TString& peerName)
        : Id(id)
        , Uid(uid)
        , Kind(kind)
        , SettingsSerialized(SerializeSettings(settingsPb))
        , Settings(settingsPb)
        , DomainPathId(domainPathId)
        , PeerName(peerName)
    {
    }

public:

    TString ToString() const;

    bool IsFinished() const;

    void AddNotifySubscriber(const TActorId& actorId);

    struct TFillItemsFromSchemaMappingResult {
        bool Success = true;
        TString ErrorMessage;
        size_t ErrorsCount = 0;

        void AddError(const TString& err);
    };

    // Erases encryption key and syncronize it with SettingsSerialized
    // Returns true if settings changed
    bool EraseEncryptionKey() {
        return std::visit([this](auto& settings) {
            if (settings.encryption_settings().has_symmetric_key()) {
                settings.mutable_encryption_settings()->clear_symmetric_key();
                Y_ABORT_UNLESS(settings.SerializeToString(&SettingsSerialized));
                return true;
            }
            return false;
        }, Settings);
    }

    // Fills items from schema mapping:
    // - if user specified no items, fills all from schema mapping;
    // - if user specified explicit filtering, takes from schema mapping only those allowed by filter.
    //
    // Replaces current items list with a new list of items.
    // Generates an error if there are no item explicitly specified by filter.
    TFillItemsFromSchemaMappingResult FillItemsFromSchemaMapping(TSchemeShard* ss);
}; // TImportInfo
// } // NImport

}
}

Y_DECLARE_OUT_SPEC(inline, NKikimrIndexBuilder::TMeteringStats, stream, value) {
    stream << value.ShortDebugString();
}
