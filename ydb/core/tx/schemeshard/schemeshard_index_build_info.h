#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {
using namespace NTableIndex;

struct TIndexBuildShardStatus {
    TSerializedTableRange Range;
    TString LastKeyAck;
    ui64 SeqNoRound = 0;
    size_t Index = 0; // used only in prefixed vector index: a unique number of shard in the list

    // Used in fulltext index build:
    ui64 DocCount = 0;
    ui64 TotalDocLength = 0;
    TString FirstToken;
    TString LastToken;
    NTableIndex::NFulltext::TDocCount FirstTokenRows = 0;
    NTableIndex::NFulltext::TDocCount LastTokenRows = 0;

    NKikimrIndexBuilder::EBuildStatus Status = NKikimrIndexBuilder::EBuildStatus::INVALID;

    Ydb::StatusIds::StatusCode UploadStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TString DebugMessage;

    TMeteringStats Processed = TMeteringStatsHelper::ZeroValue();

    TIndexBuildShardStatus(TSerializedTableRange range, TString lastKeyAck);

    TString ToString(TShardIdx shardIdx = InvalidShardIdx) const {
        TStringBuilder result;

        result << "TShardStatus {";

        if (shardIdx) {
            result << " ShardIdx: " << shardIdx;
        }
        result << " Status: " << NKikimrIndexBuilder::EBuildStatus_Name(Status);
        result << " UploadStatus: " << Ydb::StatusIds::StatusCode_Name(UploadStatus);
        result << " DebugMessage: " << DebugMessage;
        result << " SeqNoRound: " << SeqNoRound;
        result << " Processed: " << Processed.ShortDebugString();

        result << " }";

        return result;
    }
};

// TODO(mbkkt) separate it to 3 classes: TBuildColumnsInfo TBuildSecondaryInfo TBuildVectorInfo with single base TBuildInfo
struct TIndexBuildInfo: public TSimpleRefCount<TIndexBuildInfo> {
    using TPtr = TIntrusivePtr<TIndexBuildInfo>;

    enum class EState: ui32 {
        Invalid = 0,
        AlterMainTable = 5,
        Locking = 10,
        GatheringStatistics = 20,
        Initiating = 30,
        Filling = 40,
        DropBuild = 45,
        CreateBuild = 46,
        LockBuild = 47,
        Applying = 50,
        Unlocking = 60,
        AlterSequence = 61,
        Done = 200,

        Cancellation_Applying = 350,
        Cancellation_Unlocking = 360,
        Cancellation_DroppingColumns = 370,
        Cancelled = 400,

        Rejection_Applying = 500,
        Rejection_Unlocking = 510,
        Rejection_DroppingColumns = 520,
        Rejected = 550
    };

    enum class ESubState: ui32 {
        // Common
        None = 0,

        // Filling
        UniqIndexValidation = 100,

        // Fulltext
        FulltextIndexStats = 200,
        FulltextIndexDictionary = 201,
        FulltextIndexBorders = 202,
    };

    struct TColumnBuildInfo {
        TString ColumnName;
        Ydb::TypedValue DefaultFromLiteral;
        bool NotNull = false;
        TString FamilyName;

        TColumnBuildInfo(const TString& name, const TString& serializedLiteral, bool notNull, const TString& familyName)
            : ColumnName(name)
            , NotNull(notNull)
            , FamilyName(familyName)
        {
            Y_ENSURE(DefaultFromLiteral.ParseFromString(serializedLiteral));
        }

        TColumnBuildInfo(const TString& name, const Ydb::TypedValue& defaultFromLiteral, bool notNull, const TString& familyName)
            : ColumnName(name)
            , DefaultFromLiteral(defaultFromLiteral)
            , NotNull(notNull)
            , FamilyName(familyName)
        {
        }

        void SerializeToProto(NKikimrIndexBuilder::TColumnBuildSetting* setting) const {
            setting->SetColumnName(ColumnName);
            setting->mutable_default_from_literal()->CopyFrom(DefaultFromLiteral);
            setting->SetNotNull(NotNull);
            setting->SetFamily(FamilyName);
        }
    };

    enum class EBuildKind : ui32 {
        BuildKindUnspecified = 0,
        BuildSecondaryIndex = 10,
        BuildVectorIndex = 11,
        BuildPrefixedVectorIndex = 12,
        BuildSecondaryUniqueIndex = 13,
        BuildColumns = 20,
        BuildFulltext = 30,
    };

    TActorId CreateSender;
    ui64 SenderCookie = 0;

    TIndexBuildId Id;
    TString Uid;
    TMaybe<TString> UserSID;

    TPathId DomainPathId;
    TPathId TablePathId;
    NKikimrSchemeOp::EIndexType IndexType = NKikimrSchemeOp::EIndexTypeInvalid;

    EBuildKind BuildKind = EBuildKind::BuildKindUnspecified;

    TString IndexName;
    TVector<TString> IndexColumns;
    TVector<TString> DataColumns;
    TVector<TString> FillIndexColumns;
    TVector<TString> FillDataColumns;

    NKikimrIndexBuilder::TIndexBuildScanSettings ScanSettings;

    TVector<TColumnBuildInfo> BuildColumns;

    TString TargetName;
    TVector<NKikimrSchemeOp::TTableDescription> ImplTableDescriptions;

    std::variant<std::monostate,
        NKikimrSchemeOp::TVectorIndexKmeansTreeDescription,
        NKikimrSchemeOp::TFulltextIndexDescription> SpecializedIndexDescription;

    struct TKMeans {
        // TODO(mbkkt) move to TVectorIndexKmeansTreeDescription
        ui32 K = 0;
        ui32 Levels = 0;
        ui32 Rounds = 0;
        ui32 OverlapClusters = 0;
        double OverlapRatio = 0;
        bool IsPrefixed = false;

        // progress
        enum EState : ui32 {
            Sample = 0,
            Reshuffle,
            MultiLocal,
            Recompute,
            Filter,
            FilterBorders,
        };
        ui32 Level = 1;
        ui32 Round = 0;
        bool IsEmpty = false;

        EState State = Sample;

        bool AlterPrefixSequenceDone = false;

        NTableIndex::NKMeans::TClusterId ParentBegin = 0;  // included
        NTableIndex::NKMeans::TClusterId Parent = ParentBegin;

        NTableIndex::NKMeans::TClusterId ChildBegin = 1;  // included
        NTableIndex::NKMeans::TClusterId Child = ChildBegin;

        TVector<TString> FilterBorderRows;

        ui64 TableSize = 0;

        ui64 ParentEnd() const noexcept;
        ui64 ChildEnd() const noexcept;

        ui64 ParentCount() const noexcept;
        ui64 ChildCount() const noexcept;

        TString DebugString() const;

        bool NeedsAnotherLevel() const noexcept;
        bool NeedsAnotherParent() const noexcept;
        bool NextParent() noexcept;
        bool NextLevel() noexcept;
        void PrefixIndexDone(ui64 shards);

        void Set(ui32 level,
            NTableIndex::NKMeans::TClusterId parentBegin, NTableIndex::NKMeans::TClusterId parent,
            NTableIndex::NKMeans::TClusterId childBegin, NTableIndex::NKMeans::TClusterId child,
            ui32 state, ui64 tableSize, ui32 round, bool isEmpty);

        NKikimrTxDataShard::EKMeansState GetUpload() const;

        TString WriteTo(bool needsBuildTable = false) const;
        TString ReadFrom() const;
        int NextBuildIndex() const;
        const char* NextBuildSuffix() const;

        std::pair<NTableIndex::NKMeans::TClusterId, NTableIndex::NKMeans::TClusterId> RangeToBorders(const TSerializedTableRange& range) const;

        TString RangeToDebugStr(const TSerializedTableRange& range) const;

    private:
        void NextLevel(ui64 childCount) noexcept;
    };
    TKMeans KMeans;

    EState State = EState::Invalid;
    ESubState SubState = ESubState::None;
private:
    TString Issue;
public:
    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();
    bool IsBroken = false;

    TSet<TActorId> Subscribers;

    bool CancelRequested = false;

    bool AlterMainTableTxDone = false;
    bool LockTxDone = false;
    bool InitiateTxDone = false;
    bool ApplyTxDone = false;
    bool UnlockTxDone = false;
    bool DropColumnsTxDone = false;

    bool BillingEventIsScheduled = false;

    TTxId AlterMainTableTxId = TTxId();
    TTxId LockTxId = TTxId();
    TTxId InitiateTxId = TTxId();
    TTxId ApplyTxId = TTxId();
    TTxId UnlockTxId = TTxId();
    TTxId DropColumnsTxId = TTxId();

    NKikimrScheme::EStatus AlterMainTableTxStatus = NKikimrScheme::StatusSuccess;
    NKikimrScheme::EStatus LockTxStatus = NKikimrScheme::StatusSuccess;
    NKikimrScheme::EStatus InitiateTxStatus = NKikimrScheme::StatusSuccess;
    NKikimrScheme::EStatus ApplyTxStatus = NKikimrScheme::StatusSuccess;
    NKikimrScheme::EStatus UnlockTxStatus = NKikimrScheme::StatusSuccess;
    NKikimrScheme::EStatus DropColumnsTxStatus = NKikimrScheme::StatusSuccess;

    TStepId SnapshotStep;
    TTxId SnapshotTxId;

    TDuration ReBillPeriod = TDuration::Seconds(10);

    TMap<TShardIdx, TIndexBuildShardStatus> Shards;
    TDeque<TShardIdx> ToUploadShards;
    THashSet<TShardIdx> InProgressShards;
    std::vector<TShardIdx> DoneShards;
    ui32 MaxInProgressShards = 32;

    TMeteringStats Processed = TMeteringStatsHelper::ZeroValue();
    TMeteringStats Billed = TMeteringStatsHelper::ZeroValue();

    struct TSample {
        struct TRow {
            ui64 P = 0;
            TString Row;

            explicit TRow(ui64 p, TString&& row)
                : P{p}
                , Row{std::move(row)}
            {
            }

            bool operator<(const TRow& other) const {
                return P < other.P;
            }
        };
        using TRows = TVector<TRow>;

        TRows Rows;
        ui64 MaxProbability = std::numeric_limits<ui64>::max();
        enum class EState {
            Collect = 0,
            Upload,
            Done,
        };
        EState State = EState::Collect;

        TString DebugString() const {
            return TStringBuilder()
                << "{ "
                << "State = " << State
                << ", Rows = " << Rows.size()
                << ", MaxProbability = " << MaxProbability
                << " }";
        }

        bool MakeWeakTop(ui64 k) {
            // 2 * k is needed to make it linear, 2 * N at all.
            // x * k approximately is x / (x - 1) * N, but with larger x more memory used
            if (Rows.size() < 2 * k) {
                return false;
            }
            MakeTop(k);
            return true;
        }

        void MakeStrictTop(ui64 k) {
            // The idea is send to shards smallest possible max probability
            // Even if only single element was pushed from last time,
            // we want to account it and decrease max possible probability.
            // to potentially decrease counts of serialized and sent by network rows
            if (Rows.size() < k) {
                return;
            }
            if (Rows.size() == k) {
                if (Y_UNLIKELY(MaxProbability == std::numeric_limits<ui64>::max())) {
                    MaxProbability = std::max_element(Rows.begin(), Rows.end())->P;
                }
                return;
            }
            MakeTop(k);
        }

        void Clear() {
            Rows.clear();
            MaxProbability = std::numeric_limits<ui64>::max();
            State = EState::Collect;
        }

        void Add(ui64 probability, TString data) {
            Rows.emplace_back(probability, std::move(data));
            MaxProbability = std::max(probability + 1, MaxProbability + 1) - 1;
        }

    private:
        void MakeTop(ui64 k) {
            Y_ENSURE(k > 0);
            auto kth = Rows.begin() + k - 1;
            // TODO(mbkkt) use floyd rivest
            std::nth_element(Rows.begin(), kth, Rows.end());
            Rows.erase(kth + 1, Rows.end());
            Y_ENSURE(kth->P < MaxProbability);
            MaxProbability = kth->P;
        }
    };
    TSample Sample;

    std::unique_ptr<NKikimr::NKMeans::IClusters> Clusters;

    TString DebugString() const {
        auto result = TStringBuilder() << BuildKind << " " << State << "/" << SubState << " ";

        if (IsBuildVectorIndex()) {
            result << KMeans.DebugString() << ", "
                << "{ Rows = " << Sample.Rows.size()
                << ", Sample = " << Sample.State
                << ", Clusters = " << Clusters->GetClusters().size() << " }, ";
        }

        result
            << "{ Done = " << DoneShards.size()
            << ", ToUpload = " << ToUploadShards.size()
            << ", InProgress = " << InProgressShards.size() << " }";

        return result;
    }

    static bool IsValidState(EState value);
    static bool IsValidSubState(ESubState value);
    static bool IsValidBuildKind(EBuildKind value);

    struct TClusterShards {
        NTableIndex::NKMeans::TClusterId From = std::numeric_limits<NTableIndex::NKMeans::TClusterId>::max();
        std::vector<TShardIdx> Shards;
    };
    TMap<NTableIndex::NKMeans::TClusterId, TClusterShards> Cluster2Shards; // To => { From, Shards }

    void AddParent(const TSerializedTableRange& range, TShardIdx shard);

    template<class TRow>
    void AddBuildColumnInfo(const TRow& row) {
        TString columnName = row.template GetValue<Schema::BuildColumnOperationSettings::ColumnName>();
        TString defaultFromLiteral = row.template GetValue<Schema::BuildColumnOperationSettings::DefaultFromLiteral>();
        bool notNull = row.template GetValue<Schema::BuildColumnOperationSettings::NotNull>();
        TString familyName = row.template GetValue<Schema::BuildColumnOperationSettings::FamilyName>();
        BuildColumns.push_back(TColumnBuildInfo(columnName, defaultFromLiteral, notNull, familyName));
    }

    template<class TRowSetType>
    void AddIndexColumnInfo(const TRowSetType& row) {

        TString columnName =
            row.template GetValue<Schema::IndexBuildColumns::ColumnName>();
        EIndexColumnKind columnKind =
            row.template GetValueOrDefault<Schema::IndexBuildColumns::ColumnKind>(
                EIndexColumnKind::KeyColumn);
        ui32 columnNo = row.template GetValue<Schema::IndexBuildColumns::ColumnNo>();

        Y_ENSURE(columnNo == (IndexColumns.size() + DataColumns.size()),
                   "Unexpected non contiguous column number# "
                       << columnNo << " indexColumns# "
                       << IndexColumns.size() << " dataColumns# "
                       << DataColumns.size());

        switch (columnKind) {
        case EIndexColumnKind::KeyColumn:
            IndexColumns.push_back(columnName);
            break;
        case EIndexColumnKind::DataColumn:
            DataColumns.push_back(columnName);
            break;
        default:
            Y_ENSURE(false, "Unknown column kind# " << (int)columnKind);
            break;
        }
    }

    template<class TRow>
    static void FillFromRow(const TRow& row, TIndexBuildInfo* indexInfo) {
        Y_ENSURE(indexInfo); // TODO: pass by ref

        TIndexBuildId id = row.template GetValue<Schema::IndexBuild::Id>();
        TString uid = row.template GetValue<Schema::IndexBuild::Uid>();

        // note: essential fields go first to be filled if an error occurs
        indexInfo->Id = id;
        indexInfo->Uid = uid;

        indexInfo->Issue =
            row.template GetValueOrDefault<Schema::IndexBuild::Issue>();

        indexInfo->State = TIndexBuildInfo::EState(
            row.template GetValue<Schema::IndexBuild::State>());
        if (!IsValidState(indexInfo->State)) {
            indexInfo->IsBroken = true;
            indexInfo->AddIssue(TStringBuilder() << "Unknown build state: " << ui32(indexInfo->State));
            indexInfo->State = TIndexBuildInfo::EState::Invalid;
        }
        indexInfo->SubState = TIndexBuildInfo::ESubState(
            row.template GetValueOrDefault<Schema::IndexBuild::SubState>(ui32(TIndexBuildInfo::ESubState::None)));
        if (!IsValidSubState(indexInfo->SubState)) {
            indexInfo->IsBroken = true;
            indexInfo->AddIssue(TStringBuilder() << "Unknown build sub-state: " << ui32(indexInfo->SubState));
            indexInfo->SubState = TIndexBuildInfo::ESubState::None;
        }

        // note: please note that here we specify BuildSecondaryIndex as operation default,
        // because previously this table was dedicated for build secondary index operations only.
        indexInfo->BuildKind = TIndexBuildInfo::EBuildKind(
            row.template GetValueOrDefault<Schema::IndexBuild::BuildKind>(
                ui32(TIndexBuildInfo::EBuildKind::BuildSecondaryIndex)));
        if (!IsValidBuildKind(indexInfo->BuildKind)) {
            indexInfo->IsBroken = true;
            indexInfo->AddIssue(TStringBuilder() << "Unknown build kind: " << ui32(indexInfo->BuildKind));
            indexInfo->BuildKind = TIndexBuildInfo::EBuildKind::BuildKindUnspecified;
        }

        indexInfo->DomainPathId =
            TPathId(row.template GetValue<Schema::IndexBuild::DomainOwnerId>(),
                    row.template GetValue<Schema::IndexBuild::DomainLocalId>());

        indexInfo->TablePathId =
            TPathId(row.template GetValue<Schema::IndexBuild::TableOwnerId>(),
                    row.template GetValue<Schema::IndexBuild::TableLocalId>());

        indexInfo->IndexName = row.template GetValue<Schema::IndexBuild::IndexName>();
        indexInfo->IndexType = row.template GetValue<Schema::IndexBuild::IndexType>();

        indexInfo->CancelRequested =
            row.template GetValueOrDefault<Schema::IndexBuild::CancelRequest>(false);
        if (row.template HaveValue<Schema::IndexBuild::UserSID>()) {
            indexInfo->UserSID = row.template GetValue<Schema::IndexBuild::UserSID>();
        }
        indexInfo->StartTime = TInstant::Seconds(row.template GetValueOrDefault<Schema::IndexBuild::StartTime>());
        indexInfo->EndTime = TInstant::Seconds(row.template GetValueOrDefault<Schema::IndexBuild::EndTime>());

        indexInfo->LockTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::LockTxId>(
                indexInfo->LockTxId);
        indexInfo->LockTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::LockTxStatus>(
                indexInfo->LockTxStatus);
        indexInfo->LockTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::LockTxDone>(
                indexInfo->LockTxDone);

        indexInfo->InitiateTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::InitiateTxId>(
                indexInfo->InitiateTxId);
        indexInfo->InitiateTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::InitiateTxStatus>(
                indexInfo->InitiateTxStatus);
        indexInfo->InitiateTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::InitiateTxDone>(
                indexInfo->InitiateTxDone);

        indexInfo->ScanSettings.SetMaxBatchRows(
            row.template GetValue<Schema::IndexBuild::MaxBatchRows>());
        indexInfo->ScanSettings.SetMaxBatchBytes(
            row.template GetValue<Schema::IndexBuild::MaxBatchBytes>());
        indexInfo->MaxInProgressShards =
            row.template GetValue<Schema::IndexBuild::MaxShards>();
        indexInfo->ScanSettings.SetMaxBatchRetries(
            row.template GetValueOrDefault<Schema::IndexBuild::MaxRetries>(
                indexInfo->ScanSettings.GetMaxBatchRetries()));

        indexInfo->ApplyTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::ApplyTxId>(
                indexInfo->ApplyTxId);
        indexInfo->ApplyTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::ApplyTxStatus>(
                indexInfo->ApplyTxStatus);
        indexInfo->ApplyTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::ApplyTxDone>(
                indexInfo->ApplyTxDone);

        indexInfo->UnlockTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::UnlockTxId>(
                indexInfo->UnlockTxId);
        indexInfo->UnlockTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::UnlockTxStatus>(
                indexInfo->UnlockTxStatus);
        indexInfo->UnlockTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::UnlockTxDone>(
                indexInfo->UnlockTxDone);

        indexInfo->AlterMainTableTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::AlterMainTableTxId>(
                indexInfo->AlterMainTableTxId);
        indexInfo->AlterMainTableTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::AlterMainTableTxStatus>(
                indexInfo->AlterMainTableTxStatus);
        indexInfo->AlterMainTableTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::AlterMainTableTxDone>(
                indexInfo->AlterMainTableTxDone);

        indexInfo->DropColumnsTxId =
            row.template GetValueOrDefault<Schema::IndexBuild::DropColumnsTxId>(
                indexInfo->DropColumnsTxId);
        indexInfo->DropColumnsTxStatus =
            row.template GetValueOrDefault<Schema::IndexBuild::DropColumnsTxStatus>(
                indexInfo->DropColumnsTxStatus);
        indexInfo->DropColumnsTxDone =
            row.template GetValueOrDefault<Schema::IndexBuild::DropColumnsTxDone>(
                indexInfo->DropColumnsTxDone);

        indexInfo->Billed.SetUploadRows(row.template GetValueOrDefault<Schema::IndexBuild::UploadRowsBilled>(0));
        indexInfo->Billed.SetUploadBytes(row.template GetValueOrDefault<Schema::IndexBuild::UploadBytesBilled>(0));
        indexInfo->Billed.SetReadRows(row.template GetValueOrDefault<Schema::IndexBuild::ReadRowsBilled>(0));
        indexInfo->Billed.SetReadBytes(row.template GetValueOrDefault<Schema::IndexBuild::ReadBytesBilled>(0));
        indexInfo->Billed.SetCpuTimeUs(row.template GetValueOrDefault<Schema::IndexBuild::CpuTimeUsBilled>(0));

        indexInfo->Processed.SetUploadRows(row.template GetValueOrDefault<Schema::IndexBuild::UploadRowsProcessed>(0));
        indexInfo->Processed.SetUploadBytes(row.template GetValueOrDefault<Schema::IndexBuild::UploadBytesProcessed>(0));
        indexInfo->Processed.SetReadRows(row.template GetValueOrDefault<Schema::IndexBuild::ReadRowsProcessed>(0));
        indexInfo->Processed.SetReadBytes(row.template GetValueOrDefault<Schema::IndexBuild::ReadBytesProcessed>(0));
        indexInfo->Processed.SetCpuTimeUs(row.template GetValueOrDefault<Schema::IndexBuild::CpuTimeUsProcessed>(0));

        // Restore the operation details: ImplTableDescriptions and SpecializedIndexDescription.
        if (row.template HaveValue<Schema::IndexBuild::CreationConfig>()) {
            NKikimrSchemeOp::TIndexCreationConfig creationConfig;
            Y_ENSURE(creationConfig.ParseFromString(row.template GetValue<Schema::IndexBuild::CreationConfig>()));

            auto& descriptions = *creationConfig.MutableIndexImplTableDescriptions();
            indexInfo->ImplTableDescriptions.reserve(descriptions.size());
            for (auto& description : descriptions) {
                indexInfo->ImplTableDescriptions.emplace_back(std::move(description));
            }

            switch (creationConfig.GetSpecializedIndexDescriptionCase()) {
                case NKikimrSchemeOp::TIndexCreationConfig::kVectorIndexKmeansTreeDescription: {
                    auto& desc = *creationConfig.MutableVectorIndexKmeansTreeDescription();
                    TString createError;
                    Y_ENSURE(NKikimr::NKMeans::ValidateSettings(desc.settings(), createError), createError);
                    indexInfo->KMeans.K = desc.settings().clusters();
                    indexInfo->KMeans.Levels = indexInfo->IsBuildPrefixedVectorIndex() + desc.settings().levels();
                    indexInfo->KMeans.IsPrefixed = indexInfo->IsBuildPrefixedVectorIndex();
                    indexInfo->KMeans.Rounds = NTableIndex::NKMeans::DefaultKMeansRounds;
                    indexInfo->KMeans.OverlapClusters = desc.settings().overlap_clusters()
                        ? desc.settings().overlap_clusters()
                        : NTableIndex::NKMeans::DefaultOverlapClusters;
                    indexInfo->KMeans.OverlapRatio = desc.settings().has_overlap_ratio()
                        ? desc.settings().overlap_ratio()
                        : NTableIndex::NKMeans::DefaultOverlapRatio;
                    indexInfo->Clusters = NKikimr::NKMeans::CreateClusters(desc.settings().settings(), indexInfo->KMeans.Rounds, createError);
                    Y_ENSURE(indexInfo->Clusters, createError);
                    indexInfo->SpecializedIndexDescription = std::move(desc);
                    break;
                }
                case NKikimrSchemeOp::TIndexCreationConfig::kFulltextIndexDescription: {
                    auto& desc = *creationConfig.MutableFulltextIndexDescription();
                    indexInfo->SpecializedIndexDescription = std::move(desc);
                    break;
                }
                case NKikimrSchemeOp::TIndexCreationConfig::SPECIALIZEDINDEXDESCRIPTION_NOT_SET:
                    /* do nothing */
                    break;
                }
        }

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::BUILD_INDEX,
            "Restored index build id# " << indexInfo->Id << ": " << *indexInfo);
    }

    template<class TRow>
    void AddShardStatus(const TRow& row) {
        TShardIdx shardIdx =
            TShardIdx(row.template GetValue<
                          Schema::IndexBuildShardStatus::OwnerShardIdx>(),
                      row.template GetValue<
                          Schema::IndexBuildShardStatus::LocalShardIdx>());

        NKikimrTx::TKeyRange range =
            row.template GetValue<Schema::IndexBuildShardStatus::Range>();
        TString lastKeyAck =
            row.template GetValue<Schema::IndexBuildShardStatus::LastKeyAck>();

        TSerializedTableRange bound{range};
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::BUILD_INDEX,
            "AddShardStatus id# " << Id << " shard " << shardIdx);
        if (BuildKind == TIndexBuildInfo::EBuildKind::BuildVectorIndex &&
            KMeans.State != TIndexBuildInfo::TKMeans::Filter &&
            KMeans.State != TIndexBuildInfo::TKMeans::FilterBorders)
        {
            AddParent(bound, shardIdx);
        }
        Shards.emplace(
            shardIdx, TIndexBuildShardStatus(std::move(bound), std::move(lastKeyAck)));
        TIndexBuildShardStatus &shardStatus = Shards.at(shardIdx);

        shardStatus.Status =
            row.template GetValue<Schema::IndexBuildShardStatus::Status>();

        shardStatus.DebugMessage = row.template GetValueOrDefault<
            Schema::IndexBuildShardStatus::Message>();
        shardStatus.UploadStatus = row.template GetValueOrDefault<
            Schema::IndexBuildShardStatus::UploadStatus>(
            Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);

        shardStatus.Processed.SetUploadRows(row.template GetValueOrDefault<Schema::IndexBuildShardStatus::UploadRowsProcessed>(0));
        shardStatus.Processed.SetUploadBytes(row.template GetValueOrDefault<Schema::IndexBuildShardStatus::UploadBytesProcessed>(0));
        shardStatus.Processed.SetReadRows(row.template GetValueOrDefault<Schema::IndexBuildShardStatus::ReadRowsProcessed>(0));
        shardStatus.Processed.SetReadBytes(row.template GetValueOrDefault<Schema::IndexBuildShardStatus::ReadBytesProcessed>(0));
        shardStatus.Processed.SetCpuTimeUs(row.template GetValueOrDefault<Schema::IndexBuildShardStatus::CpuTimeUsProcessed>(0));
        Processed += shardStatus.Processed;

        shardStatus.DocCount = row.template GetValueOrDefault<Schema::IndexBuildShardStatus::DocCount>(0);
        shardStatus.TotalDocLength = row.template GetValueOrDefault<Schema::IndexBuildShardStatus::TotalDocLength>(0);
        shardStatus.FirstToken = row.template GetValueOrDefault<Schema::IndexBuildShardStatus::FirstToken>();
        shardStatus.FirstTokenRows = row.template GetValueOrDefault<Schema::IndexBuildShardStatus::FirstTokenRows>();
        shardStatus.LastToken = row.template GetValueOrDefault<Schema::IndexBuildShardStatus::LastToken>();
        shardStatus.LastTokenRows = row.template GetValueOrDefault<Schema::IndexBuildShardStatus::LastTokenRows>();
    }

    bool IsCancellationRequested() const {
        return CancelRequested;
    }

    TString InvalidBuildKind() {
        return TStringBuilder() << "Invalid index build kind " << static_cast<int>(BuildKind)
            << " for index type " << static_cast<int>(IndexType);
    }

    bool IsBuildSecondaryIndex() const {
        return BuildKind == EBuildKind::BuildSecondaryIndex;
    }

    bool IsBuildSecondaryUniqueIndex() const {
        return BuildKind == EBuildKind::BuildSecondaryUniqueIndex;
    }

    bool IsBuildPrefixedVectorIndex() const {
        return BuildKind == EBuildKind::BuildPrefixedVectorIndex;
    }

    bool IsBuildVectorIndex() const {
        return BuildKind == EBuildKind::BuildVectorIndex || IsBuildPrefixedVectorIndex();
    }

    bool IsBuildFulltextIndex() const {
        return BuildKind == EBuildKind::BuildFulltext;
    }

    bool IsBuildIndex() const {
        return IsBuildSecondaryIndex() || IsBuildSecondaryUniqueIndex() || IsBuildVectorIndex() || IsBuildFulltextIndex();
    }

    bool IsBuildColumns() const {
        return BuildKind == EBuildKind::BuildColumns;
    }

    bool IsPreparing() const {
        return State == EState::AlterMainTable ||
               State == EState::Locking ||
               State == EState::GatheringStatistics ||
               State == EState::Initiating;
    }

    bool IsTransferring() const {
        return State == EState::Filling ||
               State == EState::DropBuild ||
               State == EState::CreateBuild ||
               State == EState::LockBuild ||
               State == EState::AlterSequence;
    }

    bool IsApplying() const {
        return State == EState::Applying ||
               State == EState::Unlocking;
    }

    bool IsDone() const {
        return State == EState::Done;
    }

    bool IsCancelled() const {
        return State == EState::Cancelled || State == EState::Rejected;
    }

    bool IsFinished() const {
        return IsDone() || IsCancelled();
    }

    bool IsValidatingUniqueIndex() const {
        return SubState == ESubState::UniqIndexValidation;
    }

    bool IsFlatRelevanceFulltext() const {
        if (BuildKind != EBuildKind::BuildFulltext) {
            return false;
        }
        return IndexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance;
    }

    void AddNotifySubscriber(const TActorId& actorID) {
        Y_ENSURE(!IsFinished());
        Subscribers.insert(actorID);
    }

    const TString& GetIssue() const {
        return Issue;
    }

    bool AddIssue(TString issue) {
        if (Issue.Contains(issue)) { // deduplication
            return false;
        }

        if (Issue) {
            // TODO: store as list?
            Issue += "; ";
        }
        Issue += issue;
        return true;
    }

    float CalcProgressPercent() const {
        const auto total = Shards.size();
        const auto done = DoneShards.size();
        if (IsBuildVectorIndex()) {
            const auto inProgress = InProgressShards.size();
            const auto toUpload = ToUploadShards.size();
            Y_ENSURE(KMeans.Level != 0);
            if (!KMeans.NeedsAnotherLevel() && !KMeans.NeedsAnotherParent()
                && toUpload == 0 && inProgress == 0) {
                return 100.f;
            }
            // TODO(mbkkt) more detailed progress?
            return (100.f * (KMeans.Level - 1)) / KMeans.Levels;
        }
        if (Shards) {
            return (100.f * done) / total;
        }
        // No shards - no progress
        return 0.f;
    }

    void SerializeToProto(TSchemeShard* ss, NKikimrIndexBuilder::TColumnBuildSettings* to) const;
    void SerializeToProto(TSchemeShard* ss, NKikimrSchemeOp::TIndexBuildConfig* to) const;

};


}

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NSchemeShard::TIndexBuildShardStatus, stream, value) {
    stream << value.ToString();
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NSchemeShard::TIndexBuildInfo, o, info) {
    o << "TBuildInfo{";
    o << " IndexBuildId: " << info.Id;
    o << ", Uid: " << info.Uid;
    o << ", DomainPathId: " << info.DomainPathId;
    o << ", TablePathId: " << info.TablePathId;
    o << ", IndexType: " << NKikimrSchemeOp::EIndexType_Name(info.IndexType);
    o << ", IndexName: " << info.IndexName;
    for (const auto& x: info.IndexColumns) {
        o << ", IndexColumn: " << x;
    }
    for (const auto& x: info.DataColumns) {
        o << ", DataColumns: " << x;
    }

    o << ", State: " << info.State;
    o << ", SubState: " << info.SubState;
    o << ", IsBroken: " << info.IsBroken;
    o << ", IsCancellationRequested: " << info.CancelRequested;

    o << ", Issue: " << info.GetIssue();
    o << ", SubscribersCount: " << info.Subscribers.size();

    o << ", CreateSender: " << info.CreateSender.ToString();

    o << ", AlterMainTableTxId: " << info.AlterMainTableTxId;
    o << ", AlterMainTableTxStatus: " <<  NKikimrScheme::EStatus_Name(info.AlterMainTableTxStatus);
    o << ", AlterMainTableTxDone: " << info.AlterMainTableTxDone;

    o << ", LockTxId: " << info.LockTxId;
    o << ", LockTxStatus: " << NKikimrScheme::EStatus_Name(info.LockTxStatus);
    o << ", LockTxDone: " << info.LockTxDone;

    o << ", InitiateTxId: " << info.InitiateTxId;
    o << ", InitiateTxStatus: " << NKikimrScheme::EStatus_Name(info.InitiateTxStatus);
    o << ", InitiateTxDone: " << info.InitiateTxDone;

    o << ", SnapshotStepId: " << info.SnapshotStep;

    o << ", ApplyTxId: " << info.ApplyTxId;
    o << ", ApplyTxStatus: " << NKikimrScheme::EStatus_Name(info.ApplyTxStatus);
    o << ", ApplyTxDone: " << info.ApplyTxDone;

    o << ", DropColumnsTxId: " << info.DropColumnsTxId;
    o << ", DropColumnsTxStatus: " << NKikimrScheme::EStatus_Name(info.DropColumnsTxStatus);
    o << ", DropColumnsTxDone: " << info.DropColumnsTxDone;

    o << ", UnlockTxId: " << info.UnlockTxId;
    o << ", UnlockTxStatus: " << NKikimrScheme::EStatus_Name(info.UnlockTxStatus);
    o << ", UnlockTxDone: " << info.UnlockTxDone;

    o << ", ToUploadShards: " << info.ToUploadShards.size();
    o << ", DoneShards: " << info.DoneShards.size();

    for (const auto& x: info.InProgressShards) {
        o << ", ShardsInProgress: " << x;
    }

    o << ", Processed: " << info.Processed;
    o << ", Billed: " << info.Billed;

    o << "}";
}
