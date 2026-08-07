// Unit tests for TKqpVectorSearchActor's state machine (level traversal -> posting scan ->
// main read). Its phase transitions depend on the order in which the inner reads hand rows
// over, which integration tests cannot control, so every inner read here is a fake the test
// scripts: which rows it returns, when it finishes, when it fails.
//
// InvalidCentroidReportedOnce is intentionally left red: ProcessReadRow returns after reporting
// an error, but its caller's rows.ForEachRow keeps iterating the batch, so N broken rows produce
// N error events instead of one. Goes green once ProcessReadRow checks Failed on entry.

#include <ydb/core/kqp/runtime/kqp_vector_search_actor.h>

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <deque>
#include <variant>

namespace NKikimr::NKqp {
    namespace {

        using namespace NActors;
        using namespace NKikimr::NMiniKQL;
        using namespace NYql::NDq;
        using NTableIndex::NKMeans::TClusterId;

        constexpr ui64 TestInputIndex = 0;

        const TString LevelTablePath = "/Root/TestTable/index1/indexImplLevelTable";
        const TString PostingTablePath = "/Root/TestTable/index1/indexImplPostingTable";
        const TString MainTablePath = "/Root/TestTable";

        // Column ids: the same column has different ids in the main and posting tables.
        constexpr ui32 MainPkColumnId = 1;
        constexpr ui32 MainEmbColumnId = 2;
        constexpr ui32 MainPkBColumnId = 4;
        constexpr ui32 PostingParentColumnId = 11;
        constexpr ui32 PostingPkColumnId = 12;
        constexpr ui32 PostingEmbColumnId = 13;
        constexpr ui32 PostingPkBColumnId = 15;
        constexpr ui32 LevelParentColumnId = 21;
        constexpr ui32 LevelIdColumnId = 22;
        constexpr ui32 LevelCentroidColumnId = 23;

        constexpr ui64 OwnerId = 72057594046644480ull;
        constexpr ui64 LevelTableId = 3;

        TPathId LevelTablePathId() {
            return TPathId(OwnerId, LevelTableId);
        }

        // ---- test data -------------------------------------------------------------

        // A uint8 Knn vector: payload plus the trailing format byte CreateClustersAutoDetect
        // derives the type and the dimension from.
        TString Vec(std::initializer_list<ui8> payload) {
            TString vector;
            for (ui8 byte : payload) {
                vector.push_back(static_cast<char>(byte));
            }
            vector.push_back(static_cast<char>(2)); // EFormat::Uint8Vector
            return vector;
        }

        // The target vector every test searches for.
        const TString Target = Vec({0x67, 0x71});
        // Equal to the target: cosine distance 0, always ranks first.
        const TString NearVec = Vec({0x67, 0x71});
        // Another direction: cosine distance ~0.32, so it always ranks after NearVec.
        const TString FarVec = Vec({0x7f, 0x01});
        // Nearly the target's direction but much shorter, which only a similarity metric sees.
        const TString WeakVec = Vec({0x10, 0x10});
        // Rejected by IClusters::IsExpectedFormat: 0xff is not a valid format byte.
        const TString BrokenVec = "\xff\xff\xff";

        // One cell of a fake read row: Uint64 keys, String embeddings, or a null embedding.
        using TFakeCell = std::variant<ui64, TString, std::monostate>;
        using TFakeRow = TVector<TFakeCell>;

        // A null cell: an unranked row, which AddCandidate scores as distance = max().
        const TFakeCell NullCell = std::monostate{};

        NUdf::TUnboxedValuePod MakeCellValue(const TFakeCell& cell) {
            if (const ui64* number = std::get_if<ui64>(&cell)) {
                return NUdf::TUnboxedValuePod(*number);
            }
            if (const TString* text = std::get_if<TString>(&cell)) {
                return MakeString(*text);
            }
            return NUdf::TUnboxedValuePod();
        }

        // Level table row layout, as ProcessReadRow reads it: (parent, id, centroid).
        TFakeRow LevelRow(TClusterId parent, TClusterId child, const TString& centroid) {
            return {parent, child, centroid};
        }

        // ---- settings --------------------------------------------------------------

        void FillColumn(NKikimrTxDataShard::TKqpTransaction::TColumnMeta& column,
                        ui32 id, const TString& name, ui32 typeId)
        {
            column.SetId(id);
            column.SetName(name);
            column.SetType(typeId);
            column.SetNotNull(true);
        }

        void FillTable(NKikimrTxDataShard::TKqpTransaction::TTableMeta& table, const TString& path, ui64 tableId) {
            table.SetTablePath(path);
            table.MutableTableId()->SetOwnerId(OwnerId);
            table.MutableTableId()->SetTableId(tableId);
            table.SetSchemaVersion(1);
        }

        // ---- level cache -----------------------------------------------------------

        TIntrusivePtr<TVectorIndexLevelsCache> MakeLevelsCache() {
            auto cache = MakeIntrusive<TVectorIndexLevelsCache>();
            // UseLevelCache is off until the cache has a non-zero byte cap.
            cache->SetMaxBytes(1 << 20);
            return cache;
        }

        // Cached level rows have their own layout, narrower than the read row: [id, centroid].
        TCachedLevelTableDataPtr CachedChildren(const TVector<std::pair<TClusterId, TString>>& children) {
            auto data = MakeIntrusive<TCachedLevelTableData>();
            for (const auto& [child, centroid] : children) {
                TCell cells[2] = {
                    TCell::Make(child),
                    TCell(centroid.data(), centroid.size()),
                };
                data->BatchRows.Append(TConstArrayRef<TCell>(cells, 2));
            }
            return data;
        }

        TString ParentCacheKey(TClusterId parent) {
            return TSerializedCellVec::Serialize({TCell::Make(parent)});
        }

        void PutCachedChildren(TVectorIndexLevelsCache& cache, TClusterId parent,
                               const TVector<std::pair<TClusterId, TString>>& children)
        {
            cache.Put(LevelTablePathId(), ParentCacheKey(parent), CachedChildren(children));
        }

        // Settings for the simplest shape the actor supports, with per-test overrides:
        //   main table    (pk Uint64), output columns (pk, emb)
        //   posting table (__ydb_parent, pk)
        //   level table   (__ydb_parent, __ydb_id, __ydb_centroid)
        // A snapshot is always set: without a snapshot, a lock or AllowInconsistentReads the
        // real read actor rejects the read, and TFakeInnerRead checks the same.
        class TSettingsBuilder {
        public:
            TSettingsBuilder() {
                auto* index = Settings.MutableIndexSettings();
                index->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
                index->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
                index->set_vector_dimension(2);

                Settings.SetIndexLevels(1);
                Settings.SetTopK(3);
                Settings.SetLevelTop(2);
                Settings.SetOverlapClusters(1);
                Settings.SetDatabase("/Root");
                Settings.MutableSnapshot()->SetStep(1000);
                Settings.MutableSnapshot()->SetTxId(2000);

                FillTable(*Settings.MutableLevelTable(), LevelTablePath, LevelTableId);
                Settings.SetLevelTableParentColumnId(LevelParentColumnId);
                Settings.SetLevelTableClusterColumnId(LevelIdColumnId);
                Settings.SetLevelTableCentroidColumnId(LevelCentroidColumnId);

                FillTable(*Settings.MutablePostingTable(), PostingTablePath, 2);
                Settings.AddPostingTableKeyColumnIds(PostingParentColumnId);
                Settings.AddPostingTableKeyColumnIds(PostingPkColumnId);

                FillTable(*Settings.MutableMainTable(), MainTablePath, 1);
                FillColumn(*Settings.AddMainTableKeyColumns(), MainPkColumnId, "pk", NScheme::NTypeIds::Uint64);
                FillColumn(*Settings.AddOutputColumns(), MainPkColumnId, "pk", NScheme::NTypeIds::Uint64);
                FillColumn(*Settings.AddOutputColumns(), MainEmbColumnId, "emb", NScheme::NTypeIds::String);
                Settings.SetVectorColumnIndex(1);
            }

            TSettingsBuilder& TopK(ui32 topK) {
                Settings.SetTopK(topK);
                return *this;
            }

            TSettingsBuilder& LevelTop(ui32 levelTop) {
                Settings.SetLevelTop(levelTop);
                return *this;
            }

            TSettingsBuilder& Levels(ui32 levels) {
                Settings.SetIndexLevels(levels);
                return *this;
            }

            TSettingsBuilder& Overlap(ui32 overlap) {
                Settings.SetOverlapClusters(overlap);
                return *this;
            }

            TSettingsBuilder& Prefixed() {
                Settings.SetHasPrefix(true);
                return *this;
            }

            // Covered index: every output column lives in the posting table, so no main read.
            TSettingsBuilder& Covered() {
                Settings.SetPostingCovers(true);
                Settings.AddPostingOutputColumnIds(PostingPkColumnId);
                Settings.AddPostingOutputColumnIds(PostingEmbColumnId);
                return *this;
            }

            // Partially covered: the posting table holds the embedding, not every output column.
            TSettingsBuilder& PostingEmbedding() {
                Settings.SetPostingEmbeddingColumnId(PostingEmbColumnId);
                return *this;
            }

            // Covered index whose posting table does not hold the PK as an output column: the PK
            // is appended after the output columns and dedup reads it there.
            TSettingsBuilder& CoveredEmbeddingOnly() {
                Settings.ClearOutputColumns();
                FillColumn(*Settings.AddOutputColumns(), MainEmbColumnId, "emb", NScheme::NTypeIds::String);
                Settings.SetVectorColumnIndex(0);
                Settings.SetPostingCovers(true);
                Settings.ClearPostingOutputColumnIds();
                Settings.AddPostingOutputColumnIds(PostingEmbColumnId);
                return *this;
            }

            // A two-column PK: takes the main-key sort off its single-Uint64 fast path.
            TSettingsBuilder& TwoColumnPk() {
                Settings.ClearMainTableKeyColumns();
                FillColumn(*Settings.AddMainTableKeyColumns(), MainPkColumnId, "pkA", NScheme::NTypeIds::Uint64);
                FillColumn(*Settings.AddMainTableKeyColumns(), MainPkBColumnId, "pkB", NScheme::NTypeIds::Uint64);

                Settings.ClearOutputColumns();
                FillColumn(*Settings.AddOutputColumns(), MainPkColumnId, "pkA", NScheme::NTypeIds::Uint64);
                FillColumn(*Settings.AddOutputColumns(), MainPkBColumnId, "pkB", NScheme::NTypeIds::Uint64);
                FillColumn(*Settings.AddOutputColumns(), MainEmbColumnId, "emb", NScheme::NTypeIds::String);
                Settings.SetVectorColumnIndex(2);

                Settings.ClearPostingTableKeyColumnIds();
                Settings.AddPostingTableKeyColumnIds(PostingParentColumnId);
                Settings.AddPostingTableKeyColumnIds(PostingPkColumnId);
                Settings.AddPostingTableKeyColumnIds(PostingPkBColumnId);
                return *this;
            }

            // A vector index may index a key column, so the PK *is* the String embedding column
            // -- and a read must not then request the same column id twice.
            TSettingsBuilder& EmbeddingIsPkColumn() {
                Settings.ClearMainTableKeyColumns();
                FillColumn(*Settings.AddMainTableKeyColumns(), MainEmbColumnId, "emb", NScheme::NTypeIds::String);

                Settings.ClearOutputColumns();
                FillColumn(*Settings.AddOutputColumns(), MainEmbColumnId, "emb", NScheme::NTypeIds::String);
                Settings.SetVectorColumnIndex(0);

                Settings.ClearPostingTableKeyColumnIds();
                Settings.AddPostingTableKeyColumnIds(PostingParentColumnId);
                Settings.AddPostingTableKeyColumnIds(PostingEmbColumnId);
                Settings.SetPostingEmbeddingColumnId(PostingEmbColumnId);
                return *this;
            }

            // Stale-RO: the index impl tables are read from followers. The query snapshot is
            // kept, as the executer sets it whenever it is valid regardless of followers.
            TSettingsBuilder& Followers() {
                Settings.SetUseFollowers(true);
                return *this;
            }

            // A deliberately non-default lock mode: OPTIMISTIC is the proto's zero value, so
            // asserting on it would also pass if the actor dropped the field.
            TSettingsBuilder& Lock(ui64 lockTxId) {
                Settings.SetLockTxId(lockTxId);
                Settings.SetLockNodeId(7);
                Settings.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
                return *this;
            }

            // Inconsistent online RO: neither a snapshot nor a lock, so the reads must say so.
            TSettingsBuilder& InconsistentReads() {
                Settings.ClearSnapshot();
                Settings.SetAllowInconsistentReads(true);
                return *this;
            }

            // A similarity metric: TMaxInnerProductSimilarity::Distance returns -dotProduct, so
            // the distances the actor ranks on are negative.
            // The embedding column accepts nulls, which is what makes an unranked row possible.
            TSettingsBuilder& NullableEmbedding() {
                for (auto& column : *Settings.MutableOutputColumns()) {
                    if (column.GetId() == MainEmbColumnId) {
                        column.SetNotNull(false);
                    }
                }
                return *this;
            }

            TSettingsBuilder& InnerProductMetric() {
                Settings.MutableIndexSettings()->set_metric(
                    Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
                return *this;
            }

            TSettingsBuilder& NoIndexSettings() {
                Settings.ClearIndexSettings();
                return *this;
            }

            TSettingsBuilder& UnknownMetric() {
                Settings.MutableIndexSettings()->set_metric(Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED);
                return *this;
            }

            NKikimrTxDataShard::TKqpVectorSearchSettings Build() const {
                return Settings;
            }

        private:
            NKikimrTxDataShard::TKqpVectorSearchSettings Settings;
        };

        // ---- target vector input ---------------------------------------------------

        // The transform input: structs whose element 0 is the target vector and, for a prefixed
        // index, element 1 the prefix group's root cluster id. Yields first when asked, which is
        // how the WaitInput -> WaitInput path is reached.
        class TTargetStream: public TComputationValue<TTargetStream> {
        public:
            struct TRow {
                TString Target;
                TMaybe<ui64> Parent;
                // An empty element 0, the only way to reach the actor's !IsString() &&
                // !IsEmbedded() guard: a numeric TUnboxedValuePod is Embedded and would pass it.
                bool NullTarget = false;
            };

            TTargetStream(TMemoryUsageInfo* memInfo, const THolderFactory& holderFactory,
                          TVector<TRow> rows, ui32 yieldsBeforeData)
                : TComputationValue(memInfo)
                , HolderFactory(holderFactory)
                , Rows(std::move(rows))
                , YieldsLeft(yieldsBeforeData)
            {
            }

            NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
                if (YieldsLeft > 0) {
                    --YieldsLeft;
                    return NUdf::EFetchStatus::Yield;
                }
                if (Index >= Rows.size()) {
                    return NUdf::EFetchStatus::Finish;
                }
                const TRow& row = Rows[Index++];
                NUdf::TUnboxedValue* items = nullptr;
                result = HolderFactory.CreateDirectArrayHolder(row.Parent ? 2 : 1, items);
                if (row.NullTarget) {
                    items[0] = NUdf::TUnboxedValue();
                } else {
                    items[0] = MakeString(row.Target);
                }
                if (row.Parent) {
                    items[1] = NUdf::TUnboxedValuePod(*row.Parent);
                }
                return NUdf::EFetchStatus::Ok;
            }

        private:
            const THolderFactory& HolderFactory;
            const TVector<TRow> Rows;
            ui32 YieldsLeft = 0;
            size_t Index = 0;
        };

        // ---- fake inner read -------------------------------------------------------

        enum class EReadKind {
            Level,
            Posting,
            Main,
        };

        // One inner read's observable facts. Outlives the read actor, so the settings it was
        // launched with stay inspectable after teardown.
        struct TReadObservation: public TSimpleRefCount<TReadObservation> {
            EReadKind Kind = EReadKind::Level;
            NKikimrTxDataShard::TKqpReadRangesSourceSettings Settings;
            TVector<TSerializedCellVec> KeyPoints;
            bool PassedAway = false;
            ui32 StatsDrains = 0;
            // Read-contract violations; TSearchEnv asserts them from the test thread (see Require).
            TVector<TString> Violations;
        };

        using TReadObservationPtr = TIntrusivePtr<TReadObservation>;

        // Stand-in for the inner TKqpReadActor. Passive by design: it hands over only what the
        // test queued, when the test wakes the search actor -- scripting the delivery order.
        class TFakeInnerRead: public IDqComputeActorAsyncInput, public TActorBootstrapped<TFakeInnerRead> {
        public:
            struct TStep {
                TVector<TFakeRow> Rows;
                bool Finished = false;
            };

            TFakeInnerRead(TReadObservationPtr observation, const THolderFactory& holderFactory,
                           TVector<NScheme::TTypeInfo> mainKeyTypes, TString targetVector, ui64 lockId)
                : Observation(std::move(observation))
                , HolderFactory(holderFactory)
                , MainKeyTypes(std::move(mainKeyTypes))
                , TargetVector(std::move(targetVector))
                , LockId(lockId)
            {
                CheckReadPreconditions();
            }

            void Bootstrap() {
                Become(&TFakeInnerRead::StateFunc);
            }

            STATEFN(StateFunc) {
                // Driven by the test through the search actor, not by its own events.
                Y_UNUSED(ev);
            }

            void Push(TVector<TFakeRow> rows, bool finished) {
                Steps.push_back(TStep{std::move(rows), finished});
            }

            // ---- IDqComputeActorAsyncInput ----

            ui64 GetInputIndex() const override {
                return TestInputIndex;
            }

            const TDqAsyncStats& GetIngressStats() const override {
                return IngressStats;
            }

            // One scripted step per call, and freeSpace is ignored: unlike TKqpReadActor, this
            // fake never splits a batch, so the actor's own 32MB inner freeSpace is not exercised.
            i64 GetAsyncInputData(TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished,
                                  i64 /* freeSpace */) override {
                finished = false;
                if (Steps.empty()) {
                    return 0;
                }
                TStep step = std::move(Steps.front());
                Steps.pop_front();
                i64 size = 0;
                for (const TFakeRow& row : step.Rows) {
                    NUdf::TUnboxedValue* items = nullptr;
                    NUdf::TUnboxedValue value = HolderFactory.CreateDirectArrayHolder(row.size(), items);
                    for (size_t i = 0; i < row.size(); ++i) {
                        items[i] = MakeCellValue(row[i]);
                    }
                    batch.emplace_back(std::move(value));
                    size += static_cast<i64>(row.size()) * 8;
                }
                finished = step.Finished;
                return size;
            }

            // One lock per read: a test can check the actor reports each finished read's lock.
            TMaybe<google::protobuf::Any> ExtraData() override {
                NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
                auto* lock = resultInfo.AddLocks();
                lock->SetLockId(LockId);
                lock->SetDataShard(LockId);
                google::protobuf::Any packed;
                packed.PackFrom(resultInfo);
                return packed;
            }

            void FillExtraStats(NYql::NDqProto::TDqTaskStats* stats, bool last, const TDqMeteringStats*) override {
                Require(last, "the search actor must drain inner read stats with last = true");
                ++Observation->StatsDrains;
                auto* table = stats->AddTables();
                table->SetTablePath(Observation->Settings.GetTable().GetTablePath());
                table->SetReadRows(StatsRows);
                table->SetReadBytes(StatsBytes);
            }

            void SaveState(const NYql::NDqProto::TCheckpoint&, TSourceState&) override {
            }

            void LoadState(const TSourceState&) override {
            }

            void CommitState(const NYql::NDqProto::TCheckpoint&) override {
            }

            void PassAway() override {
                Observation->PassedAway = true;
                TActorBootstrapped<TFakeInnerRead>::PassAway();
            }

            static constexpr ui64 StatsRows = 10;
            static constexpr ui64 StatsBytes = 100;

        private:
            // Records a violated read contract instead of asserting: this runs inside the search
            // actor's StateFunc, whose catch (const yexception&) would swallow the assertion
            // (NUnitTest::TAssertException is one). TSearchEnv asserts these on the test thread.
            void Require(bool condition, const TString& what) const {
                if (!condition) {
                    Observation->Violations.push_back(
                        TStringBuilder() << "read of " << Observation->Settings.GetTable().GetTablePath()
                                         << ": " << what);
                }
            }

            // Preconditions the real TKqpReadActor (and the datashard behind it) imposes,
            // checked for every read of every test: this branch has shipped a pushed-down
            // top-K with limit 0, a column id requested twice and a follower read with a lock.
            void CheckReadPreconditions() const {
                const auto& settings = Observation->Settings;

                THashSet<ui32> columnIds;
                for (const auto& column : settings.GetColumns()) {
                    Require(columnIds.insert(column.GetId()).second,
                            TStringBuilder() << "column id " << column.GetId() << " requested twice");
                }

                // Key column types must describe the keys the read carries, or it misparses.
                Require(settings.KeyColumnTypesSize() == settings.KeyColumnTypeInfosSize(),
                        "key column types and type infos disagree in size");
                if (!Observation->KeyPoints.empty()) {
                    Require(settings.KeyColumnTypesSize() == MainKeyTypes.size(),
                            TStringBuilder() << "point lookups carry " << MainKeyTypes.size()
                                             << " key columns but the read declares " << settings.KeyColumnTypesSize());
                }

                // The read actor matches ranges against partitions in lockstep, so ranges (and
                // the pre-parsed point lookups) must be globally sorted ascending. Each range
                // must also be the (parent - 1, parent] shape that selects one parent's rows.
                TMaybe<ui64> previousParent;
                for (const auto& keyRange : settings.GetRanges().GetKeyRanges()) {
                    TSerializedTableRange range(keyRange);
                    const auto cells = range.To.GetCells();
                    Require(cells.size() == 1, "parent range must be a single cell");
                    if (cells.size() != 1) {
                        continue;
                    }
                    const ui64 parent = cells[0].AsValue<ui64>();
                    Require(!previousParent || *previousParent < parent, "parent ranges are not sorted");
                    previousParent = parent;

                    Require(!range.FromInclusive && range.ToInclusive,
                            TStringBuilder() << "parent range " << parent << " is not (parent - 1, parent]");
                    const auto fromCells = range.From.GetCells();
                    Require(fromCells.size() == 1, "parent range lower bound must be a single cell");
                    if (fromCells.size() == 1) {
                        if (parent == 0) {
                            Require(fromCells[0].IsNull(), "parent range 0 must open at null (-inf)");
                        } else {
                            Require(!fromCells[0].IsNull() && fromCells[0].AsValue<ui64>() == parent - 1,
                                    TStringBuilder() << "parent range " << parent << " does not open at " << (parent - 1));
                        }
                    }
                }
                for (size_t i = 1; i < Observation->KeyPoints.size(); ++i) {
                    const int cmp = CompareTypedCellVectors(
                        Observation->KeyPoints[i - 1].GetCells().data(),
                        Observation->KeyPoints[i].GetCells().data(),
                        MainKeyTypes.data(), MainKeyTypes.size());
                    Require(cmp <= 0, "key points are not sorted");
                }

                // A follower read must carry neither a snapshot (the read actor silently routes
                // to the leader instead) nor a lock (it YQL_ENSUREs no locks come back).
                if (settings.GetUseFollowers()) {
                    Require(!settings.HasLockTxId(), "follower read carries a lock");
                    Require(!settings.HasSnapshot(), "follower read carries a snapshot");
                }

                // Without one of these three the read actor fails the query as UNAVAILABLE.
                Require(settings.HasSnapshot() || settings.HasLockTxId() || settings.GetAllowInconsistentReads(),
                        "neither snapshot, nor lock, nor AllowInconsistentReads");

                if (settings.HasVectorTopK()) {
                    const auto& topK = settings.GetVectorTopK();
                    // The datashard rejects a pushed-down top-K with limit 0.
                    Require(topK.GetLimit() > 0, "pushed-down top-K with limit 0");
                    // The datashard ranks against these: a wrong target vector or metric
                    // silently returns the wrong rows instead of failing the query.
                    Require(topK.GetTargetVector() == TargetVector,
                            "pushed-down top-K carries the wrong target vector");
                    Require(topK.GetSettings().metric() != Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED,
                            "pushed-down top-K carries no metric");
                    // Column is an index into the requested columns, not a column id.
                    Require(topK.GetColumn() < settings.ColumnsSize(),
                            TStringBuilder() << "top-K column " << topK.GetColumn() << " out of range");
                    for (ui32 distinct : topK.GetDistinctColumns()) {
                        Require(distinct < settings.ColumnsSize(),
                                TStringBuilder() << "top-K distinct column " << distinct << " out of range");
                    }
                }
            }

        private:
            const TReadObservationPtr Observation;
            const THolderFactory& HolderFactory;
            const TVector<NScheme::TTypeInfo> MainKeyTypes;
            const TString TargetVector;
            const ui64 LockId;
            std::deque<TStep> Steps;
            TDqAsyncStats IngressStats;
        };

        // ---- fake compute actor ----------------------------------------------------

        enum EPrivateEvents {
            EvTearDown = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvRegisterTransform,
        };

        struct TEvTearDown: public TEventLocal<TEvTearDown, EvTearDown> {
        };

        struct TEvRegisterTransform: public TEventLocal<TEvRegisterTransform, EvRegisterTransform> {
            explicit TEvRegisterTransform(IActor* transform)
                : Transform(transform)
            {
            }

            IActor* const Transform;
        };

        struct TDrain {
            TVector<TVector<TString>> Rows;
            bool Finished = false;
        };

        // What the fake compute actor saw, written straight into a structure the test owns:
        // grabbing the events off an edge actor instead would need a dispatch each, and a
        // dispatch with nothing pending throws TEmptyEventQueueException.
        struct TCollected {
            TVector<TDrain> Drains;
            TVector<NYql::TIssues> Errors;
            TActorId TransformId;
        };

        using TCollectedPtr = std::shared_ptr<TCollected>;

        // Impersonates the compute actor: GetAsyncInputData must be called from inside an actor,
        // never from the test thread, since the search actor's Send and RegisterWithSameMailbox
        // need an actor context, which the mock runtime provides only while dispatching.
        class TFakeComputeActor: public TActorBootstrapped<TFakeComputeActor> {
        public:
            TFakeComputeActor(const TTypeEnvironment& typeEnv, TVector<NScheme::TTypeInfo> outputTypes,
                              TCollectedPtr collected, i64 freeSpace)
                : TypeEnv(typeEnv)
                , OutputTypes(std::move(outputTypes))
                , Collected(std::move(collected))
                , FreeSpace(freeSpace)
            {
            }

            // Set after construction: the search actor needs this actor's id to be constructed.
            void SetInput(IDqComputeActorAsyncInput* input) {
                Input = input;
            }

            void Bootstrap() {
                Become(&TFakeComputeActor::StateFunc);
            }

            STATEFN(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvRegisterTransform, HandleRegisterTransform);
                    hFunc(IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived, HandlePoll);
                    hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, HandleError);
                    cFunc(EvTearDown, HandleTearDown);
                }
            }

        private:
            // The search actor must share this actor's mailbox, as in production, where
            // TDqComputeActorBase registers its transform with RegisterWithSameMailbox. Teardown
            // depends on it: inner reads land in whichever mailbox is executing, while
            // IActor::FinishPassAway detaches from the *current* context's mailbox and
            // TMailbox::DetachActor aborts on a foreign actor -- with the search actor in a
            // mailbox of its own, its reads split across two and the first StopRead aborts.
            void HandleRegisterTransform(TEvRegisterTransform::TPtr& ev) {
                Collected->TransformId = RegisterWithSameMailbox(ev->Get()->Transform);
            }

            void HandlePoll(IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::TPtr&) {
                TDrain drain;
                {
                    auto guard = TypeEnv.BindAllocator();
                    TUnboxedValueBatch batch;
                    TMaybe<TInstant> watermark;
                    i64 freeSpace = FreeSpace;
                    Input->GetAsyncInputData(batch, watermark, drain.Finished, freeSpace);
                    batch.ForEachRow([&](NUdf::TUnboxedValue& row) {
                        TVector<TString> decoded;
                        decoded.reserve(OutputTypes.size());
                        for (size_t i = 0; i < OutputTypes.size(); ++i) {
                            NUdf::TUnboxedValue element = row.GetElement(i);
                            if (OutputTypes[i].GetTypeId() == NScheme::NTypeIds::String) {
                                decoded.push_back(TString(element.AsStringRef()));
                            } else {
                                decoded.push_back(ToString(element.Get<ui64>()));
                            }
                        }
                        drain.Rows.push_back(std::move(decoded));
                    });
                }
                Collected->Drains.push_back(std::move(drain));
            }

            void HandleError(IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
                Collected->Errors.push_back(ev->Get()->Issues);
            }

            // Like the real compute actor: synchronously, from its own actor context.
            void HandleTearDown() {
                Input->PassAway();
                Input = nullptr;
            }

        private:
            IDqComputeActorAsyncInput* Input = nullptr;
            const TTypeEnvironment& TypeEnv;
            const TVector<NScheme::TTypeInfo> OutputTypes;
            const TCollectedPtr Collected;
            const i64 FreeSpace;
        };

        // ---- environment -----------------------------------------------------------


        // Kept outside TSearchEnv: a nested class's default member initializers are not usable
        // in the enclosing class's own declarations, which a defaulted TOptions parameter needs.
        struct TSearchOptions {
            TVector<TTargetStream::TRow> Input = {{Target, {}, false}};
            ui32 YieldsBeforeInput = 0;
            i64 FreeSpace = 32 * 1024 * 1024;
            TIntrusivePtr<TVectorIndexLevelsCache> LevelsCache;
            // Unless a test is about an error, an error is a failure: tests that only assert
            // that no rows came out would otherwise stay green if the actor failed the search.
            bool ExpectErrors = false;
        };

        class TSearchEnv {
        public:
            explicit TSearchEnv(NKikimrTxDataShard::TKqpVectorSearchSettings settings, TSearchOptions options = {})
                : Settings(std::move(settings))
                , ExpectErrors(options.ExpectErrors)
                , TargetVector(options.Input.empty() ? TString() : options.Input.front().Target)
            {
                Runtime.Initialize(TAppPrepare().Unwrap());
                // Every Pump ends with an empty queue, which DispatchEvents answers by waiting
                // in 10ms steps until this timeout expires (60s by default) and then throwing.
                // No event here comes from a real thread -- Send is delivered inline, the rest
                // already queued -- so the wait has nothing to wait for.
                Runtime.SetDispatchTimeout(TDuration::MilliSeconds(20));
                Edge = Runtime.AllocateEdgeActor();

                for (const auto& pk : Settings.GetMainTableKeyColumns()) {
                    MainKeyTypes.push_back(NScheme::TypeInfoFromProto(pk.GetType(), pk.GetTypeInfo()));
                }
                TVector<NScheme::TTypeInfo> outputTypes;
                for (const auto& column : Settings.GetOutputColumns()) {
                    outputTypes.push_back(NScheme::TypeInfoFromProto(column.GetType(), column.GetTypeInfo()));
                }

                NUdf::TUnboxedValue input;
                {
                    auto guard = TypeEnv.BindAllocator();
                    input = HolderFactory.Create<TTargetStream>(HolderFactory, options.Input, options.YieldsBeforeInput);
                }

                auto* computeActor = new TFakeComputeActor(TypeEnv, std::move(outputTypes), Collected, options.FreeSpace);
                ComputeActorId = Runtime.Register(computeActor);
                // Run its Bootstrap first: TActorBootstrapped aborts unless its first event is
                // the queued Bootstrap, and the runtime delivers Send inline.
                Pump();

                auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
                auto factory = [this](const NKikimrTxDataShard::TKqpReadRangesSourceSettings* readSettings,
                                      TIntrusivePtr<NActors::TProtoArenaHolder>,
                                      const NActors::TActorId&,
                                      TVector<TSerializedCellVec>&& keyPoints) {
                    return CreateFakeRead(*readSettings, std::move(keyPoints));
                };

                auto settingsCopy = Settings;
                auto [searchInput, searchActor] = CreateKqpVectorSearchActor(
                    std::move(settingsCopy), TestInputIndex, input, TCollectStatsLevel::Basic,
                    TTxId(), /* taskId */ 1, ComputeActorId, TypeEnv, HolderFactory, Alloc,
                    NWilson::TTraceId(), counters, options.LevelsCache, std::move(factory));
                SearchInput = searchInput;
                computeActor->SetInput(SearchInput);
                // Into the fake compute actor's mailbox, from inside it: see its
                // HandleRegisterTransform for why that is not optional.
                Runtime.Send(ComputeActorId, Edge, new TEvRegisterTransform(searchActor));
                Pump();
                UNIT_ASSERT_C(Collected->TransformId, "the search actor was not registered");
            }

            // ---- driving ----

            // The first poll from the compute actor: this is what starts the search.
            void Poll() {
                Runtime.Send(ComputeActorId, Edge,
                             new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(TestInputIndex));
                Pump();
            }

            void Wake(const TFakeInnerRead& read) {
                Runtime.Send(Collected->TransformId, read.SelfId(),
                             new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(TestInputIndex));
                Pump();
            }

            // An inner read fails, exactly as the real read actor reports failures.
            void FailRead(const TFakeInnerRead& read, const TString& message = "read failed") {
                Runtime.Send(Collected->TransformId, read.SelfId(),
                             new IDqComputeActorAsyncInput::TEvAsyncInputError(
                                 TestInputIndex, NYql::TIssues{NYql::TIssue(message)},
                                 NYql::NDqProto::StatusIds::UNAVAILABLE));
                Pump();
            }

            // Tears the transform down the way the compute actor does: SearchInput and every
            // fake read are destroyed, so only the observations survive.
            void TearDown() {
                Runtime.Send(ComputeActorId, Edge, new TEvTearDown());
                Pump();
                SearchInput = nullptr;
            }

            // ---- inspection ----

            // Indexes live reads only, unlike ObservedRead, which indexes every read ever
            // created. A read is deleted by its PassAway, so never touch one that has finished.
            TFakeInnerRead& Read(EReadKind kind, size_t index = 0) {
                TVector<TFakeInnerRead*> live;
                for (size_t i = 0; i < Reads.size(); ++i) {
                    if (Observations[i]->Kind == kind && !Observations[i]->PassedAway) {
                        live.push_back(Reads[i]);
                    }
                }
                UNIT_ASSERT_C(index < live.size(),
                              "no live read of the requested kind at index " << index << ", have " << live.size());
                return *live[index];
            }

            // Reads created since the search started, including the finished ones.
            ui32 CreatedReads(EReadKind kind) const {
                ui32 count = 0;
                for (const auto& observation : Observations) {
                    count += observation->Kind == kind;
                }
                return count;
            }

            ui32 ActiveReads(EReadKind kind) const {
                ui32 count = 0;
                for (const auto& observation : Observations) {
                    count += observation->Kind == kind && !observation->PassedAway;
                }
                return count;
            }

            const TVector<TReadObservationPtr>& Observed() const {
                return Observations;
            }

            TReadObservationPtr ObservedRead(EReadKind kind, size_t index = 0) const {
                TVector<TReadObservationPtr> ofKind;
                for (const auto& observation : Observations) {
                    if (observation->Kind == kind) {
                        ofKind.push_back(observation);
                    }
                }
                UNIT_ASSERT_C(index < ofKind.size(), "no read of the requested kind at index " << index);
                return ofKind[index];
            }

            const TVector<TDrain>& Drains() const {
                return Collected->Drains;
            }

            const TVector<NYql::TIssues>& Errors() const {
                return Collected->Errors;
            }

            TVector<TVector<TString>> Rows() const {
                TVector<TVector<TString>> rows;
                for (const auto& drain : Collected->Drains) {
                    rows.insert(rows.end(), drain.Rows.begin(), drain.Rows.end());
                }
                return rows;
            }

            // The first column of every drained row, the tests' row identity.
            TVector<TString> RowKeys() const {
                TVector<TString> keys;
                for (const auto& row : Rows()) {
                    keys.push_back(row.at(0));
                }
                return keys;
            }

            bool Finished() const {
                return !Collected->Drains.empty() && Collected->Drains.back().Finished;
            }

            // The locks the search actor reports in its own ExtraData.
            TVector<NKikimrDataEvents::TLock> Locks() {
                TVector<NKikimrDataEvents::TLock> locks;
                auto extra = SearchInput->ExtraData();
                if (extra) {
                    NKikimrTxDataShard::TEvKqpInputActorResultInfo info;
                    UNIT_ASSERT(extra->UnpackTo(&info));
                    for (const auto& lock : info.GetLocks()) {
                        locks.push_back(lock);
                    }
                }
                return locks;
            }

            NYql::NDqProto::TDqTaskStats Stats() {
                NYql::NDqProto::TDqTaskStats stats;
                SearchInput->FillExtraStats(&stats, /* last */ true, /* mstats */ nullptr);
                return stats;
            }

        private:
            std::pair<IDqComputeActorAsyncInput*, IActor*> CreateFakeRead(
                const NKikimrTxDataShard::TKqpReadRangesSourceSettings& readSettings,
                TVector<TSerializedCellVec>&& keyPoints)
            {
                auto observation = MakeIntrusive<TReadObservation>();
                observation->Settings = readSettings;
                observation->KeyPoints = keyPoints;
                const TString& path = readSettings.GetTable().GetTablePath();
                if (path == LevelTablePath) {
                    observation->Kind = EReadKind::Level;
                } else if (path == PostingTablePath) {
                    observation->Kind = EReadKind::Posting;
                } else {
                    observation->Kind = EReadKind::Main;
                    if (path != MainTablePath) {
                        // Recorded, not asserted: see TFakeInnerRead::Require.
                        observation->Violations.push_back(TStringBuilder() << "read of an unknown table " << path);
                    }
                }
                auto* read = new TFakeInnerRead(observation, HolderFactory, MainKeyTypes, TargetVector,
                                                /* lockId */ 100 + Observations.size());
                Observations.push_back(observation);
                Reads.push_back(read);
                return {read, read};
            }

            // Let everything the last action set in motion run to completion.
            void Pump() {
                try {
                    Runtime.DispatchEvents(TDispatchOptions(), TDuration::Zero());
                } catch (const TEmptyEventQueueException&) {
                    // Nothing was queued (a poll answered inline, say): DispatchEvents reports
                    // an empty queue by throwing, which here is the normal case.
                }
                AssertReadContractsHeld();
            }

            // On the test thread, where an assertion is not swallowed: TFakeInnerRead::Require.
            void AssertReadContractsHeld() const {
                for (const auto& observation : Observations) {
                    for (const TString& violation : observation->Violations) {
                        UNIT_ASSERT_C(false, violation);
                    }
                }
                if (!ExpectErrors && !Collected->Errors.empty()) {
                    UNIT_ASSERT_C(false, "the actor failed the search: " << Collected->Errors.front().ToOneLineString());
                }
            }

        private:
            const NKikimrTxDataShard::TKqpVectorSearchSettings Settings;
            const bool ExpectErrors;
            const TString TargetVector;

            std::shared_ptr<TScopedAlloc> Alloc = std::make_shared<TScopedAlloc>(__LOCATION__);
            TTypeEnvironment TypeEnv{*Alloc};
            TMemoryUsageInfo MemInfo{"VectorSearchActorUt"};
            THolderFactory HolderFactory{Alloc->Ref(), MemInfo};

            // Declared after the minikql members on purpose: the runtime is destroyed first, so
            // the actors it still owns (tests that never TearDown) run their destructors while
            // the allocator and type environment are alive.
            TTestActorRuntime Runtime;
            TActorId Edge;
            TActorId ComputeActorId;
            IDqComputeActorAsyncInput* SearchInput = nullptr;
            const TCollectedPtr Collected = std::make_shared<TCollected>();

            TVector<NScheme::TTypeInfo> MainKeyTypes;
            TVector<TReadObservationPtr> Observations;
            TVector<TFakeInnerRead*> Reads;
        };

        // Runs the level phase of the default settings (child 10 near the target, 11 far from
        // it) and leaves the search in the posting phase.
        void RunSingleLevel(TSearchEnv& env) {
            env.Poll();
            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(0, 10, NearVec), LevelRow(0, 11, FarVec)}, /* finished */ true);
            env.Wake(level);
        }

        // Parent ids of a read's ranges, in the order the read got them.
        TVector<ui64> RangeParents(const TReadObservation& observation) {
            TVector<ui64> parents;
            for (const auto& keyRange : observation.Settings.GetRanges().GetKeyRanges()) {
                TSerializedTableRange range(keyRange);
                parents.push_back(range.To.GetCells()[0].AsValue<ui64>());
            }
            return parents;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(KqpVectorSearchActor) {

        // ---- input and early exits ----

        Y_UNIT_TEST(WaitsForTargetVectorInput) {
            TSearchEnv env(TSettingsBuilder().Build(), {.YieldsBeforeInput = 1});

            // The input is not ready: the actor must stay in WaitInput without reading.
            env.Poll();
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Level), 0u);
            UNIT_ASSERT(!env.Finished());

            // The compute actor re-polls once the input is ready.
            env.Poll();
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Level), 1u);
        }

        Y_UNIT_TEST(ZeroTopKFinishesWithoutReads) {
            // topK = 0 is reachable through a parameter, and must produce an empty result
            // rather than a read: the datashard rejects a pushed-down top-K with limit 0.
            TSearchEnv env(TSettingsBuilder().TopK(0).Build());
            env.Poll();

            UNIT_ASSERT_VALUES_EQUAL(env.Observed().size(), 0u);
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT(env.Rows().empty());
        }

        Y_UNIT_TEST(EmptyInputFinishesWithoutReads) {
            TSearchEnv env(TSettingsBuilder().Build(), {.Input = {}});
            env.Poll();

            UNIT_ASSERT_VALUES_EQUAL(env.Observed().size(), 0u);
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT(env.Rows().empty());
        }

        Y_UNIT_TEST(NullTargetFinishesWithoutReads) {
            TSearchEnv env(TSettingsBuilder().Build(), {.Input = {{"", {}, /* NullTarget */ true}}});
            env.Poll();

            UNIT_ASSERT_VALUES_EQUAL(env.Observed().size(), 0u);
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT(env.Rows().empty());
        }

        Y_UNIT_TEST(MissingIndexSettingsFails) {
            TSearchEnv env(TSettingsBuilder().NoIndexSettings().Build(), {.ExpectErrors = true});
            env.Poll();

            UNIT_ASSERT_VALUES_EQUAL(env.Errors().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(env.Observed().size(), 0u);
            UNIT_ASSERT(!env.Finished());
        }

        Y_UNIT_TEST(UnknownMetricFails) {
            TSearchEnv env(TSettingsBuilder().UnknownMetric().Build(), {.ExpectErrors = true});
            env.Poll();

            UNIT_ASSERT_VALUES_EQUAL(env.Errors().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(env.Observed().size(), 0u);
        }

        // ---- level traversal ----

        Y_UNIT_TEST(LevelThenPostingThenMain) {
            TSearchEnv env(TSettingsBuilder().Build());
            env.Poll();

            // Level phase: one read, over the single root cluster.
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Level), 1u);
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Level)), TVector<ui64>{0});

            // The level read ranks on the centroid at position 2 of its requested columns and
            // keeps LevelTop of them; a snapshot read must not also allow inconsistent reads.
            {
                const auto& levelSettings = env.ObservedRead(EReadKind::Level)->Settings;
                UNIT_ASSERT(levelSettings.HasVectorTopK());
                UNIT_ASSERT_VALUES_EQUAL(levelSettings.GetVectorTopK().GetColumn(), 2u);
                UNIT_ASSERT_VALUES_EQUAL(levelSettings.GetVectorTopK().GetLimit(), 2u);
                UNIT_ASSERT(levelSettings.HasSnapshot());
                UNIT_ASSERT(!levelSettings.GetAllowInconsistentReads());
                UNIT_ASSERT(!levelSettings.GetUseFollowers());
            }

            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(0, 10, NearVec), LevelRow(0, 11, FarVec)}, /* finished */ true);
            env.Wake(level);

            // Posting phase: the level read is gone, a posting read covers both children.
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Level), 0u);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Posting), 1u);
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)), (TVector<ui64>{10, 11}));

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}}, /* finished */ true);
            env.Wake(posting);

            // Main phase: the buffered PK went into a main read as a point lookup that ranks on
            // the output embedding and keeps TopK.
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Main), 1u);
            const auto& mainSettings = env.ObservedRead(EReadKind::Main)->Settings;
            UNIT_ASSERT_VALUES_EQUAL(env.ObservedRead(EReadKind::Main)->KeyPoints.size(), 1u);
            UNIT_ASSERT(mainSettings.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(mainSettings.GetVectorTopK().GetColumn(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(mainSettings.GetVectorTopK().GetLimit(), 3u);

            auto& main = env.Read(EReadKind::Main);
            main.Push({{ui64(7), NearVec}}, /* finished */ true);
            env.Wake(main);

            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), TVector<TString>{"7"});
        }

        Y_UNIT_TEST(LevelRoundIsBarriered) {
            TSearchEnv env(TSettingsBuilder().Build());
            env.Poll();

            // Rows without a finish must not close the round.
            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(0, 10, NearVec)}, /* finished */ false);
            env.Wake(level);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Posting), 0u);
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Level), 1u);

            level.Push({LevelRow(0, 11, FarVec)}, /* finished */ true);
            env.Wake(level);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Posting), 1u);
        }

        Y_UNIT_TEST(MultipleLevelRounds) {
            TSearchEnv env(TSettingsBuilder().Levels(2).LevelTop(1).Build());
            env.Poll();

            auto& firstRound = env.Read(EReadKind::Level);
            firstRound.Push({LevelRow(0, 10, NearVec), LevelRow(0, 11, FarVec)}, /* finished */ true);
            env.Wake(firstRound);

            // Still in the level phase: round two reads only the nearest cluster's children.
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Level), 2u);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Posting), 0u);
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Level, 1)), TVector<ui64>{10});

            auto& secondRound = env.Read(EReadKind::Level);
            secondRound.Push({LevelRow(10, 100, NearVec)}, /* finished */ true);
            env.Wake(secondRound);

            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Level), 2u);
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)), TVector<ui64>{100});
        }

        Y_UNIT_TEST(LevelTopKeepsNearestClusters) {
            TSearchEnv env(TSettingsBuilder().LevelTop(2).Build());
            env.Poll();

            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(0, 10, NearVec),
                        LevelRow(0, 11, FarVec),
                        LevelRow(0, 12, NearVec)},
                       /* finished */ true);
            env.Wake(level);

            // The far cluster is evicted by the bounded heap; the two near ones go to posting,
            // sorted as the read actor requires.
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)), (TVector<ui64>{10, 12}));
        }

        Y_UNIT_TEST(EmptyLevelRoundFinishesEmpty) {
            TSearchEnv env(TSettingsBuilder().Build());
            env.Poll();

            // No children at all: nothing to scan in the posting table either.
            auto& level = env.Read(EReadKind::Level);
            level.Push({}, /* finished */ true);
            env.Wake(level);

            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Posting), 0u);
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT(env.Rows().empty());
        }

        // ---- posting and main ----

        Y_UNIT_TEST(CoveredIndexSkipsMainRead) {
            TSearchEnv env(TSettingsBuilder().Covered().Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7), FarVec}, {ui64(8), NearVec}}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Main), 0u);
            UNIT_ASSERT(env.Finished());
            // Ranked by distance, nearest first.
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), (TVector<TString>{"8", "7"}));
        }

        Y_UNIT_TEST(PostingFlushesIntoOverlappingMainRead) {
            TSearchEnv env(TSettingsBuilder().Build());
            RunSingleLevel(env);

            // Each drain cycle with buffered PKs dispatches them into an overlapping main read.
            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(100)}, {ui64(101)}}, /* finished */ false);
            env.Wake(posting);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Main), 1u);
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Posting), 1u);

            posting.Push({{ui64(102)}}, /* finished */ true);
            env.Wake(posting);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Main), 2u);
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Main), 2u);

            // The search is not done while any main read is still running.
            auto& firstMain = env.Read(EReadKind::Main, 0);
            firstMain.Push({{ui64(100), FarVec}}, /* finished */ true);
            env.Wake(firstMain);
            UNIT_ASSERT(!env.Finished());

            auto& lastMain = env.Read(EReadKind::Main);
            lastMain.Push({{ui64(102), NearVec}}, /* finished */ true);
            env.Wake(lastMain);

            UNIT_ASSERT(env.Finished());
            // Top-K merged across the per-batch main reads, nearest first.
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), (TVector<TString>{"102", "100"}));
        }

        Y_UNIT_TEST(EmptyPostingScanFinishesEmpty) {
            TSearchEnv env(TSettingsBuilder().Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Main), 0u);
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT(env.Rows().empty());
        }

        Y_UNIT_TEST(MainKeyPointsAreSortedAcrossClusters) {
            TSearchEnv env(TSettingsBuilder().Build());
            RunSingleLevel(env);

            // The posting scan yields PKs per leaf cluster, so they arrive unordered; the main
            // read requires them globally sorted (the fake read checks this too).
            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(500)}, {ui64(3)}, {ui64(42)}}, /* finished */ true);
            env.Wake(posting);

            auto keyPoints = env.ObservedRead(EReadKind::Main)->KeyPoints;
            UNIT_ASSERT_VALUES_EQUAL(keyPoints.size(), 3u);
            TVector<ui64> keys;
            for (const auto& point : keyPoints) {
                keys.push_back(point.GetCells()[0].AsValue<ui64>());
            }
            UNIT_ASSERT_VALUES_EQUAL(keys, (TVector<ui64>{3, 42, 500}));
        }

        Y_UNIT_TEST(OverlapDedupsPostingKeys) {
            TSearchEnv env(TSettingsBuilder().Overlap(3).Build());
            RunSingleLevel(env);

            // With overlapping clusters the same PK shows up under several of them.
            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}, {ui64(7)}, {ui64(8)}}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT_VALUES_EQUAL(env.ObservedRead(EReadKind::Main)->KeyPoints.size(), 2u);
        }

        Y_UNIT_TEST(NoOverlapKeepsRepeatedKeys) {
            TSearchEnv env(TSettingsBuilder().Build());
            RunSingleLevel(env);

            // Without overlap the dedup is skipped, so a repeated key is passed through.
            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}, {ui64(7)}}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT_VALUES_EQUAL(env.ObservedRead(EReadKind::Main)->KeyPoints.size(), 2u);
        }

        Y_UNIT_TEST(CoveredOverlapDedupsRows) {
            TSearchEnv env(TSettingsBuilder().Covered().Overlap(3).Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7), NearVec}, {ui64(7), NearVec}, {ui64(8), FarVec}}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), (TVector<TString>{"7", "8"}));
        }

        Y_UNIT_TEST(TopKBoundsResults) {
            TSearchEnv env(TSettingsBuilder().Covered().TopK(2).Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(1), FarVec},
                          {ui64(2), NearVec},
                          {ui64(3), FarVec},
                          {ui64(4), NearVec}},
                         /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.Rows().size(), 2u);
            // Both survivors are the near ones; the far rows were dropped by the bounded heap.
            for (const auto& key : env.RowKeys()) {
                UNIT_ASSERT_C(key == "2" || key == "4", "unexpected row " << key);
            }
        }

        Y_UNIT_TEST(PostingTopKIsPushedDownWhenPostingHasEmbedding) {
            TSearchEnv env(TSettingsBuilder().PostingEmbedding().Overlap(3).Build());
            RunSingleLevel(env);

            const auto& settings = env.ObservedRead(EReadKind::Posting)->Settings;
            UNIT_ASSERT(settings.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().GetLimit(), 3u);
            // With overlap the pushed-down top-K dedups by PK, so duplicates cannot crowd out
            // distinct nearest rows.
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().DistinctColumnsSize(), 1u);
        }

        Y_UNIT_TEST(PartiallyCoveredPostingFeedsMainRead) {
            // The posting table holds the embedding but not every output column: the read asks
            // for the PK columns plus the embedding appended after them, ranks on it, and the
            // surviving PKs still go through a main read for the full output row.
            TSearchEnv env(TSettingsBuilder().PostingEmbedding().Build());
            RunSingleLevel(env);

            const auto& settings = env.ObservedRead(EReadKind::Posting)->Settings;
            UNIT_ASSERT_VALUES_EQUAL(settings.ColumnsSize(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetColumns(0).GetId(), PostingPkColumnId);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetColumns(1).GetId(), PostingEmbColumnId);
            UNIT_ASSERT(settings.HasVectorTopK());
            // Ranking happens on the appended embedding, not on the PK.
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().GetColumn(), 1u);

            // The PK still comes from positions 0..N-1, so the embedding must not shift it.
            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7), NearVec}, {ui64(8), FarVec}}, /* finished */ true);
            env.Wake(posting);

            auto keyPoints = env.ObservedRead(EReadKind::Main)->KeyPoints;
            UNIT_ASSERT_VALUES_EQUAL(keyPoints.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(keyPoints[0].GetCells()[0].AsValue<ui64>(), 7u);
            UNIT_ASSERT_VALUES_EQUAL(keyPoints[1].GetCells()[0].AsValue<ui64>(), 8u);

            auto& main = env.Read(EReadKind::Main);
            main.Push({{ui64(8), FarVec}, {ui64(7), NearVec}}, /* finished */ true);
            env.Wake(main);

            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), (TVector<TString>{"7", "8"}));
        }

        // ---- prefixed index ----

        Y_UNIT_TEST(PrefixedDirectPostingSkipsLevelTraversal) {
            const ui64 directRoot = NTableIndex::NKMeans::PostingParentFlag | 5;
            TSearchEnv env(TSettingsBuilder().Prefixed().Build(), {.Input = {{Target, directRoot, false}}});
            env.Poll();

            // A root carrying the posting flag has no level subtree: straight to posting.
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Level), 0u);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Posting), 1u);
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)), TVector<ui64>{directRoot});
        }

        Y_UNIT_TEST(PrefixedMixedRootsReadBothWays) {
            const ui64 directRoot = NTableIndex::NKMeans::PostingParentFlag | 5;
            TSearchEnv env(TSettingsBuilder().Prefixed().Build(),
                           {.Input = {{Target, ui64(7), false}, {Target, directRoot, false}}});
            env.Poll();

            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Level)), TVector<ui64>{7});

            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(7, 70, NearVec)}, /* finished */ true);
            env.Wake(level);

            // The posting scan covers the level root's children and the direct posting root.
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)),
                                     (TVector<ui64>{70, directRoot}));
        }

        // ---- errors ----

        // A failed search must never deliver rows, and only the Failed checks stand in the way:
        // the failed read is also the last active one and afterwards hands over a finished batch
        // with a row. Without the `if (Failed)` break inside PollActiveReads' drain loop and the
        // `if (Failed) return;` after it, that batch would reach FinalizeResults and deliver.
        Y_UNIT_TEST(FailedSearchNeverDeliversRows) {
            TSearchEnv env(TSettingsBuilder().Build(), {.ExpectErrors = true});
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(100)}}, /* finished */ true);
            env.Wake(posting);
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Posting), 0u);
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Main), 1u);

            auto& main = env.Read(EReadKind::Main);
            env.FailRead(main);
            UNIT_ASSERT_VALUES_EQUAL(env.Errors().size(), 1u);

            main.Push({{ui64(100), NearVec}}, /* finished */ true);
            env.Wake(main);

            UNIT_ASSERT_VALUES_EQUAL(env.Errors().size(), 1u);
            UNIT_ASSERT(!env.Finished());
            UNIT_ASSERT(env.Rows().empty());
        }

        // Intentionally red: a batch of N broken centroids currently sends N error events to the
        // compute actor instead of one (see the file header).
        Y_UNIT_TEST(InvalidCentroidReportedOnce) {
            TSearchEnv env(TSettingsBuilder().Build(), {.ExpectErrors = true});
            env.Poll();

            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(0, 10, BrokenVec), LevelRow(0, 11, BrokenVec), LevelRow(0, 12, BrokenVec)},
                       /* finished */ false);
            env.Wake(level);

            UNIT_ASSERT_VALUES_EQUAL(env.Errors().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Posting), 0u);
        }

        Y_UNIT_TEST(ForeignParentIsRejected) {
            TSearchEnv env(TSettingsBuilder().Build(), {.ExpectErrors = true});
            env.Poll();

            // A row whose parent was never asked for means the read returned foreign clusters.
            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(999, 10, NearVec)}, /* finished */ false);
            env.Wake(level);

            UNIT_ASSERT_VALUES_EQUAL(env.Errors().size(), 1u);
            UNIT_ASSERT(!env.Finished());
        }

        // ---- draining, locks, stats, teardown ----

        Y_UNIT_TEST(ReplyResultRespectsFreeSpace) {
            // Free space for about one row per poll: the results come out over several drains.
            TSearchEnv env(TSettingsBuilder().Covered().Build(), {.FreeSpace = 1});
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(1), NearVec}, {ui64(2), FarVec}}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT(!env.Drains().empty());
            UNIT_ASSERT(!env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.Rows().size(), 1u);

            // The second poll empties the deque: last row plus finished.
            env.Poll();
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), (TVector<TString>{"1", "2"}));
        }

        Y_UNIT_TEST(LocksCollectedFromEveryRead) {
            TSearchEnv env(TSettingsBuilder().Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}}, /* finished */ true);
            env.Wake(posting);
            auto& main = env.Read(EReadKind::Main);
            main.Push({{ui64(7), NearVec}}, /* finished */ true);
            env.Wake(main);

            // One lock per finished read: level, posting, main.
            UNIT_ASSERT_VALUES_EQUAL(env.Locks().size(), 3u);
        }

        Y_UNIT_TEST(StatsAccumulatedPerTable) {
            TSearchEnv env(TSettingsBuilder().Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}}, /* finished */ true);
            env.Wake(posting);
            auto& main = env.Read(EReadKind::Main);
            main.Push({{ui64(7), NearVec}}, /* finished */ true);
            env.Wake(main);

            auto stats = env.Stats();
            THashMap<TString, ui64> rowsByTable;
            for (const auto& table : stats.GetTables()) {
                rowsByTable[table.GetTablePath()] = table.GetReadRows();
            }
            UNIT_ASSERT_VALUES_EQUAL(rowsByTable.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(rowsByTable.at(LevelTablePath), TFakeInnerRead::StatsRows);
            UNIT_ASSERT_VALUES_EQUAL(rowsByTable.at(PostingTablePath), TFakeInnerRead::StatsRows);
            UNIT_ASSERT_VALUES_EQUAL(rowsByTable.at(MainTablePath), TFakeInnerRead::StatsRows);

            // Each inner read's stats are drained exactly once, when it is torn down.
            for (const auto& observation : env.Observed()) {
                UNIT_ASSERT_VALUES_EQUAL(observation->StatsDrains, 1u);
            }
        }

        Y_UNIT_TEST(TearDownStopsLiveReads) {
            TSearchEnv env(TSettingsBuilder().Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}}, /* finished */ false);
            env.Wake(posting);
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Posting), 1u);
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Main), 1u);

            env.TearDown();

            // Every read must be torn down, or the datashard read iterators leak.
            for (const auto& observation : env.Observed()) {
                UNIT_ASSERT_C(observation->PassedAway,
                              "read of " << observation->Settings.GetTable().GetTablePath() << " was not stopped");
            }
        }

        Y_UNIT_TEST(TearDownStopsLiveLevelRead) {
            // Teardown during the level phase, before any read has finished.
            TSearchEnv env(TSettingsBuilder().Build());
            env.Poll();
            UNIT_ASSERT_VALUES_EQUAL(env.ActiveReads(EReadKind::Level), 1u);

            env.TearDown();

            for (const auto& observation : env.Observed()) {
                UNIT_ASSERT_C(observation->PassedAway,
                              "read of " << observation->Settings.GetTable().GetTablePath() << " was not stopped");
            }
        }

        Y_UNIT_TEST(SimilarityMetricRanksNegativeDistances) {
            // For a similarity metric CalcDistance returns -similarity, so every ranked row has
            // a negative distance and "nearer is smaller" still holds. The unranked row's
            // fallback is pinned by UnrankedRowLosesToRankedOne instead.
            TSearchEnv env(TSettingsBuilder().Covered().NullableEmbedding().InnerProductMetric().TopK(2).Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(1), NullCell},  // unranked: distance = max()
                          {ui64(2), WeakVec},   // -(0x10*0x67 + 0x10*0x71)
                          {ui64(3), NearVec}},  // -(0x67^2 + 0x71^2), the most similar
                         /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), (TVector<TString>{"3", "2"}));
        }

        Y_UNIT_TEST(UnrankedRowLosesToRankedOne) {
            // AddCandidate scores an unranked row (no embedding) as max(). With a distance
            // metric every real distance is >= 0, so only a fallback worse than all of them
            // keeps the unranked row out of a single-slot result -- a fallback of 0 would win.
            TSearchEnv env(TSettingsBuilder().Covered().NullableEmbedding().TopK(1).Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(1), NullCell}, {ui64(2), FarVec}}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), TVector<TString>{"2"});
        }

        Y_UNIT_TEST(DeepPostingToMainPipelineDoesNotOverflow) {
            // LaunchRead kicks a nested PollActiveReads, which can finish reads and launch more,
            // recursing back into LaunchRead; nothing bounds that depth, and every nested drain
            // hands over one more posting batch. A smoke check, not a proof (500 frames are far
            // from a stack limit): it pins one main read per batch and no error at depth.
            constexpr ui32 batches = 500;
            TSearchEnv env(TSettingsBuilder().TopK(1).Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            for (ui32 i = 0; i < batches; ++i) {
                posting.Push({{ui64(i)}}, /* finished */ i + 1 == batches);
            }
            env.Wake(posting);

            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Main), batches);
        }

        // ---- level cache ----

        Y_UNIT_TEST(LevelCacheFullHitSkipsRead) {
            auto cache = MakeLevelsCache();
            PutCachedChildren(*cache, /* parent */ 0, {{10, NearVec}, {11, FarVec}});

            TSearchEnv env(TSettingsBuilder().Build(), {.LevelsCache = cache});
            env.Poll();

            // The whole round is served from the cache: no inner read, straight to posting.
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Level), 0u);
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)), (TVector<ui64>{10, 11}));
        }

        Y_UNIT_TEST(LevelCachePartialHitReadsOnlyMissedParents) {
            auto cache = MakeLevelsCache();
            PutCachedChildren(*cache, /* parent */ 7, {{70, NearVec}});

            // Two prefix groups, so the round has two parents and only one of them is cached.
            TSearchEnv env(TSettingsBuilder().Prefixed().Build(),
                           {.Input = {{Target, ui64(7), false}, {Target, ui64(8), false}},
                            .LevelsCache = cache});
            env.Poll();

            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Level)), TVector<ui64>{8});

            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(8, 80, NearVec)}, /* finished */ true);
            env.Wake(level);

            // Cached and freshly read children are ranked together.
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)), (TVector<ui64>{70, 80}));
        }

        Y_UNIT_TEST(LevelCacheIsPopulatedAfterRound) {
            auto cache = MakeLevelsCache();
            TSearchEnv env(TSettingsBuilder().Build(), {.LevelsCache = cache});
            env.Poll();

            // With the cache on the level read must not push the per-round top-K down: the cache
            // stores every child, not just this target vector's nearest ones.
            UNIT_ASSERT(!env.ObservedRead(EReadKind::Level)->Settings.HasVectorTopK());

            auto& level = env.Read(EReadKind::Level);
            level.Push({LevelRow(0, 10, NearVec), LevelRow(0, 11, FarVec)}, /* finished */ true);
            env.Wake(level);

            UNIT_ASSERT_VALUES_EQUAL(cache->Size(), 1u);
            auto cached = cache->Get(LevelTablePathId(), ParentCacheKey(0));
            UNIT_ASSERT(cached);
            UNIT_ASSERT_VALUES_EQUAL(cached->BatchRows.Size(), 2u);
        }

        Y_UNIT_TEST(LevelCacheIsPopulatedPerRound) {
            auto cache = MakeLevelsCache();
            TSearchEnv env(TSettingsBuilder().Levels(2).LevelTop(1).Build(), {.LevelsCache = cache});
            env.Poll();

            auto& firstRound = env.Read(EReadKind::Level);
            firstRound.Push({LevelRow(0, 10, NearVec), LevelRow(0, 11, FarVec)}, /* finished */ true);
            env.Wake(firstRound);

            // Each round flushes its cache-miss parents: the root is cached before round two.
            UNIT_ASSERT_VALUES_EQUAL(cache->Size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Level, 1)), TVector<ui64>{10});

            auto& secondRound = env.Read(EReadKind::Level);
            secondRound.Push({LevelRow(10, 100, NearVec)}, /* finished */ true);
            env.Wake(secondRound);

            UNIT_ASSERT_VALUES_EQUAL(cache->Size(), 2u);
            UNIT_ASSERT(cache->Get(LevelTablePathId(), ParentCacheKey(10)));
            UNIT_ASSERT_VALUES_EQUAL(RangeParents(*env.ObservedRead(EReadKind::Posting)), TVector<ui64>{100});
        }

        Y_UNIT_TEST(LevelCacheSkipsEmptyResults) {
            auto cache = MakeLevelsCache();
            TSearchEnv env(TSettingsBuilder().Build(), {.LevelsCache = cache});
            env.Poll();

            auto& level = env.Read(EReadKind::Level);
            level.Push({}, /* finished */ true);
            env.Wake(level);

            // An empty result is not cached: it would pin "no children" for every later query.
            UNIT_ASSERT_VALUES_EQUAL(cache->Size(), 0u);
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT(env.Rows().empty());
        }

        Y_UNIT_TEST(LevelCacheBrokenCentroidFails) {
            auto cache = MakeLevelsCache();
            PutCachedChildren(*cache, /* parent */ 0, {{10, BrokenVec}});

            TSearchEnv env(TSettingsBuilder().Build(), {.LevelsCache = cache, .ExpectErrors = true});
            env.Poll();

            // Centroids are validated on the cache-hit path too, not only on read rows.
            UNIT_ASSERT_VALUES_EQUAL(env.Errors().size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(env.Observed().size(), 0u);
            UNIT_ASSERT(!env.Finished());
        }

        // ---- column layouts ----

        Y_UNIT_TEST(CoveredExtraPkColumnIsAppended) {
            TSearchEnv env(TSettingsBuilder().CoveredEmbeddingOnly().Overlap(3).Build());
            RunSingleLevel(env);

            // The output column comes first, then the PK appended for dedup.
            const auto& settings = env.ObservedRead(EReadKind::Posting)->Settings;
            UNIT_ASSERT_VALUES_EQUAL(settings.ColumnsSize(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetColumns(0).GetId(), PostingEmbColumnId);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetColumns(1).GetId(), PostingPkColumnId);
            UNIT_ASSERT(settings.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().GetColumn(), 0u);
            // Dedup happens on the appended PK position.
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().DistinctColumnsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().GetDistinctColumns(0), 1u);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{NearVec, ui64(7)}, {NearVec, ui64(7)}, {FarVec, ui64(8)}}, /* finished */ true);
            env.Wake(posting);

            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.CreatedReads(EReadKind::Main), 0u);
            // Deduped by the appended PK, and the result row holds only the output column.
            UNIT_ASSERT_VALUES_EQUAL(env.Rows().size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(env.Rows()[0].size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(env.RowKeys(), (TVector<TString>{NearVec, FarVec}));
        }

        Y_UNIT_TEST(MultiColumnPkIsSortedGenerically) {
            TSearchEnv env(TSettingsBuilder().TwoColumnPk().Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(2), ui64(5)}, {ui64(1), ui64(9)}, {ui64(2), ui64(1)}}, /* finished */ true);
            env.Wake(posting);

            // Sorted by the whole key, not by its first column.
            auto keyPoints = env.ObservedRead(EReadKind::Main)->KeyPoints;
            UNIT_ASSERT_VALUES_EQUAL(keyPoints.size(), 3u);
            TVector<std::pair<ui64, ui64>> keys;
            for (const auto& point : keyPoints) {
                keys.emplace_back(point.GetCells()[0].AsValue<ui64>(), point.GetCells()[1].AsValue<ui64>());
            }
            const TVector<std::pair<ui64, ui64>> expected{{1, 9}, {2, 1}, {2, 5}};
            UNIT_ASSERT_VALUES_EQUAL(keys, expected);

            auto& main = env.Read(EReadKind::Main);
            main.Push({{ui64(1), ui64(9), NearVec}}, /* finished */ true);
            env.Wake(main);
            UNIT_ASSERT(env.Finished());
            UNIT_ASSERT_VALUES_EQUAL(env.Rows()[0].size(), 3u);
        }

        Y_UNIT_TEST(EmbeddingAsPkColumnIsNotRequestedTwice) {
            TSearchEnv env(TSettingsBuilder().EmbeddingIsPkColumn().Overlap(3).Build());
            RunSingleLevel(env);

            // The embedding is the PK column: requested once, ranked at its own position.
            const auto& settings = env.ObservedRead(EReadKind::Posting)->Settings;
            UNIT_ASSERT_VALUES_EQUAL(settings.ColumnsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetColumns(0).GetId(), PostingEmbColumnId);
            UNIT_ASSERT(settings.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().GetColumn(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().DistinctColumnsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetVectorTopK().GetDistinctColumns(0), 0u);
        }

        // ---- transaction context ----

        Y_UNIT_TEST(FollowerReadsForImplTablesOnly) {
            TSearchEnv env(TSettingsBuilder().Followers().Build());
            RunSingleLevel(env);

            for (EReadKind kind : {EReadKind::Level, EReadKind::Posting}) {
                const auto& settings = env.ObservedRead(kind)->Settings;
                UNIT_ASSERT(settings.GetUseFollowers());
                // Follower reads skip the MVCC snapshot, so they must allow inconsistency.
                UNIT_ASSERT(settings.GetAllowInconsistentReads());
                UNIT_ASSERT(!settings.HasSnapshot());
                UNIT_ASSERT(!settings.HasLockTxId());
            }

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}}, /* finished */ true);
            env.Wake(posting);

            // The main table is mutable: always the leader, always the query snapshot.
            const auto& mainSettings = env.ObservedRead(EReadKind::Main)->Settings;
            UNIT_ASSERT(!mainSettings.GetUseFollowers());
            UNIT_ASSERT(mainSettings.HasSnapshot());
        }

        Y_UNIT_TEST(FollowerReadFallsBackWhenLockHeld) {
            const ui64 lockTxId = 555;
            TSearchEnv env(TSettingsBuilder().Followers().Lock(lockTxId).Build());
            env.Poll();

            // A follower read takes no locks, so holding one forces a leader read instead.
            const auto& settings = env.ObservedRead(EReadKind::Level)->Settings;
            UNIT_ASSERT(!settings.GetUseFollowers());
            UNIT_ASSERT(settings.HasSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(settings.GetLockTxId(), lockTxId);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetLockMode(), NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            UNIT_ASSERT_VALUES_EQUAL(settings.GetLockNodeId(), 7u);
        }

        Y_UNIT_TEST(InconsistentReadsArePropagated) {
            TSearchEnv env(TSettingsBuilder().InconsistentReads().Build());
            RunSingleLevel(env);

            auto& posting = env.Read(EReadKind::Posting);
            posting.Push({{ui64(7)}}, /* finished */ true);
            env.Wake(posting);

            // Neither a snapshot nor a lock, so every read must carry the flag to be accepted.
            for (EReadKind kind : {EReadKind::Level, EReadKind::Posting, EReadKind::Main}) {
                const auto& settings = env.ObservedRead(kind)->Settings;
                UNIT_ASSERT(settings.GetAllowInconsistentReads());
                UNIT_ASSERT(!settings.HasSnapshot());
            }
        }
    }

} // namespace NKikimr::NKqp
