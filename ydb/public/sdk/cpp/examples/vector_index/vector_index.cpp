#include "clusterizer.h"
#include "thread_pool.h"
#include "vector_index.h"

#include <library/cpp/dot_product/dot_product.h>
#include <format>

#include <ydb/public/api/protos/ydb_value.pb.h>

template <>
struct std::formatter<TString>: std::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const TString& param, FormatContext& fc) const {
        return std::formatter<std::string_view>::format(std::string_view{param}, fc);
    }
};

using namespace NLastGetopt;
using namespace NYdb;
using namespace NTable;

static constexpr ui64 kBulkSize = 1000;
static constexpr ui64 kSmallClusterSize = 20'000;

static constexpr std::string_view FlatIndex = "flat";
static constexpr std::string_view KMeansIndex = "kmeans";

namespace NQuantizer {
static constexpr std::string_view None = "none";
static constexpr std::string_view Int8 = "int8";
// static constexpr std::string_view Uint8 = "uint8";
static constexpr std::string_view Bit = "bit";
}

ECommand Parse(std::string_view command) {
    if (command == "DropIndex") {
        return ECommand::DropIndex;
    }
    if (command == "CreateIndex") {
        return ECommand::CreateIndex;
    }
    if (command == "UpdateIndex") {
        return ECommand::UpdateIndex;
    }
    if (command == "RecreateIndex") {
        return ECommand::RecreateIndex;
    }
    if (command == "TopK") {
        return ECommand::TopK;
    }
    return ECommand::None;
}

static void PrintTop(TResultSetParser&& parser) {
    while (parser.TryNextRow()) {
        Y_ASSERT(parser.ColumnsCount() >= 2);
        Cout // << parser.ColumnParser(0).GetUint64() << "    \t"
            << *parser.ColumnParser(1).GetOptionalFloat() << "\t";
        for (size_t i = 2; i < parser.ColumnsCount(); ++i) {
            Cout << *parser.ColumnParser(2).GetOptionalUtf8() << "\t";
        }
        Cout << "\n";
    }
    Cout << Endl;
}

static TString FullName(const TOptions& options, const TString& name) {
    return TString::Join(options.Database, "/", name);
}

static TString IndexName(const TOptions& options) {
    return TString::Join(options.Table, "_", options.IndexType, "_", options.IndexQuantizer);
}

static TString FullIndexName(const TOptions& options) {
    return FullName(options, IndexName(options));
}

static void DropTable(TTableClient& client, const TString& table) {
    auto r = client.RetryOperationSync([&](TSession session) {
        TDropTableSettings settings;
        return session.DropTable(table).ExtractValueSync();
    });
    if (!r.IsSuccess() && r.GetStatus() != EStatus::SCHEME_ERROR) {
        ythrow TVectorException{r};
    }
}

static void DropIndex(TTableClient& client, const TOptions& options) {
    DropTable(client, FullIndexName(options));
}

static void CreateFlat(TTableClient& client, const TOptions& options) {
    auto r = client.RetryOperationSync([&](TSession session) {
        auto desc = TTableBuilder()
                        .AddNonNullableColumn(options.PrimaryKey, EPrimitiveType::Uint32)
                        .AddNullableColumn(options.Embedding, EPrimitiveType::String)
                        .SetPrimaryKeyColumn(options.PrimaryKey)
                        .Build();

        return session.CreateTable(FullIndexName(options), std::move(desc)).ExtractValueSync();
    });
    if (!r.IsSuccess()) {
        ythrow TVectorException{r};
    }
}

static void CreateKMeans(TTableClient& client, const TOptions& options, std::string_view suffix = {}) {
    auto parentPK = "parent_" + options.PrimaryKey;
    auto r = client.RetryOperationSync([&](TSession session) {
        auto desc = TTableBuilder()
                        .AddNonNullableColumn(parentPK, EPrimitiveType::Uint32)
                        .AddNonNullableColumn(options.PrimaryKey, EPrimitiveType::Uint32)
                        .AddNullableColumn(options.Embedding, EPrimitiveType::String)
                        .SetPrimaryKeyColumns({parentPK, options.PrimaryKey})
                        .Build();

        return session.CreateTable(FullIndexName(options) + suffix, std::move(desc)).ExtractValueSync();
    });
    if (!r.IsSuccess()) {
        ythrow TVectorException{r};
    }
}

static void UpdateFlatBit(TTableClient& client, const TOptions& options) {
    TString query = std::format(R"(
        DECLARE $begin AS Uint64;
        DECLARE $rows AS Uint64;

        UPSERT INTO {1}
        SELECT {2}, Yql::Map(Knn::ToBinaryStringBit(Knn::FloatFromBinaryString({3})), ($e) -> Untag($e, "BitVector")) AS {3}
        FROM {0}
        WHERE $begin <= {2} AND {2} < $begin + $rows
    )",
                                options.Table,
                                IndexName(options),
                                options.PrimaryKey,
                                options.Embedding);
    for (ui64 i = 0; i < options.Rows; i += kBulkSize) {
        TParamsBuilder paramsBuilder;
        paramsBuilder.AddParam("$begin").Uint64(i).Build();
        paramsBuilder.AddParam("$rows").Uint64(kBulkSize).Build();
        auto r = client.RetryOperationSync([&](TSession session) {
            return session.ExecuteDataQuery(query,
                                            TTxControl::BeginTx(TTxSettings::SerializableRW())
                                                .CommitTx(),
                                            paramsBuilder.Build())
                .ExtractValueSync();
        });
        if (!r.IsSuccess()) {
            ythrow TVectorException{r};
        }
    }
}

class TTableIterator final: public TDatasetIterator {
public:
    TTableIterator(const TOptions& options, TTableClient& client, NVectorIndex::TThreadPool& tp)
        : Options{options}
        , Client{client}
        , ThreadPool{tp}
    {
    }

    void UseLevel(ui8 level, TId parentId, ui64 rows) {
        ParentId = parentId;
        RowsCount = rows;

        TString newTable;
        if (level == 1) {
            newTable = Options.Table;
        } else {
            newTable = IndexName(Options) + "_" + std::to_string(level % 2);
        }
        if (newTable != Table) {
            Table = newTable;
        }
        Embeddings.clear();
    }

    ui64 Rows() const final {
        return RowsCount;
    }

    TString RandomKQuery(ui64 k) {
        Y_ASSERT(kMinClusterSize * k < Rows());
        auto r = std::max(0.5 / k, static_cast<double>(k * kMinClusterSize) / Rows());
        if (ParentId != 0 && !Options.ShuffleWithEmbeddings) {
            TString query = std::format(R"(
                $ids = SELECT id FROM {0} WHERE parent_id = {2} AND RANDOM(id) < {3} LIMIT {4};
                SELECT embedding FROM {1} WHERE id IN $ids LIMIT {4};
            )", Table, Options.Table, ParentId, r, k);
            // Cout << query << Endl;
            return query;
        }

        TString query = std::format(R"(SELECT {0} FROM {1} WHERE)",
                                    Options.Embedding,
                                    Table);
        if (ParentId != 0) {
            query = std::format(R"({0} {1} = {2} AND)", query, "parent_" + Options.PrimaryKey, ParentId);
        }
        query = std::format(R"({0} RANDOM({1}) < {2} LIMIT {3})", query, Options.Embedding, r, k);
        // Cout << query << Endl;
        return query;
    }

    void RandomK(ui64 k, std::function<void(TRawEmbedding)> cb) final {
        if (k == 0) {
            return;
        }
        auto query = RandomKQuery(k);
        auto r = Client.RetryOperationSync([&](TSession session) -> TStatus {
            cb({});
            auto r = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW())
                                                         .CommitTx())
                         .ExtractValueSync();
            for (auto& part : r.GetResultSets()) {
                TResultSetParser batch(part);
                Y_ASSERT(batch.ColumnsCount() == 1);
                auto embeddingIdx = batch.ColumnIndex(Options.Embedding);
                auto& embedding = batch.ColumnParser(embeddingIdx);
                while (batch.TryNextRow()) {
                    cb(*embedding.GetOptionalString());
                }
            }
            return r;
        });
        if (!r.IsSuccess()) {
            ythrow TVectorException{r};
        }
    }

    void IterateEmbedding(TReadCallback& read) final {
        if (Rows() > kSmallClusterSize) {
            IterateImpl<false>(&read, [&](TRawEmbedding rawEmbedding) {
                read.Handle(std::move(rawEmbedding));
            });
            return;
        }
        if (Embeddings.empty()) {
            Embeddings.reserve(Rows());
            IterateImpl<false>(&read, [&](TRawEmbedding embedding) {
                Embeddings.push_back(std::move(embedding));
            });
        }
        read.Handle(Embeddings);
    }

    void IterateId(TReadCallback& read) final {
        Embeddings.clear();
        IterateImpl<true>(&read, [&](TId id, TRawEmbedding rawEmbedding) {
            read.Handle(id, std::move(rawEmbedding));
        });
    }

    void IterateId(std::function<void(TId, TRawEmbedding)> cb) {
        Embeddings.clear();
        IterateImpl<true>(nullptr, [&](TId id, TRawEmbedding rawEmbedding) {
            cb(id, std::move(rawEmbedding));
        });
    }

private:
    template <bool WithPK>
    void IterateImpl(TReadCallback* read, auto&& cb) {
        ReadImpl(WithPK);
        JoinImpl(WithPK);
        ProcessImpl<WithPK>(read, cb);
    }

    auto ReadSettings(bool withPK) {
        TReadTableSettings settings;
        settings.ReturnNotNullAsOptional(false);
        if (ParentId != 0) {
            TValueBuilder pk;

            pk.BeginTuple();
            pk.AddElement().Uint32(ParentId);
            pk.AddElement().Uint32(0);
            pk.EndTuple();
            settings.From(TKeyBound::Inclusive(pk.Build()));

            pk.BeginTuple();
            pk.AddElement().Uint32(ParentId + 1);
            pk.AddElement().Uint32(0);
            pk.EndTuple();
            settings.To(TKeyBound::Exclusive(pk.Build()));
        }
        if (ParentId == 0 || Options.ShuffleWithEmbeddings) {
            if (withPK) {
                settings.AppendColumns(Options.PrimaryKey);
            }
            settings.AppendColumns(Options.Embedding);
        } else {
            settings.AppendColumns(Options.PrimaryKey);
        }
        return settings;
    }

    bool ReadPart(TTablePartIterator& it, ui64& rows) {
        auto next = it.ReadNext();
        auto part = next.ExtractValueSync();
        if (part.EOS()) {
            rows = std::numeric_limits<ui64>::max();
        } else if (!part.IsSuccess() || part.GetPart().RowsCount() == 0) {
            return true;
        } else {
            rows += part.GetPart().RowsCount();
        }
        if (Y_UNLIKELY(rows > Rows())) {
            Read.Stop();
            return false;
        }
        Read.Push(part.ExtractPart());
        return true;
    }

    void ReadImpl(bool WithPK) {
        auto settings = ReadSettings(WithPK);
        auto r = Client.RetryOperationSync([&](TSession session) {
            auto fit = session.ReadTable(FullName(Options, Table), settings);
            auto r = fit.ExtractValueSync();
            if (!r.IsSuccess()) {
                ythrow TVectorException{r};
            }
            ThreadPool.Submit([this, it = std::move(r)]() mutable {
                ui64 rows = 0;
                while (ReadPart(it, rows)) {
                }
            });
            return TStatus{EStatus::SUCCESS, {}};
        });
        if (!r.IsSuccess()) {
            ythrow TVectorException{r};
        }
    }

    bool JoinPart(bool withPK, TResultSet&& r) {
        TResultSetParser batch{r};
        if (!batch.TryNextRow()) {
            return false;
        }
        Y_ASSERT(batch.ColumnsCount() == 1);
        auto& builder = ParamsBuilder.AddParam("$ids").BeginList();
        auto primaryKeyIdx = batch.ColumnIndex(Options.PrimaryKey);
        auto& primaryKey = batch.ColumnParser(primaryKeyIdx);
        do {
            builder.AddListItem().Uint32(primaryKey.GetUint32());
        } while (batch.TryNextRow());
        builder.EndList().Build();

        TString query = std::format("embedding FROM {0} WHERE id IN $ids", Options.Table);
        if (withPK) {
            query = "SELECT id, " + query;
        } else {
            query = "SELECT " + query;
        }

        auto fit = Client.StreamExecuteScanQuery(query, ParamsBuilder.Build());

        auto it = fit.ExtractValueSync();
        if (!it.IsSuccess()) {
            ythrow TVectorException{it};
        }

        while (true) {
            auto next = it.ReadNext();
            auto part = next.ExtractValueSync();
            if (part.EOS()) {
                break;
            } else if (!part.IsSuccess() || part.GetResultSet().RowsCount() == 0) {
                continue;
            }
            Join.Push(part.ExtractResultSet());
        }
        return true;
    }

    void JoinImpl(bool withPK) {
        if (ParentId == 0 || Options.ShuffleWithEmbeddings) {
            Process = &Read;
            return;
        }
        Process = &Join;
        ThreadPool.Submit([this, withPK]() mutable {
            while (true) {
                auto part = Read.Pop();
                if (!JoinPart(withPK, std::move(part))) {
                    Join.Stop();
                    return;
                }
            }
        });
    }

    template <bool WithPK>
    bool ProcessPart(TResultSet&& r, auto&& cb) {
        TResultSetParser batch{r};
        if (!batch.TryNextRow()) {
            return false;
        }
        if constexpr (WithPK) {
            Y_ASSERT(batch.ColumnsCount() == 2);
            auto primaryKeyIdx = batch.ColumnIndex(Options.PrimaryKey);
            auto embeddingIdx = batch.ColumnIndex(Options.Embedding);
            auto& primaryKey = batch.ColumnParser(primaryKeyIdx);
            auto& embedding = batch.ColumnParser(embeddingIdx);
            do {
                TId pk = 0;
                if constexpr (TABLE == 64) {
                    if (ParentId == 0) {
                        pk = primaryKey.GetUint64();
                    } else if (Options.ShuffleWithEmbeddings) {
                        pk = primaryKey.GetUint32();
                    } else {
                        pk = primaryKey.GetUint64();
                    }
                } else {
                    pk = primaryKey.GetUint32();
                }
                cb(pk, *embedding.GetOptionalString());
            } while (batch.TryNextRow());
        } else {
            Y_ASSERT(batch.ColumnsCount() == 1);
            auto embeddingIdx = batch.ColumnIndex(Options.Embedding);
            auto& embedding = batch.ColumnParser(embeddingIdx);
            do {
                cb(*embedding.GetOptionalString());
            } while (batch.TryNextRow());
        }
        return true;
    }

    template <bool WithPK>
    void ProcessImpl(TReadCallback* read, auto&& cb) {
        while (true) {
            std::unique_lock lock{Process->M};
            if (read && Process->Queue.empty()) {
                lock.unlock();
                if constexpr (WithPK) {
                    read->TriggerIds();
                } else {
                    read->TriggerEmbeddings();
                }
                lock.lock();
            }
            auto part = Process->PopImpl(lock);
            lock.unlock();
            if (!ProcessPart<WithPK>(std::move(part), cb)) {
                return;
            }
        }
    }

    const TOptions& Options;
    TTableClient& Client;
    NVectorIndex::TThreadPool& ThreadPool;
    TString Table;
    TId ParentId = 0;
    ui64 RowsCount = 0;
    TParamsBuilder ParamsBuilder;

    struct Stream {
        static constexpr ui64 kPrefetch = 1;
        inline static const TResultSet EmptyPart{Ydb::ResultSet{}};

        std::mutex M;
        std::condition_variable WasPush;
        std::condition_variable WasPop;
        std::queue<TResultSet> Queue;

        void Push(TResultSet&& part) {
            std::unique_lock lock{M};
            PushImpl(std::move(part));
            while (Queue.size() > kPrefetch) {
                WasPop.wait(lock);
            }
        }

        void Stop() {
            auto part = EmptyPart;
            std::unique_lock lock{M};
            PushImpl(std::move(part));
        }

        TResultSet PopImpl(std::unique_lock<std::mutex>& lock) {
            while (Queue.empty()) {
                WasPush.wait(lock);
            }
            auto part = std::move(Queue.front());
            Queue.pop();
            if (Queue.size() == kPrefetch) {
                WasPop.notify_one();
            }
            return part;
        }

        TResultSet Pop() {
            std::unique_lock lock{M};
            return PopImpl(lock);
        }

    private:
        void PushImpl(TResultSet&& part) {
            Queue.emplace(std::move(part));
            if (Queue.size() == 1) {
                WasPush.notify_one();
            }
        }
    };

    // Read push to Read Stream
    Stream Read;
    // Join pop from Read Stream and push to Join Stream
    Stream Join; // Join is optional step
    // Process pop from Join Stream if it's present otherwise Read
    Stream* Process = nullptr;

    std::vector<TString> Embeddings;
};

template <typename T>
static float CosineDistance(std::span<const T> lhs, std::span<const T> rhs) {
    Y_ASSERT(lhs.size() == rhs.size());
    auto* l = lhs.data();
    auto* r = rhs.data();
    if constexpr (std::is_same_v<T, float>) {
        auto res = TriWayDotProduct(l, r, lhs.size());
        float norm = std::sqrt(res.LL * res.RR);
        return norm != 0 ? 1 - (res.LR / norm) : 1;
    } else {
        float ll = DotProduct(l, l, lhs.size());
        float rr = DotProduct(r, r, lhs.size());
        float lr = DotProduct(l, r, lhs.size());
        float norm = std::sqrt(ll * rr);
        return norm != 0 ? 1 - (lr / norm) : 1;
    }
}

struct TBulkSender {
    TBulkSender(const TOptions& options, TTableClient& client, ui32 cores)
        : Options{options}
        , Client{client}
    {
        RetrySettings
            .MaxRetries(60)
            .GetSessionClientTimeout(TDuration::Seconds(60))
            .Idempotent(true)
            .RetryUndefined(true);

        ToSend.reserve(std::max<ui32>(cores, 2));
        ToWait.reserve(std::max<ui32>(cores, 2));
    }

    void CreateLevel(ui8 level) {
        Count = 0;
        Cout << "Start create level: " << (int)level << Endl;
        if (level >= Options.Levels) {
            return;
        }
        if (level % 2 == 0) {
            CreateKMeans(Client, Options, "_1");
        } else {
            CreateKMeans(Client, Options, "_0");
        }
    }

    void DropLevel(ui8 level) {
        Cout << "Finish create level: " << (int)level << Endl;
        auto indexTable = FullIndexName(Options);
        if (level % 2 == 0) {
            DropTable(Client, indexTable + "_0");
        } else {
            DropTable(Client, indexTable + "_1");
        }
    }

    void Send(TString table, TValue&& value) {
        std::lock_guard lock{M};
        if (ToSend.size() == ToSend.capacity()) {
            WaitImpl();
        }
        auto f = Client.RetryOperation([table = std::move(table), value = std::move(value)](TTableClient& client) {
            auto r = value;
            return client.BulkUpsert(table, std::move(r)).Apply([](TAsyncBulkUpsertResult result) -> TStatus {
                return result.ExtractValueSync();
            });
        }, RetrySettings);
        ToSend.emplace_back(std::move(f));
    }

    void Wait() {
        std::lock_guard lock{M};
        WaitImpl();
        WaitImpl();
    }

    const TOptions& Options;

private:
    void WaitImpl() {
        if (!ToWait.empty()) {
            Count += ToWait.size();
            // Cout << "Wait for insertions " << Count << " / " << Options.Rows / kBulkSize << Endl;
            for (auto& f : ToWait) {
                auto r = f.ExtractValueSync();
                if (!r.IsSuccess()) {
                    ythrow TVectorException{r};
                }
            }
            ToWait.clear();
        }
        ToWait.swap(ToSend);
    }

    TRetryOperationSettings RetrySettings;
    std::mutex M;
    TTableClient& Client;
    std::vector<TAsyncStatus> ToSend;
    std::vector<TAsyncStatus> ToWait;
    ui64 Count = 0;
};

enum EFormat: ui8 {
    FloatVector = 1, // 4-byte per element
    Uint8Vector = 2, // 1-byte per element, better than Int8 for positive-only Float
    Int8Vector = 3,  // 1-byte per element
    BitVector = 10,  // 1-bit  per element
};

template <typename T>
struct TTypeToFormat;

template <>
struct TTypeToFormat<float> {
    static constexpr auto Format = EFormat::FloatVector;
};

template <>
struct TTypeToFormat<i8> {
    static constexpr auto Format = EFormat::Int8Vector;
};

template <typename T>
inline constexpr auto Format = TTypeToFormat<T>::Format;

struct TBulkWriter {
    TBulkWriter(TBulkSender& sender)
        : Sender{sender}
    {
        Rows.BeginList();
    }

    void UseLevel(ui8 level) {
        auto indexTable = FullIndexName(Sender.Options);
        Embeddings = Sender.Options.LastLevelEmbeddings;
        if (level < Sender.Options.Levels) {
            Embeddings = Sender.Options.ShuffleWithEmbeddings;
            indexTable += "_" + std::to_string((level + 1) % 2);
        }
        if (indexTable != Table) {
            Send();
            Table = indexTable;
        }
    }

    void WritePosting(TId parentId, TId id, TRawEmbedding rawEmbedding) {
        Y_ASSERT(parentId <= std::numeric_limits<ui32>::max());
        if (!Embeddings) {
            rawEmbedding = {};
        }
        Rows.AddListItem()
            .BeginStruct()
            .AddMember("parent_" + Sender.Options.PrimaryKey)
            .Uint32(static_cast<ui32>(parentId))
            .AddMember(Sender.Options.PrimaryKey)
            .Uint32(static_cast<ui32>(id))
            .AddMember(Sender.Options.Embedding)
            .String(std::move(rawEmbedding))
            .EndStruct();
        if (++Count == kBulkSize) {
            Send();
        }
    }

    template <typename T>
    void WriteCluster(TId parentId, TId id, const std::vector<T>& embedding) {
        auto byteSize = embedding.size() * sizeof(T);
        TString rawEmbedding(byteSize + 1, 0);
        auto* data = const_cast<char*>(rawEmbedding.data());
        std::memcpy(data, embedding.data(), byteSize);
        data[byteSize] = Format<T>;
        bool embeddings = std::exchange(Embeddings, true);
        WritePosting(parentId, id, std::move(rawEmbedding));
        Embeddings = embeddings;
    }

    void Send() {
        if (Count == 0) {
            return;
        }
        auto value = Rows.EndList().Build();
        Sender.Send(Table, std::move(value));
        Rows.BeginList();
        Count = 0;
    }

private:
    TBulkSender& Sender;
    TString Table;
    NYdb::TValueBuilder Rows;
    ui32 Count = 0;
    bool Embeddings = true;
};

template <typename TFunc>
struct [[nodiscard]] Finally {
    Finally(TFunc&& func)
        : Func{std::move(func)}
    {
    }

    ~Finally() noexcept {
        Func();
    }

private:
    [[no_unique_address]] TFunc Func;
};

template <typename T>
static void UpdateKMeans(TTableClient& client, const TOptions& options) {
    using TClusterizer = TClusterizer<T>;
    using TClusters = typename TClusterizer::TClusters;
    const auto cores = std::thread::hardware_concurrency() / 2;
    NVectorIndex::TThreadPool tpCompute{cores};
    NVectorIndex::TThreadPool tpIO{cores * (options.ShuffleWithEmbeddings ? 2 : 1)};

    auto makeClustersImpl = [&options](TTableIterator& reader, TBulkWriter& writer, TClusterizer& clusterizer, ui8 level, TId parentId, ui64 count) {
        reader.UseLevel(level, parentId, count);
        writer.UseLevel(level);
        auto clusters = clusterizer.Run({
            .parentId = parentId,
            .maxIterations = options.Iterations,
            .maxK = options.Clusters,
        });

        writer.UseLevel(options.Levels);
        for (size_t i = 0; auto id : clusters.Ids) {
            if (id) {
                writer.WriteCluster(parentId, id, clusters.Coords[i]);
            }
            ++i;
        }
        writer.Send();
        if (level >= options.Levels) {
            return TClusters{};
        }
        return clusters;
    };

    struct TMeta {
        TId ParentId = 0;
        ui64 Count = 0;
    };
    std::vector<TMeta> next;
    auto processCluster = [&](const TClusters& clusters) {
        for (size_t i = 0; auto id : clusters.Ids) {
            auto count = clusters.Count[i];
            if (count != 0) {
                next.push_back({id, count});
            }
            ++i;
        }
    };

    NVectorIndex::TWaitGroup wg;
    std::deque<TClusters> clusters;
    ui64 countDoneClusters = 0;
    ui64 countAllClusters = 0;

    auto processClusters = [&](ui8 level, ui32 count) {
        if (clusters.size() < count) {
            return;
        }
        countDoneClusters += clusters.size();
        Cout << "Wait for " << countDoneClusters << " / " << countAllClusters << " clusters on " << (int)level << " level" << Endl;
        wg.Wait();
        for (auto& cluster : clusters) {
            processCluster(cluster);
        }
        clusters.clear();
    };
    TTableIterator reader{options, client, tpIO};
    TBulkSender sender{options, client, cores};
    TBulkWriter writer{sender};
    TClusterizer clusterizer{reader, CosineDistance<T>,
                             [&](TId parentId, TId id, TRawEmbedding embedding) {
                                 writer.WritePosting(parentId, id, std::move(embedding));
                             },
                             &tpCompute};
    sender.DropLevel(options.Levels);

    auto makeClusters = [&](ui8 level, TId parentId, ui64 count) {
        if (ui64(level - 1) * options.Clusters * 2 < cores) {
            auto clusters = makeClustersImpl(reader, writer, clusterizer, level, parentId, count);
            processCluster(clusters);
            return;
        }
        processClusters(level, cores);
        auto& p = clusters.emplace_back();
        wg.Add();
        tpCompute.Submit([&, level, parentId, count]() mutable {
            Finally done = [&] {
                wg.Done();
            };
            TTableIterator reader{options, client, tpIO};
            TBulkWriter writer{sender};
            TClusterizer clusterizer{reader, CosineDistance<T>,
                                     [&](TId parentId, TId id, TRawEmbedding embedding) {
                                         writer.WritePosting(parentId, id, std::move(embedding));
                                     }};
            p = makeClustersImpl(reader, writer, clusterizer, level, parentId, count);
        });
    };
    Finally join = [&] {
        Cout << "Start join pools" << Endl;
        tpCompute.Join();
        tpIO.Join();
        Cout << "Finish UpdateKMeans " << Endl;
    };
    // TODO(mbkkt) start as bfs but continue as dfs?
    next.push_back({0, options.Rows});
    for (ui8 level = 1; !next.empty(); ++level) {
        sender.CreateLevel(level);
        auto curr = std::move(next);
        countDoneClusters = 0;
        countAllClusters = curr.size();
        if (level < options.Levels) {
            next.reserve(countAllClusters * options.Clusters);
        }
        for (auto& meta : curr) {
            makeClusters(level, meta.ParentId, meta.Count);
        }
        curr = {};
        processClusters(level, 1);
        sender.DropLevel(level);
        sender.Wait();
    }
}

// TODO use user defined target
static constexpr std::string_view kTargetQuery = R"($Target = Cast([0.1961289,0.51426697,0.03864574,0.5552187,-0.041873194,0.24177523,0.46322846,-0.3476358,-0.0802049,0.44246107,-0.06727136,-0.04970105,-0.0012320493,0.29773152,-0.3771864,0.047693416,0.30664062,0.15911901,0.27795044,0.11875397,-0.056650203,0.33322853,-0.28901896,-0.43791273,-0.014167095,0.36109218,-0.16923136,0.29162315,-0.22875166,0.122518055,0.030670911,-0.13762642,-0.13884683,0.31455114,-0.21587017,0.32154146,-0.4452795,-0.058932953,0.07103838,0.4289945,-0.6023675,-0.14161813,0.11005565,0.19201005,0.2591869,-0.24074492,0.18088372,-0.16547637,0.08194011,0.10669302,-0.049760908,0.15548608,0.011035396,0.16121127,-0.4862669,0.5691393,-0.4885568,0.90131176,0.20769958,0.010636337,-0.2094356,-0.15292564,-0.2704138,-0.01326699,0.11226809,0.37113565,-0.018971693,0.86532146,0.28991342,0.004782651,-0.0024367527,-0.0861291,0.39704522,0.25665164,-0.45121723,-0.2728092,0.1441502,-0.5042585,0.3507123,-0.38818485,0.5468399,0.16378048,-0.11177127,0.5224827,-0.05927702,0.44906104,-0.036211397,-0.08465567,-0.33162776,0.25222498,-0.22274417,0.15050206,-0.012386843,0.23640677,-0.18704978,0.1139806,0.19379948,-0.2326912,0.36477265,-0.2544955,0.27143118,-0.095495716,-0.1727166,0.29109988,0.32738894,0.0016002139,0.052142758,0.37208632,0.034044757,0.17740013,0.16472393,-0.20134833,0.055949032,-0.06671674,0.04691583,0.13196157,-0.13174891,-0.17132106,-0.4257385,-1.1067779,0.55262613,0.37117195,-0.37033138,-0.16229,-0.31594914,-0.87293816,0.62064904,-0.32178572,0.28461748,0.41640115,-0.050539408,0.009697271,0.3483608,0.4401717,-0.08273758,0.4873984,0.057845585,0.28128678,-0.43955156,-0.18790118,0.40001884,0.54413813,0.054571174,0.65416795,0.04503013,0.40744695,-0.048226677,0.4787822,0.09700139,0.07739511,0.6503141,0.39685145,-0.54047453,0.041596334,-0.22190939,0.25528133,0.17406437,-0.17308964,0.22076453,0.31207982,0.8434676,0.2086337,-0.014262581,0.05081182,-0.30908328,-0.35717097,0.17224313,0.5266846,0.58924395,-0.29272506,0.01910475,0.061457288,0.18099669,0.04807291,0.34706554,0.32477927,0.17174402,-0.070991516,0.5819317,0.71045977,0.07172716,0.32184732,0.19009985,0.04727492,0.3004647,0.26943457,0.61640364,0.1655051,-0.6033329,0.09797926,-0.20623252,0.10987298,1.016591,-0.29540864,0.25161317,0.19790122,0.14642714,0.5081536,-0.22128952,0.4286613,-0.029895071,0.23768105,-0.0023987228,0.086968,0.42884818,-0.33578634,-0.38033295,-0.16163215,-0.18072455,-0.5015756,0.28035417,-0.0066010267,0.67613393,-0.026721207,0.22796173,-0.008428602,-0.38017297,-0.33044866,0.4519961,-0.05542353,-0.2976922,0.37046987,0.23409955,-0.24246313,-0.12839256,-0.4206849,-0.049280513,-0.7651326,0.1649417,-0.2321146,0.106625736,-0.37506104,0.14470209,-0.114986554,-0.17738944,0.612335,0.25292027,-0.092776075,-0.3876576,-0.08905502,0.3793106,0.7376429,-0.3080258,-0.3869677,0.5239047,-0.41152182,0.22852719,0.42226496,-0.28244498,0.0651847,0.3525671,-0.5396397,-0.17514983,0.29470462,-0.47671098,0.43471992,0.38677526,0.054752454,0.2183725,0.06853758,-0.12792642,0.67841107,0.24607432,0.18936129,0.24056062,-0.30873874,0.62442464,0.5792256,0.20426203,0.54328054,0.56583667,-0.7724596,-0.08384111,-0.16767848,-0.21682987,0.05710991,-0.015403866,0.38889074,-0.6050326,0.4075437,0.40839496,0.2507789,-0.32695654,0.24276069,0.1271161,-0.010688765,-0.31864303,0.15747054,-0.4670915,-0.21059138,0.7470888,0.47273478,-0.119508654,-0.63659865,0.64500844,0.5370401,0.28596714,0.0046216915,0.12771192,-0.18660222,0.47342712,-0.32039297,0.10946048,0.25172964,0.021965463,-0.12397459,-0.048939236,0.2881649,-0.61231786,-0.33459276,-0.29495123,-0.14027011,-0.23020774,0.73250633,0.71871173,0.78408533,0.4140183,0.1398299,0.7395877,0.06801048,-0.8895956,-0.64981127,-0.37226167,0.1905936,0.12819989,-0.47098637,-0.14334664,-0.933116,0.4597078,0.09895813,0.38114703,0.14368558,-0.42793563,-0.10805895,0.025374172,0.40162122,-0.1686769,0.5257471,-0.3540743,0.08181256,-0.34759146,0.0053078625,0.09163392,0.074487045,-0.14934056,0.034427803,0.19613744,-0.00032829077,0.27792764,0.09889235,-0.029708104,0.3528952,0.22679164,-0.27263018,0.6655268,-0.21362385,0.13035864,0.41666874,0.1253278,-0.22861275,0.105085365,0.09412938,0.03228179,0.11568338,0.23504587,-0.044100706,0.0104857525,-0.07461301,0.1034835,0.3078725,0.5257031,-0.015183647,-0.0060899477,-0.02852683,-0.39821762,-0.20495597,-0.14892153,0.44850922,0.40366673,-0.10324784,0.4095244,0.8356313,0.21190739,-0.12822983,0.06830399,0.036365107,0.044244137,0.26112562,0.033477627,-0.41074416,-0.009961431,0.23717403,0.12438699,-0.05255729,-0.18411024,-0.18563229,-0.16543737,-0.122300245,0.40962145,-0.4751102,0.5309857,0.04474563,0.103834346,0.14118321,4.2373734,0.45751426,0.21709882,0.6866778,0.14838168,-0.1831362,0.10963214,-0.33557487,-0.1084519,0.3299757,0.076113895,0.12850489,-0.07326015,-0.23770756,0.11080451,0.29712623,-0.13904962,0.25797644,-0.5074562,0.4018296,-0.23186816,0.24427155,0.39540753,0.015477164,0.14021018,0.273185,0.013538655,0.47227964,0.52339536,0.54428,0.16983595,0.5470162,-0.0042650895,0.21768,0.090606116,-0.13433483,0.5818122,-0.1384567,0.2354754,0.08440857,-0.2166868,0.48664945,-0.13175073,0.45613387,0.089229666,0.15436831,0.08720108,0.37597507,0.52855235,-0.019367872,0.544358,-0.327109,-0.20839518,-0.33598265,0.033363096,0.42312673,0.13452567,0.40526676,0.08402101,-0.19661862,-0.24802914,0.23069139,0.5153508,0.13562717,-0.23842931,-0.23257096,-0.009195984,0.41388315,0.56304437,-0.23492545,-0.2642354,0.3038204,-0.09548942,-0.22467934,-0.2561862,-0.34057313,-0.19744347,0.0007430283,-0.12842518,-0.13980682,0.6849243,0.1795335,-0.5626032,-0.07626079,-0.062749654,0.6660117,-0.4479761,0.07978033,0.6269782,0.536793,0.6801336,-0.22563715,0.38902125,-0.09493616,0.21312712,0.17763247,0.1796997,-3.868085,0.08134122,0.10347531,-0.034904435,-0.2792477,-0.17850947,0.083218865,0.26535586,-0.25551575,0.28172702,0.1383222,0.10376686,-0.123248994,0.1985073,-0.40000066,0.44763976,0.028454497,0.37575415,0.071487874,-0.16965964,0.38927504,0.29088503,-0.011822928,-0.19522227,-0.1766321,0.1731763,0.49192554,0.44358602,-0.49064636,0.024170646,0.025736902,-0.17963372,0.38337404,0.07339889,0.042465065,0.5910191,0.07904464,-0.043729525,-0.16969916,0.4008944,-0.04921039,-0.3757768,0.6075314,-0.24661873,-0.1780646,0.60300773,-0.09518917,0.2213779,-0.46496615,-0.41421738,0.23309247,0.14687467,-0.36499617,0.04227981,0.88024706,0.57489127,0.21026954,-0.13666761,0.05710815,0.22095469,-0.033460964,0.13861561,0.22527887,0.1660716,-0.3286249,-0.060175333,-0.2971499,0.2454142,0.6536238,-0.22991207,0.046677545,-0.026631566,-0.04271381,-0.53681016,0.11866242,-0.24970472,-0.37882543,0.33650783,0.7634871,-0.2858582,0.029164914,0.28833458,-0.39263156,0.64842117,2.6358266,0.058920268,2.2507918,0.6809379,-0.41290292,0.36954543,-0.60793567,0.42561662,0.2498035,0.27133986,-0.005307673,0.32910514,-0.03169463,-0.02270061,-0.14702365,-0.25256258,0.54468036,-0.46112943,-0.07411629,-0.030253865,0.20578359,0.6495886,-0.11674013,0.029835526,0.019896187,-0.008101909,0.3706806,-0.26088533,-0.018712807,0.17228629,0.15223767,0.0675542,0.6338221,-0.15303946,0.02908536,0.27217266,-0.10829474,4.503505,-0.37745082,0.20543274,-0.087563366,-0.14404398,0.5562983,0.41639867,-0.38191214,-0.16266975,-0.46071815,0.51874137,0.36326376,0.027115177,-0.06804209,0.35159302,-0.41162485,0.30493516,0.18828706,0.63608,-0.04735176,0.13811842,0.09368063,0.037441075,-0.0012712433,-0.19929455,0.34804425,0.46975428,0.38857734,-0.061463855,0.122808196,0.37608445,5.2436657,0.25659403,-0.19236223,-0.25611007,0.22265173,0.5898642,-0.28255892,-0.4123271,-0.4214137,0.09197922,-0.060595497,-0.13819462,-0.13570791,0.25433356,0.5907837,0.2548469,-0.39375016,-0.37651995,0.701745,-0.0359955,-0.048193086,0.4458719,0.088069156,-0.015497342,0.52568024,-0.4795603,-0.025876174,0.76476455,-0.32245165,-0.038828112,0.6325802,0.06385053,-0.26389623,0.2439906,-0.4231506,0.19213657,0.5828574,0.053197365,0.45217928,0.040650904,0.83714896,0.63782233,-0.737095,-0.41026706,0.23113042,0.19471557,-0.24410644,-0.35155243,0.20881484,-0.01721743,-0.29494065,-0.114185065,1.2226206,-0.16469914,0.083336286,0.63608664,0.41011855,-0.032080106,-0.08833447,-0.6261006,0.22665286,0.08313674,-0.16372047,0.5235312,0.39580458,0.0007253827,0.10186727,-0.15955615,0.54162663,0.32992217,-0.02491269,0.16312002,0.118171245,-0.029900813,0.038405042,0.31396118,0.45241603,-0.07010825,0.07611299,0.084779754,0.34168348,-0.60676336,0.054825004,-0.16054128,0.2525291,0.20532744,-0.1510394,0.4857572,0.32150552,0.35749313,0.4483151,0.0057622716,0.28705776,-0.018361313,0.08605509,-0.08649293,0.26918742,0.4806176,0.098294765,0.3284613,0.00010664656,0.43832678,-0.33351916,0.02354738,0.004953976,-0.14319824,-0.33351237,-0.7268964,0.56292313,0.1275613,0.4438945,0.7984555,-0.19372283,0.2940397,-0.11770557] AS List<Float>);
  {0})";

static constexpr std::string_view kTopKFlatBitQuery = R"($TargetEmbeddingFloat = Knn::ToBinaryStringFloat($Target);
  $TargetEmbeddingBit = Knn::ToBinaryStringBit($Target);

  $IndexIds = SELECT {1}, Knn::{0}({4}, $TargetEmbeddingBit) as distance
    FROM {2}
    ORDER BY distance
    LIMIT {6} * 4;

  SELECT {1}, Knn::{0}({4}, $TargetEmbeddingFloat) as distance, {5}
    FROM {3}
    WHERE {1} IN (SELECT {1} FROM $IndexIds)
    ORDER BY distance
    LIMIT {6};)";

static constexpr std::string_view kTopKKMeansNoneQuery = R"($TargetEmbeddingFloat = Knn::ToBinaryStringFloat($Target);
$LimitCentroid0 = 1;
$LimitVectors = {6};

$Centroid0Ids = SELECT Knn::{0}({4}, $TargetEmbeddingFloat) AS distance, {1}
  FROM {2}
  WHERE parent_id = 0
  ORDER BY distance
  LIMIT $LimitCentroid0;

$VectorIds = SELECT Knn::{0}({4}, $TargetEmbeddingFloat) AS distance, {1}
  FROM {2}
  WHERE parent_id IN (SELECT {1} FROM $Centroid0Ids)
  ORDER BY distance
  LIMIT $LimitVectors;

SELECT {1}, Knn::{0}({4}, $TargetEmbeddingFloat) as distance, {5}
  FROM {3}
  WHERE {1} IN (SELECT {1} FROM $VectorIds)
  ORDER BY distance
  LIMIT $LimitVectors;
)";

static void TopKFlatBit(TTableClient& client, const TOptions& options) {
    TString query = std::format(kTopKFlatBitQuery,
                                options.Distance,
                                options.PrimaryKey,
                                IndexName(options),
                                options.Table,
                                options.Embedding,
                                options.Data,
                                options.TopK);
    // Cout << query << Endl;
    query = std::format(kTargetQuery, query);
    auto r = client.RetryOperationSync([&](TSession session) -> TStatus {
        auto prepareResult = session.PrepareDataQuery(query).ExtractValueSync();
        if (!prepareResult.IsSuccess()) {
            return prepareResult;
        }

        auto query = prepareResult.GetQuery();
        auto result = query.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                          .ExtractValueSync();

        if (result.IsSuccess()) {
            PrintTop(result.GetResultSetParser(0));
        }

        return result;
    });
    if (!r.IsSuccess()) {
        ythrow TVectorException{r};
    }
}

static void TopKKMeansNone(TTableClient& client, const TOptions& options) {
    TString query = std::format(kTopKKMeansNoneQuery,
                                options.Distance,
                                options.PrimaryKey,
                                IndexName(options),
                                options.Table,
                                options.Embedding,
                                options.Data,
                                options.TopK);
    // Cout << query << Endl;
    query = std::format(kTargetQuery, query);
    auto r = client.RetryOperationSync([&](TSession session) -> TStatus {
        auto prepareResult = session.PrepareDataQuery(query).ExtractValueSync();
        if (!prepareResult.IsSuccess()) {
            return prepareResult;
        }

        auto query = prepareResult.GetQuery();
        auto result = query.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                          .ExtractValueSync();

        if (result.IsSuccess()) {
            PrintTop(result.GetResultSetParser(0));
        }

        return result;
    });
    if (!r.IsSuccess()) {
        ythrow TVectorException{r};
    }
}

int DropIndex(NYdb::TDriver& driver, const TOptions& options) {
    TTableClient client(driver);
    DropIndex(client, options);
    return 0;
}

int CreateIndex(NYdb::TDriver& driver, const TOptions& options) {
    TTableClient client(driver);
    if (options.IndexType == FlatIndex) {
        CreateFlat(client, options);
        return 0;
    } else if (options.IndexType == KMeansIndex) {
        CreateKMeans(client, options);
        return 0;
    }
    return 1;
}

int UpdateIndex(NYdb::TDriver& driver, const TOptions& options) {
    TTableClient client(driver);
    if (options.IndexType == FlatIndex) {
        if (options.IndexQuantizer == NQuantizer::None) {
            return 0;
        }
        if (options.IndexQuantizer == NQuantizer::Bit) {
            UpdateFlatBit(client, options);
            return 0;
        }
    } else if (options.IndexType == KMeansIndex) {
        if (options.IndexQuantizer == NQuantizer::None) {
            UpdateKMeans<float>(client, options);
            return 0;
        } else if (options.IndexQuantizer == NQuantizer::Int8) {
            UpdateKMeans<i8>(client, options);
            return 0;
        }
    }
    return 1;
}

int TopK(NYdb::TDriver& driver, const TOptions& options) {
    TTableClient client(driver);
    if (options.IndexType == FlatIndex) {
        if (options.IndexQuantizer == NQuantizer::None) {
            return 0;
        }
        if (options.IndexQuantizer == NQuantizer::Bit) {
            TopKFlatBit(client, options);
            return 0;
        }
    } else if (options.IndexType == KMeansIndex) {
        if (options.IndexQuantizer == NQuantizer::None) {
            TopKKMeansNone(client, options);
            return 0;
        }
    }
    return 1;
}