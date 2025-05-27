#include "vector_index.h"
#include <format>
#include <fstream>
#include <sstream>
#include <thread>

template <>
struct std::formatter<std::string>: std::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const std::string& param, FormatContext& fc) const {
        return std::formatter<std::string_view>::format(std::string_view{param}, fc);
    }
};

using namespace NYdb;
using namespace NTable;
namespace {

constexpr uint64_t kBulkSize = 1000;
constexpr std::string_view FlatIndex = "flat";

namespace NQuantizer {

static constexpr std::string_view None = "None";
static constexpr std::string_view Int8 = "Int8";
static constexpr std::string_view Uint8 = "Uint8";
static constexpr std::string_view Bit = "Bit";

} // namespace NQuantizer

bool EqualsICase(std::string_view l, std::string_view r) {
    return std::equal(l.begin(), l.end(), r.begin(), r.end(), [](char l, char r) {
        return std::tolower(l) == std::tolower(r);
    });
}

void PrintTop(TResultSetParser&& parser) {
    while (parser.TryNextRow()) {
        Y_ASSERT(parser.ColumnsCount() >= 1);
        std::cout << *parser.ColumnParser(0).GetOptionalFloat() << "\t";
        for (size_t i = 1; i < parser.ColumnsCount(); ++i) {
            std::cout << *parser.ColumnParser(1).GetOptionalUtf8() << "\t";
        }
        std::cout << "\n";
    }
    std::cout << std::endl;
}

std::string FullName(const TOptions& options, const std::string& name) {
    return options.Database + std::string("/") + name;
}

std::string IndexName(const TOptions& options) {
    return options.Table + std::string("_") + options.IndexType + std::string("_") + options.IndexQuantizer;
}

std::string FullIndexName(const TOptions& options) {
    return FullName(options, IndexName(options));
}

void DropTable(TTableClient& client, const std::string& table) {
    auto r = client.RetryOperationSync([&](TSession session) {
        TDropTableSettings settings;
        return session.DropTable(table).ExtractValueSync();
    });
    if (!r.IsSuccess() && r.GetStatus() != EStatus::SCHEME_ERROR) {
        ythrow TVectorException{r};
    }
}

void DropIndex(TTableClient& client, const TOptions& options) {
    DropTable(client, FullIndexName(options));
}

void CreateFlat(TTableClient& client, const TOptions& options) {
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

void UpdateFlat(TTableClient& client, const TOptions& options, std::string_view type) {
    std::string query = std::format(R"(
        DECLARE $begin AS Uint64;
        DECLARE $rows AS Uint64;

        UPSERT INTO {1}
        SELECT COALESCE(CAST({2} AS Uint32), 0) AS {2}, Untag(Knn::ToBinaryString{4}(CAST(Knn::FloatFromBinaryString({3}) AS List<{5}>)), "{4}Vector") AS {3}
        FROM {0}
        WHERE $begin <= {2} AND {2} < $begin + $rows;
    )",
                                options.Table,
                                IndexName(options),
                                options.PrimaryKey,
                                options.Embedding,
                                type,
                                type == NQuantizer::Bit ? "Float" : type);
    std::cout << query << std::endl;

    auto last = std::chrono::steady_clock::now();
    uint64_t current = 0;
    uint64_t overall = (options.Rows + kBulkSize - 1) / kBulkSize;
    auto report = [&](auto curr) {
        std::cout << "Already done " << current << " / " << overall << " upserts, time spent: " << std::chrono::duration<double>{curr - last}.count() << std::endl;
        last = curr;
    };
    auto waitRequest = [&](auto& request) {
        auto r = request.ExtractValueSync();
        if (!r.IsSuccess()) {
            ythrow TVectorException{r};
        }
        ++current;
        if (auto curr = std::chrono::steady_clock::now(); (curr - last) >= std::chrono::seconds{1}) {
            report(curr);
        }
    };

    std::deque<TAsyncStatus> requests;
    auto waitFirst = [&] {
        if (requests.size() < std::thread::hardware_concurrency()) {
            return;
        }
        waitRequest(requests.front());
        requests.pop_front();
    };

    TParamsBuilder paramsBuilder;
    TRetryOperationSettings retrySettings;
    retrySettings
        .MaxRetries(10)
        .GetSessionClientTimeout(TDuration::Seconds(1));
    for (uint64_t i = 0; i < options.Rows; i += kBulkSize) {
        waitFirst();
        paramsBuilder.AddParam("$begin").Uint64(i).Build();
        paramsBuilder.AddParam("$rows").Uint64(kBulkSize).Build();
        auto f = client.RetryOperation([&, p = paramsBuilder.Build()](TSession session) {
            auto params = p;
            return session.ExecuteDataQuery(
                              query,
                              TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                              std::move(params))
                .Apply([](auto result) -> TStatus {
                    return result.ExtractValueSync();
                });
        }, retrySettings);
        requests.push_back(std::move(f));
    }

    for (auto& request : requests) {
        waitRequest(request);
    }
    report(std::chrono::steady_clock::now());
}

void TopKFlat(TTableClient& client, const TOptions& options, std::string_view type) {
    std::string query = std::format(R"(
          $TargetBinary = Knn::ToBinaryStringFloat($Target);
          $TargetSQ = Knn::ToBinaryString{7}(CAST($Target AS List<{8}>));
        
          $IndexIds = SELECT {1}, Knn::{0}({4}, $TargetSQ) as distance
            FROM {2}
            ORDER BY distance
            LIMIT {6} * 2;
        
          SELECT Knn::{0}({4}, $TargetBinary) as distance, {5}
            FROM {3}
            WHERE {1} IN (SELECT {1} FROM $IndexIds)
            ORDER BY distance
            LIMIT {6};
        )",
                                options.Distance,
                                options.PrimaryKey,
                                IndexName(options),
                                options.Table,
                                options.Embedding,
                                options.Data,
                                options.TopK,
                                type,
                                type == NQuantizer::Bit ? "Float" : type);
    std::cout << query << std::endl;
    std::ifstream targetFileStream(options.Target);
    std::stringstream targetStrStream;
    targetStrStream << targetFileStream.rdbuf();
    query = std::format(R"($Target = CAST({0} AS List<Float>); {1})", targetStrStream.view(), query);
    TExecDataQuerySettings settings;
    settings.KeepInQueryCache(true);
    auto r = client.RetryOperationSync([&](TSession session) -> TStatus {
        auto f = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),settings);
        auto r = f.ExtractValueSync();
        if (r.IsSuccess()) {
            PrintTop(r.GetResultSetParser(0));
        }
        return r;
    });
    if (!r.IsSuccess()) {
        ythrow TVectorException{r};
    }
}

} // namespace

ECommand Parse(std::string_view command) {
    if (EqualsICase(command, "DropIndex")) {
        return ECommand::DropIndex;
    }
    if (EqualsICase(command, "CreateIndex")) {
        return ECommand::CreateIndex;
    }
    if (EqualsICase(command, "BuildIndex")) {
        return ECommand::BuildIndex;
    }
    if (EqualsICase(command, "RecreateIndex")) {
        return ECommand::RecreateIndex;
    }
    if (EqualsICase(command, "TopK")) {
        return ECommand::TopK;
    }
    return ECommand::None;
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
    }
    return 1;
}

int BuildIndex(NYdb::TDriver& driver, const TOptions& options) {
    TTableClient client(driver);
    if (EqualsICase(options.IndexType, FlatIndex)) {
        if (EqualsICase(options.IndexQuantizer, NQuantizer::None)) {
            return 0;
        }
        if (EqualsICase(options.IndexQuantizer, NQuantizer::Int8)) {
            UpdateFlat(client, options, NQuantizer::Int8);
            return 0;
        }
        if (EqualsICase(options.IndexQuantizer, NQuantizer::Uint8)) {
            UpdateFlat(client, options, NQuantizer::Uint8);
            return 0;
        }
        if (EqualsICase(options.IndexQuantizer, NQuantizer::Bit)) {
            UpdateFlat(client, options, NQuantizer::Bit);
            return 0;
        }
    }
    return 1;
}

int TopK(NYdb::TDriver& driver, const TOptions& options) {
    TTableClient client(driver);
    if (EqualsICase(options.IndexType, FlatIndex)) {
        if (EqualsICase(options.IndexQuantizer, NQuantizer::None)) {
            return 0;
        }
        if (EqualsICase(options.IndexQuantizer, NQuantizer::Int8)) {
            TopKFlat(client, options, NQuantizer::Int8);
            return 0;
        }
        if (EqualsICase(options.IndexQuantizer, NQuantizer::Uint8)) {
            TopKFlat(client, options, NQuantizer::Uint8);
            return 0;
        }
        if (EqualsICase(options.IndexQuantizer, NQuantizer::Bit)) {
            TopKFlat(client, options, NQuantizer::Bit);
            return 0;
        }
    }
    return 1;
}
