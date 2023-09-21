#include "pagination.h"

#include <util/folder/pathsplit.h>
#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NTable;

const ui32 MaxPages = 10;

class TYdbErrorException : public yexception {
public:
    TYdbErrorException(const TStatus& status)
        : Status(status) {}

    TStatus Status;
};

static void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    }
}

static void PrintStatus(const TStatus& status) {
    Cerr << "Status: " << status.GetStatus() << Endl;
    status.GetIssues().PrintTo(Cerr);
}

static TString JoinPath(const TString& basePath, const TString& path) {
    if (basePath.empty()) {
        return path;
    }

    TPathSplitUnix prefixPathSplit(basePath);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}

//! Creates sample table with CrateTable API.
static void CreateTable(TTableClient client, const TString& path) {
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto schoolsDesc = TTableBuilder()
            .AddNullableColumn("city", EPrimitiveType::Utf8)
            .AddNullableColumn("number", EPrimitiveType::Uint32)
            .AddNullableColumn("address", EPrimitiveType::Utf8)
            .SetPrimaryKeyColumns({ "city", "number" })
            .Build();

        return session.CreateTable(JoinPath(path, "schools"), std::move(schoolsDesc)).GetValueSync();
    }));
}

//! Fills sample tables with data in single parameterized data query.
static TStatus FillTableDataTransaction(TSession& session, const TString& path) {
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $schoolsData AS List<Struct<
            city: Utf8,
            number: Uint32,
            address: Utf8>>;

        REPLACE INTO schools
        SELECT
            city,
            number,
            address
        FROM AS_TABLE($schoolsData);
    )", path.c_str());

    auto params = GetTablesDataParams();

    return session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();
}

//! Shows usage of query paging.
static TStatus SelectPagingTransaction(TSession& session, const TString& path,
    ui64 pageLimit, const TString& lastCity, ui32 lastNumber, TMaybe<TResultSet>& resultSet)
{
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $limit AS Uint64;
        DECLARE $lastCity AS Utf8;
        DECLARE $lastNumber AS Uint32;

        $part1 = (
            SELECT * FROM schools
            WHERE city = $lastCity AND number > $lastNumber
            ORDER BY city, number LIMIT $limit
        );

        $part2 = (
            SELECT * FROM schools
            WHERE city > $lastCity
            ORDER BY city, number LIMIT $limit
        );

        $union = (
            SELECT * FROM $part1
            UNION ALL
            SELECT * FROM $part2
        );

        SELECT * FROM $union
        ORDER BY city, number LIMIT $limit;
    )", path.c_str());

    auto params = session.GetParamsBuilder()
        .AddParam("$limit")
            .Uint64(pageLimit)
            .Build()
        .AddParam("$lastCity")
            .Utf8(lastCity)
            .Build()
        .AddParam("$lastNumber")
            .Uint32(lastNumber)
            .Build()
        .Build();

    auto result = session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}

bool SelectPaging(TTableClient client, const TString& path, ui64 pageLimit, TString& lastCity, ui32& lastNumber) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryOperationSync([path, pageLimit, &lastCity, lastNumber, &resultSet](TSession session) {
        return SelectPagingTransaction(session, path, pageLimit, lastCity, lastNumber, resultSet);
    }));

    TResultSetParser parser(*resultSet);

    if (!parser.TryNextRow()) {
        return false;
    }
    do {
        lastCity = parser.ColumnParser("city").GetOptionalUtf8().GetRef();
        lastNumber = parser.ColumnParser("number").GetOptionalUint32().GetRef();
        Cout << lastCity << ", Школа №" << lastNumber << ", Адрес: " << parser.ColumnParser("address").GetOptionalUtf8() << Endl;
    } while (parser.TryNextRow());
    return true;
}

bool Run(const TDriver& driver, const TString& path) {
    TTableClient client(driver);

    try {
        CreateTable(client, path);

        ThrowOnError(client.RetryOperationSync([path](TSession session) {
            return FillTableDataTransaction(session, path);
        }));

        ui64 limit = 3;
        TString lastCity;
        ui32 lastNumber = 0;
        ui32 page = 0;
        bool pageNotEmpty = true;

        Cout << "> Pagination, Limit=" << limit << Endl;

        // show first MaxPages=10 pages:
        while (pageNotEmpty && page <= MaxPages) {
            ++page;
            Cout << "> Page " << page << ":" << Endl;
            pageNotEmpty = SelectPaging(client, path, limit, lastCity, lastNumber);
        }
    }
    catch (const TYdbErrorException& e) {
        Cerr << "Execution failed due to fatal error:" << Endl;
        PrintStatus(e.Status);
        return false;
    }

    return true;
}
