#pragma once

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
#include <util/system/file.h>

namespace NYdb::NBackup {

class TQueryBuilder {
    TVector<TColumn> Columns;
    const TString Query;
    TValueBuilder Value;

    TString BuildQuery(const TString& path);
    void AddMemberFromString(TTypeParser& type, const TString& name, TStringBuf ss);
    void AddPrimitiveMember(EPrimitiveType type, TStringBuf buf);
    static void CheckNull(const TString& name, TStringBuf buf);
    static void BuildType(TTypeParser& typeParser, TTypeBuilder& typeBuilder, const TString& name);
    static TType GetType(TTypeParser& typeParser, const TString& name);

public:
    TQueryBuilder(const TString& path, TVector<TColumn> columns)
        : Columns(std::move(columns))
        , Query(BuildQuery(path))
    {}

    void Begin();
    void AddLine(TStringBuf line);
    TValue EndAndGetResultingValue();
    TParams EndAndGetResultingParams();
    TString GetQueryString() const;
};


class TQueryFromFileIterator {
    TFile DataFile;
    TQueryBuilder Query;

    const i64 BufferMaxSize;
    TString IoBuff;
    i64 CurrentOffset;
    i64 BytesRemaining;
    const i64 MaxRowsPerQuery; // 0 for inf
    const i64 MaxBytesPerQuery; // 0 for inf

    TStringBuf LinesBunch;

    void TryReadNextLines();

    template<bool GetValue>
    std::conditional_t<GetValue, TValue, TParams> ReadNext();

public:
    TQueryFromFileIterator(const TString& path, const TString& dataFileName, TVector<TColumn> columns, i64 buffSize,
            i64 maxRowsPerQuery, i64 maxBytesPerQuery)
      : DataFile(dataFileName, OpenExisting | RdOnly)
      , Query(path, std::move(columns))
      , BufferMaxSize(buffSize)
      , IoBuff(TString::Uninitialized(BufferMaxSize))
      , CurrentOffset(0)
      , BytesRemaining(DataFile.GetLength())
      , MaxRowsPerQuery(maxRowsPerQuery)
      // If MaxBytesPerQuery is not specified use 2MiB as default value. Since size of each row is rounded up
      // to nearest multiple of 1024 this effectively limits number of rows in query in case of small rows
      , MaxBytesPerQuery(maxBytesPerQuery ? maxBytesPerQuery : BufferMaxSize)
    {}

    bool Empty() const {
        return BytesRemaining == 0 && LinesBunch.empty();
    }

    TParams ReadNextGetParams() {
        return ReadNext<false>();
    }

    TValue ReadNextGetValue() {
        return ReadNext<true>();
    }

    TString GetQueryString() const;
};

} // NYdb::NBackup
