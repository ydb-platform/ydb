#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/strbuf.h>
#include <util/memory/pool.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/csv/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>

namespace NKikimr::NFormats {

struct TYdbDump {
    // Parse YdbDump-formatted line
    static bool ParseLine(TStringBuf line, const std::vector<std::pair<i32, NScheme::TTypeInfo>>& columnOrderTypes, TMemoryPool& pool,
                          TVector<TCell>& keys, TVector<TCell>& values, TString& strError, ui64& numBytes);
};

class TArrowCSV {
public:
    static constexpr ui32 DEFAULT_BLOCK_SIZE = 1024 * 1024;

    /// If header is true read column names from first line after skipRows. Parse columns as strings in this case.
    /// @note It's possible to skip header with skipRows and use typed columns instead.
    TArrowCSV(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, bool header = false, const std::set<std::string>& notNullColumns = {});

    std::shared_ptr<arrow::RecordBatch> ReadNext(const TString& csv, TString& errString);
    std::shared_ptr<arrow::RecordBatch> ReadSingleBatch(const TString& csv, TString& errString);

    void Reset() {
        Reader = {};
    }

    void SetSkipRows(ui32 skipRows) {
        ReadOptions.skip_rows = skipRows;
    }

    void SetBlockSize(ui32 blockSize = DEFAULT_BLOCK_SIZE) {
        ReadOptions.block_size = blockSize;
    }

    void SetDelimiter(std::optional<char> delimiter) {
        if (delimiter) {
            ParseOptions.delimiter = *delimiter;
        }
    }

    void SetQuoting(bool quoting = true, char quoteChar = '"', bool doubleQuote = true) {
        ParseOptions.quoting = quoting;
        ParseOptions.quote_char = quoteChar;
        ParseOptions.double_quote = doubleQuote;
    }

    void SetEscaping(bool escaping = false, char escapeChar = '\\') {
        ParseOptions.escaping = escaping;
        ParseOptions.escape_char = escapeChar;
    }

    void SetNullValue(const TString& null = "");

private:
    arrow::csv::ReadOptions ReadOptions;
    arrow::csv::ParseOptions ParseOptions;
    arrow::csv::ConvertOptions ConvertOptions;
    std::shared_ptr<arrow::csv::StreamingReader> Reader;
    std::vector<TString> ResultColumns;
    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> OriginalColumnTypes;
    std::set<std::string> NotNullColumns;

    std::shared_ptr<arrow::RecordBatch> ConvertColumnTypes(std::shared_ptr<arrow::RecordBatch> parsedBatch) const;

    static TString ErrorPrefix() {
        return "Cannot read CSV: ";
    }
};

}
