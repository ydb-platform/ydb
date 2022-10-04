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
    TArrowCSV(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns,
              ui32 skipRows = 0, bool header = false, ui32 blockSize = DEFAULT_BLOCK_SIZE);

    std::shared_ptr<arrow::RecordBatch> ReadNext(const TString& csv, TString& errString);

    void Reset() {
        Reader = {};
    }

    void SetDelimiter(char delimiter = ',') {
        ParseOptions.delimiter = delimiter;
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

    void SetNullValue(const TString& null) {
        if (!null.empty()) {
            ConvertOptions.null_values = { std::string(null.data(), null.size()) };
            ConvertOptions.strings_can_be_null = true;
            ConvertOptions.quoted_strings_can_be_null = true;
        } else {
            ConvertOptions.null_values.clear();
            ConvertOptions.strings_can_be_null = false;
            ConvertOptions.quoted_strings_can_be_null = true;
        }
    }

private:
    arrow::csv::ReadOptions ReadOptions;
    arrow::csv::ParseOptions ParseOptions;
    arrow::csv::ConvertOptions ConvertOptions;
    std::shared_ptr<arrow::csv::StreamingReader> Reader;
    std::vector<TString> ResultColumns;
};

}
