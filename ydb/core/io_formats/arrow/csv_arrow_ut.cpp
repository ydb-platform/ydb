#include "csv_arrow.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NFormats {

namespace {

TString MakeHeader(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, char delimiter) {
    TString header;
    for (auto& [name, _] : columns) {
        header += name + delimiter;
    }
    if (header.size()) {
        header.resize(header.size() - 1);
    }
    return header;
}

TString TestIntsData(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, ui32 numRows,
                     char delimiter, TString endLine = "\n", bool addEmptyLine = false) {
    TString data;
    for (ui32 row = 0; row < numRows; ++row) {
        if (data.size()) {
            data.resize(data.size() - 1);
            data += endLine;
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            data += ToString(row) + delimiter;
        }
    }
    data.resize(data.size() - 1);
    if (addEmptyLine) {
        data += endLine;
    }
    return data;
}

std::shared_ptr<arrow::RecordBatch>
TestReadSingleBatch(TArrowCSV& reader,
                    const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, const TString& data, ui32 numRows) {
    TString errorMessage;
    auto batch = reader.ReadSingleBatch(data, errorMessage);
    if (!errorMessage.empty()) {
        Cerr << errorMessage << "\n";
    }
    UNIT_ASSERT(batch);
    UNIT_ASSERT(errorMessage.empty());
    UNIT_ASSERT(batch->ValidateFull().ok());
    UNIT_ASSERT_EQUAL(batch->num_rows(), numRows);
    UNIT_ASSERT_EQUAL((size_t)batch->num_columns(), columns.size());

    for (size_t i = 0; i < columns.size(); ++i) {
        UNIT_ASSERT_EQUAL(columns[i].first, batch->schema()->field(i)->name());
        auto arrowType = NArrow::GetArrowType(columns[i].second);
        UNIT_ASSERT_C(arrowType.ok(), arrowType.status().ToString());
        UNIT_ASSERT(arrowType.ValueUnsafe()->Equals(batch->schema()->field(i)->type()));
        // TODO: check data
    }
    return batch;
}

std::shared_ptr<arrow::RecordBatch>
TestReadSingleBatch(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, const TString& data,
                    char delimiter, bool header, ui32 numRows, ui32 skipRows = 0, std::optional<char> escape = {}) {
    auto reader = TArrowCSV::Create(columns, header);
    UNIT_ASSERT_C(reader.ok(), reader.status().ToString());
    reader->SetDelimiter(delimiter);
    if (skipRows) {
        reader->SetSkipRows(skipRows);
    }
    if (escape) {
        reader->SetEscaping(true, *escape);
    }

    return TestReadSingleBatch(*reader, columns, data, numRows);
}

}

Y_UNIT_TEST_SUITE(FormatCSV) {
    Y_UNIT_TEST(Instants) {
        const TString dateTimeString = "2005-08-09T18:31:42";
        const TString data = "11,12,2013-07-15," + dateTimeString + "," + dateTimeString + ",,,";
        TVector<std::pair<TString, NScheme::TTypeInfo>> columns;

        {
            columns = {
                {"datetime_int", NScheme::TTypeInfo(NScheme::NTypeIds::Datetime)},
                {"timestamp_int", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)},
                {"date", NScheme::TTypeInfo(NScheme::NTypeIds::Date) },
                {"datetime", NScheme::TTypeInfo(NScheme::NTypeIds::Datetime)},
                {"timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)},
                {"date_null", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)},
                {"datetime_null", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)},
                {"timestamp_null", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)},
            };
            TInstant dtInstant;
            Y_ABORT_UNLESS(TInstant::TryParseIso8601(dateTimeString, dtInstant));
            auto reader = TArrowCSV::Create(columns, false);
            UNIT_ASSERT_C(reader.ok(), reader.status().ToString());

            TString errorMessage;
            auto batch = reader->ReadNext(data, errorMessage);
            Cerr << errorMessage << "\n";
            UNIT_ASSERT(!!batch);
            UNIT_ASSERT(errorMessage.empty());

            auto cDateNull = batch->GetColumnByName("date_null");
            auto cDatetimeNull = batch->GetColumnByName("datetime_null");
            auto cTimestampNull = batch->GetColumnByName("timestamp_null");

            auto cDatetimeInt = batch->GetColumnByName("datetime_int");
            auto cTimestampInt = batch->GetColumnByName("timestamp_int");
            auto cDate = batch->GetColumnByName("date");
            auto cDatetime = batch->GetColumnByName("datetime");
            auto cTimestamp = batch->GetColumnByName("timestamp");

            Y_ABORT_UNLESS(cDate->type()->id() == arrow::UInt16Type::type_id);
            Y_ABORT_UNLESS(cDatetime->type()->id() == arrow::UInt32Type::type_id);
            Y_ABORT_UNLESS(cTimestamp->type()->id() == arrow::TimestampType::type_id);
            Y_ABORT_UNLESS(cDatetimeInt->type()->id() == arrow::UInt32Type::type_id);
            Y_ABORT_UNLESS(cTimestampInt->type()->id() == arrow::TimestampType::type_id);
            Y_ABORT_UNLESS(batch->num_rows() == 1);

            {
                auto& ui16Column = static_cast<arrow::UInt32Array&>(*cDateNull);
                Y_ABORT_UNLESS(ui16Column.IsNull(0));
            }
            {
                auto& ui32Column = static_cast<arrow::UInt32Array&>(*cDatetimeNull);
                Y_ABORT_UNLESS(ui32Column.IsNull(0));
            }
            {
                auto& tsColumn = static_cast<arrow::TimestampArray&>(*cTimestampNull);
                Y_ABORT_UNLESS(tsColumn.IsNull(0));
            }
            {
                auto& ui32Column = static_cast<arrow::UInt32Array&>(*cDatetimeInt);
                Y_ABORT_UNLESS(ui32Column.Value(0) == 11, "%d", ui32Column.Value(0));
            }
            {
                auto& tsColumn = static_cast<arrow::TimestampArray&>(*cTimestampInt);
                Cerr << tsColumn.Value(0) << Endl;
                Y_ABORT_UNLESS(tsColumn.Value(0) == 12 * 1000000);
            }
            auto& ui16Column = static_cast<arrow::UInt16Array&>(*cDate);
            Y_ABORT_UNLESS(ui16Column.Value(0) == 15901, "%d", ui16Column.Value(0));
            auto& ui32Column = static_cast<arrow::UInt32Array&>(*cDatetime);
            Y_ABORT_UNLESS(ui32Column.Value(0) == dtInstant.Seconds(), "%d", ui32Column.Value(0));
            auto& tsColumn = static_cast<arrow::TimestampArray&>(*cTimestamp);
            Y_ABORT_UNLESS(tsColumn.Value(0) == (i64)dtInstant.MicroSeconds());
        }
    }

    Y_UNIT_TEST(EmptyData) {
        TString data = "";
        TVector<std::pair<TString, NScheme::TTypeInfo>> columns;

        {
            auto reader = TArrowCSV::Create(columns, false);
            UNIT_ASSERT_C(reader.ok(), reader.status().ToString());

            TString errorMessage;
            auto batch = reader->ReadNext(data, errorMessage);
            Cerr << errorMessage << "\n";
            UNIT_ASSERT(!batch);
            UNIT_ASSERT(!errorMessage.empty());
        }

        {
            columns = {
                {"u32", NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)},
                {"i64", NScheme::TTypeInfo(NScheme::NTypeIds::Int64)}
            };

            auto reader = TArrowCSV::Create(columns, false);
            UNIT_ASSERT_C(reader.ok(), reader.status().ToString());

            TString errorMessage;
            auto batch = reader->ReadNext(data, errorMessage);
            Cerr << errorMessage << "\n";
            UNIT_ASSERT(!batch);
            UNIT_ASSERT(!errorMessage.empty());
        }
    }

    Y_UNIT_TEST(Common) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> columns = {
            {"u8", NScheme::TTypeInfo(NScheme::NTypeIds::Uint8)},
            {"u16", NScheme::TTypeInfo(NScheme::NTypeIds::Uint16)},
            {"u32", NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)},
            {"u64", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)},
            {"i8", NScheme::TTypeInfo(NScheme::NTypeIds::Int8)},
            {"i16", NScheme::TTypeInfo(NScheme::NTypeIds::Int16)},
            {"i32", NScheme::TTypeInfo(NScheme::NTypeIds::Int32)},
            {"i64", NScheme::TTypeInfo(NScheme::NTypeIds::Int64)}
        };

        // half of columns
        auto uColumns = columns;
        uColumns.resize(columns.size() / 2);

        // another half of columns
        TVector<std::pair<TString, NScheme::TTypeInfo>> sColumns(
            columns.begin() + (columns.size() / 2), columns.end());

        std::vector<char> delimiters = {',', ';', '\t'};
        std::vector<TString> endlines = {"\n", "\r\n", "\r"};
        bool addEmptyLine = false;
        ui32 numRows = 10;

        for (auto& endLine : endlines) {
            for (auto delim : delimiters) {
                // no header
                addEmptyLine = !addEmptyLine;
                TString csv = TestIntsData(columns, numRows, delim, endLine, addEmptyLine);
                TestReadSingleBatch(columns, csv, delim, false, numRows);

                // header, all columns
                TString header = MakeHeader(columns, delim);
                TestReadSingleBatch(columns, header + endLine + csv, delim, true, numRows);

                // header, skip rows, all columns
                TestReadSingleBatch(columns, TString("line1") + endLine + "line2" + endLine + header + endLine + csv,
                                    delim, true, numRows, 2);

                // header, some columns
                TestReadSingleBatch(uColumns, header + endLine + csv, delim, true, numRows);
                TestReadSingleBatch(sColumns, header + endLine + csv, delim, true, numRows);

                // header, skip rows, some columns
                TestReadSingleBatch(uColumns, endLine + header + endLine + csv, delim, true, numRows, 1);
                TestReadSingleBatch(sColumns, endLine + header + endLine + csv, delim, true, numRows, 1);
            }
        }
    }

    Y_UNIT_TEST(Strings) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> columns = {
            {"string", NScheme::TTypeInfo(NScheme::NTypeIds::String)},
            {"utf8", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)}
        };

        // TODO: SetQuoting
        std::vector<TString> quotes = {"\"", "\'", ""};

        char delimiter = ',';
        TString endLine = "\n";

        for (auto& q : quotes) {
            TString csv;
            csv += q + "aaa" + q + delimiter + q + "bbbbb" + q + endLine;
            csv += q + "123" + q + delimiter + q + "456" + q + endLine;
            csv += q + "+-/*=" + q + delimiter + q + "~!@#$%^&*()?" + q + endLine;

            TestReadSingleBatch(columns, csv, delimiter, false, 3);
        }

        for (auto& q : quotes) {
            TString csv;
            csv += q + "d\\'Artagnan" + q + delimiter + q + "Jeanne d'Arc" + q + endLine;
            csv += q + "\\\'\\\"\\\'" + q + delimiter + q + "\\\"\\\'\\\"" + q + endLine;

            auto batch = TestReadSingleBatch(columns, csv, delimiter, false, 2, 0, '\\');
            for (auto& col : batch->columns()) {
                auto& typedColumn = static_cast<arrow::BinaryArray&>(*col);
                for (int i = 0; i < typedColumn.length(); ++i) {
                    auto view = typedColumn.GetView(i);
                    std::string_view value(view.data(), view.size());
                    Cerr << value << "\n";
                }
            }
        }
    }

    Y_UNIT_TEST(Nulls) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> columns = {
            {"u32", NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)},
            {"string", NScheme::TTypeInfo(NScheme::NTypeIds::String)},
            {"utf8", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)}
        };

        std::vector<TString> nulls = {"", "", "\\N", "NULL"};
        bool defaultNull = true;

        char delimiter = ',';
        TString endLine = "\n";
        TString q = "\"";

        std::string nullChar = "ᴺᵁᴸᴸ";

        for (auto& null : nulls) {
            TString csv;
            csv += TString() + null + delimiter + q + q + delimiter + q + q + endLine;
            csv += TString() + null + delimiter + q + null + q + delimiter + q + null + q + endLine;
            csv += TString() + null + delimiter + null + delimiter + null + endLine;

            auto reader = TArrowCSV::Create(columns, false);
            UNIT_ASSERT_C(reader.ok(), reader.status().ToString());
            if (!nulls.empty() || !defaultNull) {
                reader->SetNullValue(null);
            } else {
                defaultNull = false;
            }

            auto batch = TestReadSingleBatch(*reader, columns, csv, 3);

            Cerr << "src:\n" << csv;

            auto& ui32Column = static_cast<arrow::UInt32Array&>(*batch->columns()[0]);
            auto& strColumn = static_cast<arrow::BinaryArray&>(*batch->columns()[1]);
            auto& utf8Column = static_cast<arrow::StringArray&>(*batch->columns()[2]);

            Cerr << "parsed:\n";

            for (int i = 0; i < batch->num_rows(); ++i) {
                if (ui32Column.IsNull(i)) {
                    Cerr << nullChar << delimiter;
                } else {
                    Cerr << ui32Column.Value(i) << delimiter;
                }

                if (strColumn.IsNull(i)) {
                    Cerr << nullChar << delimiter;
                } else {
                    auto view = strColumn.GetView(i);
                    std::string_view value(view.data(), view.size());
                    Cerr << value << delimiter;
                }

                if (utf8Column.IsNull(i)) {
                    Cerr << nullChar << "\n";
                } else {
                    auto view = utf8Column.GetView(i);
                    std::string_view value(view.data(), view.size());
                    Cerr << value << "\n";
                }

                UNIT_ASSERT(ui32Column.IsNull(i));
                UNIT_ASSERT(i == 2 || !strColumn.IsNull(i));
                UNIT_ASSERT(i == 2 || !utf8Column.IsNull(i));
                UNIT_ASSERT(i != 2 || strColumn.IsNull(i));
                UNIT_ASSERT(i != 2 || utf8Column.IsNull(i));
            }
        }
    }
#if 0
    Y_UNIT_TEST(Dates) {
        // TODO
    }
#endif
}

}
