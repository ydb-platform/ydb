#include <library/cpp/string_utils/csv/csv.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

constexpr size_t MaxRecords = 8;
constexpr size_t MaxFields = 8;
constexpr size_t MaxFieldSize = 64;

TString EncodeField(TStringBuf field, char delimiter) {
    TString encoded;
    bool quote = field.empty();
    for (char c : field) {
        quote = quote || c == delimiter || c == '"' || c == '\r' || c == '\n';
    }

    if (!quote) {
        return TString(field);
    }

    encoded.reserve(field.size() + 2);
    encoded.push_back('"');
    for (char c : field) {
        encoded.push_back(c);
        if (c == '"') {
            encoded.push_back('"');
        }
    }
    encoded.push_back('"');
    return encoded;
}

TString EncodeRecord(const TVector<TString>& fields, char delimiter) {
    TString record;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i) {
            record.push_back(delimiter);
        }
        record += EncodeField(fields[i], delimiter);
    }
    return record;
}

TVector<TString> ParseRecord(const TString& record, char delimiter, char quote = '"') {
    TVector<TString> fields;
    NCsvFormat::CsvSplitter splitter(record, delimiter, quote);
    do {
        const TStringBuf field = splitter.Consume();
        fields.emplace_back(field.Data(), field.Size());
    } while (splitter.Step());
    return fields;
}

TVector<TString> SplitPlainModel(const TString& input, char delimiter) {
    TVector<TString> fields;
    size_t begin = 0;
    for (size_t pos = 0; pos <= input.size(); ++pos) {
        if (pos == input.size() || input[pos] == delimiter) {
            fields.emplace_back(input.data() + begin, pos - begin);
            begin = pos + 1;
        }
    }
    return fields;
}

TString NormalizeReadLineCrLf(TStringBuf input) {
    TString normalized;
    for (size_t pos = 0; pos < input.size(); ++pos) {
        if (input[pos] == '\r' && pos + 1 < input.size() && input[pos + 1] == '\n') {
            continue;
        }
        normalized.push_back(input[pos]);
    }
    return normalized;
}

TVector<TString> NormalizeReadLineCrLf(const TVector<TString>& fields) {
    TVector<TString> normalized;
    normalized.reserve(fields.size());
    for (const auto& field : fields) {
        normalized.push_back(NormalizeReadLineCrLf(field));
    }
    return normalized;
}

TString LogicalLineModel(TStringBuf record, char quote = '"') {
    TString logicalLine;
    bool escape = false;
    size_t lineBegin = 0;
    while (lineBegin <= record.size()) {
        size_t lineEnd = lineBegin;
        while (lineEnd < record.size() && record[lineEnd] != '\n') {
            ++lineEnd;
        }

        TStringBuf physicalLine(record.data() + lineBegin, lineEnd - lineBegin);
        if (!physicalLine.empty() && physicalLine.back() == '\r') {
            physicalLine.Chop(1);
        }

        const size_t quoteCount = std::count(physicalLine.cbegin(), physicalLine.cend(), quote);
        if (quoteCount & 1) {
            escape = !escape;
        }

        if (!logicalLine) {
            logicalLine = physicalLine;
        } else {
            logicalLine += physicalLine;
        }

        if (lineEnd == record.size()) {
            break;
        }

        Y_ABORT_UNLESS(escape);
        logicalLine += "\n";
        lineBegin = lineEnd + 1;
    }

    Y_ABORT_UNLESS(!escape);
    return logicalLine;
}

bool IsCsvFormatViolation(const yexception& error) {
    return TStringBuf(error.what()).find("RFC4180 violation:") != TStringBuf::npos;
}

void CheckGeneratedCsv(FuzzedDataProvider& provider) {
    static constexpr char Delimiters[] = {',', ';', '\t', '|'};
    const char delimiter = Delimiters[provider.ConsumeIntegralInRange<size_t>(0, sizeof(Delimiters) - 1)];

    const size_t recordCount = provider.ConsumeIntegralInRange<size_t>(1, MaxRecords);
    TVector<TVector<TString>> expected;
    TVector<TString> encodedRecords;
    expected.reserve(recordCount);
    encodedRecords.reserve(recordCount);

    for (size_t recordIndex = 0; recordIndex < recordCount; ++recordIndex) {
        const size_t fieldCount = provider.ConsumeIntegralInRange<size_t>(1, MaxFields);
        TVector<TString> fields;
        fields.reserve(fieldCount);
        for (size_t fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
            fields.push_back(provider.ConsumeRandomLengthString(MaxFieldSize));
        }
        encodedRecords.push_back(EncodeRecord(fields, delimiter));
        expected.push_back(std::move(fields));
    }

    TString csv;
    for (size_t i = 0; i < encodedRecords.size(); ++i) {
        if (i) {
            csv.push_back('\n');
        }
        csv += encodedRecords[i];
    }

    TStringInput input(csv);
    NCsvFormat::TLinesSplitter lines(input);
    for (size_t recordIndex = 0; recordIndex < expected.size(); ++recordIndex) {
        Y_ABORT_UNLESS(ParseRecord(encodedRecords[recordIndex], delimiter) == expected[recordIndex]);

        const TString logicalLine = lines.ConsumeLine();
        Y_ABORT_UNLESS(logicalLine == LogicalLineModel(encodedRecords[recordIndex]));
        Y_ABORT_UNLESS(ParseRecord(logicalLine, delimiter) == NormalizeReadLineCrLf(expected[recordIndex]));
    }
    Y_ABORT_UNLESS(lines.ConsumeLine().empty());
}

void CheckPlainSplit(FuzzedDataProvider& provider) {
    static constexpr char Delimiters[] = {',', ';', '\t', '|', '\0'};
    const char delimiter = Delimiters[provider.ConsumeIntegralInRange<size_t>(0, sizeof(Delimiters) - 1)];
    const TString input = provider.ConsumeRandomLengthString(512);

    Y_ABORT_UNLESS(ParseRecord(input, delimiter, '\0') == SplitPlainModel(input, delimiter));
}

void ExerciseArbitraryInput(FuzzedDataProvider& provider) {
    static constexpr char Delimiters[] = {',', ';', '\t', '|'};
    const char delimiter = Delimiters[provider.ConsumeIntegralInRange<size_t>(0, sizeof(Delimiters) - 1)];
    const TString input = provider.ConsumeRemainingBytesAsString();

    try {
        NCsvFormat::CsvSplitter splitter(input, delimiter);
        do {
            (void)splitter.Consume();
        } while (splitter.Step());
    } catch (const yexception& error) {
        if (!IsCsvFormatViolation(error)) {
            throw;
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    CheckGeneratedCsv(provider);
    CheckPlainSplit(provider);
    ExerciseArbitraryInput(provider);

    return 0;
}
