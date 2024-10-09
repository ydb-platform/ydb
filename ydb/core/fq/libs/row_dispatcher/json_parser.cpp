#include "json_parser.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <contrib/libs/simdjson/include/simdjson.h>

namespace {

TString LogPrefix = "JsonParser: ";

} // anonymous namespace

namespace NFq {

//// TParserBuffer

TJsonParserBuffer::TJsonParserBuffer()
    : NumberValues(0)
    , Finished(false)
{}

void TJsonParserBuffer::Reserve(size_t size) {
    Y_ENSURE(!Finished, "Cannot reserve finished buffer");
    Values.reserve(2 * (size + simdjson::SIMDJSON_PADDING));
}

void TJsonParserBuffer::AddValue(const TString& value) {
    Y_ENSURE(!Finished, "Cannot add value into finished buffer");
    NumberValues++;
    Values << value;
}

std::string_view TJsonParserBuffer::AddHolder(std::string_view value) {
    Y_ENSURE(Values.size() + value.size() <= Values.capacity(), "Requested too large holders");
    const size_t startPos = Values.size();
    Values << value;
    return std::string_view(Values).substr(startPos, value.length());
}

std::pair<const char*, size_t> TJsonParserBuffer::Finish() {
    Y_ENSURE(!Finished, "Cannot finish buffer twice");
    Finished = true;
    Values << TString(simdjson::SIMDJSON_PADDING, ' ');
    Values.reserve(2 * Values.size());
    return {Values.data(), Values.size()};
}

void TJsonParserBuffer::Clear() {
    Y_ENSURE(Finished, "Cannot clear not finished buffer");
    NumberValues = 0;
    Finished = false;
    Values.clear();
}

//// TJsonParser

class TJsonParser::TImpl {
public:
    TImpl(const TVector<TString>& columns, const TVector<TString>& types)
        : ParsedValues(columns.size())
    {
        Y_UNUSED(types);  // TODO: Will be used for UV creation

        Columns.reserve(columns.size());
        for (const auto& column : columns) {
            Columns.emplace_back(column);
        }

        ColumnsIndex.reserve(columns.size());
        for (size_t i = 0; i < Columns.size(); i++) {
            ColumnsIndex.emplace(std::string_view(Columns[i]), i);
        }
    }

    const TVector<TVector<std::string_view>>& Parse() {
        const auto [values, size] = Buffer.Finish();
        LOG_ROW_DISPATCHER_TRACE("Parse values:\n" << values);

        for (auto& parsedColumn : ParsedValues) {
            parsedColumn.clear();
            parsedColumn.reserve(Buffer.GetNumberValues());
        }

        simdjson::ondemand::parser parser;
        parser.threaded = false;

        simdjson::ondemand::document_stream documents = parser.iterate_many(values, size, simdjson::dom::DEFAULT_BATCH_SIZE);
        for (auto document : documents) {
            for (auto item : document.get_object()) {
                const auto it = ColumnsIndex.find(item.escaped_key().value());
                if (it == ColumnsIndex.end()) {
                    continue;
                }

                auto& parsedColumn = ParsedValues[it->second];
                if (item.value().is_string()) {
                    parsedColumn.emplace_back(CreateHolderIfNeeded(
                        values, size, item.value().get_string().value()
                    ));
                } else {
                    parsedColumn.emplace_back(CreateHolderIfNeeded(
                        values, size, item.value().raw_json_token().value()
                    ));
                }
            }
        }
        return ParsedValues;
    }

    TJsonParserBuffer& GetBuffer() {
        if (Buffer.GetFinished()) {
            Buffer.Clear();
        }
        return Buffer;
    }

    TString GetDescription() const {
        TStringBuilder description = TStringBuilder() << "Columns: ";
        for (const auto& column : Columns) {
            description << "'" << column << "' ";
        }
        description << "\nBuffer size: " << Buffer.GetNumberValues() << ", finished: " << Buffer.GetFinished();
        return description;
    }

    TString GetDebugString(const TVector<TVector<std::string_view>>& parsedValues) const {
        TStringBuilder result;
        for (size_t i = 0; i < Columns.size(); ++i) {
            result << "Parsed column '" << Columns[i] << "': ";
            for (const auto& value : parsedValues[i]) {
                result << "'" << value << "' ";
            }
            result << "\n";
        }
        return result;
    }

private:
    std::string_view CreateHolderIfNeeded(const char* dataHolder, size_t size, std::string_view value) {
        ptrdiff_t diff = value.data() - dataHolder;
        if (0 <= diff && static_cast<size_t>(diff) < size) {
            return value;
        }
        return Buffer.AddHolder(value);
    }

private:
    TVector<std::string> Columns;
    absl::flat_hash_map<std::string_view, size_t> ColumnsIndex;

    TJsonParserBuffer Buffer;
    TVector<TVector<std::string_view>> ParsedValues;
};

TJsonParser::TJsonParser(const TVector<TString>& columns, const TVector<TString>& types)
    : Impl(std::make_unique<TJsonParser::TImpl>(columns, types))
{}

TJsonParser::~TJsonParser() {
}

TJsonParserBuffer& TJsonParser::GetBuffer() {
    return Impl->GetBuffer();
}

const TVector<TVector<std::string_view>>& TJsonParser::Parse() {
    return Impl->Parse();
}

TString TJsonParser::GetDescription() const {
    return Impl->GetDescription();
}

TString TJsonParser::GetDebugString(const TVector<TVector<std::string_view>>& parsedValues) const {
    return Impl->GetDebugString(parsedValues);
}

std::unique_ptr<TJsonParser> NewJsonParser(const TVector<TString>& columns, const TVector<TString>& types) {
    return std::unique_ptr<TJsonParser>(new TJsonParser(columns, types));
}

} // namespace NFq
