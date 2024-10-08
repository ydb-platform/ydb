#include "json_parser.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <contrib/libs/simdjson/include/simdjson.h>

namespace {

using TCallback = NFq::TJsonParser::TCallback;
TString LogPrefix = "JsonParser: ";
constexpr ui64 MAX_NUMBER_BUFFERS = 10;

} // anonymous namespace

namespace NFq {

//// TParserBuffer

TJsonParserBuffer::TJsonParserBuffer()
    : Offset(0)
    , NumberValues(0)
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
    Offset = 0;
    NumberValues = 0;
    Finished = false;
    Values.clear();
}

//// TJsonParser

class TJsonParser::TImpl {
public:
    TImpl(const TVector<TString>& columns, const TVector<TString>& types, TCallback callback)
        : Callback(callback)
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

    void Parse() {
        Y_ENSURE(UsedBuffers, "Nothing to parse");

        TJsonParserBuffer::TPtr buffer = UsedBuffers.begin();
        const auto [values, size] = buffer->Finish();
        LOG_ROW_DISPATCHER_TRACE("Parse values for offset " << buffer->GetOffset() << ":\n" << values);

        const ui64 numberValues = buffer->GetNumberValues();
        TVector<TVector<std::string_view>> parsedValues(Columns.size());
        for (auto& parsedColumn : parsedValues) {
            parsedColumn.reserve(numberValues);
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

                auto& parsedColumn = parsedValues[it->second];
                if (item.value().is_string()) {
                    parsedColumn.emplace_back(CreateHolderIfNeeded(
                        values, size, buffer, item.value().get_string().value()
                    ));
                } else {
                    parsedColumn.emplace_back(CreateHolderIfNeeded(
                        values, size, buffer, item.value().raw_json_token().value()
                    ));
                }
            }
        }

        Callback(std::move(parsedValues), buffer);
    }

    TJsonParserBuffer& GetBuffer(ui64 offset) {
        if (FreeBuffers) {
            UsedBuffers.emplace_front(std::move(FreeBuffers.front()));
            FreeBuffers.erase(FreeBuffers.begin());
        } else {
            UsedBuffers.emplace_front();
        }

        return UsedBuffers.front().SetOffset(offset);
    }

    void ReleaseBuffer(TJsonParserBuffer::TPtr buffer) {
        buffer->Clear();
        if (FreeBuffers.size() + UsedBuffers.size() <= MAX_NUMBER_BUFFERS) {
            FreeBuffers.emplace_back(std::move(*buffer));
        }
        UsedBuffers.erase(buffer);
    }

    TString GetDescription() const {
        TStringBuilder description = TStringBuilder() << "Columns: ";
        for (const auto& column : Columns) {
            description << "'" << column << "' ";
        }
        return description;
    }

private:
    std::string_view CreateHolderIfNeeded(const char* dataHolder, size_t size, TJsonParserBuffer::TPtr buffer, std::string_view value) {
        ptrdiff_t diff = value.data() - dataHolder;
        if (0 <= diff && static_cast<size_t>(diff) < size) {
            return value;
        }
        return buffer->AddHolder(value);
    }

private:
    const TCallback Callback;
    TVector<std::string> Columns;
    absl::flat_hash_map<std::string_view, size_t> ColumnsIndex;

    TList<TJsonParserBuffer> UsedBuffers;
    TList<TJsonParserBuffer> FreeBuffers;
};

TJsonParser::TJsonParser(const TVector<TString>& columns, const TVector<TString>& types, TCallback callback)
    : Impl(std::make_unique<TJsonParser::TImpl>(columns, types, callback))
{}

TJsonParser::~TJsonParser() {
}

TJsonParserBuffer& TJsonParser::GetBuffer(ui64 offset) {
    return Impl->GetBuffer(offset);
}

void TJsonParser::ReleaseBuffer(TJsonParserBuffer::TPtr buffer) {
    Impl->ReleaseBuffer(buffer);
}

void TJsonParser::Parse() {
    Impl->Parse();
}

TString TJsonParser::GetDescription() const {
    return Impl->GetDescription();
}

std::unique_ptr<TJsonParser> NewJsonParser(const TVector<TString>& columns, const TVector<TString>& types, TCallback callback) {
    return std::unique_ptr<TJsonParser>(new TJsonParser(columns, types, callback));
}

} // namespace NFq
