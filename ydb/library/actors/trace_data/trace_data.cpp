#include "trace_data.h"

#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>

#include <cstring>
#include <limits>
#include <utility>

namespace NActors::NTracing {

    namespace {

        constexpr size_t MinDictionaryEntrySize = 2 * sizeof(ui32);

        static_assert(sizeof(size_t) <= sizeof(ui64));

        ui32 ToUi32(size_t value, TStringBuf description) {
            Y_ENSURE(value <= std::numeric_limits<ui32>::max(), description << " is too large");
            return static_cast<ui32>(value);
        }

        size_t CheckedAdd(size_t lhs, size_t rhs) {
            Y_ENSURE(rhs <= std::numeric_limits<size_t>::max() - lhs, "Trace data is too large");
            return lhs + rhs;
        }

        size_t CheckedMultiply(size_t lhs, size_t rhs) {
            Y_ENSURE(!lhs || rhs <= std::numeric_limits<size_t>::max() / lhs, "Trace data is too large");
            return lhs * rhs;
        }

        template <typename T>
        void WriteValue(TBuffer& buffer, T value) {
            buffer.Append(reinterpret_cast<const char*>(&value), sizeof(value));
        }

        void WriteString(TBuffer& buffer, TStringBuf value) {
            WriteValue(buffer, ToUi32(value.size(), "Trace dictionary string"));
            if (!value.empty()) {
                buffer.Append(value.data(), value.size());
            }
        }

        template <typename TDict>
        void WriteDictionary(TBuffer& buffer, const TDict& dictionary) {
            WriteValue(buffer, ToUi32(dictionary.size(), "Trace dictionary"));
            for (const auto& [index, name] : dictionary) {
                WriteValue(buffer, index);
                WriteString(buffer, name);
            }
        }

        TBuffer SerializeMetadata(const TTraceChunk& chunk) {
            TBuffer metadata;
            WriteDictionary(metadata, chunk.ActivityDict);
            WriteDictionary(metadata, chunk.EventNamesDict);
            WriteDictionary(metadata, chunk.ThreadPoolDict);
            return metadata;
        }

        bool IsValidEventType(ui8 type) {
            return type <= static_cast<ui8>(ETraceEventType::ForwardLocal);
        }

        class TReader {
        public:
            explicit TReader(const TBuffer& data)
                : Data(data.Data())
                , Size(data.Size())
            {}

            explicit TReader(TStringBuf data)
                : Data(data.data())
                , Size(data.size())
            {}

            template <typename T>
            bool ReadValue(T& value) {
                if (sizeof(value) > Remaining()) {
                    return false;
                }
                std::memcpy(&value, Data + Position, sizeof(value));
                Position += sizeof(value);
                return true;
            }

            bool ReadString(TString& value) {
                ui32 size = 0;
                TStringBuf encoded;
                if (!ReadValue(size) || !ReadBytes(size, encoded)) {
                    return false;
                }
                value.assign(encoded.data(), encoded.size());
                return true;
            }

            bool ReadBytes(size_t size, TStringBuf& value) {
                if (size > Remaining()) {
                    return false;
                }
                value = TStringBuf(Data + Position, size);
                Position += size;
                return true;
            }

            size_t Remaining() const noexcept {
                return Size - Position;
            }

            bool Empty() const noexcept {
                return Position == Size;
            }

        private:
            const char* Data;
            size_t Size;
            size_t Position = 0;
        };

        template <typename TConsumer>
        bool ReadDictionary(TReader& reader, TConsumer&& consume) {
            ui32 count = 0;
            if (!reader.ReadValue(count) || count > reader.Remaining() / MinDictionaryEntrySize) {
                return false;
            }

            for (ui32 i = 0; i < count; ++i) {
                ui32 index = 0;
                TString name;
                if (!reader.ReadValue(index) || !reader.ReadString(name)) {
                    return false;
                }
                consume(index, std::move(name));
            }
            return true;
        }

        bool DeserializeMetadata(TStringBuf metadata, TTraceChunk& chunk) {
            TReader reader(metadata);

            if (!ReadDictionary(reader, [&chunk](ui32 index, TString name) {
                chunk.ActivityDict.emplace_back(index, std::move(name));
            })) {
                return false;
            }

            if (!ReadDictionary(reader, [&chunk](ui32 typeId, TString name) {
                chunk.EventNamesDict[typeId] = std::move(name);
            })) {
                return false;
            }

            if (!ReadDictionary(reader, [&chunk](ui32 threadIdx, TString name) {
                chunk.ThreadPoolDict.emplace_back(threadIdx, std::move(name));
            })) {
                return false;
            }

            return reader.Empty();
        }

    } // anonymous namespace

    TBuffer SerializeTrace(const TTraceChunk& chunk, ui32 nodeId) {
        for (const auto& event : chunk.Events) {
            Y_ENSURE(IsValidEventType(event.Type),
                "Invalid trace event type " << static_cast<ui32>(event.Type));
        }

        TBuffer metadata = SerializeMetadata(chunk);
        const size_t eventsSize = CheckedMultiply(chunk.Events.size(), sizeof(TTraceEvent));
        const size_t resultSize = CheckedAdd(CheckedAdd(sizeof(TTraceFileHeader), metadata.Size()), eventsSize);

        TBuffer result(resultSize);
        WriteValue(result, TraceFileMagic);
        WriteValue(result, TraceFileVersion);
        WriteValue(result, nodeId);
        WriteValue(result, ToUi32(metadata.Size(), "Trace metadata"));
        WriteValue(result, static_cast<ui64>(chunk.Events.size()));
        WriteValue(result, chunk.StartTimestampUs);

        if (metadata) {
            result.Append(metadata.Data(), metadata.Size());
        }
        if (eventsSize) {
            result.Append(reinterpret_cast<const char*>(chunk.Events.data()), eventsSize);
        }

        Y_ENSURE(result.Size() == resultSize, "Unexpected serialized trace size");
        return result;
    }

    bool DeserializeTrace(const TBuffer& data, TTraceChunk& chunk, ui32& nodeId) {
        TReader reader(data);

        TTraceFileHeader header;
        if (!reader.ReadValue(header.Magic)
            || !reader.ReadValue(header.Version)
            || !reader.ReadValue(header.NodeId)
            || !reader.ReadValue(header.MetadataSize)
            || !reader.ReadValue(header.EventCount)
            || !reader.ReadValue(header.StartTimestampUs))
        {
            return false;
        }

        if (header.Magic != TraceFileMagic || header.Version != TraceFileVersion) {
            return false;
        }

        TStringBuf metadata;
        if (!reader.ReadBytes(header.MetadataSize, metadata)) {
            return false;
        }

        TTraceChunk decoded;
        decoded.StartTimestampUs = header.StartTimestampUs;
        if (!DeserializeMetadata(metadata, decoded)) {
            return false;
        }

        if (reader.Remaining() % sizeof(TTraceEvent) != 0
            || header.EventCount != static_cast<ui64>(reader.Remaining() / sizeof(TTraceEvent)))
        {
            return false;
        }

        const size_t eventCount = reader.Remaining() / sizeof(TTraceEvent);
        if (eventCount > decoded.Events.max_size()) {
            return false;
        }
        decoded.Events.resize(eventCount);
        TStringBuf encodedEvents;
        if (!reader.ReadBytes(eventCount * sizeof(TTraceEvent), encodedEvents)) {
            return false;
        }
        if (eventCount) {
            std::memcpy(decoded.Events.data(), encodedEvents.data(), encodedEvents.size());
        }
        for (const auto& event : decoded.Events) {
            if (!IsValidEventType(event.Type)) {
                return false;
            }
        }

        if (!reader.Empty()) {
            return false;
        }

        chunk = std::move(decoded);
        nodeId = header.NodeId;
        return true;
    }

} // namespace NActors::NTracing
