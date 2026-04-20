#include "trace_data.h"

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>

#include <cstring>

namespace NActors::NTracing {

    namespace {

        void WriteU32(TBuffer& buf, ui32 val) {
            buf.Append(reinterpret_cast<const char*>(&val), sizeof(val));
        }

        void WriteString(TBuffer& buf, const TString& str) {
            WriteU32(buf, static_cast<ui32>(str.size()));
            buf.Append(str.data(), str.size());
        }

        bool ReadU32(const char*& ptr, const char* end, ui32& val) {
            if (ptr + sizeof(ui32) > end) return false;
            std::memcpy(&val, ptr, sizeof(ui32));
            ptr += sizeof(ui32);
            return true;
        }

        bool ReadString(const char*& ptr, const char* end, TString& str) {
            ui32 len = 0;
            if (!ReadU32(ptr, end, len)) return false;
            if (ptr + len > end) return false;
            str.assign(ptr, len);
            ptr += len;
            return true;
        }

        void SerializeDicts(TBuffer& buf,
                            const TActivityDict& activityDict,
                            const TEventNamesDict& eventNamesDict,
                            const TThreadPoolDict& threadPoolDict) {
            WriteU32(buf, static_cast<ui32>(activityDict.size()));
            for (const auto& [index, name] : activityDict) {
                WriteU32(buf, index);
                WriteString(buf, name);
            }

            WriteU32(buf, static_cast<ui32>(eventNamesDict.size()));
            for (const auto& [typeId, name] : eventNamesDict) {
                WriteU32(buf, typeId);
                WriteString(buf, name);
            }

            WriteU32(buf, static_cast<ui32>(threadPoolDict.size()));
            for (const auto& [threadIdx, name] : threadPoolDict) {
                WriteU32(buf, threadIdx);
                WriteString(buf, name);
            }
        }

        bool DeserializeDicts(const char*& ptr, const char* end,
                              TActivityDict& activityDict,
                              TEventNamesDict& eventNamesDict,
                              TThreadPoolDict& threadPoolDict) {
            ui32 activityCount = 0;
            if (!ReadU32(ptr, end, activityCount)) return false;
            activityDict.reserve(activityCount);
            for (ui32 i = 0; i < activityCount; ++i) {
                ui32 index = 0;
                TString name;
                if (!ReadU32(ptr, end, index)) return false;
                if (!ReadString(ptr, end, name)) return false;
                activityDict.emplace_back(index, std::move(name));
            }

            ui32 eventNamesCount = 0;
            if (!ReadU32(ptr, end, eventNamesCount)) return false;
            for (ui32 i = 0; i < eventNamesCount; ++i) {
                ui32 typeId = 0;
                TString name;
                if (!ReadU32(ptr, end, typeId)) return false;
                if (!ReadString(ptr, end, name)) return false;
                eventNamesDict[typeId] = std::move(name);
            }

            if (ptr < end) {
                ui32 threadPoolCount = 0;
                if (!ReadU32(ptr, end, threadPoolCount)) return false;
                threadPoolDict.reserve(threadPoolCount);
                for (ui32 i = 0; i < threadPoolCount; ++i) {
                    ui32 threadIdx = 0;
                    TString name;
                    if (!ReadU32(ptr, end, threadIdx)) return false;
                    if (!ReadString(ptr, end, name)) return false;
                    threadPoolDict.emplace_back(threadIdx, std::move(name));
                }
            }

            return true;
        }

    } // anonymous namespace

    TBuffer SerializeTrace(const TTraceChunk& chunk, ui32 nodeId) {
        TBuffer dictBuf;
        SerializeDicts(dictBuf, chunk.ActivityDict, chunk.EventNamesDict, chunk.ThreadPoolDict);

        TTraceFileHeader header;
        header.NodeId = nodeId;
        header.HeaderSize = static_cast<ui32>(dictBuf.Size());
        header.EventCount = chunk.Events.size();
        header.StartTimestampUs = chunk.StartTimestampUs;

        TBuffer result;
        result.Reserve(sizeof(header) + dictBuf.Size() + chunk.Events.size() * sizeof(TTraceEvent));

        result.Append(reinterpret_cast<const char*>(&header), sizeof(header));
        result.Append(dictBuf.Data(), dictBuf.Size());
        result.Append(
            reinterpret_cast<const char*>(chunk.Events.data()),
            chunk.Events.size() * sizeof(TTraceEvent)
        );

        return result;
    }

    bool DeserializeTrace(const TBuffer& data, TTraceChunk& chunk, ui32& nodeId) {
        const char* ptr = data.Data();
        const char* end = ptr + data.Size();

        if (static_cast<size_t>(end - ptr) < sizeof(TTraceFileHeader)) {
            return false;
        }

        TTraceFileHeader header;
        std::memcpy(&header, ptr, sizeof(header));
        ptr += sizeof(header);

        if (header.Magic != TraceFileMagic) {
            return false;
        }

        if (header.Version != TraceFileVersion) {
            return false;
        }

        nodeId = header.NodeId;
        chunk.StartTimestampUs = header.StartTimestampUs;

        const char* dictEnd = ptr + header.HeaderSize;
        if (dictEnd > end) {
            return false;
        }

        if (!DeserializeDicts(ptr, dictEnd, chunk.ActivityDict, chunk.EventNamesDict, chunk.ThreadPoolDict)) {
            return false;
        }
        ptr = dictEnd;

        size_t eventsBytes = header.EventCount * sizeof(TTraceEvent);
        if (ptr + eventsBytes > end) {
            return false;
        }

        chunk.Events.resize(header.EventCount);
        std::memcpy(chunk.Events.data(), ptr, eventsBytes);

        return true;
    }

} // namespace NActors::NTracing
