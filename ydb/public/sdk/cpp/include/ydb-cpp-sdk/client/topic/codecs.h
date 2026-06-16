#pragma once

#include <util/stream/mem.h>
#include <util/stream/str.h>
#include <util/stream/output.h>
#include <util/system/spinlock.h>
#include <util/generic/yexception.h>

#include <util/generic/buffer.h>
#include <util/datetime/base.h>
#include <util/system/types.h>

#include <optional>
#include <string_view>
#include <vector>

#include <unordered_map>
#include <memory>

namespace NYdb::inline Dev::NTopic {

enum class ECodec : uint32_t {
    RAW = 1,
    GZIP = 2,
    LZOP = 3,
    ZSTD = 4,
    KAFKA_BATCH = 5,
    CUSTOM = 10000,
};

inline const std::string& GetCodecId(const ECodec codec) {
    static std::unordered_map<ECodec, std::string> idByCodec{
        {ECodec::RAW, std::string(1, '\0')},
        {ECodec::GZIP, "\1"},
        {ECodec::LZOP, "\2"},
        {ECodec::ZSTD, "\3"}
    };
    Y_ABORT_UNLESS(idByCodec.contains(codec));
    return idByCodec[codec];
}

struct TWriteBlockCompression {
    ECodec Codec = ECodec::RAW;
    std::vector<std::string_view>& Payloads;
    const std::vector<TInstant>& CreatedAt;
    TBuffer& Data;
    ui32& CodecID;
    bool& Compressed;
    i64 BaseSequence = 0;
    std::optional<ECodec> BatchInnerCodec;
    int CompressionLevel = 0;
};

//! Per-record metadata extracted from codec payload (e.g. Kafka record batch).
//! When not set, read path uses metadata from outer message_data.
struct TDecompressedMessageMeta {
    std::optional<i32> OffsetDelta;
    std::optional<i64> SequenceDelta;
    std::optional<i64> TimestampDelta;
};

struct TDecompressedMessage {
    std::string Data;
    std::optional<TDecompressedMessageMeta> Meta;
};

struct TDecompressionResult {
    std::vector<TDecompressedMessage> Messages;
    //! Populated when codec expands one blob into many messages (Kafka batch).
    std::optional<i64> BatchBaseOffset;
    std::optional<i64> BatchBaseSequence;
    std::optional<i64> BatchBaseTimestampMs;
};

class ICodec {
public:
    virtual ~ICodec() = default;
    virtual std::string Decompress(const std::string& data) const = 0;
    virtual TDecompressionResult DecompressData(const std::string& data) const;
    virtual std::unique_ptr<IOutputStream> CreateCoder(TBuffer& result, int quality) const = 0;
    virtual void CompressWriteBlock(TWriteBlockCompression& ctx) const;
};

class TGzipCodec final : public ICodec {
    std::string Decompress(const std::string& data) const override;

    std::unique_ptr<IOutputStream> CreateCoder(TBuffer& result, int quality) const override;
};

class TZstdCodec final : public ICodec {
    std::string Decompress(const std::string& data) const override;

    std::unique_ptr<IOutputStream> CreateCoder(TBuffer& result, int quality) const override;
};

class TUnsupportedCodec final : public ICodec {
    std::string Decompress(const std::string&) const override;

    std::unique_ptr<IOutputStream> CreateCoder(TBuffer&, int) const override;
};

class TKafkaBatchCodec final : public ICodec {
public:
    std::string Decompress(const std::string& data) const override;

    TDecompressionResult DecompressData(const std::string& data) const override;

    std::unique_ptr<IOutputStream> CreateCoder(TBuffer&, int) const override;

    void CompressWriteBlock(TWriteBlockCompression& ctx) const override;
};

class TCodecMap {
public:
    static TCodecMap& GetTheCodecMap() {
        static TCodecMap instance;
        return instance;
    }

    void Set(uint32_t codecId, std::unique_ptr<ICodec>&& codecImpl) {
        with_lock(Lock) {
            Codecs[codecId] = std::move(codecImpl);
        }
    }

    const ICodec* GetOrThrow(uint32_t codecId) const {
        with_lock(Lock) {
            if (!Codecs.contains(codecId)) {
                throw yexception() << "codec with id " << uint32_t(codecId) << " not provided";
            }
            return Codecs.at(codecId).get();
        }
    }


    TCodecMap(const TCodecMap&) = delete;
    TCodecMap(TCodecMap&&) = delete;
    TCodecMap& operator=(const TCodecMap&) = delete;
    TCodecMap& operator=(TCodecMap&&) = delete;

private:
    TCodecMap() = default;

private:
    std::unordered_map<uint32_t, std::unique_ptr<ICodec>> Codecs;
    TAdaptiveLock Lock;
};

inline std::string TakeFirstDecompressedMessage(TDecompressionResult&& result) {
    if (result.Messages.empty()) {
        throw yexception() << "empty decompression result";
    }
    return std::move(result.Messages.front().Data);
}

} // namespace NYdb::NTopic
