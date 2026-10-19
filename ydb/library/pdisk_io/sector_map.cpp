#include "sector_map.h"

#include <contrib/libs/snappy/snappy.h>

#include <ydb/library/pdisk_io/protos/sector_map.pb.h>
#include <util/string/split.h>

namespace NKikimr::NPDisk {

TSectorMap::TSectorMap(ui64 deviceSize, NSectorMap::EDiskMode diskMode, NSectorMap::ECompression compression)
  : DeviceSize(deviceSize)
  , IsLocked(false)
  , IoErrorEveryNthRequests(0)
  , ReadIoErrorEveryNthRequests(0)
  , AllocatedBytes(0)
  , DiskMode(diskMode)
  , Compression(compression)
{
    if (Compression == NSectorMap::ECompression::Lz4) {
        Y_ABORT_UNLESS(LZ4_initStream(&CompressionState, sizeof(CompressionState)));
    } else if (Compression == NSectorMap::ECompression::Snappy) {
        SnappyState = MakeHolder<snappy::CompressionContext>();
    } else {
        Y_ABORT_UNLESS(Compression == NSectorMap::ECompression::None);
    }
    FailureProbabilities.WriteErrorProbability = 0.0;
    FailureProbabilities.ReadErrorProbability = 0.0;
    FailureProbabilities.SilentWriteFailProbability = 0.0;
    FailureProbabilities.ReadReplayProbability = 0.0;
    EmulatedWriteErrors = 0;
    EmulatedReadErrors = 0;
    EmulatedSilentWriteFails = 0;
    EmulatedReadReplays = 0;
    InitSectorOperationThrottler();
}

TSectorMap::~TSectorMap() = default;

std::optional<NSectorMap::ECompression> NSectorMap::TryGetCompressionFromPath(const TString& path) {
    if (!path.StartsWith("SectorMap:")) {
        return ECompression::Lz4;
    }
    const auto parts = StringSplitter(path).Split(':').ToList<TString>();
    if (parts.size() < 5) {
        return ECompression::Lz4;
    }
    if (parts.size() != 5) {
        return std::nullopt;
    }
    if (parts[4] == "lz4") return ECompression::Lz4;
    if (parts[4] == "snappy") return ECompression::Snappy;
    if (parts[4] == "none") return ECompression::None;
    return std::nullopt;
}


const char* TSectorMap::GetCompressionName() const {
    switch (Compression) {
    case NSectorMap::ECompression::None: return "none";
    case NSectorMap::ECompression::Lz4: return "lz4";
    case NSectorMap::ECompression::Snappy: return "snappy";
    }
    Y_ABORT("Unknown SectorMap compression");
}

TString TSectorMap::CompressSector(const char* data) {
    if (Compression == NSectorMap::ECompression::None) {
        return TString(data, NSectorMap::SECTOR_SIZE);
    }
    char tmp[4 * NSectorMap::SECTOR_SIZE];
    size_t written = 0;
    if (Compression == NSectorMap::ECompression::Lz4) {
        // Reset the dictionary before every sector; only the scratch table is reused.
        LZ4_resetStream_fast(&CompressionState);
        const int result = LZ4_compress_fast_continue(&CompressionState, data, tmp,
            NSectorMap::SECTOR_SIZE, sizeof(tmp), 1);
        Y_VERIFY_S(result > 0, "compressed size# " << result);
        written = result;
    } else {
        Y_ABORT_UNLESS(snappy::MaxCompressedLength(NSectorMap::SECTOR_SIZE) <= sizeof(tmp));
        snappy::RawCompress(data, NSectorMap::SECTOR_SIZE, tmp, &written, snappy::CompressionOptions(), SnappyState.Get());
    }
    Y_ABORT_UNLESS(written > 0 && written <= sizeof(tmp));
    return TString(tmp, written);
}

void TSectorMap::DecompressSector(const TString& value, char* data) const {
    switch (Compression) {
    case NSectorMap::ECompression::None:
        Y_ABORT_UNLESS(value.size() == NSectorMap::SECTOR_SIZE);
        memcpy(data, value.data(), NSectorMap::SECTOR_SIZE);
        break;
    case NSectorMap::ECompression::Lz4: {
        const int size = LZ4_decompress_safe(value.data(), data, value.size(), NSectorMap::SECTOR_SIZE);
        Y_VERIFY_S(size == NSectorMap::SECTOR_SIZE, "processed# " << size);
        break;
    }
    case NSectorMap::ECompression::Snappy: {
        size_t size = 0;
        Y_ABORT_UNLESS(snappy::GetUncompressedLength(value.data(), value.size(), &size));
        Y_ABORT_UNLESS(size == NSectorMap::SECTOR_SIZE);
        Y_ABORT_UNLESS(snappy::RawUncompress(value.data(), value.size(), data));
        break;
    }
    }
}

void TSectorMap::LoadFromFile(const TString& path) {
    TString raw = TFileInput(path).ReadAll();
    NKikimrPDisk::TSectorMapSnapshot snap;
    bool success = snap.ParseFromString(raw);
    Y_VERIFY_S(success, path);

    TGuard<TTicketLock> guard(MapLock);
    Map.reserve(snap.SectorsSize());
    for (auto& s : snap.GetSectors()) {
        Y_VERIFY_S(s.GetCompressionType() == NKikimrPDisk::ECompression::LZ4, path);
        if (Compression == NSectorMap::ECompression::Lz4) {
            Map[s.GetOffset()] = s.GetData();
        } else {
            char data[NSectorMap::SECTOR_SIZE];
            const int size = LZ4_decompress_safe(s.GetData().data(), data, s.GetData().size(), sizeof(data));
            Y_VERIFY_S(size == NSectorMap::SECTOR_SIZE, path);
            Map[s.GetOffset()] = CompressSector(data);
        }
    }
}

void TSectorMap::StoreToFile(const TString& path) {
    TGuard<TTicketLock> guard(MapLock);
    NKikimrPDisk::TSectorMapSnapshot snap;
    for (auto& [offset, dataVar] : Map) {
        NKikimrPDisk::TSectorData *sd = snap.AddSectors();
        sd->SetOffset(offset);
        if (std::holds_alternative<TString>(dataVar)) {
            sd->SetData(std::get<TString>(dataVar));
        } else {
            sd->SetData(std::get<std::pair<TString, TString>>(dataVar).second);
        }
        // Snapshots remain LZ4 regardless of the in-memory codec, so existing
        // tools and binaries built with another codec can still read them.
        if (Compression != NSectorMap::ECompression::Lz4) {
            char data[NSectorMap::SECTOR_SIZE];
            DecompressSector(sd->GetData(), data);
            char compressed[LZ4_COMPRESSBOUND(NSectorMap::SECTOR_SIZE)];
            const int size = LZ4_compress_default(data, compressed, sizeof(data), sizeof(compressed));
            Y_ABORT_UNLESS(size > 0);
            sd->SetData(compressed, size);
        }
        sd->SetCompressionType(NKikimrPDisk::ECompression::LZ4);
    }

    TString raw;
    Y_PROTOBUF_SUPPRESS_NODISCARD snap.SerializeToString(&raw);
    TFileOutput(path).Write(raw);
}

}
