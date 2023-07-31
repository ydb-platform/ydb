#include "map.h"

#include <library/cpp/reverse_geocoder/library/log.h>
#include <library/cpp/reverse_geocoder/library/system.h>
#include <library/cpp/reverse_geocoder/proto/geo_data.pb.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/algorithm.h>
#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/system/filemap.h>
#include <util/system/unaligned_mem.h>

using namespace NReverseGeocoder;

static const TNumber CRC_SIZE = 3;

void NReverseGeocoder::TGeoDataMap::Init() {
#define GEO_BASE_DEF_VAR(TVar, Var) \
    Var##_ = TVar();

#define GEO_BASE_DEF_ARR(TArr, Arr) \
    Arr##_ = nullptr;               \
    Arr##Number_ = 0;

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR
}

NReverseGeocoder::TGeoDataMap::TGeoDataMap()
    : Data_(nullptr)
    , Size_(0)
{
    Init();
}

static bool CheckMemoryConsistency(const NProto::TGeoData& g) {
    TVector<std::pair<intptr_t, intptr_t>> segments;

#define GEO_BASE_DEF_VAR(TVar, Var) \
    // undef

#define GEO_BASE_DEF_ARR(TArr, Arr)                                              \
    if (g.Get##Arr##Number() > 0) {                                              \
        intptr_t const beg = g.Get##Arr();                                       \
        intptr_t const end = g.Get##Arr() + g.Get##Arr##Number() * sizeof(TArr); \
        segments.emplace_back(beg, end);                                         \
    }

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    Sort(segments.begin(), segments.end());

    for (size_t i = 0; i + 1 < segments.size(); ++i)
        if (segments[i].second > segments[i + 1].first)
            return false;

    return true;
}

void NReverseGeocoder::TGeoDataMap::Remap() {
    Init();

    if (!Data_)
        return;

    const ui64 headerSize = ntohl(ReadUnaligned<ui64>(Data_));

    NProto::TGeoData header;
    if (!header.ParseFromArray(Data_ + sizeof(ui64), headerSize))
        ythrow yexception() << "Unable parse geoData header";

    if (header.GetMagic() != SYSTEM_ENDIAN_FLAG)
        ythrow yexception() << "Different endianness in geoData and host";

    if (!CheckMemoryConsistency(header))
        ythrow yexception() << "Memory is not consistent!";

#define GEO_BASE_DEF_VAR(TVar, Var) \
    Var##_ = header.Get##Var();

#define GEO_BASE_DEF_ARR(TArr, Arr)                                                        \
    GEO_BASE_DEF_VAR(TNumber, Arr##Number);                                                \
    if (Arr##Number() > 0) {                                                               \
        const intptr_t offset = header.Get##Arr();                                         \
        Arr##_ = (TArr*)(((intptr_t)Data_) + offset);                                      \
        const ui32 hash = Crc32c(Arr##_, std::min(Arr##Number_, CRC_SIZE) * sizeof(TArr)); \
        if (hash != header.Get##Arr##Crc32())                                              \
            ythrow yexception() << "Wrong crc32 for " << #Arr;                             \
    }

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    if (Version() != GEO_DATA_CURRENT_VERSION)
        ythrow yexception() << "Unable use version " << Version()
                            << "(current version is " << GEO_DATA_CURRENT_VERSION << ")";
}

static size_t HeaderSize() {
    NProto::TGeoData header;
    header.SetMagic(std::numeric_limits<decltype(header.GetMagic())>::max());

#define GEO_BASE_DEF_VAR(TVar, Var) \
    header.Set##Var(std::numeric_limits<decltype(header.Get##Var())>::max());

#define GEO_BASE_DEF_ARR(TArr, Arr)                                           \
    GEO_BASE_DEF_VAR(TNumber, Arr##Number);                                   \
    header.Set##Arr(std::numeric_limits<decltype(header.Get##Arr())>::max()); \
    header.Set##Arr##Crc32(std::numeric_limits<decltype(header.Get##Arr##Crc32())>::max());

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    return header.ByteSize();
}

static const char* Serialize(const IGeoData& g, TBlockAllocator* allocator, size_t* size) {
    size_t const preAllocatedSize = allocator->TotalAllocatedSize();
    char* data = (char*)allocator->Allocate(HeaderSize() + sizeof(ui64));

    NProto::TGeoData header;
    header.SetMagic(SYSTEM_ENDIAN_FLAG);

#define GEO_BASE_DEF_VAR(TVar, Var) \
    header.Set##Var(g.Var());

#define GEO_BASE_DEF_ARR(TArr, Arr)                                                              \
    GEO_BASE_DEF_VAR(TNumber, Arr##Number);                                                      \
    if (g.Arr##Number() > 0) {                                                                   \
        TArr* arr = (TArr*)allocator->Allocate(sizeof(TArr) * g.Arr##Number());                  \
        memcpy(arr, g.Arr(), sizeof(TArr) * g.Arr##Number());                                    \
        header.Set##Arr((ui64)(((intptr_t)arr) - ((intptr_t)data)));                             \
        header.Set##Arr##Crc32(Crc32c(arr, std::min(g.Arr##Number(), CRC_SIZE) * sizeof(TArr))); \
    };

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    const auto str = header.SerializeAsString();
    WriteUnaligned<ui64>(data, (ui64)htonl(str.size()));
    memcpy(data + sizeof(ui64), str.data(), str.size());

    if (size)
        *size = allocator->TotalAllocatedSize() - preAllocatedSize;

    return data;
}

static size_t TotalByteSize(const IGeoData& g) {
    size_t total_size = TBlockAllocator::AllocateSize(HeaderSize() + sizeof(ui64));

#define GEO_BASE_DEF_VAR(TVar, Var) \
    // undef

#define GEO_BASE_DEF_ARR(TArr, Arr) \
    total_size += TBlockAllocator::AllocateSize(sizeof(TArr) * g.Arr##Number());

    GEO_BASE_DEF_GEO_DATA

#undef GEO_BASE_DEF_VAR
#undef GEO_BASE_DEF_ARR

    return total_size;
}

NReverseGeocoder::TGeoDataMap::TGeoDataMap(const IGeoData& geoData, TBlockAllocator* allocator)
    : TGeoDataMap()
{
    Data_ = Serialize(geoData, allocator, &Size_);
    Remap();
}

void NReverseGeocoder::TGeoDataMap::SerializeToFile(const TString& path, const IGeoData& data) {
    TBlob data_blob = SerializeToBlob(data);

    TFile file(path, CreateAlways | RdWr);
    file.Write(data_blob.Data(), data_blob.Length());
}

TBlob NReverseGeocoder::TGeoDataMap::SerializeToBlob(const IGeoData& data) {
    TBuffer buf;
    buf.Resize(TotalByteSize(data));
    memset(buf.data(), 0, buf.size());

    TBlockAllocator allocator(buf.Data(), buf.Size());
    TGeoDataMap(data, &allocator);

    return TBlob::FromBuffer(buf);
}
