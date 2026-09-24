#include "spilled_storage.h"
#include <charconv>
#include <util/string/printf.h>

namespace NKikimr::NMiniKQL {

NThreading::TFuture<ISpiller::TKey> SpillPage(ISpiller& spiller, TPackResult&& page) {
    MKQL_ENSURE(!page.Empty(), "sanity check");
    return spiller.Put(Serialize(std::move(page)));
}

NThreading::TFuture<ISpiller::TKey> SpillMatchBits(ISpiller& spiller, const TDynBitMap& bits, size_t rows) {
    MKQL_ENSURE(rows > 0 && bits.Size() >= rows, "invalid probe match bitmap");

    NYql::TChunkedBuffer buffer;
    NYql::TChunkedBufferOutput output(buffer);
    ::Save(&output, ui8(sizeof(TDynBitMap::TChunk)));
    ::Save(&output, ui64(rows));
    const size_t chunks = (rows + sizeof(TDynBitMap::TChunk) * 8 - 1) / (sizeof(TDynBitMap::TChunk) * 8);
    ::SavePodArray(&output, bits.GetChunks(), chunks);
    return spiller.Put(std::move(buffer));
}

NYql::TChunkedBuffer Serialize(TPackResult&& result) {
    MKQL_ENSURE(!result.Empty(), "spilling empty page?");
    NYql::TChunkedBuffer buff{};
    constexpr int size = sizeof(result.NTuples);
    char ntuplesBuff[size]{};
    std::memcpy(ntuplesBuff, &result.NTuples, size);
    buff.Append(TString{ntuplesBuff, ntuplesBuff+size});
    buff.Append(TString{reinterpret_cast<const char*>(result.PackedTuples.data()), result.PackedTuples.size()});
    buff.Append(TString{reinterpret_cast<const char*>(result.Overflow.data()), result.Overflow.size()});

    return buff;
}
struct OutputStreamTo: public IOutputStream{
    std::span<char> To;
    void DoWrite(const void *buf, size_t len) override{
        MKQL_ENSURE(len <= To.size(), "too bug write");
        std::memcpy(To.data(), buf, len);
        To = To.subspan(len);
    }
};

struct TChunkedBufferInput final : public IInputStream {
    explicit TChunkedBufferInput(NYql::TChunkedBuffer&& buffer)
        : Buffer(std::move(buffer))
    {}

    size_t DoRead(void* data, size_t len) override {
        const size_t toRead = Min(len, Buffer.Size());
        OutputStreamTo output;
        output.To = std::span<char>{static_cast<char*>(data), toRead};
        const size_t read = Buffer.CopyTo(output, toRead);
        Buffer.Erase(read);
        return read;
    }

    NYql::TChunkedBuffer Buffer;
};

void ParseMatchBits(NYql::TChunkedBuffer&& buffer, TDynBitMap& bits, size_t expectedRows) {
    TChunkedBufferInput input(std::move(buffer));
    TDynBitMap parsed;
    parsed.Load(&input);
    MKQL_ENSURE(input.Buffer.Empty(), "unexpected trailing data in probe match bitmap");
    MKQL_ENSURE(expectedRows > 0 && parsed.Size() >= expectedRows && parsed.Size() - expectedRows < 64,
                "probe page and match bitmap sizes differ");
    bits.Swap(parsed);
}

TPackResult Parse(NYql::TChunkedBuffer&& buff, const NPackedTuple::TTupleLayout* layout) {
    TPackResult res;
    OutputStreamTo str;
    auto fillTo = [&] {
        while(!str.To.empty()) {
            size_t copied = buff.CopyTo(str, str.To.size());
            buff.Erase(copied);
        }
    };

    str.To = std::span<char>{reinterpret_cast<char*>(&res.NTuples), sizeof(res.NTuples)}; 
    fillTo();
    
    res.PackedTuples.resize(res.NTuples*layout->TotalRowSize);
    str.To = std::span<char>{reinterpret_cast<char*>(res.PackedTuples.data()), res.PackedTuples.size()};
    fillTo();

    res.Overflow.resize(buff.Size());
    str.To = std::span<char>{reinterpret_cast<char*>(res.Overflow.data()), res.Overflow.size()};
    fillTo();

    return res;
}

} // namespace NKikimr::NMiniKQL
