#include "spilled_storage.h"
#include <charconv>
#include <util/string/printf.h>

namespace NKikimr::NMiniKQL {

NThreading::TFuture<ISpiller::TKey> SpillPage(ISpiller& spiller, TPackResult&& page) {
    return spiller.Put(Serialize(std::move(page)));
}

void PopFront(NYql::TChunkedBuffer& buff) {
    buff.Erase(buff.Front().Buf.size());
}

NYql::TChunkedBuffer Serialize(TPackResult&& result) {
    NYql::TChunkedBuffer buff{};
    constexpr int size = sizeof(result.NTuples);
    char ntuplesBuff[size]{};
    std::memcpy(ntuplesBuff, &result.NTuples, size);
    buff.Append(TString{ntuplesBuff, ntuplesBuff+size});
    buff.Append(TString{reinterpret_cast<const char*>(result.PackedTuples.data()), result.PackedTuples.size()});
    buff.Append(TString{reinterpret_cast<const char*>(result.Overflow.data()), result.Overflow.size()});

    MKQL_ENSURE(result.NTuples != 0, "spilling empty page?");
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
