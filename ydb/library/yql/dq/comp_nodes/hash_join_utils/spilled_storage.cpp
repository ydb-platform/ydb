#include "spilled_storage.h"
#include <charconv>
#include <util/string/printf.h>

namespace NKikimr::NMiniKQL {

NThreading::TFuture<ISpiller::TKey> SpillPage(ISpiller& spiller, TPackResult&& page) {
    MKQL_ENSURE(!page.Empty(), "sanity check");
    return spiller.Put(Serialize(std::move(page)));
}

NYql::TChunkedBuffer Serialize(TPackResult&& result) {
    MKQL_ENSURE(!result.Empty(), "spilling empty page?");
    NYql::TChunkedBuffer buff{};
    const i64 matchFlagsSize = result.MatchFlags.size();
    char header[sizeof(result.NTuples) + sizeof(matchFlagsSize)]{};
    std::memcpy(header, &result.NTuples, sizeof(result.NTuples));
    std::memcpy(header + sizeof(result.NTuples), &matchFlagsSize, sizeof(matchFlagsSize));
    buff.Append(TString{header, header + sizeof(header)});
    buff.Append(TString{reinterpret_cast<const char*>(result.PackedTuples.data()), result.PackedTuples.size()});
    buff.Append(TString{reinterpret_cast<const char*>(result.Overflow.data()), result.Overflow.size()});
    buff.Append(TString{reinterpret_cast<const char*>(result.MatchFlags.data()), result.MatchFlags.size()});

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

    i64 matchFlagsSize = 0;
    str.To = std::span<char>{reinterpret_cast<char*>(&matchFlagsSize), sizeof(matchFlagsSize)};
    fillTo();
    MKQL_ENSURE(matchFlagsSize >= 0 && (matchFlagsSize == 0 || matchFlagsSize == res.NTuples),
                "corrupted match flags size");
    
    res.PackedTuples.resize(res.NTuples*layout->TotalRowSize);
    str.To = std::span<char>{reinterpret_cast<char*>(res.PackedTuples.data()), res.PackedTuples.size()};
    fillTo();

    MKQL_ENSURE(static_cast<ui64>(matchFlagsSize) <= buff.Size(), "corrupted spilled page");
    res.Overflow.resize(buff.Size() - matchFlagsSize);
    str.To = std::span<char>{reinterpret_cast<char*>(res.Overflow.data()), res.Overflow.size()};
    fillTo();

    res.MatchFlags.resize(matchFlagsSize);
    str.To = std::span<char>{reinterpret_cast<char*>(res.MatchFlags.data()), res.MatchFlags.size()};
    fillTo();

    return res;
}

} // namespace NKikimr::NMiniKQL
