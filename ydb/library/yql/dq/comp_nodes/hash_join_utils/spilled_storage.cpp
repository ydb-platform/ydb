#include "spilled_storage.h"
#include <charconv>
#include <util/string/printf.h>
namespace NKikimr::NMiniKQL {

NThreading::TFuture<ISpiller::TKey> SpillPage(ISpiller& spiller, TPackResult&& page){
    // page.
    return spiller.Put(Serialize(std::move(page)));
}

void PopFront(NYql::TChunkedBuffer& buff) {
    buff.Erase(buff.Front().Buf.size());
}


NYql::TChunkedBuffer Serialize(TPackResult&& result) {
    NYql::TChunkedBuffer buff{};
    buff.Append(TStringBuilder() << result.NTuples);
    buff.Append(TString{reinterpret_cast<const char*>(result.PackedTuples.data()), result.PackedTuples.size()});
    buff.Append(TString{reinterpret_cast<const char*>(result.Overflow.data()), result.Overflow.size()});

    // Cout << "spilling page, buff.size() == " << buff.Size() << Endl;
    MKQL_ENSURE(result.NTuples != 0,"spilling empty page?");
    return buff;
}


TPackResult Parse(NYql::TChunkedBuffer&& buff) {
    // MKQL
    // MKQL_ENSURE(buff.Size() == 3, Sprintf("pack result must have 3 pages, has%i", buff.Size()));
    // buff.
    TPackResult res;
    auto size = buff.Front().Buf;
    auto code = std::from_chars( size.data(), size.data() + size.size(), res.NTuples);
    MKQL_ENSURE(code.ec == std::errc{}, "invalid integer in size?");
    PopFront(buff);
    res.PackedTuples.resize(buff.Front().Buf.size());
    std::ranges::copy(buff.Front().Buf, res.PackedTuples.data());
    PopFront(buff);
    if (!buff.Empty()){
        res.Overflow.resize(buff.Front().Buf.size());
        std::ranges::copy(buff.Front().Buf, res.Overflow.data());
    }
    // MKQL_ENSURE(tupleBatch.AllocatedBytes() < 2*Settings.BucketSizeBytes, "too big ")

    return res;
}

}
