#include <library/cpp/comptable/comptable.h>

#include <util/random/random.h>
#include <util/random/fast.h>

#include <time.h>
#include <stdlib.h>

using namespace NCompTable;

template <bool HQ>
void DoTest(const TCompressorTable& table, const TVector<TString>& lines) {
    TVector<char> compressed;
    TVector<char> decompressed;

    TChunkCompressor compressor(HQ, table);
    TChunkDecompressor deCompressor(HQ, table);

    size_t origSize = 0;
    size_t compSize = 0;
    float cl1 = clock();
    for (size_t i = 0; i < lines.size(); ++i) {
        const TString& line = lines[i];
        compressor.Compress(line, &compressed);
        origSize += line.size();
        compSize += compressed.size();
        TStringBuf in(compressed.data(), compressed.size());
        deCompressor.Decompress(in, &decompressed);
        if (decompressed.size() != line.size() || memcmp(decompressed.data(), line.data(), decompressed.size())) {
            Cout << i << "\n";
            Cout << line << "\n"
                 << TString(decompressed.data(), decompressed.size()) << "\n";
            abort();
        }
    }
    float cl2 = clock();
    float secs = (cl2 - cl1) / CLOCKS_PER_SEC;
    Cout << "origSize: " << origSize << "\tcompSize: " << compSize << Endl;
    Cout << "yep! compression + decompression speed " << origSize / 1024.0f / 1024.0f / secs << " mbps\n";
    Cout << "yep! compression ratio " << double(origSize) / double(compSize + 1) << "\n";
}

int main(int argc, const char* argv[]) {
    TReallyFastRng32 rr(17);
    TVector<TString> lines;
    /*FILE *fp = fopen("res", "rb");
    while (!feof(fp)) {
        char buff[4096];
        fscanf(fp, "%s", buff);
        lines.push_back(TString(buff));
    }*/
    //for (size_t i = 0; i < 10000000; ++i) {
    //for (size_t i = 0; i < 1000000; ++i) {
    for (size_t i = 0; i < 1000000; ++i) {
        size_t size = rr.Uniform(32);
        TString res = "www.yandex.ru/yandsearch?text=";
        for (size_t j = 0; j < size; ++j) {
            res += "qwer"[rr.Uniform(4)];
        }
        lines.push_back(res);
    }
    THolder<TDataSampler> sampler(new TDataSampler);
    for (size_t i = 0; i < lines.size(); ++i) {
        sampler->AddStat(lines[i]);
    }
    TCompressorTable table;
    sampler->BuildTable(table);

    DoTest<true>(table, lines);
    DoTest<false>(table, lines);

    Y_UNUSED(argc);
    Y_UNUSED(argv);
    return 0;
}
