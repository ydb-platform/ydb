#include <library/cpp/comptable/comptable.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>
#include <util/random/fast.h>

using namespace NCompTable;

template <bool HQ>
void DoTest(const TCompressorTable& table, const TVector<TString>& lines) {
    TVector<char> compressed;
    TVector<char> decompressed;

    TChunkCompressor compressor(HQ, table);
    TStringStream tmp;
    Save(&tmp, table);
    TCompressorTable tableLoaded;
    Load(&tmp, tableLoaded);
    UNIT_ASSERT(memcmp(&table, &tableLoaded, sizeof(table)) == 0);
    TChunkDecompressor deCompressor(HQ, tableLoaded);

    size_t origSize = 0;
    size_t compSize = 0;
    for (size_t i = 0; i < lines.size(); ++i) {
        const TString& line = lines[i];
        compressor.Compress(line, &compressed);
        origSize += line.size();
        compSize += compressed.size();
        TStringBuf in(compressed.data(), compressed.size());
        deCompressor.Decompress(in, &decompressed);
        UNIT_ASSERT(decompressed.size() == line.size() && memcmp(decompressed.data(), line.data(), decompressed.size()) == 0);
    }
    UNIT_ASSERT_EQUAL(origSize, 45491584);
    if (HQ) {
        UNIT_ASSERT_EQUAL(compSize, 11074583);
    } else {
        UNIT_ASSERT_EQUAL(compSize, 17459336);
    }
    UNIT_ASSERT(compSize < origSize);
}

Y_UNIT_TEST_SUITE(TestComptable) {
    Y_UNIT_TEST(TestComptableCompressDecompress) {
        TReallyFastRng32 rr(17);
        TVector<TString> lines;
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
    }
}
