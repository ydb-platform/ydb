#include "common.h"

#include <library/cpp/tvmauth/client/logger.h>
#include <library/cpp/tvmauth/client/misc/disk_cache.h>

#include <library/cpp/tvmauth/src/utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/system/sysstat.h>

#include <thread>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(ClientDisk) {
    Y_UNIT_TEST(Hash) {
        TString hash = TDiskReader::GetHash("asd");
        UNIT_ASSERT(hash);
        UNIT_ASSERT_VALUES_EQUAL(32, hash.size());
        UNIT_ASSERT_VALUES_EQUAL("Zj5_qYg31bPlqjBW76z8IV0rCsHmv-iN-McV6ybS1-g", NUtils::Bin2base64url(hash));
    }

    Y_UNIT_TEST(Timestamp) {
        time_t t = 100500;

        TString s = TDiskWriter::WriteTimestamp(t);
        UNIT_ASSERT_VALUES_EQUAL("lIgBAAAAAAA", NUtils::Bin2base64url(s));
        UNIT_ASSERT_VALUES_EQUAL(t, TDiskReader::GetTimestamp(s));

        t = 123123123213089;
        s = TDiskWriter::WriteTimestamp(t);
        UNIT_ASSERT_VALUES_EQUAL("IdMF1vpvAAA", NUtils::Bin2base64url(s));
        UNIT_ASSERT_VALUES_EQUAL(t, TDiskReader::GetTimestamp(s));

        t = time(nullptr);
        s = TDiskWriter::WriteTimestamp(t);
        UNIT_ASSERT_VALUES_EQUAL(t, TDiskReader::GetTimestamp(s));
    }

    const TInstant TIME = TInstant::Seconds(100500);
    const TString DATA = "oiweuhn \n vw3ut hweoi uhgewproritjhwequtherwoiughfdsv 8ty34q01u   34  1=3";

    Y_UNIT_TEST(ParseData_Ok) {
        TLogger l;

        const TInstant time = TInstant::Seconds(1523446554789);

        TString toFile = TDiskWriter::PrepareData(time, DATA);
        UNIT_ASSERT_VALUES_EQUAL(113, toFile.size());
        UNIT_ASSERT_VALUES_EQUAL("T8BnRIMoC6mlMXexPg9cV5jYxeFtgDWk97JTajHDunCloH20YgEAAG9pd2V1aG4gCiB2dzN1dCBod2VvaSB1aGdld3Byb3JpdGpod2VxdXRoZXJ3b2l1Z2hmZHN2IDh0eTM0cTAxdSAgIDM0ICAxPTM",
                                 NUtils::Bin2base64url(toFile));

        TDiskReader r("qwerty", &l);
        UNIT_ASSERT(r.ParseData(toFile));
        UNIT_ASSERT_VALUES_EQUAL(DATA, r.Data());
        UNIT_ASSERT_VALUES_EQUAL(time, r.Time());
        UNIT_ASSERT_VALUES_EQUAL("6: File 'qwerty' was successfully read\n",
                                 l.Stream.Str());
    }

    Y_UNIT_TEST(ParseData_SmallFile) {
        TLogger l;

        TString toFile = TDiskWriter::PrepareData(TIME, DATA);
        TDiskReader r("qwerty", &l);
        UNIT_ASSERT(!r.ParseData(toFile.substr(0, 17)));
        UNIT_ASSERT_VALUES_EQUAL("4: File 'qwerty' is too small\n",
                                 l.Stream.Str());
    }

    Y_UNIT_TEST(ParseData_Changed) {
        TLogger l;

        TString toFile = TDiskWriter::PrepareData(TIME, DATA);
        toFile[17] = toFile[17] + 1;
        TDiskReader r("qwerty", &l);
        UNIT_ASSERT(!r.ParseData(toFile));
        UNIT_ASSERT_VALUES_EQUAL("4: Content of 'qwerty' was incorrectly changed\n",
                                 l.Stream.Str());
    }

    Y_UNIT_TEST(Read_Ok) {
        TLogger l;

        TDiskReader r(GetFilePath("ok.cache"), &l);
        UNIT_ASSERT(r.Read());
        UNIT_ASSERT_VALUES_EQUAL(DATA, r.Data());
        UNIT_ASSERT_VALUES_EQUAL(TIME, r.Time());
        UNIT_ASSERT_C(l.Stream.Str().find("was successfully read") != TString::npos, l.Stream.Str());
    }

    Y_UNIT_TEST(Read_NoFile) {
        TLogger l;

        TDiskReader r("missing", &l);
        UNIT_ASSERT(!r.Read());
        UNIT_ASSERT_VALUES_EQUAL("7: File 'missing' does not exist\n",
                                 l.Stream.Str());
    }

#ifdef _unix_
    Y_UNIT_TEST(Read_NoPermitions) {
        TLogger l;

        const TString path = GetWorkPath() + "/123";
        {
            TFileOutput output(path);
        }
        Chmod(path.data(), S_IWUSR);

        TDiskReader r(path, &l);
        UNIT_ASSERT(!r.Read());
        UNIT_ASSERT_C(l.Stream.Str().find("Permission denied") != TString::npos, l.Stream.Str());

        Chmod(path.data(), S_IRWXU);
        NFs::Remove(path);
    }
#endif

    Y_UNIT_TEST(Write_Ok) {
        TLogger l;

        const TString path = "./tmp_file";
        TDiskWriter w(path, &l);
        UNIT_ASSERT_C(w.Write(DATA), l.Stream.Str());
        UNIT_ASSERT_C(l.Stream.Str().find("was successfully written") != TString::npos, l.Stream.Str());
        l.Stream.Clear();

        TDiskReader r(path, &l);
        UNIT_ASSERT_C(r.Read(), l.Stream.Str());
        UNIT_ASSERT_VALUES_EQUAL(DATA, r.Data());
        UNIT_ASSERT(TInstant::Now() - r.Time() < TDuration::Minutes(5));
        UNIT_ASSERT_C(l.Stream.Str().find("was successfully read") != TString::npos, l.Stream.Str());

        NFs::Remove(path);
    }

    Y_UNIT_TEST(Write_NoPermitions) {
        TLogger l;

        TDiskWriter w("/some_file", &l);
        UNIT_ASSERT(!w.Write(DATA));
        UNIT_ASSERT_C(l.Stream.Str().Contains("3: Failed to write '/some_file': ("), l.Stream.Str());
        UNIT_ASSERT_C(l.Stream.Str().Contains("denied"), l.Stream.Str());
    }

    Y_UNIT_TEST(race) {
        const TString path = "./tmp_file";
        const TString data = "ejufhsadkjfvbhsaoicnaofssdahfasdfhasdofdsaf";
        NFs::Remove(path);

        std::atomic<bool> fail = false;
        std::vector<std::thread> thrs;
        for (size_t idx = 0; idx < 16; ++idx) {
            thrs.push_back(std::thread([&fail, data, path]() {
                TDiskWriter w(path);
                for (size_t k = 0; k < 1000; ++k) {
                    if (!w.Write(data)) {
                        fail = true;
                    }
                }
            }));
        }
        for (std::thread& t : thrs) {
            t.join();
        }
        thrs.clear();
        UNIT_ASSERT(fail);
        {
            TDiskWriter w(path);
            UNIT_ASSERT(w.Write(data)); // checks unlocked flock
        }

        fail = false;

        for (size_t idx = 0; idx < 4; ++idx) {
            thrs.push_back(std::thread([&fail, data, path]() {
                TLogger l;
                TDiskReader r(path, &l);
                for (size_t k = 0; k < 100; ++k) {
                    if (!r.Read()) {
                        Cerr << l.Stream.Str() << Flush;
                        fail = true;
                        return;
                    }
                    if (r.Data() != data) {
                        Cerr << (TStringBuilder() << "'" << data << "' vs '" << r.Data() << "'" << Endl) << Flush;
                        fail = true;
                        return;
                    }
                }
            }));
        }
        for (std::thread& t : thrs) {
            t.join();
        }
        thrs.clear();
        UNIT_ASSERT(!fail);
    }
}
