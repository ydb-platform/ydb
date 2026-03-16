#include <library/cpp/testing/unittest/registar.h>

#include <util/network/socket.h>

#undef closesocket

#include <contrib/deprecated/bdb/db_config.h>
#include <contrib/deprecated/bdb/db.h>
#include <contrib/deprecated/bdb/db_int.h>

#include <util/system/tempfile.h>
#include <library/cpp/digest/old_crc/crc.h>
#include <util/memory/blob.h>
#include <util/generic/ylimits.h>
#include <util/stream/output.h>

#define	DATABASE "access.db"

struct TDBClose {
    static inline void Destroy(DB* db) noexcept {
        db->close(db, 0);
    }
};

typedef THolder<DB, TDBClose> TDBHolder;

class TBerkleyDBTest: public TTestBase {
        UNIT_TEST_SUITE(TBerkleyDBTest);
            UNIT_TEST(TestBaseCrc)
            UNIT_TEST(TestInvariants)
        UNIT_TEST_SUITE_END();
    private:
        inline DB* Create() {
            int ret;
            DB* dbp = 0;

            if ((ret = db_create(&dbp, NULL, 0)) != 0) {
                ythrow yexception() << "db create error(" <<  db_strerror(ret) << ")";
            }

            return dbp;
        }

        inline void TestBaseCrc() {
            TTempFile tmpFile(DATABASE);

            int ret = 0;
            TDBHolder dbpAuto(Create());
            DB* dbp = dbpAuto.Get();

            dbp->set_errfile(dbp, stderr);
            dbp->set_errpfx(dbp, "test");

            if ((ret = dbp->set_pagesize(dbp, 1024)) != 0) {
                ythrow yexception() <<  db_strerror(ret);
            }

            if ((ret = dbp->set_cachesize(dbp, 0, 32 * 1024, 0)) != 0) {
                ythrow yexception() <<  db_strerror(ret);
            }

            if ((ret = dbp->open(dbp, NULL, DATABASE, NULL, DB_HASH, DB_CREATE, 0664)) != 0) {
                ythrow yexception() <<  db_strerror(ret);
            }

            TString test("1234567890asdfhasrdghasdhasfdjhdgjhjhdf");

            for (size_t i = 1; i < test.size(); ++i) {
                DBT key;
                DBT data;

                memset(&key, 0, sizeof(DBT));
                memset(&data, 0, sizeof(DBT));

                key.data = (void*)test.data();
                data.data = (void*)test.data();
                data.size = key.size = i;

                if ((ret = dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE)) != 0) {
                    ythrow yexception() <<  db_strerror(ret);
                }
            }

            dbpAuto.Destroy();

            TBlob data = TBlob::FromFile(DATABASE);
            const size_t over = 1024;
            const ui64 res = Crc<ui64>((const char*)data.Data() + over, data.Length() - 2 * over);

            if (0) {
                UNIT_ASSERT_EQUAL(res, ULL(2404647397835125719));
            }
        }

        inline void TestInvariants() {
            UNIT_ASSERT_EQUAL(UINT16_MAX, Max<ui16>());
            UNIT_ASSERT_EQUAL(INT_MAX, Max<int>());
            UNIT_ASSERT_EQUAL(INT_MIN, Min<int>());
            UNIT_ASSERT_EQUAL(UINT_MAX, Max<unsigned int>());
            UNIT_ASSERT_EQUAL(LONG_MAX, Max<long>());
            UNIT_ASSERT_EQUAL(LONG_MIN, Min<long>());
            UNIT_ASSERT_EQUAL(ULONG_MAX, Max<unsigned long>());

#if defined(HAVE_64BIT_TYPES)
            UNIT_ASSERT_EQUAL(INT64_MAX, Max<i64>());
            UNIT_ASSERT_EQUAL(INT64_MIN, Min<i64>());
            UNIT_ASSERT_EQUAL(UINT64_MAX, Max<ui64>());
#endif
        }
};

UNIT_TEST_SUITE_REGISTRATION(TBerkleyDBTest);
