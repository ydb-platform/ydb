#include "blobstorage_synclogmsgwriter.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NSyncLog;

Y_UNIT_TEST_SUITE(NaiveFragmentWriterTest) {

    void AppendBlock(TString &data, ui64 lsn, ui64 tabletId, ui32 gen) {
        char buf[MaxRecFullSize];
        ui32 len = TSerializeRoutines::SetBlock(buf, lsn, tabletId, gen, 0);
        data.append(buf, len);
    }

    Y_UNIT_TEST(Basic) {
        TString data;
        AppendBlock(data, 100, 66, 1);
        AppendBlock(data, 101, 66, 2);
        AppendBlock(data, 102, 66, 3);
        AppendBlock(data, 103, 66, 4);
        AppendBlock(data, 104, 66, 5);

        TNaiveFragmentWriter w;
        TString result;
        const TRecordHdr *begin = (const TRecordHdr *)(data.data());
        const TRecordHdr *end = (const TRecordHdr *)(data.data() + data.size());
        for (const TRecordHdr *it = begin; it < end; it = it->Next()) {
            w.Push(it);
            result.append((const char *)it, it->GetSize());
            UNIT_ASSERT_VALUES_EQUAL(w.GetSize(), result.size());
        }
        TString temp;
        w.Finish(&temp);
        UNIT_ASSERT_STRINGS_EQUAL(temp, result);
    }

    Y_UNIT_TEST(Long) {
        TString data;
        AppendBlock(data, 100, 66, 1);
        const TRecordHdr *rec = (const TRecordHdr *)(data.data());

        TNaiveFragmentWriter w;
        TString result;
        while (result.size() < (5 << 20)) {
            w.Push(rec);
            result.append((const char *)rec, rec->GetSize());
            UNIT_ASSERT_VALUES_EQUAL(w.GetSize(), result.size());
        }

        TString temp;
        w.Finish(&temp);
        UNIT_ASSERT_STRINGS_EQUAL(temp, result);
    }
}
