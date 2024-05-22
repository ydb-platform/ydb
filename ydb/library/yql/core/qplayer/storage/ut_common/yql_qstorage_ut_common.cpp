#include "yql_qstorage_ut_common.h"

using namespace NYql;

TVector<TQItem> DrainIterator(IQIterator& iterator) {
    TVector<TQItem> res;
    for (;;) {
        auto value = iterator.Next().GetValueSync();
        if (!value) {
            break;
        }

        res.emplace_back(*value);
    }

    return res;
}

void QStorageTestEmpty_Impl(const NYql::IQStoragePtr& storage) {
    auto reader = storage->MakeReader("foo", {});
    UNIT_ASSERT(!reader->Get({"comp", "label"}).GetValueSync().Defined());
    auto iterator = storage->MakeIterator("foo", {});
    UNIT_ASSERT(!iterator->Next().GetValueSync().Defined());
}

void QStorageTestNoCommit_Impl(const NYql::IQStoragePtr& storage) {
    auto writer = storage->MakeWriter("foo", {});
    writer->Put({"comp", "label"}, "value").GetValueSync();
}

void QStorageTestOne_Impl(const NYql::IQStoragePtr& storage) {
    auto writer = storage->MakeWriter("foo", {});
    writer->Put({"comp", "label"}, "value").GetValueSync();
    writer->Commit().GetValueSync();
    auto reader = storage->MakeReader("foo", {});
    auto value = reader->Get({"comp", "label"}).GetValueSync();
    UNIT_ASSERT(value.Defined());
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Component, "comp");
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Label, "label");
    UNIT_ASSERT_VALUES_EQUAL(value->Value, "value");
    auto iterator = storage->MakeIterator("foo", {});
    value = iterator->Next().GetValueSync();
    UNIT_ASSERT(value.Defined());
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Component, "comp");
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Label, "label");
    UNIT_ASSERT_VALUES_EQUAL(value->Value, "value");
    value = iterator->Next().GetValueSync();
    UNIT_ASSERT(!value.Defined());
}

void QStorageTestManyKeys_Impl(const NYql::IQStoragePtr& storage) {
    const size_t N = 10;
    auto writer = storage->MakeWriter("foo", {});
    for (size_t i = 0; i < N; ++i) {
        writer->Put({"comp", "label" + ToString(i)}, "value" + ToString(i)).GetValueSync();
    }

    writer->Commit().GetValueSync();
    auto reader = storage->MakeReader("foo", {});
    for (size_t i = 0; i < N; ++i) {
        auto value = reader->Get({"comp", "label" + ToString(i)}).GetValueSync();
        UNIT_ASSERT(value.Defined());
        UNIT_ASSERT_VALUES_EQUAL(value->Key.Component, "comp");
        UNIT_ASSERT_VALUES_EQUAL(value->Key.Label, "label" + ToString(i));
        UNIT_ASSERT_VALUES_EQUAL(value->Value, "value" + ToString(i));
    }

    auto iterator = storage->MakeIterator("foo", {});
    TVector<TQItem> res = DrainIterator(*iterator);
    UNIT_ASSERT_VALUES_EQUAL(res.size(), N);
    Sort(res);
    for (size_t i = 0; i < N; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(res[i].Key.Component, "comp");
        UNIT_ASSERT_VALUES_EQUAL(res[i].Key.Label, "label" + ToString(i));
        UNIT_ASSERT_VALUES_EQUAL(res[i].Value, "value" + ToString(i));
    }
}

void QStorageTestInterleaveReadWrite_Impl(const NYql::IQStoragePtr& storage) {
    auto reader = storage->MakeReader("foo", {});
    auto value = reader->Get({"comp", "label"}).GetValueSync();
    UNIT_ASSERT(!value.Defined());
    auto iterator1 = storage->MakeIterator("foo", {});
    value = iterator1->Next().GetValueSync();
    UNIT_ASSERT(!value.Defined());
    auto writer = storage->MakeWriter("foo", {});
    writer->Put({"comp", "label"}, "value").GetValueSync();
    reader = storage->MakeReader("foo", {});
    value = reader->Get({"comp", "label"}).GetValueSync();
    UNIT_ASSERT(!value.Defined());
    auto iterator2 = storage->MakeIterator("foo", {});
    value = iterator2->Next().GetValueSync();
    UNIT_ASSERT(!value.Defined());
    writer->Commit().GetValueSync();
    reader = storage->MakeReader("foo", {});
    value = reader->Get({"comp", "label"}).GetValueSync();
    UNIT_ASSERT(value.Defined());
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Component, "comp");
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Label, "label");
    UNIT_ASSERT_VALUES_EQUAL(value->Value, "value");
    auto iterator3 = storage->MakeIterator("foo", {});
    value = iterator3->Next().GetValueSync();
    UNIT_ASSERT(value.Defined());
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Component, "comp");
    UNIT_ASSERT_VALUES_EQUAL(value->Key.Label, "label");
    UNIT_ASSERT_VALUES_EQUAL(value->Value, "value");
    value = iterator2->Next().GetValueSync();
    UNIT_ASSERT(!value.Defined());
}

void QStorageTestLimitWriterItems_Impl(const NYql::IQStoragePtr& storage) {
    TQWriterSettings settings;
    settings.ItemsLimit = 1;
    auto writer = storage->MakeWriter("foo", settings);
    writer->Put({"comp", "label1"}, "value1").GetValueSync();
    writer->Put({"comp", "label2"}, "value2").GetValueSync();
    UNIT_ASSERT_EXCEPTION(writer->Commit().GetValueSync(), yexception);
}

void QStorageTestLimitWriterBytes_Impl(const NYql::IQStoragePtr& storage) {
    TQWriterSettings settings;
    settings.BytesLimit = 7;
    auto writer = storage->MakeWriter("foo", settings);
    writer->Put({"comp", "label1"}, "value1").GetValueSync();
    writer->Put({"comp", "label2"}, "value2").GetValueSync();
    UNIT_ASSERT_EXCEPTION(writer->Commit().GetValueSync(), yexception);
}

