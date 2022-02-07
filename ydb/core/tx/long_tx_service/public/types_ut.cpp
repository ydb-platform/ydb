#include "types.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NLongTxService {

Y_UNIT_TEST_SUITE(LongTxServicePublicTypes) {

    Y_UNIT_TEST(LongTxId) {
        TLongTxId txId;
        TString errStr;
        bool ok = txId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1234", &errStr);
        UNIT_ASSERT_C(ok, errStr);
        UNIT_ASSERT_VALUES_EQUAL(txId.UniqueId.ToString(), "01ezvvxjdk2hd4vdgjs68knvp8");
        UNIT_ASSERT_VALUES_EQUAL(txId.NodeId, 1234u);
        UNIT_ASSERT_VALUES_EQUAL(txId.ToString(), "ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1234");
    }

    Y_UNIT_TEST(Snapshot) {
        TLongTxId txId;
        TString errStr;
        bool ok = txId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1234&snapshot=123%3A456", &errStr);
        UNIT_ASSERT_C(ok, errStr);
        UNIT_ASSERT_VALUES_EQUAL(txId.UniqueId.ToString(), "01ezvvxjdk2hd4vdgjs68knvp8");
        UNIT_ASSERT_VALUES_EQUAL(txId.NodeId, 1234u);
        UNIT_ASSERT_VALUES_EQUAL(txId.Snapshot.Step, 123u);
        UNIT_ASSERT_VALUES_EQUAL(txId.Snapshot.TxId, 456u);
        UNIT_ASSERT_VALUES_EQUAL(txId.ToString(), "ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1234&snapshot=123%3A456");
    }

    Y_UNIT_TEST(SnapshotMaxTxId) {
        TLongTxId txId;
        TString errStr;
        bool ok = txId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1234&snapshot=123%3Amax", &errStr);
        UNIT_ASSERT_C(ok, errStr);
        UNIT_ASSERT_VALUES_EQUAL(txId.UniqueId.ToString(), "01ezvvxjdk2hd4vdgjs68knvp8");
        UNIT_ASSERT_VALUES_EQUAL(txId.NodeId, 1234u);
        UNIT_ASSERT_VALUES_EQUAL(txId.Snapshot.Step, 123u);
        UNIT_ASSERT_VALUES_EQUAL(txId.Snapshot.TxId, Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(txId.ToString(), "ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1234&snapshot=123%3Amax");
    }

    Y_UNIT_TEST(SnapshotReadOnly) {
        TLongTxId txId;
        TString errStr;
        bool ok = txId.ParseString("ydb://long-tx/read-only?snapshot=123%3Amax", &errStr);
        UNIT_ASSERT_C(ok, errStr);
        UNIT_ASSERT_VALUES_EQUAL(txId.UniqueId.ToString(), "00000000000000000000000000");
        UNIT_ASSERT_VALUES_EQUAL(txId.NodeId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(txId.Snapshot.Step, 123u);
        UNIT_ASSERT_VALUES_EQUAL(txId.Snapshot.TxId, Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(txId.ToString(), "ydb://long-tx/read-only?snapshot=123%3Amax");
    }

} // Y_UNIT_TEST_SUITE(LongTxServicePublicTypes)

} // namespace NLongTxService
} // namespace NKikimr
