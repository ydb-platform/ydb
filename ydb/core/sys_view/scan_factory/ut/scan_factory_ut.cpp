#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/events/scan.h>
#include <ydb/core/sys_view/scan_factory/scan_factory.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSysView {
namespace {

class TRuntime : public NActors::TTestActorRuntimeBase {
protected:
    void InitNodeImpl(TNodeDataBase* node, size_t index) override {
        node->AppData0 = std::make_shared<TAppData>(0, 0, 0, 0, TMap<TString, ui32>{},
            nullptr, nullptr, nullptr, nullptr);
        TTestActorRuntimeBase::InitNodeImpl(node, index);
    }
};

class TEmptyScan : public NActors::TActorBootstrapped<TEmptyScan> {
public:
    explicit TEmptyScan(NActors::TActorId owner)
        : Owner(owner)
    {}

    void Bootstrap() {
        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(17);
        batch->Finished = true;
        Send(Owner, batch.Release());
        PassAway();
    }

private:
    const NActors::TActorId Owner;
};

class TFactory : public ISystemViewScanFactory {
public:
    THolder<NActors::IActor> Create(const NActors::TActorId& ownerId, ui32 scanId,
        const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
        const TTableId& tableId, const TString& tablePath,
        TVector<TSerializedTableRange> ranges, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse) const override
    {
        UNIT_ASSERT(tableId == TTableId(1, 2));
        UNIT_ASSERT(userToken);
        UNIT_ASSERT_VALUES_EQUAL(userToken->GetUserSID(), "test-user");
        UNIT_ASSERT_VALUES_EQUAL(scanId, 17);
        UNIT_ASSERT_VALUES_EQUAL(database, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(tablePath, "/Root/.sys/nodes");
        UNIT_ASSERT(sysViewInfo);
        UNIT_ASSERT(sysViewInfo->GetType() == NKikimrSysView::ENodes);
        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
        UNIT_ASSERT(ranges[0].FromInclusive);
        UNIT_ASSERT(!ranges[0].ToInclusive);
        UNIT_ASSERT(!ranges[1].FromInclusive);
        UNIT_ASSERT(ranges[1].ToInclusive);
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(columns[0].Tag, 42);
        UNIT_ASSERT(reverse);
        return MakeHolder<TEmptyScan>(ownerId);
    }

    THolder<NActors::IActor> Create(const NActors::TActorId& ownerId, ui32 scanId,
        const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
        const TTableId& tableId, const TString& tablePath,
        const TTableRange& range, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse) const override
    {
        UNIT_ASSERT(range.InclusiveFrom);
        UNIT_ASSERT(!range.InclusiveTo);
        return Create(ownerId, scanId, database, sysViewInfo, tableId, tablePath,
            {TSerializedTableRange(range), TSerializedTableRange(TTableRange({}, false, {}, true))},
            columns, std::move(userToken), reverse);
    }
};

template <typename TRange>
void StartScan(TRuntime& runtime, NActors::TActorId owner, const TRange& range) {
    runtime.RunCall([&] {
        TMaybe<NKikimrSysView::TSysViewDescription> description;
        description.ConstructInPlace().SetType(NKikimrSysView::ENodes);
        TVector<NMiniKQL::TKqpComputeColumn> columns(1);
        columns[0].Tag = 42;
        auto scan = CreateSystemViewScan(owner, 17, "/Root", description, TTableId(1, 2),
            "/Root/.sys/nodes", range, columns,
            MakeIntrusive<NACLib::TUserToken>("test-user", TVector<NACLib::TSID>{}), true);
        return runtime.Register(scan.Release());
    });
}

template <typename TRange>
void CheckUnsupported(const TRange& range) {
    TRuntime runtime;
    runtime.Initialize();
    const auto owner = runtime.AllocateEdgeActor();
    StartScan(runtime, owner, range);
    const auto error = runtime.GrabEdgeEvent<NKqp::TEvKqpCompute::TEvScanError>(owner);
    UNIT_ASSERT_VALUES_EQUAL(error->Get()->Record.GetStatus(), Ydb::StatusIds::UNSUPPORTED);
    UNIT_ASSERT_VALUES_EQUAL(error->Get()->Record.IssuesSize(), 1);
    UNIT_ASSERT_STRING_CONTAINS(error->Get()->Record.GetIssues(0).message(), "not supported");
}

template <typename TRange>
void CheckFactory(const TRange& range) {
    TFactory factory;
    TRuntime runtime;
    runtime.Initialize();
    const auto owner = runtime.AllocateEdgeActor();
    runtime.RunCall([&] {
        AppData()->SystemViewScanFactory = &factory;
        return true;
    });
    StartScan(runtime, owner, range);
    const auto batch = runtime.GrabEdgeEvent<NKqp::TEvKqpCompute::TEvScanData>(owner);
    UNIT_ASSERT_VALUES_EQUAL(batch->Get()->ScanId, 17);
    UNIT_ASSERT(batch->Get()->Finished);
}

} // namespace

Y_UNIT_TEST_SUITE(SystemViewScanFactory) {
    Y_UNIT_TEST(UnsupportedSingleRange) {
        CheckUnsupported(TTableRange({}, true, {}, false));
    }

    Y_UNIT_TEST(UnsupportedMultipleRanges) {
        CheckUnsupported(TVector<TSerializedTableRange>{});
    }

    Y_UNIT_TEST(ApplicationFactorySingleRange) {
        CheckFactory(TTableRange({}, true, {}, false));
    }

    Y_UNIT_TEST(ApplicationFactoryMultipleRanges) {
        CheckFactory(TVector<TSerializedTableRange>{
            TSerializedTableRange(TTableRange({}, true, {}, false)),
            TSerializedTableRange(TTableRange({}, false, {}, true))});
    }
}

} // namespace NKikimr::NSysView
