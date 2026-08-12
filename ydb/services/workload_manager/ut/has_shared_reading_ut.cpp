#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>

#include <google/protobuf/any.pb.h>


namespace NKikimr::NWorkloadManager {

namespace {

constexpr char SHARED_READING_POOL[] = "shared_reading_pool";
constexpr char USER_POOL[] = "user_pool";

TClassifierConfigsView MakeDefaultClassifierView() {
    auto classifierSnap = MakeClassifierSnapshot({
        MakeClassifierConfig(TEST_DB, "c1", 100, "pool_from_classifier"),
    });
    return TClassifierConfigsView(classifierSnap, TEST_DB);
}

TResourcePoolMapPtr MakeDefaultPoolSnap() {
    return MakeResourcePoolMap({
        {_JoinPath(TEST_DB, SHARED_READING_POOL), MakePoolEntry(10)},
        {_JoinPath(TEST_DB, USER_POOL), MakePoolEntry(10)},
        {_JoinPath(TEST_DB, "pool_from_classifier"), MakePoolEntry(10)},
        {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
    });
}

std::shared_ptr<IQueryClassifier> MakeClassifier(
    const TString& resourcePoolForSharedReading,
    const TString& explicitPoolId = {},
    std::optional<TClassifierConfigsView> classifierView = std::nullopt,
    TResourcePoolMapPtr poolSnap = nullptr,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr)
{
    TClassifyContext ctx{
        .PoolId = explicitPoolId,
        .AppName = "",
        .UserToken = userToken,
    };
    return CreateQueryClassifier(
        poolSnap ? poolSnap : MakeDefaultPoolSnap(),
        classifierView ? *classifierView : MakeDefaultClassifierView(),
        TEST_DB,
        std::move(ctx),
        resourcePoolForSharedReading.empty() ? std::optional<TString>() : resourcePoolForSharedReading);
}

NKqp::TUserRequestContext MakeStreamingUserContext() {
    NKqp::TUserRequestContext userCtx{};
    userCtx.IsStreamingQuery = true;
    return userCtx;
}

NKqp::TPreparedQueryHolder MakePreparedQueryWithSharedReading(bool sharedReading)
{
    auto proto = std::make_unique<NKikimrKqp::TPreparedQuery>();
    auto* phyQuery = proto->MutablePhysicalQuery();
    auto* tx = phyQuery->AddTransactions();
    auto* stage = tx->AddStages();
    auto* source = stage->AddSources();

    NYql::NPq::NProto::TDqPqTopicSource pqSource;
    pqSource.SetTopicPath("/Root/topic");
    pqSource.SetSharedReading(sharedReading);
    auto* externalSource = source->MutableExternalSource();
    externalSource->SetType(TString{NYql::PqSource});
    externalSource->MutableSettings()->PackFrom(pqSource);

    return NKqp::TPreparedQueryHolder(proto.release(), nullptr, /*noFillTables=*/true);
}

TString GetPostPoolId(const IQueryClassifier::TPostCompileClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TResolvedPoolId>(result),
        TStringBuilder() << "Expected TResolvedPoolId, got variant index: " << result.index());
    return std::get<IQueryClassifier::TResolvedPoolId>(result).PoolId;
}

const IQueryClassifier::TReject& GetPostReject(const IQueryClassifier::TPostCompileClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TReject>(result),
        TStringBuilder() << "Expected TReject, got variant index: " << result.index());
    return std::get<IQueryClassifier::TReject>(result);
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TQueryClassifierSharedReadingPool) {

    Y_UNIT_TEST(PreCompileDefersWhenConfigSet) {
        auto classifier = MakeClassifier(SHARED_READING_POOL, USER_POOL);
        auto result = classifier->PreCompileClassify(MakeStreamingUserContext());
        UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TPendingCompilation>(result),
            TStringBuilder() << "Expected TPendingCompilation, got variant index: " << result.index());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(classifier->GetState()),
            static_cast<int>(IQueryClassifier::EState::WaitCompile));
    }

    Y_UNIT_TEST(PreCompileIgnoresConfigForNonStreaming) {
        auto classifier = MakeClassifier(SHARED_READING_POOL, USER_POOL);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPoolId(classifier->PreCompileClassify(NKqp::TUserRequestContext{})),
            USER_POOL);
    }

    Y_UNIT_TEST(ForcesPoolWhenSharedReading) {
        auto classifier = MakeClassifier(SHARED_READING_POOL);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPostPoolId(classifier->PostCompileClassify(prepared, userCtx)),
            SHARED_READING_POOL);
    }

    Y_UNIT_TEST(ForcesPoolWhenExplicitPoolMatchesConfig) {
        auto classifier = MakeClassifier(SHARED_READING_POOL, SHARED_READING_POOL);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPostPoolId(classifier->PostCompileClassify(prepared, userCtx)),
            SHARED_READING_POOL);
    }

    Y_UNIT_TEST(RejectsWhenExplicitPoolConflictsWithSharedReading) {
        auto classifier = MakeClassifier(SHARED_READING_POOL, USER_POOL);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        const auto& reject = GetPostReject(classifier->PostCompileClassify(prepared, userCtx));
        UNIT_ASSERT_VALUES_EQUAL(reject.Code, Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT_STRING_CONTAINS(reject.Message, USER_POOL);
        UNIT_ASSERT_STRING_CONTAINS(reject.Message, SHARED_READING_POOL);
        UNIT_ASSERT_VALUES_EQUAL(reject.Resolver, "ResourcePoolForSharedReading");
    }

    Y_UNIT_TEST(ForcesPoolWithEmptyClassifierView) {
        auto classifier = MakeClassifier(
            SHARED_READING_POOL, /*explicitPoolId=*/{}, TClassifierConfigsView{});
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPostPoolId(classifier->PostCompileClassify(prepared, userCtx)),
            SHARED_READING_POOL);
    }

    Y_UNIT_TEST(FallsBackToUserPoolWithoutSharedReading) {
        auto classifier = MakeClassifier(SHARED_READING_POOL, USER_POOL);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPostPoolId(classifier->PostCompileClassify(prepared, userCtx)),
            USER_POOL);
    }

    Y_UNIT_TEST(FallsBackToClassifierWithoutSharedReadingAndWithoutUserPool) {
        auto classifier = MakeClassifier(SHARED_READING_POOL);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(false);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPostPoolId(classifier->PostCompileClassify(prepared, userCtx)),
            "pool_from_classifier");
    }

    Y_UNIT_TEST(RejectsWhenSharedReadingPoolMissing) {
        // A shared-reading query must be rejected when the configured pool is
        // not present in the snapshot — it must not soft-resolve to the id.
        auto classifier = MakeClassifier("missing_shared_reading_pool");
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        const auto& reject = GetPostReject(classifier->PostCompileClassify(prepared, userCtx));
        UNIT_ASSERT_VALUES_EQUAL(reject.Code, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(reject.Message, "missing_shared_reading_pool");
        UNIT_ASSERT_VALUES_EQUAL(reject.Resolver, "ResourcePoolForSharedReading");
    }

    Y_UNIT_TEST(RejectsWhenNoSelectAccessToSharedReadingPool) {
        auto securityObject = NACLib::TSecurityObject(/*owner=*/"owner", /*isContainer=*/false);
        securityObject.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "alice");

        auto poolSnap = MakeResourcePoolMap({
            {_JoinPath(TEST_DB, SHARED_READING_POOL), TResourcePoolEntry{
                .Config = MakePoolEntry(10).Config,
                .SecurityObject = securityObject,
            }},
            {_JoinPath(TEST_DB, USER_POOL), MakePoolEntry(10)},
            {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
        });

        auto token = MakeIntrusive<NACLib::TUserToken>(
            NACLib::TSID("alice"), TVector<NACLib::TSID>{});
        token->SaveSerializationInfo();

        auto classifier = MakeClassifier(
            SHARED_READING_POOL, /*explicitPoolId=*/{}, /*classifierView=*/std::nullopt, poolSnap, token);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        const auto& reject = GetPostReject(classifier->PostCompileClassify(prepared, userCtx));
        UNIT_ASSERT_VALUES_EQUAL(reject.Code, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(reject.Resolver, "ResourcePoolForSharedReading");
    }

    Y_UNIT_TEST(RejectsWhenNoDescribeAccessToSharedReadingPool) {
        // No permissions at all: DescribeSchema fails first, so the pool must be
        // reported as not found (NOT_FOUND) rather than resolved.
        auto securityObject = NACLib::TSecurityObject(/*owner=*/"owner", /*isContainer=*/false);

        auto poolSnap = MakeResourcePoolMap({
            {_JoinPath(TEST_DB, SHARED_READING_POOL), TResourcePoolEntry{
                .Config = MakePoolEntry(10).Config,
                .SecurityObject = securityObject,
            }},
            {_JoinPath(TEST_DB, USER_POOL), MakePoolEntry(10)},
            {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
        });

        auto token = MakeIntrusive<NACLib::TUserToken>(
            NACLib::TSID("alice"), TVector<NACLib::TSID>{});
        token->SaveSerializationInfo();

        auto classifier = MakeClassifier(
            SHARED_READING_POOL, /*explicitPoolId=*/{}, /*classifierView=*/std::nullopt, poolSnap, token);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        const auto& reject = GetPostReject(classifier->PostCompileClassify(prepared, userCtx));
        UNIT_ASSERT_VALUES_EQUAL(reject.Code, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(reject.Message, SHARED_READING_POOL);
        UNIT_ASSERT_VALUES_EQUAL(reject.Resolver, "ResourcePoolForSharedReading");
    }

    Y_UNIT_TEST(UnconstrainedSharedReadingPoolRetainsPoolId) {
        // An unconstrained pool (no WMS/admission settings) would normally resolve
        // to TBypass, losing the pool id. Shared-reading queries must always run in
        // their configured pool, so the resolver retains the pool id (SkipAdmission)
        // instead of bypassing.
        auto poolSnap = MakeResourcePoolMap({
            {_JoinPath(TEST_DB, SHARED_READING_POOL), MakePoolEntry(/*concurrentQueryLimit=*/-1)},
            {_JoinPath(TEST_DB, USER_POOL), MakePoolEntry(10)},
            {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
        });
        auto classifier = MakeClassifier(
            SHARED_READING_POOL, /*explicitPoolId=*/{}, /*classifierView=*/std::nullopt, poolSnap);
        auto userCtx = MakeStreamingUserContext();
        (void)classifier->PreCompileClassify(userCtx);

        auto prepared = MakePreparedQueryWithSharedReading(true);
        auto result = classifier->PostCompileClassify(prepared, userCtx);
        UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TResolvedPoolId>(result),
            TStringBuilder() << "Expected TResolvedPoolId, got variant index: " << result.index());
        const auto& resolved = std::get<IQueryClassifier::TResolvedPoolId>(result);
        UNIT_ASSERT_VALUES_EQUAL(resolved.PoolId, SHARED_READING_POOL);
        UNIT_ASSERT(resolved.SkipAdmission);
    }

    Y_UNIT_TEST(NoConfigKeepsExplicitPoolInPreCompile) {
        auto classifier = MakeClassifier(/*resourcePoolForSharedReading=*/"", USER_POOL);
        UNIT_ASSERT_VALUES_EQUAL(
            GetPoolId(classifier->PreCompileClassify(MakeStreamingUserContext())),
            USER_POOL);
    }

}

}  // namespace NKikimr::NWorkloadManager
