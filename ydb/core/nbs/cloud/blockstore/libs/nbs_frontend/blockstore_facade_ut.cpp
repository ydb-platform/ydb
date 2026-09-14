#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/blockstore_facade.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNbsFrontendBlockStoreTest)
{
    Y_UNIT_TEST(ShouldRejectEveryMethodOutsideAcceptingState)
    {
        auto blockStore = CreateNbsFrontendBlockStore();

        // A newly created facade must keep every method behind the closed
        // admission gate until Start() is called.
#define TEST_METHOD(name, ...)                                                 \
    {                                                                          \
        auto response =                                                        \
            blockStore                                                         \
                ->name(                                                        \
                    MakeIntrusive<TCallContext>(),                             \
                    std::make_shared<NYdb::NBS::NNbs1CompatApi::NBlockStore::  \
                                         NProto::T##name##Request>())          \
                .GetValueSync();                                               \
        UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetCode(), E_REJECTED);   \
        UNIT_ASSERT_VALUES_EQUAL(                                              \
            response.GetError().GetMessage(),                                  \
            "NBS2 frontend is not accepting requests");                        \
    }

        NBS1_COMPAT_SERVICE(TEST_METHOD)

        // Enter the accepting state so that the first Stop() performs an
        // observable open-to-closed transition.
        blockStore->Start();
        blockStore->Stop();

        // The first Stop() must close the admission gate for every method.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

        blockStore->Stop();

        // A repeated Stop() must preserve the same closed state.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

#undef TEST_METHOD
    }

    Y_UNIT_TEST(ShouldServePingAndRejectEveryOtherMethod)
    {
        auto blockStore = CreateNbsFrontendBlockStore();

#define TEST_METHOD(name, ...)                                                 \
    {                                                                          \
        auto response =                                                        \
            blockStore                                                         \
                ->name(                                                        \
                    MakeIntrusive<TCallContext>(),                             \
                    std::make_shared<NYdb::NBS::NNbs1CompatApi::NBlockStore::  \
                                         NProto::T##name##Request>())          \
                .GetValueSync();                                               \
        if (TStringBuf(#name) == "Ping") {                                     \
            UNIT_ASSERT(!HasError(response));                                  \
        } else {                                                               \
            UNIT_ASSERT_VALUES_EQUAL(                                          \
                response.GetError().GetCode(),                                 \
                E_NOT_IMPLEMENTED);                                            \
            UNIT_ASSERT_STRING_CONTAINS(                                       \
                response.GetError().GetMessage(),                              \
                #name);                                                        \
        }                                                                      \
    }

        blockStore->Start();

        // The first Start() must open the admission gate and expose the
        // implemented-method behavior.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

        blockStore->Start();

        // A repeated Start() must preserve the same open state.
        NBS1_COMPAT_SERVICE(TEST_METHOD)

#undef TEST_METHOD

        // The frontend skeleton does not provide data-path buffer allocation.
        UNIT_ASSERT(!blockStore->AllocateBuffer(4096));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

}   // namespace NYdb::NBS::NBlockStore
