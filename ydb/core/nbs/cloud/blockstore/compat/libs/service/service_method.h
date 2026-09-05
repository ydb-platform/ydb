#pragma once

#include "service.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <utility>

namespace NCloud::NBlockStore {

template <typename TRequest, typename TResponse>
struct TBlockStoreMethod;

// Describes each method in the MVP subset for generic facade and transport
// code.
#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    template <>                                                                \
    struct TBlockStoreMethod<                                                  \
        NProto::T##name##Request,                                              \
        NProto::T##name##Response>                                             \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        static constexpr const char* Name = #name;                             \
                                                                               \
        [[nodiscard]] static NThreading::TFuture<TResponse> Execute(           \
            IBlockStore* blockStore,                                           \
            NYdb::NBS::NBlockStore::TCallContextPtr callContext,               \
            std::shared_ptr<TRequest> request)                                 \
        {                                                                      \
            return blockStore->name(                                           \
                std::move(callContext),                                        \
                std::move(request));                                           \
        }                                                                      \
    };                                                                         \
                                                                               \
    using TBlockStore##name##Method = TBlockStoreMethod<                       \
        NProto::T##name##Request,                                              \
        NProto::T##name##Response>;                                            \
    // BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

// Implements every method of the MVP IBlockStore subset through Execute().
template <typename T, typename U>
struct TBlockStoreImpl: public U
{
#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    NThreading::TFuture<NProto::T##name##Response> name(                       \
        NYdb::NBS::NBlockStore::TCallContextPtr callContext,                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        using TMethod = TBlockStoreMethod<                                     \
            NProto::T##name##Request,                                          \
            NProto::T##name##Response>;                                        \
        return static_cast<T*>(this)->template Execute<TMethod>(               \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore
