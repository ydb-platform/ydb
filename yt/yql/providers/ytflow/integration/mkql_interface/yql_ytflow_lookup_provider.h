#pragma once

#include <library/cpp/threading/future/future.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <functional>


namespace NKikimr::NMiniKQL {

struct TComputationNodeFactoryContext;

} // namespace NKikimr::NMiniKQL

namespace NYql::NUdf {

class IFunctionTypeInfoBuilder;

} // namespace NYql::NUdf

namespace NYql {

enum class ERowSelectionMode {
    All = 1, /* all */
    Any = 2, /* any */
    Unique = 3, /* unique */
};

class IYtflowLookupProvider
{
public:
    using TLookupResultCallback = std::function<TVector<TVector<NUdf::TUnboxedValue>>()>;

    virtual ~IYtflowLookupProvider() = default;

    // Perform lookup according to params passed into concrete instance creation.
    virtual NThreading::TFuture<TLookupResultCallback> Lookup(
        const TVector<NUdf::TUnboxedValue>& keys) = 0;

    // Get full table name (with cluster) for detailed error messages.
    virtual TString GetTableName() const = 0;
};

class IYtflowLookupProviderRegistry
{
public:
    struct TCreationContext
    {
        NKikimr::NMiniKQL::TRuntimeNode LookupSourceArgs;
        ERowSelectionMode LookupSourceRowSelectionMode;

        TVector<TString> StreamKeys;
        const NKikimr::NMiniKQL::TStructType* StreamRowType;

        TVector<TString> LookupSourceKeys;
        const NKikimr::NMiniKQL::TStructType* LookupSourceRowType;

        const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ComputationNodeFactoryContext;
        NYql::NUdf::IFunctionTypeInfoBuilder& FunctionTypeInfoBuilder;
    };

    using TCreationCallback = std::function<
        THolder<IYtflowLookupProvider>(TCreationContext& ctx)>;

    virtual ~IYtflowLookupProviderRegistry() = default;

    virtual void Register(const TString& providerName, TCreationCallback callback) = 0;

    virtual THolder<IYtflowLookupProvider> Create(
        const TString& providerName,
        TCreationContext& ctx) const = 0;
};

THolder<IYtflowLookupProviderRegistry> CreateYtflowLookupProviderRegistry();

} // namespace NYql
