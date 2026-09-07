#pragma once

#include <library/cpp/threading/future/future.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <functional>
#include <memory>


namespace NKikimr::NMiniKQL {

class TTypeEnvironment;

} // namespace NKikimr::NMiniKQL

namespace NYql::NUdf {

class IFunctionTypeInfoBuilder;
class ISecureParamsProvider;
class IValueBuilder;

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
    class ILookupResult {
    public:
        virtual ~ILookupResult() = default;
    };

    using ILookupResultPtr = std::shared_ptr<const ILookupResult>;

    virtual ~IYtflowLookupProvider() = default;

    // Perform lookup using parameters captured during factory creation.
    virtual NThreading::TFuture<ILookupResultPtr> Lookup(
        const TVector<NUdf::TUnboxedValue>& keys) = 0;

    // Decode a ready transport result synchronously in the computation graph.
    virtual TVector<TVector<NUdf::TUnboxedValue>> Decode(
        const ILookupResultPtr& result) = 0;

    // Get full table name (with cluster) for detailed error messages.
    virtual TString GetTableName() const = 0;
};

// Created during computation-node construction and reused to create a provider
// whose mutable state belongs to one computation context.
class IYtflowLookupProviderFactory
{
public:
    struct TCreationContext
    {
        // ValueBuilder belongs to the target computation context and must
        // outlive the provider. FunctionTypeInfoBuilder is used only by Create().
        NYql::NUdf::IValueBuilder& ValueBuilder;
        NYql::NUdf::IFunctionTypeInfoBuilder& FunctionTypeInfoBuilder;
    };

    virtual ~IYtflowLookupProviderFactory() = default;

    virtual THolder<IYtflowLookupProvider> Create(
        const TCreationContext& ctx) const = 0;
};

class IYtflowLookupProviderRegistry
{
public:
    struct TFactoryCreationContext
    {
        NKikimr::NMiniKQL::TRuntimeNode LookupSourceArgs;
        ERowSelectionMode LookupSourceRowSelectionMode;

        TVector<TString> StreamKeys;
        const NKikimr::NMiniKQL::TStructType* StreamRowType;

        TVector<TString> LookupSourceKeys;
        const NKikimr::NMiniKQL::TStructType* LookupSourceRowType;

        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnvironment;
        const NYql::NUdf::ISecureParamsProvider* SecureParamsProvider;
    };

    using TFactoryCreationCallback = std::function<
        THolder<IYtflowLookupProviderFactory>(const TFactoryCreationContext& ctx)>;

    virtual ~IYtflowLookupProviderRegistry() = default;

    virtual void Register(const TString& providerName, TFactoryCreationCallback callback) = 0;

    virtual THolder<IYtflowLookupProviderFactory> CreateFactory(
        const TString& providerName,
        const TFactoryCreationContext& ctx) const = 0;
};

THolder<IYtflowLookupProviderRegistry> CreateYtflowLookupProviderRegistry();

} // namespace NYql
