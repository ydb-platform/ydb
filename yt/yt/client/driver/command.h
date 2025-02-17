#pragma once

#include "private.h"
#include "driver.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/sticky_transaction_pool.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

struct ICommand
{
    virtual ~ICommand() = default;
    virtual void Execute(ICommandContextPtr context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICommandContext
    : public virtual TRefCounted
{
    virtual const TDriverConfigPtr& GetConfig() const = 0;
    virtual const NApi::IClientPtr& GetClient() const = 0;
    virtual const NApi::IClientPtr& GetRootClient() const = 0;
    virtual NApi::IInternalClientPtr GetInternalClientOrThrow() const = 0;
    virtual const IDriverPtr& GetDriver() const = 0;

    virtual const TDriverRequest& Request() const = 0;

    virtual const NFormats::TFormat& GetInputFormat() = 0;
    virtual const NFormats::TFormat& GetOutputFormat() = 0;

    virtual void ProduceOutputValue(const NYson::TYsonString& yson) = 0;
    virtual NYson::TYsonString ConsumeInputValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICommandContext)

////////////////////////////////////////////////////////////////////////////////

//! Calls |context->ProduceOutputValue| with |TYsonString| created by the producer.
void ProduceOutput(
    ICommandContextPtr context,
    const std::function<void(NYson::IYsonConsumer*)>& producer);

//! Produces either nothing (v3) or empty map (>=v4).
void ProduceEmptyOutput(ICommandContextPtr context);

//! Run |producer| (v3) or open a map with key |name| and run |producer| then close map (>=v4).
void ProduceSingleOutput(
    ICommandContextPtr context,
    TStringBuf name,
    const std::function<void(NYson::IYsonConsumer*)>& producer);

//! Produces either |value| (v3) or map {|name|=|value|} (>=v4).
template <typename T>
void ProduceSingleOutputValue(
    ICommandContextPtr context,
    TStringBuf name,
    const T& value);

////////////////////////////////////////////////////////////////////////////////

class TCommandBase
    : public virtual NYTree::TYsonStructLite
    , public ICommand
{
protected:
    NLogging::TLogger Logger = DriverLogger();

    virtual void DoExecute(ICommandContextPtr ) { };
    virtual bool HasResponseParameters() const;

    void ProduceResponseParameters(
        ICommandContextPtr context,
        const std::function<void(NYson::IYsonConsumer*)>& producer);

    REGISTER_YSON_STRUCT_LITE(TCommandBase)

    static void Register(TRegistrar registrar);

public:
    void Execute(ICommandContextPtr context) override;
};

template <class TOptions>
class TTypedCommandBase
    : public TCommandBase
{
protected:
    TOptions Options;
};

template <class TOptions, class = void>
class TTransactionalCommandBase
{ };

template <class TOptions>
class TTransactionalCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTransactionalOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
    REGISTER_YSON_STRUCT_LITE(TTransactionalCommandBase);

    static void Register(TRegistrar registrar);

protected:
    NApi::ITransactionPtr AttachTransaction(
        ICommandContextPtr context,
        bool required);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TMutatingCommandBase
{ };

template <class TOptions>
class TMutatingCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TMutatingOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
    REGISTER_YSON_STRUCT_LITE(TMutatingCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TReadOnlyMasterCommandBase
{ };

template <class TOptions>
class TReadOnlyMasterCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TMasterReadOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
    REGISTER_YSON_STRUCT_LITE(TReadOnlyMasterCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TReadOnlyTabletCommandBase
{ };

template <class TOptions>
class TReadOnlyTabletCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTabletReadOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
    REGISTER_YSON_STRUCT_LITE(TReadOnlyTabletCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TSuppressableAccessTrackingCommandBase
{ };

template <class TOptions>
class TSuppressableAccessTrackingCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TSuppressableAccessTrackingOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
    REGISTER_YSON_STRUCT_LITE(TSuppressableAccessTrackingCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TPrerequisiteCommandBase
{ };

template <class TOptions>
class TPrerequisiteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TPrerequisiteOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
    REGISTER_YSON_STRUCT_LITE(TPrerequisiteCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions, class = void>
class TTimeoutCommandBase
{ };

template <class TOptions>
class TTimeoutCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TTimeoutOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
    REGISTER_YSON_STRUCT_LITE(TTimeoutCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletTransactionOptions
{
    NTransactionClient::TTransactionId TransactionId;
};

template <class TOptions, class = void>
class TTabletReadCommandBase
{ };

template <class TOptions>
class TTabletReadCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletTransactionOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
public:
    NApi::IClientBasePtr GetClientBase(ICommandContextPtr context);

    REGISTER_YSON_STRUCT_LITE(TTabletReadCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletWriteOptions
    : public TTabletTransactionOptions
{
    NTransactionClient::EAtomicity Atomicity;
    NTransactionClient::EDurability Durability;
};

struct TInsertRowsOptions
    : public TTabletWriteOptions
    , public NApi::TModifyRowsOptions
{ };

struct TDeleteRowsOptions
    : public TTabletWriteOptions
    , public NApi::TModifyRowsOptions
{ };

struct TLockRowsOptions
    : public TTabletTransactionOptions
{ };

template <class TOptions, class = void>
class TTabletWriteCommandBase
{ };

template <class TOptions>
class TTabletWriteCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, TTabletWriteOptions&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
private:
    REGISTER_YSON_STRUCT_LITE(TTabletWriteCommandBase);

    static void Register(TRegistrar registrar);

protected:
    NApi::ITransactionPtr GetTransaction(ICommandContextPtr context);
    bool ShouldCommitTransaction();
};

template <class TOptions, class = void>
class TSelectRowsCommandBase
{ };

template <class TOptions>
class TSelectRowsCommandBase<
    TOptions,
    typename std::enable_if_t<std::is_convertible_v<TOptions&, NApi::TSelectRowsOptionsBase&>>
>
    : public virtual TTypedCommandBase<TOptions>
{
private:
    REGISTER_YSON_STRUCT_LITE(TSelectRowsCommandBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
class TTypedCommand
    : public virtual TTypedCommandBase<TOptions>
    , public TTransactionalCommandBase<TOptions>
    , public TTabletReadCommandBase<TOptions>
    , public TTabletWriteCommandBase<TOptions>
    , public TMutatingCommandBase<TOptions>
    , public TReadOnlyMasterCommandBase<TOptions>
    , public TReadOnlyTabletCommandBase<TOptions>
    , public TSuppressableAccessTrackingCommandBase<TOptions>
    , public TPrerequisiteCommandBase<TOptions>
    , public TTimeoutCommandBase<TOptions>
    , public TSelectRowsCommandBase<TOptions>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

#define COMMAND_INL_H
#include "command-inl.h"
#undef COMMAND_INL_H
