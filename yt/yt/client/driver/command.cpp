#include "command.h"
#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NDriver {

using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void ProduceOutput(
    ICommandContextPtr context,
    const std::function<void(IYsonConsumer*)>& producer)
{
    TStringStream stream;
    TYsonWriter writer(&stream, EYsonFormat::Binary);
    producer(&writer);
    writer.Flush();
    context->ProduceOutputValue(TYsonString(stream.Str()));
}

void ProduceEmptyOutput(ICommandContextPtr context)
{
    switch (context->GetConfig()->ApiVersion) {
        case ApiVersion3:
            break;
        default:
            context->ProduceOutputValue(NYTree::BuildYsonStringFluently().BeginMap().EndMap());
            break;
    }
}

void ProduceSingleOutput(
    ICommandContextPtr context,
    TStringBuf name,
    const std::function<void(IYsonConsumer*)>& producer)
{
    switch (context->GetConfig()->ApiVersion) {
        case ApiVersion3:
            ProduceOutput(context, producer);
            break;
        default:
            ProduceOutput(context, [&] (IYsonConsumer* consumer) {
                NYTree::BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item(name).Do([&] (auto fluent) {
                            producer(fluent.GetConsumer());
                        })
                    .EndMap();
            });
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCommandBase::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Keep);
}

void TCommandBase::Execute(ICommandContextPtr context)
{
    const auto& request = context->Request();
    Logger.AddTag("RequestId: %" PRIx64 ", User: %v",
        request.Id,
        request.AuthenticatedUser);
    Deserialize(*this, request.Parameters);

    if (!HasResponseParameters()) {
        ProduceResponseParameters(context, /* producer */ {});
    }

    DoExecute(context);
}

bool TCommandBase::HasResponseParameters() const
{
    return false;
}

void TCommandBase::ProduceResponseParameters(
    ICommandContextPtr context,
    const std::function<void(IYsonConsumer*)>& producer)
{
    if (producer) {
        YT_VERIFY(HasResponseParameters());
        producer(context->Request().ResponseParametersConsumer);
    }
    if (context->Request().ResponseParametersFinishedCallback) {
        context->Request().ResponseParametersFinishedCallback();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
