#include "helpers.h"

#include "direct_placement_transfer.h"
#include "message_handler.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TMaterializedTransferBufferTag
{ };

void MaterializeTransferAndReinvoke(
    IMessageHandlerPtr handler,
    TSharedRefArray message,
    IBusPtr replyBus,
    IDirectPlacementTransferPtr transfer)
{
    auto bufferSizes = transfer->GetExpectedBufferSizes();
    std::vector<TSharedMutableRef> buffers;
    buffers.reserve(bufferSizes.size());
    for (auto size : bufferSizes) {
        // Sizes are non-negative (a null part reports size zero); the transfer
        // restores null parts as null refs.
        buffers.push_back(TSharedMutableRef::Allocate<TMaterializedTransferBufferTag>(
            size,
            {.InitializeStorage = false}));
    }

    transfer->Run(std::move(buffers))
        .AsUnique()
        .Subscribe(BIND([
            handler = std::move(handler),
            message = std::move(message),
            replyBus = std::move(replyBus)
        ] (TErrorOr<std::vector<TSharedRef>>&& partsOrError) {
            if (!partsOrError.IsOK()) {
                // The transfer failed; there is nothing to deliver.
                return;
            }

            // The whole message is the eager parts followed by the directly-placed ones.
            auto& transferredParts = partsOrError.Value();
            TSharedRefArrayBuilder builder(message.Size() + transferredParts.size());
            for (const auto& part : message) {
                builder.Add(part);
            }
            for (auto& part : transferredParts) {
                builder.Add(std::move(part));
            }

            handler->HandleMessage(builder.Finish(), std::move(replyBus));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
