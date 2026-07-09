#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

//! Default handling of a direct placement transfer for receivers that do not
//! implement DPT themselves.
/*!
 *  Allocates plain buffers sized per #transfer->GetExpectedBufferSizes(), drives #transfer to completion, splices the
 *  fetched parts onto #message, and re-invokes #handler->HandleMessage with the
 *  reassembled (whole) message and a null transfer. If the transfer fails, the
 *  message is dropped.
 *
 *  Intended to be called from within an #IMessageHandler::HandleMessage override
 *  upon receiving a non-null #transfer.
 */
void MaterializeTransferAndReinvoke(
    IMessageHandlerPtr handler,
    TSharedRefArray message,
    IBusPtr replyBus,
    IDirectPlacementTransferPtr transfer,
    TPacketId packetId = TPacketId());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
