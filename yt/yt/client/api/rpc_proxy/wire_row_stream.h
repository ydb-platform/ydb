#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/wire_protocol.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IRowStreamEncoderPtr CreateWireRowStreamEncoder(NTableClient::TNameTablePtr nameTable);
IRowStreamDecoderPtr CreateWireRowStreamDecoder(
    NTableClient::TNameTablePtr nameTable,
    NTableClient::TWireProtocolOptions wireProtocolOptions = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
