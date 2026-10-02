#include "arrow_row_stream_decoder.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NArrow {

using namespace NApi::NRpcProxy;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

IRowStreamDecoderPtr CreateArrowRowStreamDecoder(
    TTableSchemaPtr /*schema*/,
    TNameTablePtr /*nameTable*/)
{
    THROW_ERROR_EXCEPTION("Arrow decoder is not implemented yet");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow

