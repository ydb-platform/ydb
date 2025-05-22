#pragma once

#include <ydb/library/actors/util/rope.h>
#include <yql/essentials/utils/chunked_buffer.h>

namespace NYql {

TRope MakeReadOnlyRope(TChunkedBuffer&& buffer);
TRope MakeReadOnlyRope(const TChunkedBuffer& buffer);

TChunkedBuffer MakeChunkedBuffer(TRope&& rope);
TChunkedBuffer MakeChunkedBuffer(const TRope& rope);

}