#pragma once

#include "public.h"

#include <util/generic/buffer.h>

#include <util/stream/file.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! Base interface for compression codec in logs. It is different from ICodec in yt/yt/core/compression,
//! as we must have the possibility to repair the corrupted log file if the process died unexpectedly.
/*!
 *  \note
 *  Thread affinity: any
 */
struct ILogCodec
    : public TRefCounted
{
    virtual i64 GetMaxBlockSize() const = 0;
    virtual void Compress(const TBuffer& input, TBuffer* output) = 0;
    virtual void AddSyncTag(i64 offset, TBuffer* output) = 0;
    virtual i64 Repair(TFile* file) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILogCodec)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
