#pragma once

#include <util/stream/fwd.h>

#include <yt/yt/library/ytprof/proto/profile.pb.h>

#include <tcmalloc/malloc_extension.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

//! Reads a gzip-compressed profile from a stream.
void ReadCompressedProfile(IInputStream* in, NProto::Profile* profile);

//! Writes a gzip-compressed profile to a stream.
void WriteCompressedProfile(IOutputStream* out, const NProto::Profile& profile);

//! Converts a TCMalloc profile to a protobuf format.
//! This invokes symbolizer.
NProto::Profile TCMallocProfileToProtoProfile(const tcmalloc::Profile& tcmallocProfile);

//! Captures a heap profile of the given type and converts it to a protobuf format.
NProto::Profile CaptureHeapProfile(tcmalloc::ProfileType profileType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
