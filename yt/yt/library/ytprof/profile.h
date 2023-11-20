#pragma once

#include <util/stream/fwd.h>

#include <yt/yt/library/ytprof/proto/profile.pb.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void WriteProfile(IOutputStream* out, const NProto::Profile& profile);

TString SerializeProfile(const NProto::Profile& profile);

void ReadProfile(IInputStream* in, NProto::Profile* profile);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
