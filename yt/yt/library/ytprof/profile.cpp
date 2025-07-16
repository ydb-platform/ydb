#include "profile.h"

#include "symbolize.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/memory/allocation_tags_hooks.h>

#include <util/stream/str.h>
#include <util/stream/zlib.h>

#include <util/string/join.h>

namespace NYT::NYTProf {

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ReadCompressedProfile(IInputStream* in, NProto::Profile* profile)
{
    TZLibDecompress decompress(in);
    profile->Clear();
    profile->ParseFromArcadiaStream(&decompress);
}

void WriteCompressedProfile(IOutputStream* out, const NProto::Profile& profile)
{
    TZLibCompress compress(out, ZLib::StreamType::GZip);
    profile.SerializeToArcadiaStream(&compress);
    compress.Finish();
}

NProto::Profile TCMallocProfileToProtoProfile(const tcmalloc::Profile& snapshot)
{
    NProto::Profile profile;
    profile.add_string_table();

    auto addString = [&] (const std::string& str) {
        auto index = profile.string_table_size();
        profile.add_string_table(ToProto(str));
        return index;
    };

    auto bytesUnitId = addString("bytes");

    {
        auto* sampleType = profile.add_sample_type();
        sampleType->set_type(addString("allocations"));
        sampleType->set_unit(addString("count"));
    }

    {
        auto* sampleType = profile.add_sample_type();
        sampleType->set_type(addString("space"));
        sampleType->set_unit(bytesUnitId);

        auto* periodType = profile.mutable_period_type();
        periodType->set_type(sampleType->type());
        periodType->set_unit(sampleType->unit());
    }

    profile.set_period(tcmalloc::MallocExtension::GetProfileSamplingInterval());

    auto allocatedSizeId = addString("allocated_size");
    auto requestedSizeId = addString("requested_size");
    auto requestedAlignmentId = addString("requested_alignment");

    THashMap<void*, ui64> locations;
    snapshot.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
        auto* protoSample = profile.add_sample();
        protoSample->add_value(sample.count);
        protoSample->add_value(sample.sum);

        auto* allocatedSizeLabel = protoSample->add_label();
        allocatedSizeLabel->set_key(allocatedSizeId);
        allocatedSizeLabel->set_num(sample.allocated_size);
        allocatedSizeLabel->set_num_unit(bytesUnitId);

        auto* requestedSizeLabel = protoSample->add_label();
        requestedSizeLabel->set_key(requestedSizeId);
        requestedSizeLabel->set_num(sample.requested_size);
        requestedSizeLabel->set_num_unit(bytesUnitId);

        auto* requestedAlignmentLabel = protoSample->add_label();
        requestedAlignmentLabel->set_key(requestedAlignmentId);
        requestedAlignmentLabel->set_num(sample.requested_alignment);
        requestedAlignmentLabel->set_num_unit(bytesUnitId);

        for (int i = 0; i < sample.depth; i++) {
            auto ip = sample.stack[i];

            auto it = locations.find(ip);
            if (it != locations.end()) {
                protoSample->add_location_id(it->second);
                continue;
            }

            auto locationId = locations.size() + 1;

            auto* location = profile.add_location();
            location->set_address(reinterpret_cast<ui64>(ip));
            location->set_id(locationId);

            protoSample->add_location_id(locationId);
            locations[ip] = locationId;
        }

        // TODO(gepardo): Deduplicate values in string table
        for (const auto& [key, value] : GetAllocationTagsHooks().ReadAllocationTags(sample.user_data)) {
            auto* label = protoSample->add_label();
            label->set_key(addString(key));
            label->set_str(addString(value));
        }
    });

    profile.set_drop_frames(addString(JoinSeq("|", {
        ".*SampleifyAllocation",
        ".*AllocSmall",
        "slow_alloc",
        "TBasicString::TBasicString",
    })));

    Symbolize(&profile, true);
    return profile;
}

NProto::Profile CaptureHeapProfile(tcmalloc::ProfileType profileType)
{
    auto snapshot = tcmalloc::MallocExtension::SnapshotCurrent(profileType);
    return TCMallocProfileToProtoProfile(snapshot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
