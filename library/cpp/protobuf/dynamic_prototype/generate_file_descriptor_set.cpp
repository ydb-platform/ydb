#include "generate_file_descriptor_set.h"

#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

using namespace NProtoBuf;

FileDescriptorSet GenerateFileDescriptorSet(const Descriptor* desc) {
    TVector<const FileDescriptor*> flat;
    TQueue<const FileDescriptor*> descriptors;

    for (descriptors.push(desc->file()); !descriptors.empty(); descriptors.pop()) {
        const FileDescriptor* file = descriptors.front();

        for (int i = 0; i < file->dependency_count(); ++i) {
            descriptors.push(file->dependency(i));
        }

        flat.push_back(file);
    }

    FileDescriptorSet result;
    TSet<TString> visited;

    for (auto ri = flat.rbegin(); ri != flat.rend(); ++ri) {
        if (visited.insert((*ri)->name()).second == true) {
            (*ri)->CopyTo(result.add_file());
        }
    }

    return result;
}
