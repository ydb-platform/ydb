#include "hash_types.h"

namespace NLogin {

using namespace NLoginProto;

const TVector<THashTypeDescription> THashTypesRegistry::HashTypeDescriptions = {
    { .Class = EHashClass::Argon, .Type = EHashType::Argon, .Name = "argon2id", .SaltSize = 16, .HashSize = 32, .IsEmptyPasswordAllowed = true },
    { .Class = EHashClass::Scram, .Type = EHashType::ScramSha256, .Name = "scram-sha-256", .IterationsCount = 4096, .SaltSize = 16, .HashSize = 32, .IsEmptyPasswordAllowed = false },
};

THashTypesRegistry::THashTypesRegistry() {
    HashTypesMap.reserve(HashTypeDescriptions.size());
    HashNamesMap.reserve(HashTypeDescriptions.size());
    for (const auto& record : HashTypeDescriptions) {
        HashTypesMap.emplace(record.Type, record);
        HashNamesMap.emplace(record.Name, record);
    }
}

const THashTypesRegistry HashesRegistry;

} // namespace NLogin
