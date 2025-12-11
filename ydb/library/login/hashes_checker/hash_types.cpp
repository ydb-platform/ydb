#include "hash_types.h"

namespace NLogin {

const TVector<THashTypeDescription> HashesRegistry = {
    { .Class = EHashClass::Argon, .Type = EHashType::Argon, .Name = "argon2id", .SaltSize = 16, .HashSize = 32, .IsNullPasswordAllowed = true },
    { .Class = EHashClass::Scram, .Type = EHashType::ScramSha256, .Name = "scram-sha-256", .IterationsCount = 4096, .SaltSize = 16, .HashSize = 32, .IsNullPasswordAllowed = false },
};

} // namespace NLogin
