#pragma once

#include "roles.h"
#include "types.h"

namespace NJson {
    class TJsonValue;
}

namespace NTvmAuth::NRoles {
    class TParser {
    public:
        static TRolesPtr Parse(TRawPtr decodedBlob);

    public:
        static TRolesPtr ParseImpl(TRawPtr decodedBlob);
        static TRoles::TMeta GetMeta(const NJson::TJsonValue& doc);

        template <typename Id>
        static THashMap<Id, TConsumerRolesPtr> GetConsumers(
            const NJson::TJsonValue& doc,
            TStringBuf key);

        static TConsumerRolesPtr GetConsumer(
            const NJson::TJsonValue& obj,
            TStringBuf consumer);
        static TEntitiesPtr GetEntities(
            const NJson::TJsonValue& obj,
            TStringBuf consumer,
            TStringBuf role);
        static TEntityPtr GetEntity(
            const NJson::TJsonValue& obj,
            TStringBuf consumer,
            TStringBuf role);
    };
}
