#pragma once

#include <yql/essentials/sql/v1/node.h>

#include <util/generic/string.h>

namespace NKikimrSchemeOp {

class TStreamingQueryProperties;

} // namespace NKikimrSchemeOp

namespace NKikimr::NKqp {

class TStreamingQueryMeta {
public:
    struct TColumns {
        static inline constexpr char DatabaseId[] = "database_id";
        static inline constexpr char QueryPath[] = "query_path";
        static inline constexpr char State[] = "state";
    };

    // Properties which crated during query translation
    using TSqlSettings = NSQLTranslationV1::TStreamingQuerySettings;

    struct TProperties {
        static inline constexpr char Run[] = "run";
        static inline constexpr char ResourcePool[] = "resource_pool";
        static inline constexpr char Force[] = "force";
    };
};

// Used for properties parsing after describing streaming query
class TStreamingQuerySettings {
public:
    TStreamingQuerySettings& FromProto(const NKikimrSchemeOp::TStreamingQueryProperties& info);

public:
    TString QueryText;
    bool Run = false;
    TString ResourcePool;
};

}  // namespace NKikimr::NKqp
