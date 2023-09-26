#include "client_common.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

EWorkloadCategory FromUserWorkloadCategory(EUserWorkloadCategory category)
{
    switch (category) {
        case EUserWorkloadCategory::Realtime:
            return EWorkloadCategory::UserRealtime;
        case EUserWorkloadCategory::Interactive:
            return EWorkloadCategory::UserInteractive;
        case EUserWorkloadCategory::Batch:
            return EWorkloadCategory::UserBatch;
        default:
            YT_ABORT();
    }
}

} // namespace

TUserWorkloadDescriptor::operator TWorkloadDescriptor() const
{
    TWorkloadDescriptor result;
    result.Category = FromUserWorkloadCategory(Category);
    result.Band = Band;
    return result;
}

struct TSerializableUserWorkloadDescriptor
    : public TYsonStructLite
    , public TUserWorkloadDescriptor
{
    REGISTER_YSON_STRUCT_LITE(TSerializableUserWorkloadDescriptor);

    static void Register(TRegistrar registrar)
    {
        registrar.BaseClassParameter("category", &TThis::Category);
        registrar.BaseClassParameter("band", &TThis::Band)
            .Optional();
    }

public:
    static TThis Wrap(const TUserWorkloadDescriptor& source)
    {
        TThis result;
        result.Band = source.Band;
        result.Category = source.Category;
        return result;
    }

    TUserWorkloadDescriptor Unwrap()
    {
        TUserWorkloadDescriptor result;
        result.Band = Band;
        result.Category = Category;
        return result;
    }
};

void Serialize(const TUserWorkloadDescriptor& workloadDescriptor, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(TSerializableUserWorkloadDescriptor::Wrap(workloadDescriptor), consumer);
}

void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, INodePtr node)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    NYTree::Deserialize(serializableWorkloadDescriptor, node);
    workloadDescriptor = serializableWorkloadDescriptor.Unwrap();
}

void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, NYson::TYsonPullParserCursor* cursor)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    NYTree::Deserialize(serializableWorkloadDescriptor, cursor);
    workloadDescriptor = serializableWorkloadDescriptor.Unwrap();
}

////////////////////////////////////////////////////////////////////////////////

NRpc::TMutationId TMutatingOptions::GetOrGenerateMutationId() const
{
    if (Retry && !MutationId) {
        THROW_ERROR_EXCEPTION("Cannot execute retry without mutation id");
    }
    return MutationId ? MutationId : NRpc::GenerateMutationId();
}

////////////////////////////////////////////////////////////////////////////////

void TSerializableMasterReadOptions::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("read_from", &TThis::ReadFrom)
        .Default(TMasterReadOptions{}.ReadFrom);

    registrar.BaseClassParameter("disable_per_user_cache", &TThis::DisablePerUserCache)
        .Default(TMasterReadOptions{}.DisablePerUserCache);

    registrar.BaseClassParameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Default(TMasterReadOptions{}.ExpireAfterSuccessfulUpdateTime);

    registrar.BaseClassParameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Default(TMasterReadOptions{}.ExpireAfterFailedUpdateTime);

    registrar.BaseClassParameter("cache_sticky_group_size", &TThis::CacheStickyGroupSize)
        .Default(TMasterReadOptions{}.CacheStickyGroupSize);

    registrar.BaseClassParameter("enable_client_cache_stickiness", &TThis::EnableClientCacheStickiness)
        .Default(TMasterReadOptions{}.EnableClientCacheStickiness);

    registrar.BaseClassParameter("success_staleness_bound", &TThis::SuccessStalenessBound)
        .Default(TMasterReadOptions{}.SuccessStalenessBound);
}

////////////////////////////////////////////////////////////////////////////////

void TPrerequisiteRevisionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("revision", &TThis::Revision);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
