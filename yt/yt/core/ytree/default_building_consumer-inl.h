#ifndef DEFAULT_BUILDING_CONSUMER_INL_H_
#error "Direct inclusion of this file is not allowed, include default_building_consumer.h"
// For the sake of sane code completion.
#include "default_building_consumer.h"
#endif

#include "ephemeral_node_factory.h"
#include "tree_builder.h"
#include "convert.h"

#include <yt/yt/core/yson/forwarding_consumer.h>

#include <memory>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
class TBuildingYsonConsumerViaTreeBuilder
    : public NYson::TForwardingYsonConsumer
    , public NYson::IBuildingYsonConsumer<T>
{
public:
    TBuildingYsonConsumerViaTreeBuilder(NYson::EYsonType ysonType)
        : TreeBuilder_(CreateBuilderFromFactory(GetEphemeralNodeFactory()))
        , YsonType_(ysonType)
    {
        TreeBuilder_->BeginTree();

        switch (YsonType_) {
            case NYson::EYsonType::ListFragment:
                TreeBuilder_->OnBeginList();
                break;
            case NYson::EYsonType::MapFragment:
                TreeBuilder_->OnBeginMap();
                break;
            default:
                break;
        }

        Forward(TreeBuilder_.get());
    }

    T Finish() override
    {
        switch (YsonType_) {
            case NYson::EYsonType::ListFragment:
                TreeBuilder_->OnEndList();
                break;
            case NYson::EYsonType::MapFragment:
                TreeBuilder_->OnEndMap();
                break;
            default:
                break;
        }

        T result = ConstructYTreeConvertibleObject<T>();
        Deserialize(result, TreeBuilder_->EndTree());
        return result;
    }

private:
    std::unique_ptr<ITreeBuilder> TreeBuilder_;
    NYson::EYsonType YsonType_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class T>
void CreateBuildingYsonConsumer(std::unique_ptr<NYson::IBuildingYsonConsumer<T>>* buildingConsumer, NYson::EYsonType ysonType)
{
    *buildingConsumer = std::unique_ptr<TBuildingYsonConsumerViaTreeBuilder<T>>(new TBuildingYsonConsumerViaTreeBuilder<T>(ysonType));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
