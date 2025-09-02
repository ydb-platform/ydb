#include "serializable_logger.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

TSerializableLogger::TSerializableLogger(TLogger logger)
    : TLogger(std::move(logger))
{ }

void TSerializableLogger::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    if (Category_) {
        Save(context, true);
        Save(context, Category_->Name);
    } else {
        Save(context, false);
    }

    Save(context, Essential_);
    Save(context, MinLevel_);

    Save(context, GetTag());
    TVectorSerializer<TTupleSerializer<TStructuredTag, 2>>::Save(context, GetStructuredTags());
}

void TSerializableLogger::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    if (Load<bool>(context)) {
        auto categoryName = Load<std::string>(context);
        LogManager_ = GetDefaultLogManager();
        Category_ = LogManager_->GetCategory(categoryName);
    } else {
        Category_ = nullptr;
    }

    Load(context, Essential_);
    Load(context, MinLevel_);

    TCoWState state;
    Load<std::string>(context, state.Tag);
    TVectorSerializer<TTupleSerializer<TStructuredTag, 2>>::Load(context, state.StructuredTags);
    if (state.Tag.empty() && state.StructuredTags.empty()) {
        ResetCoWState();
    } else {
        *GetMutableCoWState() = std::move(state);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
