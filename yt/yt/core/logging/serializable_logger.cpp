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
        Save(context, TString(Category_->Name));
    } else {
        Save(context, false);
    }

    Save(context, Essential_);
    Save(context, MinLevel_);
    Save(context, Tag_);
    TVectorSerializer<TTupleSerializer<TStructuredTag, 2>>::Save(context, StructuredTags_);
}

void TSerializableLogger::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    TString categoryName;

    bool categoryPresent = false;
    Load(context, categoryPresent);
    if (categoryPresent) {
        Load(context, categoryName);
        LogManager_ = GetDefaultLogManager();
        Category_ = LogManager_->GetCategory(categoryName.data());
    } else {
        Category_ = nullptr;
    }

    Load(context, Essential_);
    Load(context, MinLevel_);
    Load(context, Tag_);
    TVectorSerializer<TTupleSerializer<TStructuredTag, 2>>::Load(context, StructuredTags_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
