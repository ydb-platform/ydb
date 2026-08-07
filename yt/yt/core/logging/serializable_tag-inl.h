#ifndef SERIALIZABLE_TAG_INL_H_
#error "Direct inclusion of this file is not allowed, include serializable_tag.h"
// For the sake of sane code completion.
#include "serializable_tag.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
void TLoggingTagListSerializer::Save(C& context, const NLogging::TLoggingTagList& tags)
{
    NYT::Save(context, tags.GetPayload());
}

template <class C>
void TLoggingTagListSerializer::Load(C& context, NLogging::TLoggingTagList& tags)
{
    tags = NLogging::TLoggingTagList(NYT::Load<NLogging::TLoggingTagListPayload>(context));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
