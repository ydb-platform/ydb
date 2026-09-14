#include "load.h"

#include "type_def.h"
#include "private.h"
#include "schemas.h"

#include <library/cpp/iterator/zip.h>

namespace NYT::NPhoenix {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = PhoenixLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

bool AreFieldSchemasEquivalent(
    const TTypeDescriptor& lhsTypeDescriptor,
    const TTypeSchemaPtr& rhsTypeSchema)
{
    if (std::ssize(lhsTypeDescriptor.Fields()) != std::ssize(rhsTypeSchema->Fields)) {
        return false;
    }

    for (const auto& [lhsFieldDescriptor, rhsFieldSchema] : Zip(lhsTypeDescriptor.Fields(), rhsTypeSchema->Fields)) {
        if (lhsFieldDescriptor->GetTag() != rhsFieldSchema->Tag) {
            return false;
        }
    }

    return true;
}

std::unique_ptr<TUniverseLoadSchedule> ComputeUniverseLoadSchedule(const TUniverseSchemaPtr& loadUniverseSchema)
{
    YT_TLOG_DEBUG("Started computing universe load schedule");

    const auto& nativeUniverseDescriptor = ITypeRegistry::Get()->GetUniverseDescriptor();

    auto universeLoadSchedule = std::make_unique<TUniverseLoadSchedule>();
    bool hasCompatLoad = false;
    for (const auto& loadTypeSchema : loadUniverseSchema->Types) {
        auto typeTag = loadTypeSchema->Tag;
        const auto* nativeTypeDescriptor = nativeUniverseDescriptor.FindTypeDescriptorByTag(typeTag);
        if (!nativeTypeDescriptor) {
            YT_TLOG_DEBUG("Type is present in load schema but is missing in registry; skipped")
                .WithFormat("TypeTag", "%x", typeTag)
                .With("TypeName", loadTypeSchema->Name);
            continue;
        }

        auto loadTypeSchemaYson = ConvertToYsonString(loadTypeSchema);
        if (nativeTypeDescriptor->GetSchemaYson() == loadTypeSchemaYson) {
            YT_TLOG_DEBUG("Type schemas match; using native load")
                .With("TypeName", nativeTypeDescriptor->GetName());
            continue;
        }

        if (nativeTypeDescriptor->IsTemplate() != loadTypeSchema->Template) {
            THROW_ERROR_EXCEPTION("Type %v has different template flags in load schema and in native schema",
                nativeTypeDescriptor->GetName())
                .With("native_template", nativeTypeDescriptor->IsTemplate())
                .With("load_template", loadTypeSchema->Template);
        }

        if (loadTypeSchema->BaseTypeTags != nativeTypeDescriptor->BaseTypeTags()) {
            THROW_ERROR_EXCEPTION("Type %v has different base types in load schema and in native schema",
                nativeTypeDescriptor->GetName())
                .With("native_base_type_tags", nativeTypeDescriptor->BaseTypeTags())
                .With("load_base_type_tags", loadTypeSchema->BaseTypeTags);
        }

        if (AreFieldSchemasEquivalent(*nativeTypeDescriptor, loadTypeSchema)) {
            YT_TLOG_DEBUG("Type schemas match; using native load")
                .With("TypeName", nativeTypeDescriptor->GetName());
            continue;
        }

        TTypeLoadSchedule typeLoadSchedule;

        THashSet<TFieldTag> allFieldTags;
        allFieldTags.reserve(nativeTypeDescriptor->Fields().size());

        THashSet<TFieldTag> missingFieldTags;
        missingFieldTags.reserve(nativeTypeDescriptor->Fields().size());

        for (const auto& fieldDescriptor : nativeTypeDescriptor->Fields()) {
            InsertOrCrash(allFieldTags, fieldDescriptor->GetTag());
            InsertOrCrash(missingFieldTags, fieldDescriptor->GetTag());
        }

        for (const auto& fieldSchema : loadTypeSchema->Fields) {
            auto fieldTag = fieldSchema->Tag;
            if (!allFieldTags.contains(fieldTag)) {
                THROW_ERROR_EXCEPTION("Load schema contains unknown field %v::#%v",
                    nativeTypeDescriptor->GetName(),
                    fieldTag);
            }
            typeLoadSchedule.LoadFieldTags.push_back(fieldSchema->Tag);
            missingFieldTags.erase(fieldTag);
        }

        typeLoadSchedule.MissingFieldTags = {missingFieldTags.begin(), missingFieldTags.end()};

        YT_TLOG_DEBUG("Type schemas do not match; using compat load")
            .With("TypeName", nativeTypeDescriptor->GetName())
            .With("LoadFieldTags", typeLoadSchedule.LoadFieldTags)
            .With("MissingFieldTags", typeLoadSchedule.MissingFieldTags);

        EmplaceOrCrash(universeLoadSchedule->LoadScheduleMap, typeTag, std::move(typeLoadSchedule));
        hasCompatLoad = true;
    }

    YT_TLOG_DEBUG("Finished computing universe load schedule")
        .With("HasCompatLoad", hasCompatLoad);

    return hasCompatLoad ? std::move(universeLoadSchedule) : nullptr;
}

const TTypeLoadSchedule* TUniverseLoadSchedule::FindTypeLoadSchedule(TTypeTag tag)
{
    auto it = LoadScheduleMap.find(tag);
    return it == LoadScheduleMap.end() ? nullptr : &it->second;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

using namespace NDetail;

TLoadSessionGuard::TLoadSessionGuard(const TUniverseSchemaPtr& schema)
{
    auto& loadState = *UniverseLoadState;
    YT_VERIFY(!std::exchange(loadState.Active, true));
    static std::atomic<TLoadEpoch::TUnderlying> LoadEpochCounter;
    loadState.Epoch = TLoadEpoch(++LoadEpochCounter);
    loadState.Schedule = ComputeUniverseLoadSchedule(schema);
    YT_TLOG_DEBUG("Load session started")
        .With("Epoch", loadState.Epoch);
}

TLoadSessionGuard::~TLoadSessionGuard()
{
    auto& loadState = *UniverseLoadState;
    YT_VERIFY(std::exchange(loadState.Active, false));
    loadState.Schedule.reset();
    YT_TLOG_DEBUG("Load session finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix
