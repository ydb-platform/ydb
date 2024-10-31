#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/src_location.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <google/protobuf/descriptor.pb.h>

#include <jinja2cpp/template_env.h>
#include <jinja2cpp/template.h>
#include <jinja2cpp/value.h>
#include <jinja2cpp/reflected_value.h>
#include <cstdint>
#include <string>
#include <vector>

struct TSlot;
struct TField;

struct TSlot {
    TString Name;
    uint64_t Index;
    std::vector<const TField*> Fields;
    uint64_t DefaultValue = 0;
    uint64_t RuntimeFlagsMask = 0;

    TSlot(const TString& name, size_t index)
        : Name(name)
        , Index(index)
    {}
};

struct TField {
    TString Name;
    const TSlot* Slot;
    uint64_t HasMask = 0;
    uint64_t ValueMask = 0;
    uint64_t FullMask = 0;
    uint64_t DefaultValue = 0;
    bool IsRuntime = false;

    TField(const TString& name, const TSlot* slot)
        : Name(name)
        , Slot(slot)
    {}
};

namespace jinja2 {

    template<>
    struct TypeReflection<TSlot> : TypeReflected<TSlot> {
        static const auto& GetAccessors() {
            static std::unordered_map<std::string, FieldAccessor> accessors = {
                {"name", [](const TSlot& slot) { return Reflect(std::string(slot.Name)); }},
                {"index", [](const TSlot& slot) { return Reflect(slot.Index); }},
                {"fields", [](const TSlot& slot) { return Reflect(slot.Fields); }},
                {"default_value", [](const TSlot& slot) {
                    return Reflect(std::string(TStringBuilder() << slot.DefaultValue));
                }},
                {"runtime_flags_mask", [](const TSlot& slot) {
                    return Reflect(std::string(TStringBuilder() << slot.RuntimeFlagsMask));
                }},
            };
            return accessors;
        }
    };

    template<>
    struct TypeReflection<TField> : TypeReflected<TField> {
        static const auto& GetAccessors() {
            static std::unordered_map<std::string, FieldAccessor> accessors = {
                {"name", [](const TField& field) { return Reflect(std::string(field.Name)); }},
                {"slot", [](const TField& field) { return Reflect(field.Slot); }},
                {"has_mask", [](const TField& field) {
                    return Reflect(std::string(TStringBuilder() << field.HasMask));
                }},
                {"value_mask", [](const TField& field) {
                    return Reflect(std::string(TStringBuilder() << field.ValueMask));
                }},
                {"full_mask", [](const TField& field) {
                    return Reflect(std::string(TStringBuilder() << field.FullMask));
                }},
                {"default_value", [](const TField& field) {
                    return Reflect(std::string(TStringBuilder() << field.DefaultValue));
                }},
                {"is_runtime", [](const TField& field) { return Reflect(field.IsRuntime); }},
            };
            return accessors;
        }
    };

} // namespace jinja2

int main(int argc, char** argv) {
    if (argc < 3) {
        Cerr << "Usage: " << argv[0] << " INPUT OUTPUT ..." << Endl;
        return 1;
    }

    std::deque<TSlot> slots;
    std::deque<TField> fields;
    std::vector<const TSlot*> jinjaSlots;
    std::vector<const TField*> jinjaFields;

    TSlot* slot = nullptr;
    int currentBits = 0;

    const auto* d = NKikimrConfig::TFeatureFlags::descriptor();
    for (int fieldIndex = 0; fieldIndex < d->field_count(); ++fieldIndex) {
        const auto* protoField = d->field(fieldIndex);
        if (protoField->type() != google::protobuf::FieldDescriptor::TYPE_BOOL) {
            continue;
        }
        if (!slot || currentBits + 2 > 64) {
            size_t index = slots.size();
            TString name = TStringBuilder() << "slot" << index;
            slot = &slots.emplace_back(name, index);
            currentBits = 0;
            jinjaSlots.push_back(slot);
        }
        int shift = currentBits;
        currentBits += 2;
        TField* field = &fields.emplace_back(protoField->name(), slot);
        jinjaFields.push_back(field);
        field->HasMask = 1ULL << shift;
        field->ValueMask = 2ULL << shift;
        field->FullMask = 3ULL << shift;
        if (protoField->default_value_bool()) {
            field->DefaultValue = field->ValueMask;
        }
        field->IsRuntime = !protoField->options().GetExtension(NKikimrConfig::RequireRestart);

        slot->Fields.push_back(field);
        slot->DefaultValue |= field->DefaultValue;
        if (field->IsRuntime) {
            slot->RuntimeFlagsMask |= field->FullMask;
        }
    }

    jinja2::TemplateEnv env;
    env.AddGlobal("generator", jinja2::Reflect(std::string(__SOURCE_FILE__)));
    env.AddGlobal("slots", jinja2::Reflect(jinjaSlots));
    env.AddGlobal("fields", jinja2::Reflect(jinjaFields));

    for (int i = 1; i < argc; i += 2) {
        if (!(i + 1 < argc)) {
            Cerr << "ERROR: missing output for " << argv[i] << Endl;
            return 1;
        }

        jinja2::Template t(&env);
        auto loaded = t.Load(TFileInput(argv[i]).ReadAll(), argv[i]);
        if (!loaded) {
            Cerr << "ERROR: " << loaded.error().ToString() << Endl;
            return 1;
        }

        auto rendered = t.RenderAsString({});
        if (!rendered) {
            Cerr << "ERROR: " << rendered.error().ToString() << Endl;
            return 1;
        }

        TFileOutput(argv[i + 1]).Write(rendered.value());
        Cout << "Generated " << argv[i + 1] << " from " << argv[i] << Endl;
    }

    return 0;
}
