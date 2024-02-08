#include <iostream>
#include <functional>

#include <util/system/compiler.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/string/join.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

#include <library/cpp/protobuf/json/util.h>

#include <ydb/core/protos/config.pb.h>

using namespace NProtoBuf;

using TOnEntryFn = std::function<void(const Descriptor*, const TDeque<const Descriptor*>&, const TDeque<const FieldDescriptor*>&, const FieldDescriptor*, ssize_t)>;

ssize_t FindLoop(TDeque<const Descriptor*>& typePath, const Descriptor* child) {
    for (ssize_t i = 0; i < (ssize_t)typePath.size(); ++i) {
        if (typePath[i] == child) {
            return i;
        }
    }
    return -1;
}

void Traverse(const Descriptor* d, TDeque<const Descriptor*>& typePath, TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, TOnEntryFn onEntry) {
    ssize_t loop = FindLoop(typePath, d);

    Y_ABORT_IF(!d && !field, "Either field or descriptor must be defined");

    onEntry(d, typePath, fieldPath, field, loop);

    if (!d || loop != -1) {
        return;
    }

    typePath.push_back(d);

    for (int i = 0; i < d->field_count(); ++i) {
        const FieldDescriptor* fieldDescriptor = d->field(i);
        fieldPath.push_back(fieldDescriptor);
        Traverse(fieldDescriptor->message_type(), typePath, fieldPath, fieldDescriptor, onEntry);
        fieldPath.pop_back();
    }

    typePath.pop_back();
}

void Traverse(TOnEntryFn onEntry) {
    auto& inst = NKikimrConfig::TAppConfig::default_instance();
    const Descriptor* descriptor = inst.GetDescriptor();

    TDeque<const Descriptor*> typePath;
    TDeque<const FieldDescriptor*> fieldPath;
    fieldPath.push_back(nullptr);
    Traverse(descriptor, typePath, fieldPath, nullptr, onEntry);
    fieldPath.pop_back();
}

class TDumper {
    TString PrintLabel(const FieldDescriptor& field) const {
        switch (field.label()) {
            case FieldDescriptor::LABEL_REQUIRED:
                return UseColors ? "\033[0;31mrequired\033[0m" : "required";
            case FieldDescriptor::LABEL_OPTIONAL:
                return "optional";
            case FieldDescriptor::LABEL_REPEATED:
                return "repeated";
            default:
                Y_ABORT("unexpected");
        }
    }

    TString Loop() const {
        return UseColors ? " \033[0;31m// <- loop\033[0m" : " <- loop";
    }

    TString FieldName(const FieldDescriptor* field) const {
        TString name = field->name();
        if (UseYamlNames) {
            NProtobufJson::ToSnakeCaseDense(&name);
        }
        return name;
    }

    TString RootName() const {
        return UseYamlNames ? "app_config" : "AppConfig";
    }

    TString DescriptorName(const Descriptor* d) const {
        return UseFullyQualifiedTypes ? d->full_name() : d->name();
    }

    TString ConstructFieldPath(const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t from = 1, ssize_t to = -1) {
        to = to == -1 ? fieldPath.size() : to;
        TVector<TString> path;
        path.push_back(TString("/") + RootName());
        for (ssize_t i = from; i < to; ++i) {
            TString fieldName = FieldName(fieldPath[i]) + (fieldPath[i]->is_repeated() ? "[]" : "");
            path.push_back(fieldName);
        }
        path.push_back(FieldName(field));

        return JoinSeq("/", path);
    }

public:
    void DumpFullTree() {
        Traverse([this](const Descriptor* d, const TDeque<const Descriptor*>& typePath, const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t loop) {
            Y_UNUSED(fieldPath);
            std::cout << TString(typePath.size() * 4, ' ') <<
                    (field ? PrintLabel(*field) : "")
                    << " " << (d ? DescriptorName(d) : field->cpp_type_name())
                    << " " << (field ? FieldName(field) : RootName())
                    << " = " << (field ? field->number() : 0) << ";"
                    << (loop != -1 ? Loop() : "")  << std::endl;
        });
    }

    void DumpLoops() {
        Traverse([this](const Descriptor* d, const TDeque<const Descriptor*>& typePath, const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t loop) {
            if (loop != -1) {
                std::cout << "Loop at \"" << ConstructFieldPath(fieldPath, field, 1, loop + 1) << "\", types chain: ";

                TVector<TString> path;
                for (size_t i = loop; i < typePath.size(); ++i) {
                    path.push_back(DescriptorName(typePath[i]));
                }
                path.push_back(DescriptorName(d));
                std::cout << JoinSeq(" -> ", path) << std::endl;
            }
        });

    }

    void DumpRequired() {
        TMap<const FileDescriptor*, TMap<const Descriptor*, TMap<const FieldDescriptor*, TSet<const TString>>>> fields;
        Traverse([this, &fields](const Descriptor* d, const TDeque<const Descriptor*>& typePath, const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t loop) {
            Y_UNUSED(d, loop);
            if (field && field->is_required()) {
                fields[typePath.back()->file()][typePath.back()][field].insert(ConstructFieldPath(fieldPath, field));
            }
        });
        for (const auto& [file, typeToField] : fields) {
            for (const auto& [type, fieldToPaths] : typeToField) {
                for (const auto& [field, paths] : fieldToPaths) {
                    SourceLocation sl;
                    bool hasSourceInfo = field->GetSourceLocation(&sl);
                    std::cout << "Required field \"" << FieldName(field) << "\" in "
                              << DescriptorName(type) << " at $/" << file->name()
                              << ":" << (hasSourceInfo ? sl.start_line + 1 : 0)
                              << "\n    it can be accessed in config by following paths:" << std::endl;
                    for (const auto& path : paths) {
                        std::cout << TString(8, ' ') << path << std::endl;
                    }
                }
            }
        }
    }

    void DumpJsonScheme() {

    }

    bool UseYamlNames = false;
    bool UseColors = true;
    bool UseFullyQualifiedTypes = true;
};

auto main(int argc, char *argv[]) -> int {
    Y_UNUSED(argc, argv);

    TDumper dumper;

    /*
    dumper.DumpFullTree();
    dumper.DumpLoops();
    */
    dumper.DumpRequired();

    return 0;
}
