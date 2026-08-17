#pragma once

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <optional>

namespace NActors::NCGroupDetail {

    inline std::optional<TString> TryReadFile(const TFsPath& path) {
        try {
            return TFileInput(path.GetPath()).ReadAll();
        } catch (...) {
            return std::nullopt;
        }
    }

    inline std::optional<ui64> TryReadUInt(const TFsPath& path) {
        auto content = TryReadFile(path);
        if (!content) {
            return std::nullopt;
        }

        ui64 value = 0;
        if (!TryFromString(StripString(*content), value)) {
            return std::nullopt;
        }
        return value;
    }

    template<class TCallback>
    void ParseKeyValueFile(const TString& content, TCallback&& callback) {
        TStringInput input(content);
        TString line;
        while (input.ReadLine(line)) {
            TVector<TString> fields;
            StringSplitter(line).Split(' ').SkipEmpty().Collect(&fields);
            if (fields.size() != 2) {
                continue;
            }

            ui64 value = 0;
            if (TryFromString(fields[1], value)) {
                callback(TStringBuf(fields[0]), value);
            }
        }
    }

    inline TFsPath MakeCGroupDirectory(const TString& root, const TString& cGroupPath) {
        if (cGroupPath == "/") {
            return TFsPath(root);
        }
        return TFsPath(root) / cGroupPath.substr(1);
    }

} // namespace NActors::NCGroupDetail
