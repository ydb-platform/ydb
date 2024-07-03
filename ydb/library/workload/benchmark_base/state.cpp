#include "state.h"
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

TGeneratorStateProcessor::TGeneratorStateProcessor(const TFsPath& path, bool clear)
    : Path(path)
{
    if (Path.IsDefined()) {
        TmpPath = path.Parent() / ("~" + path.Basename());
        if (!Path.Parent().Exists()) {
            Path.Parent().MkDirs();
        }
    }
    if (Path.Exists()) {
        if (clear) {
            Path.ForceDelete();
        } else {
            Load();
        }
    }
}

void TGeneratorStateProcessor::FinishPortion(const TString& source, ui64 from, ui64 size) {
    auto g = Guard(Lock);
    bool needSave = StateImpl[source].FinishPortion(from, size, State[source].Position);
    if (needSave) {
        Save();
    }
}

bool TGeneratorStateProcessor::TSourceStateImpl::FinishPortion(ui64 from, ui64 size, ui64& position) {
    auto i = FinishedPortions.begin();
    while (i != FinishedPortions.end() && i->first < from) {
         ++i;
    }
    FinishedPortions.emplace(i, TSourcePortion{from, size});
    bool result = false;
    while (!FinishedPortions.empty() && FinishedPortions.front().first < position) {
        FinishedPortions.pop_front();
    }
    while (!FinishedPortions.empty() && FinishedPortions.front().first == position) {
        result = true;
        position += FinishedPortions.front().second;
        FinishedPortions.pop_front();
    }
    return result;
}

void TGeneratorStateProcessor::Load() {
    TFileInput fi(Path);
    NJson::TJsonValue root;
    if (NJson::ReadJsonTree(&fi, &root)) {
        for (const auto& [source, state]: root["sources"].GetMap()) {
            auto& st = State[source];
            st.Position = state["position"].GetUIntegerRobust();
        }
    }
}

void TGeneratorStateProcessor::Save() const {
    if (Path.IsDefined() && !State.empty()) {
        NJson::TJsonValue root;
        for (const auto& [source, state]: State) {
            root["sources"][source]["position"] = state.Position;
        }
        {
            TFileOutput fo(TmpPath);
            NJson::WriteJson(&fo, &root, true, true, false);
        }
        TmpPath.RenameTo(Path);
    }
}

}
