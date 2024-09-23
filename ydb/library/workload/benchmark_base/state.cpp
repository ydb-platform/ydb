#include "state.h"
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>
#include <util/stream/file.h>
#include <thread>

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

void TGeneratorStateProcessor::AddPortion(const TString& source, ui64 from, ui64 size) {
    InProcess.Get().emplace_back(TInProcessPortion{source, from, size});
}

void TGeneratorStateProcessor::FinishPortions() {
    bool needSave = false;
    auto g = Guard(Lock);
    for (const auto& p: InProcess.Get()) {
        needSave |= StateImpl[p.Source].FinishPortion(p.From, p.Size, State[p.Source].Position);
    }
    InProcess.Get().clear();
    if (needSave) {
        Save();
    }
}

bool TGeneratorStateProcessor::TSourceStateImpl::FinishPortion(ui64 from, ui64 size, ui64& position) {
    const ui64 threadId = std::hash<std::thread::id>()(std::this_thread::get_id());
    ThreadsState[threadId].FinishedPortions.emplace_back(TSourcePortion{from, size});
    TVector<TThreadSourceState::TFinishedPortions*> portions;
    for (auto& [t, ss]: ThreadsState) {
        while (!ss.FinishedPortions.empty() && ss.FinishedPortions.front().first < position) {
            ss.FinishedPortions.pop_front();
        }
        if (!ss.FinishedPortions.empty()) {
            portions.push_back(&ss.FinishedPortions);
        }
    }
    Y_VERIFY(!portions.empty());
    auto portionsCmp = [](auto l, auto r) {return l->front().first > r->front().first;};
    std::make_heap(portions.begin(), portions.end(), portionsCmp);
    bool result = false;
    while (!portions.empty() && portions.front()->front().first == position) {
        result = true;
        position += portions.front()->front().second;
        std::pop_heap(portions.begin(), portions.end(), portionsCmp);
        portions.back()->pop_front();
        if (portions.back()->empty()) {
            portions.pop_back();
        } else {
            std::push_heap(portions.begin(), portions.end(), portionsCmp);
        }
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
