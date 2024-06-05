#pragma once

#include <ydb/library/accessor/accessor.h>
#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/folder/path.h>
#include <util/system/tls.h>
#include <util/system/spinlock.h>

namespace NYdbWorkload {

class TGeneratorStateProcessor {
public:
    struct TSourceState {
        ui64 Position = 0;
    };
    using TState = TMap<TString, TSourceState>;

public:
    TGeneratorStateProcessor(const TFsPath& path, bool clear);
    void AddPortion(const TString& source, ui64 from, ui64 size);
    void FinishPortions();
    YDB_READONLY_DEF(TState, State);

private:
    using TSourcePortion = std::pair<ui64, ui64>;
    struct TThreadSourceState {
        using TFinishedPortions = TDeque<TSourcePortion>;
        TFinishedPortions FinishedPortions;
    };

    class TSourceStateImpl {
    public:
        bool FinishPortion(ui64 from, ui64 size, ui64& position);

    private:
        TMap<ui64, TThreadSourceState> ThreadsState;
    };

    struct TInProcessPortion {
        TString Source;
        ui64 From;
        ui64 Size;
    };

private:
    void Load();
    void Save() const;

private:
    TFsPath Path;
    TFsPath TmpPath;
    TMap<TString, TSourceStateImpl> StateImpl;
    TAdaptiveLock Lock;
    Y_THREAD(TVector<TInProcessPortion>) InProcess;
};

}