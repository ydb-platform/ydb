#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>
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
    void FinishPortion(const TString& source, ui64 from, ui64 size);
    YDB_READONLY_DEF(TState, State);

private:
    using TSourcePortion = std::pair<ui64, ui64>;
    class TSourceStateImpl {
    public:
        bool FinishPortion(ui64 from, ui64 size, ui64& position);

    private:
        TList<TSourcePortion> FinishedPortions;
    };

private:
    void Load();
    void Save() const;

private:
    TFsPath Path;
    TFsPath TmpPath;
    TMap<TString, TSourceStateImpl> StateImpl;
    TAdaptiveLock Lock;
};

class TDataPortionWithState: public IBulkDataGenerator::TDataPortion {
public:
    template<class T>
    TDataPortionWithState(TGeneratorStateProcessor* stateProcessor, const TString& table, const TString& stateSource, T&& data, ui64 position, ui64 size)
        : TDataPortion(table, std::move(data), size)
        , StateSource(stateSource)
        , Position(position)
        , StateProcessor(stateProcessor)
    {}

    virtual void SetSendResult(const NYdb::TStatus& status) {
        if (StateProcessor && status.IsSuccess()) {
            StateProcessor->FinishPortion(StateSource, Position, GetSize());
        }
    }
private:
    TString StateSource;
    ui64 Position;
    TGeneratorStateProcessor* StateProcessor;
};

}