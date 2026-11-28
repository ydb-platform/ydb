#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>
#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/folder/path.h>
#include <util/system/tls.h>
#include <util/system/spinlock.h>

#include <optional>

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
    TState GetState() const;
    std::optional<ui64> GetPosition(const TString& source) const;

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
    TState State;
    mutable TAdaptiveLock Lock;
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