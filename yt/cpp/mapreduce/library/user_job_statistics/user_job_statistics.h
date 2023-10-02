#pragma once

#include <util/stream/file.h>
#include <util/generic/hash.h>
#include <util/datetime/cputimer.h>

namespace NYtTools {
    class TTimeStat;
    using TTimeStatHolder = THolder<TTimeStat>;

    class TUserJobStatsProxy {
    public:
        static const FHANDLE JobStatisticsHandle;
    private:
        THolder<IOutputStream> FetchedOut;
        IOutputStream* UsingStream = &Cerr;
    public:
        // TODO: add inheritance
        THashMap<TString, i64> Stats;//will be dumped in CommitStats or desctructor
        THashMap<TString, TDuration> TimeStats;//will be dumped in CommitStats or desctructor

        TUserJobStatsProxy() { Init(nullptr); }
        ~TUserJobStatsProxy() {
            CommitStats();
        }
        TUserJobStatsProxy (IOutputStream* usingStream) {Init(usingStream);}

        void Init(IOutputStream* usingStream);
        void InitChecked(IOutputStream* ifNotInJob);
        void InitIfNotInited(IOutputStream* usingStream);
        IOutputStream* GetStream() const { return UsingStream; }
        void CommitStats();
        void WriteStat(TString name, i64 val); //immidiatly wirtes stat
        void WriteStatNoFlush(TString name, i64 val); //immidiatly wirtes stat but do not flush it

        //@param name                name of statistic to be written in millisecs from creation to destruction
        //@param commitOnFinish      if false: will update state/write on job finish; if true: write stat in destructor
        TTimeStatHolder TimerStart(TString name, bool commitOnFinish = false);
    };

    class TTimeStat {
        TUserJobStatsProxy* Parent;
        TString Name;
        bool Commit;

        TTimeStat(TUserJobStatsProxy* parent, TString name, bool commit);
        friend class TUserJobStatsProxy;

        TSimpleTimer Timer;
    public:
        ~TTimeStat();
        TDuration Get() const {
            return Timer.Get();
        }
        void Cancel();
        void Finish();
    };
}
