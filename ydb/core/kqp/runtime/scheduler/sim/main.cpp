// The real compute scheduler driven by a simulation: one command per line on stdin, one reply per line on stdout.
// The clock, the random delays and the wake-ups of the tasks come from the simulation - no actor system.
//
// Commands (ids are given by the simulation, bytes and microseconds are integers):
//   limits <cpu> <bytes>                     the total limits of the node
//   pool <pool> <bytes | -1>                 a pool of the only database, -1 = no memory limit
//   query <qid> <pool> <elastic %>           a query and the memory of its tx (SpillingPercent)
//   query_end <qid>
//   task <tid> <qid>                         a schedulable task, woken up by the callback
//   task_end <tid>                           stops the task if it executes or is throttled - like PassAway
//   at <us>                                  sets the clock, never back
//   start <tid>                              -> ok | delay <us>
//   stop <tid>
//   resumed <tid> <0 | 1>                    the throttled task is woken up: 1 = by the scheduler
//   mem <qid> <bytes> <optional 0 | 1>       -> ok | no
//   free <qid> <bytes>
//   avail <qid>                              -> bytes, negative on overuse
//   update                                   UpdateFairShare
//   state                                    -> ok <pool>:<CpuFairShare>:<MemoryFairShare>:<MemoryUsage> ...
//   quit
// Every reply ends with " | <tid> ..." if some tasks were woken up since the previous reply.
// An error is replied as "error <message>".
//
// Usage: kqp_scheduler_sim [seed]

#include "stand.h"

int main(int argc, const char* argv[]) {
    const ui64 seed = argc > 1 ? FromString<ui64>(argv[1]) : 1;
    NKikimr::NKqp::NStand::TStand stand(seed);

    TString line;
    while (Cin.ReadLine(line)) {
        TStringBuilder reply;
        bool next = true;
        try {
            next = stand.Handle(line, reply);
        } catch (const std::exception& e) {
            reply.clear();
            reply << "error " << TString(e.what()).substr(0, 500);
        }
        if (!next) {
            break;
        }

        stand.AppendWoken(reply);
        // The newlines of an error would break the protocol
        for (auto& c : reply) {
            if (c == '\n') {
                c = ' ';
            }
        }
        Cout << reply << Endl;
    }

    return 0;
}
