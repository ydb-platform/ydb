#include <util/datetime/cputimer.h>

#include <library/cpp/getopt/last_getopt.h>

#include <deque>

int main(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    ui32 nIters = 100000000;
    ui32 nRows = 100000000;
    ui32 nRepeats = 10;
    ui32 nPrefetch = 0;
    ui32 nSpin = 0;
    bool shuffle = true;
    opts.AddLongOption('r', "rows", "# of rows").StoreResult(&nRows);
    opts.AddLongOption('i', "iter", "# of iterations").StoreResult(&nIters);
    opts.AddLongOption('t', "repeats", "# of repeats").StoreResult(&nRepeats);
    opts.AddLongOption('p', "prefetch", "# of prefetch").StoreResult(&nPrefetch);
    opts.AddLongOption('h', "shuffle", "randomize").StoreResult(&shuffle);
    opts.AddLongOption('s', "spin", "spin count").StoreResult(&nSpin);
    opts.SetFreeArgsMax(0);
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    std::vector<ui32> v(nRows);
    std::vector<ui32> data(nRows);
    std::iota(v.begin(), v.end(), 0);
    if (shuffle) {
        std::random_shuffle(v.begin(), v.end());
    }

    std::vector<ui32> prefetchQueue(nPrefetch);
    ui32 queueBegin = 0;
    ui32 queueEnd = 0;
    ui32 queueSize = 0;
    volatile ui64 tmp = 0;
    std::vector<double> durations;
    for (ui32 j = 0; j < nRepeats; ++j) {
        TSimpleTimer timer;
        ui32 index = 0;
        if (nPrefetch == 0) {
            for (ui32 i = 0; i < nIters; ++i) {
                data[v[index++]]+=1;
                if (index == nRows) {
                    index = 0;
                }

                for (ui32 j = 0; j < nSpin; ++j) {
                    ++tmp;
                }
            }
        } else {
            auto handle = [&]() {
                auto prevJ = prefetchQueue[queueBegin++];
                --queueSize;
                if (queueBegin == nPrefetch) {
                    queueBegin = 0;
                }

                data[prevJ]+=1;

                for (ui32 j = 0; j < nSpin; ++j) {
                    ++tmp;
                }
            };

            for (ui32 i = 0; i < nIters; ++i) {
                auto j = v[index++];
                if (index == nRows) {
                    index = 0;
                }

                __builtin_prefetch(data.data() + j, 1, 3);
                prefetchQueue[queueEnd++] = j;
                ++queueSize;
                if (queueEnd == nPrefetch) {
                    queueEnd = 0;
                }

                if (queueSize == nPrefetch) {
                    handle();
                }
            }

            while (queueSize > 0) {
                handle();
            }
        }

        auto duration = timer.Get();
        durations.push_back(1e-6*duration.MicroSeconds());
    }

    // remove 1/3 of worst measurements
    Sort(durations.begin(), durations.end());
    durations.erase(durations.begin() + nRepeats * 2 / 3, durations.end());
    nRepeats = durations.size();
    double sumDurations = 0.0, sumDurationsQ = 0.0;
    for (auto d : durations) {
        sumDurations += d;
        sumDurationsQ += d * d;
    }

    double avgDuration = sumDurations / nRepeats;
    double dispDuration = sqrt(sumDurationsQ / nRepeats - avgDuration * avgDuration);
    Cerr << "Elapsed: " << avgDuration << ", noise: " << 100*dispDuration/avgDuration << "%\n";
    Cerr << "Speed: " << 1e-6 * (ui64(nIters) / avgDuration) << " M iters/sec\n";
    return 0;
}
