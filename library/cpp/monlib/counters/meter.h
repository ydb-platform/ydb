#pragma once

#include <util/system/types.h>
#include <util/generic/noncopyable.h>
#include <library/cpp/deprecated/atomic/atomic.h>

#include <chrono>
#include <cstdlib>
#include <cmath>

namespace NMonitoring {
    /**
     * An exponentially-weighted moving average.
     *
     * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf">
     *      UNIX Load Average Part 1: How It Works</a>
     * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf">
     *      UNIX Load Average Part 2: Not Your Average Average</a>
     * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
     */
    class TMovingAverage {
    public:
        enum {
            INTERVAL = 5 // in seconds
        };

    public:
        /**
         * Creates a new EWMA which is equivalent to the UNIX one minute load
         * average and which expects to be ticked every 5 seconds.
         *
         * @return a one-minute EWMA
         */
        static TMovingAverage OneMinute() {
            static const double M1_ALPHA = 1 - std::exp(-INTERVAL / 60.0 / 1);
            return {M1_ALPHA, std::chrono::seconds(INTERVAL)};
        }

        /**
         * Creates a new EWMA which is equivalent to the UNIX five minute load
         * average and which expects to be ticked every 5 seconds.
         *
         * @return a five-minute EWMA
         */
        static TMovingAverage FiveMinutes() {
            static const double M5_ALPHA = 1 - std::exp(-INTERVAL / 60.0 / 5);
            return {M5_ALPHA, std::chrono::seconds(INTERVAL)};
        }

        /**
         * Creates a new EWMA which is equivalent to the UNIX fifteen minute load
         * average and which expects to be ticked every 5 seconds.
         *
         * @return a fifteen-minute EWMA
         */
        static TMovingAverage FifteenMinutes() {
            static const double M15_ALPHA = 1 - std::exp(-INTERVAL / 60.0 / 15);
            return {M15_ALPHA, std::chrono::seconds(INTERVAL)};
        }

        /**
         * Create a new EWMA with a specific smoothing constant.
         *
         * @param alpha        the smoothing constant
         * @param interval     the expected tick interval
         */
        TMovingAverage(double alpha, std::chrono::seconds interval)
            : Initialized_(0)
            , Rate_(0)
            , Uncounted_(0)
            , Alpha_(alpha)
            , Interval_(std::chrono::nanoseconds(interval).count())
        {
        }

        TMovingAverage(const TMovingAverage& rhs)
            : Initialized_(AtomicGet(rhs.Initialized_))
            , Rate_(AtomicGet(rhs.Rate_))
            , Uncounted_(AtomicGet(rhs.Uncounted_))
            , Alpha_(rhs.Alpha_)
            , Interval_(rhs.Interval_)
        {
        }

        TMovingAverage& operator=(const TMovingAverage& rhs) {
            AtomicSet(Initialized_, AtomicGet(Initialized_));
            AtomicSet(Rate_, AtomicGet(rhs.Rate_));
            AtomicSet(Uncounted_, AtomicGet(rhs.Uncounted_));
            Alpha_ = rhs.Alpha_;
            Interval_ = rhs.Interval_;
            return *this;
        }

        /**
         * Update the moving average with a new value.
         *
         * @param n the new value
         */
        void Update(ui64 n = 1) {
            AtomicAdd(Uncounted_, n);
        }

        /**
         * Mark the passage of time and decay the current rate accordingly.
         */
        void Tick() {
            double instantRate = AtomicSwap(&Uncounted_, 0) / Interval_;
            if (AtomicGet(Initialized_)) {
                double rate = AsDouble(AtomicGet(Rate_));
                rate += (Alpha_ * (instantRate - rate));
                AtomicSet(Rate_, AsAtomic(rate));
            } else {
                AtomicSet(Rate_, AsAtomic(instantRate));
                AtomicSet(Initialized_, 1);
            }
        }

        /**
         * @return the rate in the seconds
         */
        double GetRate() const {
            double rate = AsDouble(AtomicGet(Rate_));
            return rate * std::nano::den;
        }

    private:
        static double AsDouble(TAtomicBase val) {
            union {
                double D;
                TAtomicBase A;
            } doubleAtomic;
            doubleAtomic.A = val;
            return doubleAtomic.D;
        }

        static TAtomicBase AsAtomic(double val) {
            union {
                double D;
                TAtomicBase A;
            } doubleAtomic;
            doubleAtomic.D = val;
            return doubleAtomic.A;
        }

    private:
        TAtomic Initialized_;
        TAtomic Rate_;
        TAtomic Uncounted_;
        double Alpha_;
        double Interval_;
    };

    /**
     * A meter metric which measures mean throughput and one-, five-, and
     * fifteen-minute exponentially-weighted moving average throughputs.
     */
    template <typename TClock>
    class TMeterImpl: private TNonCopyable {
    public:
        TMeterImpl()
            : StartTime_(TClock::now())
            , LastTick_(StartTime_.time_since_epoch().count())
            , Count_(0)
            , OneMinuteRate_(TMovingAverage::OneMinute())
            , FiveMinutesRate_(TMovingAverage::FiveMinutes())
            , FifteenMinutesRate_(TMovingAverage::FifteenMinutes())
        {
        }

        /**
         * Mark the occurrence of events.
         *
         * @param n the number of events
         */
        void Mark(ui64 n = 1) {
            TickIfNecessary();
            AtomicAdd(Count_, n);
            OneMinuteRate_.Update(n);
            FiveMinutesRate_.Update(n);
            FifteenMinutesRate_.Update(n);
        }

        /**
         * Returns the one-minute exponentially-weighted moving average rate at
         * which events have occurred since the meter was created.
         *
         * This rate has the same exponential decay factor as the one-minute load
         * average in the top Unix command.
         *
         * @return the one-minute exponentially-weighted moving average rate at
         *         which events have occurred since the meter was created
         */
        double GetOneMinuteRate() const {
            return OneMinuteRate_.GetRate();
        }

        /**
         * Returns the five-minute exponentially-weighted moving average rate at
         * which events have occurred since the meter was created.
         *
         * This rate has the same exponential decay factor as the five-minute load
         * average in the top Unix command.
         *
         * @return the five-minute exponentially-weighted moving average rate at
         *         which events have occurred since the meter was created
         */
        double GetFiveMinutesRate() const {
            return FiveMinutesRate_.GetRate();
        }

        /**
         * Returns the fifteen-minute exponentially-weighted moving average rate
         * at which events have occurred since the meter was created.
         *
         * This rate has the same exponential decay factor as the fifteen-minute
         * load average in the top Unix command.
         *
         * @return the fifteen-minute exponentially-weighted moving average rate
         *         at which events have occurred since the meter was created
         */
        double GetFifteenMinutesRate() const {
            return FifteenMinutesRate_.GetRate();
        }

        /**
         * @return the mean rate at which events have occurred since the meter
         *         was created
         */
        double GetMeanRate() const {
            if (GetCount() == 0) {
                return 0.0;
            }

            auto now = TClock::now();
            std::chrono::duration<double> elapsedSeconds = now - StartTime_;
            return GetCount() / elapsedSeconds.count();
        }

        /**
         * @return the number of events which have been marked
         */
        ui64 GetCount() const {
            return AtomicGet(Count_);
        }

    private:
        void TickIfNecessary() {
            static ui64 TICK_INTERVAL_NS =
                std::chrono::nanoseconds(
                    std::chrono::seconds(TMovingAverage::INTERVAL))
                    .count();

            auto oldTickNs = AtomicGet(LastTick_);
            auto newTickNs = TClock::now().time_since_epoch().count();
            ui64 elapsedNs = std::abs(newTickNs - oldTickNs);

            if (elapsedNs > TICK_INTERVAL_NS) {
                // adjust to interval begining
                newTickNs -= elapsedNs % TICK_INTERVAL_NS;
                if (AtomicCas(&LastTick_, newTickNs, oldTickNs)) {
                    ui64 requiredTicks = elapsedNs / TICK_INTERVAL_NS;
                    for (ui64 i = 0; i < requiredTicks; ++i) {
                        OneMinuteRate_.Tick();
                        FiveMinutesRate_.Tick();
                        FifteenMinutesRate_.Tick();
                    }
                }
            }
        }

    private:
        const typename TClock::time_point StartTime_;
        TAtomic LastTick_;
        TAtomic Count_;
        TMovingAverage OneMinuteRate_;
        TMovingAverage FiveMinutesRate_;
        TMovingAverage FifteenMinutesRate_;
    };

    using TSystemMeter = TMeterImpl<std::chrono::system_clock>;
    using TSteadyMeter = TMeterImpl<std::chrono::steady_clock>;
    using THighResMeter = TMeterImpl<std::chrono::high_resolution_clock>;
    using TMeter = THighResMeter;

}
