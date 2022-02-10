#include "idx_test.h"

#include <util/string/printf.h>
#include <util/system/mutex.h>

namespace NIdxTest {

class TStderrProgressTracker : public IProgressTracker {
public:
    TStderrProgressTracker(size_t subsampling, const TString& title)
        : Subsampling_(subsampling)
    {
        LastSampledUpdateTime_ = TInstant::Now();
        LastSampledUpdateValue_ = 0;
        Cerr << title << Endl;
    }
    void Start(const TString& progressString, const TString& freeMessage) override {
        ProgressString_ = " %lu " + progressString + " (cur: %.2f tps)";
        if (!freeMessage)
            return;
        auto msg = ShiftText(freeMessage, 1);
        Cerr << msg << Endl;
    }

    void Update(size_t pos) override {
        DoUpdate(pos, false);
    }

    void Finish(const TString& freeMessage) override {
        if (LastUpdateValue_) {
            DoUpdate(LastUpdateValue_.GetRef(), true);
            Cerr << Endl;
        }
        auto msg = ShiftText(freeMessage, 1);
        Cerr << msg << Endl;
    }

private:
    void DoUpdate(size_t pos, bool force) {
        TInstant curTime;
        TGuard lock(Mtx_);
        if (!CurUpdate_ || force) {
            curTime = TInstant::Now();
            static ui8 counter;
            counter++;
            const TString symbols[4] = {" -", " \\", " |", " /"};
            const TDuration delta =
                TDuration::MicroSeconds(curTime.MicroSeconds() - LastSampledUpdateTime_.MicroSeconds());
            float tps = 0;
            if (delta) {
                tps = (pos - LastSampledUpdateValue_) / delta.SecondsFloat();
            }
            auto msg = Sprintf((symbols[counter % 4] + ProgressString_).c_str(), pos, tps);
            UpdateSz_ = Max(UpdateSz_, msg.size());
            msg.resize(UpdateSz_);

            Cerr << msg << "\r";
            fflush(stderr);
            LastSampledUpdateTime_ = curTime;
            LastSampledUpdateValue_ = pos;
        }

        LastUpdateValue_ = pos;
        if (CurUpdate_ == Subsampling_) {
            CurUpdate_ = 0;
        } else {
            CurUpdate_++;
        }
    }

    TString ShiftText(const TString& in, size_t n) {
        TString shift;
        shift.resize(n);
        TString msg = " " + in;
        size_t pos = 0;
        while(true) {
            pos = msg.find('\n', pos);
            if (pos != msg.npos) {
                if (pos < msg.size() - 1) {
                    pos++;
                    msg.insert(pos, shift);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return msg;
    }
    const size_t Subsampling_;
    size_t CurUpdate_ = 0;
    TString ProgressString_;
    size_t UpdateSz_ = 0;
    TMaybe<size_t> LastUpdateValue_;
    TInstant LastSampledUpdateTime_;
    size_t LastSampledUpdateValue_;
    TMutex Mtx_;
};

IProgressTracker::TPtr CreateStderrProgressTracker(size_t subsampling, const TString& title) {
    return std::make_unique<TStderrProgressTracker>(subsampling, title);
}
} // namespace NIdxTest
