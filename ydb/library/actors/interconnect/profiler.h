#pragma once

#include <ydb/library/actors/util/datetime.h>

namespace NActors {

    class TProfiled {
        enum class EType : ui32 {
            ENTRY,
            EXIT,
        };

        struct TItem {
            EType Type; // entry kind
            int Line;
            const char *Marker; // name of the profiled function/part
            ui64 Timestamp; // cycles
        };

        bool Enable = false;
        mutable TDeque<TItem> Items;

        friend class TFunction;

    public:
        class TFunction {
            const TProfiled& Profiled;

        public:
            TFunction(const TProfiled& profiled, const char *name, int line)
                : Profiled(profiled)
            {
                Log(EType::ENTRY, name, line);
            }

            ~TFunction() {
                Log(EType::EXIT, nullptr, 0);
            }

        private:
            void Log(EType type, const char *marker, int line) {
                if (Profiled.Enable) {
                    Profiled.Items.push_back(TItem{
                        type,
                        line,
                        marker,
                        GetCycleCountFast()
                    });
                }
            }
        };

    public:
        void Start() {
            Enable = true;
        }

        void Finish() {
            Items.clear();
            Enable = false;
        }

        TDuration Duration() const {
            return CyclesToDuration(Items ? Items.back().Timestamp - Items.front().Timestamp : 0);
        }

        TString Format() const {
            TDeque<TItem>::iterator it = Items.begin();
            TString res = FormatLevel(it);
            Y_ABORT_UNLESS(it == Items.end());
            return res;
        }

    private:
        TString FormatLevel(TDeque<TItem>::iterator& it) const {
            struct TRecord {
                TString Marker;
                ui64 Duration;
                TString Interior;

                bool operator <(const TRecord& other) const {
                    return Duration < other.Duration;
                }
            };
            TVector<TRecord> records;

            while (it != Items.end() && it->Type != EType::EXIT) {
                Y_ABORT_UNLESS(it->Type == EType::ENTRY);
                const TString marker = Sprintf("%s:%d", it->Marker, it->Line);
                const ui64 begin = it->Timestamp;
                ++it;
                const TString interior = FormatLevel(it);
                Y_ABORT_UNLESS(it != Items.end());
                Y_ABORT_UNLESS(it->Type == EType::EXIT);
                const ui64 end = it->Timestamp;
                records.push_back(TRecord{marker, end - begin, interior});
                ++it;
            }

            TStringStream s;
            const ui64 cyclesPerMs = GetCyclesPerMillisecond();

            if (records.size() <= 10) {
                bool first = true;
                for (const TRecord& record : records) {
                    if (first) {
                        first = false;
                    } else {
                        s << " ";
                    }
                    s << record.Marker << "(" << (record.Duration * 1000000 / cyclesPerMs) << "ns)";
                    if (record.Interior) {
                        s << " {" << record.Interior << "}";
                    }
                }
            } else {
                TMap<TString, TVector<TRecord>> m;
                for (TRecord& r : records) {
                    const TString key = r.Marker;
                    m[key].push_back(std::move(r));
                }

                s << "unordered ";
                for (auto& [key, value] : m) {
                    auto i = std::max_element(value.begin(), value.end());
                    ui64 sum = 0;
                    for (const auto& item : value) {
                        sum += item.Duration;
                    }
                    sum = sum * 1000000 / cyclesPerMs;
                    s << key << " num# " << value.size() << " sum# " << sum << "ns max# " << (i->Duration * 1000000 / cyclesPerMs) << "ns";
                    if (i->Interior) {
                        s << " {" << i->Interior << "}";
                    }
                }
            }

            return s.Str();
        }
    };

} // NActors
