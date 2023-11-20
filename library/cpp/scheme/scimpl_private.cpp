#include "scimpl_private.h"

#include <util/generic/algorithm.h>
#include <utility>

namespace NSc {
    namespace NImpl {
        struct TGetKey {
            static inline TStringBuf Do(const TDict::value_type& v) {
                return v.first;
            }
        };

        struct TMoveValue {
            static inline TValue&& Do(TDict::value_type& v) {
                return std::move(v.second);
            }

            static inline TValue&& Do(TArray::value_type& v) {
                return std::move(v);
            }
        };

        template <typename TAction, typename TElement, typename TColl>
        static inline void PutToVector(TVector<TElement>& vector, TColl& coll) {
            size_t i = vector.size();
            vector.resize(vector.size() + coll.size());

            for (auto& item : coll) {
                vector[i++] = TAction::Do(item);
            }
        }

        bool TKeySortContext::Process(const TDict& self) {
            size_t oldSz = Vector.size();
            PutToVector<TGetKey>(Vector, self);
            Sort(Vector.begin() + oldSz, Vector.end());
            return true;
        }

        bool TSelfOverrideContext::Process(TValue::TScCore& self) {
            if (self.GetDict().size()) {
                PutToVector<TMoveValue>(Vector, self.Dict);
            } else if (self.GetArray().size()) {
                PutToVector<TMoveValue>(Vector, self.Array);
            }
            return true;
        }

        bool TSelfLoopContext::Process(const TValue::TScCore& self) {
            const bool ok = (Vector.end() == Find(Vector.begin(), Vector.end(), &self));

            if (!ok) {
                switch (ReportingMode) {
                case EMode::Assert:
                    Y_ASSERT(false); // make sure the debug build sees this
                    break;
                case EMode::Throw:
                    ythrow TSchemeException() << "REFERENCE LOOP DETECTED";
                case EMode::Abort:
                    Y_ABORT("REFERENCE LOOP DETECTED");
                    break;
                case EMode::Stderr:
                    Cerr << "REFERENCE LOOP DETECTED: " << JoinStrings(Vector.begin(), Vector.end(), ", ")
                         << " AND " << ToString((const void*)&self) << Endl;
                    break;
                }
            }

            Vector.push_back(&self);
            return ok;
        }
    }
}
