#include "secret_string.h"

#include <util/system/madvise.h>

namespace NSecretString {
    TSecretString::TSecretString(TStringBuf value) {
        Init(value);
    }

    TSecretString::~TSecretString() {
        try {
            Clear();
        } catch (...) {
        }
    }

    TSecretString& TSecretString::operator=(const TSecretString& o) {
        if (&o == this) {
            return *this;
        }

        Init(o.Value_);

        return *this;
    }

    /**
     * It is not honest "move". Actually it is copy-assignment with cleaning of other instance.
     * This way allowes to avoid side effects of string optimizations:
     *   Copy-On-Write or Short-String-Optimization
     */
    TSecretString& TSecretString::operator=(TSecretString&& o) {
        if (&o == this) {
            return *this;
        }

        Init(o.Value_);
        o.Clear();

        return *this;
    }

    TSecretString& TSecretString::operator=(const TStringBuf o) {
        Init(o);

        return *this;
    }

    void TSecretString::Init(TStringBuf value) {
        Clear();
        if (value.empty()) {
            return;
        }

        Value_ = value;
        MadviseExcludeFromCoreDump(Value_);
    }

    void TSecretString::Clear() {
        if (Value_.empty()) {
            return;
        }

        SecureZero((void*)Value_.data(), Value_.size());
        MadviseIncludeIntoCoreDump(Value_);
        Value_.clear();
    }
}
