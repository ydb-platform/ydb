#pragma once

#include <library/cpp/string_utils/ztstrbuf/ztstrbuf.h>

#include <util/generic/string.h>

namespace NSecretString {
    /**
     * TSecretString allowes to store some long lived secrets in "secure" storage in memory.
     * Common usage:
     *  1) read secret value from disk/env/etc
     *  2) put it into TSecretString
     *  3) destory secret copy from 1)
     *
     * Useful scenerios for TSecretString:
     *   - in memory only tasks: using key to create crypto signature;
     *   - rare network cases: db password on connection or OAuth token in background tasks.
     *     These cases disclosure the secret
     *       because of sending it over network with some I/O frameworks.
     *     Usually such frameworks copy input params to provide network protocol: gRPC, for example.
     *
     * Supported features:
     *  1. Exclude secret from core dump.
     *     madvise(MADV_DONTDUMP) in ctor excludes full memory page from core dump.
     *     madvise(MADV_DODUMP) in dtor reverts previous action.
     *  2. Zero memory before free.
     *
     * Code dump looks like this:
(gdb) print s
$1 = (const TSecretString &) @0x7fff23c4c560: {
  Value_ = {<TStringBase<TBasicString<char, std::__y1::char_traits<char> >, char, std::__y1::char_traits<char> >> = {
      static npos = <optimized out>}, Data_ = 0x107c001d8 <error: Cannot access memory at address 0x107c001d8>}}
     */

    class TSecretString {
    public:
        TSecretString() = default;
        TSecretString(TStringBuf value);
        ~TSecretString();

        TSecretString(const TSecretString& o)
            : TSecretString(o.Value())
        {
        }

        TSecretString(TSecretString&& o)
            : TSecretString(o.Value())
        {
            o.Clear();
        }

        TSecretString& operator=(const TSecretString& o);
        TSecretString& operator=(TSecretString&& o);

        TSecretString& operator=(const TStringBuf o);

        operator TZtStringBuf() const {
            return Value();
        }

        // Provides zero terminated string
        TZtStringBuf Value() const {
            return TZtStringBuf(Value_);
        }

    private:
        // TStringBuf breaks Copy-On-Write to provide correct copy-ctor and copy-assignment
        void Init(TStringBuf value);
        void Clear();

    private:
        TString Value_;
    };
}
