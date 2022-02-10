#pragma once

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/system/types.h>
#include <util/memory/pool.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <functional>
#include <util/string/vector.h>
#include <util/string/type.h>

namespace NSc {
    namespace NDefinitions {
        const size_t POOL_BLOCK_SIZE = 4000; // leave 96 bytes for overhead

        struct TPool: public TAtomicRefCount<TPool> {
            TMemoryPool Pool;

            TPool(size_t blsz = POOL_BLOCK_SIZE, TMemoryPool::IGrowPolicy* grow = TMemoryPool::TExpGrow::Instance())
                : Pool(blsz, grow)
            {
            }

            TMemoryPool* Get() {
                return &Pool;
            }

            TStringBuf AppendBuf(const TStringBuf& strb) {
                return Pool.AppendCString(strb);
            }

            template <typename T>
            T* NewWithPool() {
                return new (Pool.Allocate<T>()) T(&Pool);
            }
        };
    }

    using TStringBufs = TVector<TStringBuf>;

    class TSchemeException : public yexception {
    };

    class TSchemeParseException : public TSchemeException {
    public:
        size_t Offset = 0;
        TString Reason;

    public:
        TSchemeParseException() = default;

        TSchemeParseException(size_t off, const TString& reason)
            : Offset(off)
            , Reason(reason)
        {
        }
    };

    struct TJsonOpts: public NJson::TJsonReaderConfig {
        enum EJsonOpts {
            JO_DEFAULT = 0,     // just dump json, used to be default, actually
            JO_SORT_KEYS = 1,   // sort dict keys to make output more predictable
            JO_SKIP_UNSAFE = 2, // skip nonunicode data to make external json parsers happy
                                // will skip nonunicode dict keys and replace nonunicode values with nulls
            JO_FORMAT = 8,      // format json

            JO_PARSER_STRICT_JSON = 16, // strict standard json
            JO_PARSER_STRICT_UTF8 = 32, // strict utf8
            JO_PARSER_DISALLOW_COMMENTS = 64,
            JO_PARSER_DISALLOW_DUPLICATE_KEYS = 128,

            JO_PRETTY = JO_FORMAT | JO_SORT_KEYS,    // pretty print json
            JO_SAFE = JO_SKIP_UNSAFE | JO_SORT_KEYS, // ensure standard parser-safe json

            JO_PARSER_STRICT_WITH_COMMENTS = JO_PARSER_STRICT_JSON | JO_PARSER_STRICT_UTF8,
            JO_PARSER_STRICT = JO_PARSER_STRICT_JSON | JO_PARSER_STRICT_UTF8 | JO_PARSER_DISALLOW_COMMENTS,
        };

    public:
        TJsonOpts(int opts = JO_SORT_KEYS)
            : Opts(opts)
            , SortKeys(opts & JO_SORT_KEYS)
            , FormatJson(opts & JO_FORMAT)
            , StringPolicy((opts & JO_SKIP_UNSAFE) ? StringPolicySafe : StringPolicyUnsafe)
        {
            AllowComments = !(opts & JO_PARSER_DISALLOW_COMMENTS);
            RelaxedJson = !(opts & JO_PARSER_STRICT_JSON);
            DontValidateUtf8 = !(opts & JO_PARSER_STRICT_UTF8);
        }

    public:
        bool RelaxedJson = false;
        int Opts = 0;
        bool SortKeys = true;
        bool FormatJson = false;

        // return true to proceed with output, false to skip, optionally modify value
        std::function<bool(double&)> NumberPolicy = NumberPolicySafe;
        std::function<bool(TStringBuf&)> StringPolicy = StringPolicyUnsafe;

    public:
        static bool NumberPolicySafe(double&); // skip if nan or inf
        static bool NumberPolicyUnsafe(double&) {
            return true;
        }

        static bool StringPolicySafe(TStringBuf&); // skip if not utf8
        static bool StringPolicyUnsafe(TStringBuf&) {
            return true;
        }
    };

    struct TProtoOpts {
        // Serialization throws on unknown enum value if not set, else use default value
        bool UnknownEnumValueIsDefault = false;

        // Serialization throws on type mismatch if not set, else leaves protobuf empty
        bool SkipTypeMismatch = false;
    };

    namespace NImpl {
        class TKeySortContext;
        class TSelfLoopContext;
        class TSelfOverrideContext;
    }
}

namespace google {
    namespace protobuf {
        class Message;
        class FieldDescriptor;
    }
}
