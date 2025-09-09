#pragma once

#include <ydb/core/scheme/scheme_type_id.h>

#include <util/generic/buffer.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/singleton.h>
#include <util/system/types.h>
#include <util/system/unaligned_mem.h>

#include <string.h>

namespace NKikimr {
namespace NScheme {

class TDataRef;
class TTypeCodecs;

/***************************************************************************//**
 * TCodecType identifies an algorithm of data serialization/deserialization.
 ******************************************************************************/
enum class TCodecType : ui16 {
    Unset = 0,      // Use default writer from a different source (e.g. doc-wise).

    // Universal IType-dependent aliases.

    Default = 1,    // Flat as-is writer, no deserialization required on read.
    ZeroCopy,       // Decoders return refs on chunk buffer data.
    Compact,        // Good compression, might require data unpacking on read.
    RandomAccess,   // Decoder supports random access to values out of the box.
    Adaptive,       // Best compression algo is chosen on finishing the chunk.

    // Type-specific codecs.

    FixedLen = 10,          // For types with a fixed byte size.

    VarLen = 20,            // E.g. for strings, blobs.
    VarLenVarInt = 23,      // Uses VarInt to save sizes. TODO

    VarInt = 30,            // Unsigned ints.
    DeltaVarInt = 31,       // Increasing unsigned ints.
    DeltaRevVarInt = 32,    // Decreasing unsigned ints.
    ZigZag = 40,            // Signed ints.
    DeltaZigZag = 41,       // Monotone or densely distributed signed ints.
    // TODO: Add some bit-wise int compression algorithms.

    Bool = 50,          // 1 bit per bool (2 bits per nullable bool).

    FixedLenLz = 60,    // TODO
    VarLenLz = 61,      // TODO
    // TODO: Add gzip/snappy or some other general-case compression algos.

    MaxId = 0x3FF, // 10 bits
};

/***************************************************************************//**
 * TCodecSig identifies a concrete instance of a codec within a concrete IType
 * context.
 *
 * Internal structure:
 * ---------------------------------------------
 * | Reserved : 5 | IsNullable : 1 | Type : 10 |
 * ---------------------------------------------
 *
 * NOTE: Could not use union of ui16 and bitfield with constexpr constructor.
 ******************************************************************************/
#pragma pack(push, 2)
class TCodecSig {
public:
    TCodecSig(ui16 raw = 0)
        : Raw(raw)
    { }

    TCodecSig(TCodecType type)
        : Raw(ui16(type))
    { }

    TCodecSig(TCodecType type, bool isNullable)
        : Raw((ui16(isNullable) << 10) | ui16(type))
    { }

    operator ui16() const {
        return Raw;
    }

    TCodecType Type() const {
        return static_cast<TCodecType>(Raw & ((1 << 10) - 1));
    }

    bool IsNullable() const {
        return Raw & (1 << 10);
    }

private:
    ui16 Raw;
};
#pragma pack(pop)
static_assert(sizeof(TCodecSig) == 2, "Expected sizeof(TCodecSig) == 2.");

/***************************************************************************//**
 * IChunkCoder is used to serialize a single data chunk.
 ******************************************************************************/
class IChunkCoder {
public:
    virtual ~IChunkCoder() { }

    virtual TCodecSig Signature() const = 0;

    inline void AddData(const char* data, size_t size) {
        ++ValuesConsumed;
        ConsumedSize += size;
        DoAddData(data, size);
    }

    void AddNull() {
        ++ValuesConsumed;
        DoAddNull();
    }

    inline ui32 GetValuesConsumed() const {
        return ValuesConsumed;
    }

    inline size_t GetConsumedSize() const {
        return ConsumedSize;
    }

    virtual size_t GetEstimatedSize() const = 0;
    virtual void Seal() = 0;

protected:
    virtual void DoAddData(const char* data, size_t size) = 0;
    virtual void DoAddNull() = 0;

protected:
    ui32 ValuesConsumed = 0;
    size_t ConsumedSize = 0;
};

/***************************************************************************//**
 * IChunkIterator is a forward iterator over the chunk values.
 * NOTE: No explicit end of values is provided, it should be known to clients.
 ******************************************************************************/
class IChunkIterator {
public:
    virtual ~IChunkIterator() { }

    virtual TDataRef Next() = 0;
    virtual TDataRef Peek() const = 0;
};

class IChunkBidirIterator : public IChunkIterator {
public:
    virtual void Back() = 0;
};

/***************************************************************************//**
 * IChunkDecoder provides an interface to values of the encoded chunk.
 ******************************************************************************/
class IChunkDecoder : public TThrRefBase {
public:
    using TPtr = TIntrusiveConstPtr<IChunkDecoder>;

    virtual TCodecSig Signature() const = 0;

    virtual TAutoPtr<IChunkIterator> MakeIterator() const = 0;
    virtual TAutoPtr<IChunkBidirIterator> MakeBidirIterator() const = 0;
    virtual TDataRef GetValue(size_t index) const = 0;

    static IChunkDecoder::TPtr ReadChunk(const TDataRef&, const TTypeCodecs* codecs);
    virtual IChunkDecoder::TPtr ReadNext(const TDataRef&, const TTypeCodecs* codecs) const = 0;
    //< ReadNext looks up in the given TTypeCodecs only if the Sig doesn't match 'this' codec.
};

/***************************************************************************//**
 * ICodec encapsulates type serialization/deserialization.
 ******************************************************************************/
class ICodec {
public:
    virtual ~ICodec() { }

    virtual TCodecSig Signature() const = 0;

    virtual TAutoPtr<IChunkCoder> MakeChunk(TBuffer&) const = 0;
    virtual IChunkDecoder::TPtr ReadChunk(const TDataRef&) const = 0;

    /// Read the chunk using 'this' codec (if the codec signature matches),
    /// or look up a different codec in the table.
    virtual IChunkDecoder::TPtr ReadChunk(const TDataRef&, const TTypeCodecs* codecs) const = 0;
};

/***************************************************************************//**
 * TDataRef can either share the data (TString, TBuffer) or keep a reference (TStringBuf).
 * It uses short string optimization (SSO) to store small data (<= 16b).
 * TODO: Move to ydb/core/util
 ******************************************************************************/
class TDataRef {
public:
    /// Create null by default.
    TDataRef()
        : Data_(nullptr)
        , Size_(0)
        , ShortSize_(0)
        , IsNull_(1)
    { }

    /// No ownership of data is taken.
    TDataRef(const char* data, size_t size)
        : Data_(data)
        , Size_(size)
        , ShortSize_(LONG_SIZE)
        , IsNull_(0)
    { }

    TDataRef(const TStringBuf& data)
        : TDataRef(data.data(), data.size())
    { }

    TDataRef(const TBuffer& data)
        : TDataRef(data.data(), data.size())
    {
    }
    
     /// Copy and take ownership of a small piece of data (<= 16b).
    TDataRef(const char* data, size_t size, bool)
        : ShortSize_(size)
        , IsNull_(0)
    {
        Y_DEBUG_ABORT_UNLESS(size <= INTRUSIVE_SIZE);
        if (size) {
            ::memcpy(IntrusiveData_, data, size);
        }
    }

    /// Ownership of the TString is taken with zero-copy.
    TDataRef(const TString& data)
        : TDataRef(data, data.data(), data.size())
    { }

    /// Ownership of the TString is taken with zero-copy.
    TDataRef(const TString& data, const char* begin, size_t size)
        : SharedData_(data)
        , Data_(begin)
        , Size_(size)
        , ShortSize_(LONG_SIZE)
        , IsNull_(0)
    { }

    /// Ownership of the TString is taken with zero-copy.
    TDataRef(const TString& data, size_t begin, size_t size)
        : TDataRef(data, data.data() + begin, size)
    { }

    bool operator==(const TDataRef& other) const {
        if (IsNull_ || other.IsNull_)
            return IsNull_ == other.IsNull_;
        return ToStringBuf() == other.ToStringBuf();
    }

    const char* Data() const {
        return ShortSize_ != LONG_SIZE ? IntrusiveData_ : Data_ ;
    }

    size_t Size() const {
        return ShortSize_ != LONG_SIZE ? ShortSize_ : Size_;
    }

    const char* End() const {
        return ShortSize_ != LONG_SIZE ? IntrusiveData_ + ShortSize_ : Data_ + Size_;
    }

    bool IsNull() const {
        return IsNull_;
    }

    TStringBuf ToStringBuf() const {
        return ShortSize_ != LONG_SIZE ? TStringBuf(IntrusiveData_, ShortSize_) : TStringBuf(Data_, Size_);
    }

    /// Static factory methods.

    static inline TDataRef Ref(const char* data, size_t size) {
        return TDataRef(data, size);
    }

    static inline TDataRef CopyShort(const char* data, size_t size) {
        return TDataRef(data, size, true);
    }

    static inline TDataRef CopyLong(const char* data, size_t size) {
        return TDataRef(TString(data, size));
    }

    static inline TDataRef Copy(const char* data, size_t size) {
        return size <= INTRUSIVE_SIZE ? TDataRef(data, size, true) : TDataRef(TString(data, size));
    }

private:
    static const size_t INTRUSIVE_SIZE = 16;
    static const ui8 LONG_SIZE = 0x1F;

    TString SharedData_; // FIXME: It's a bottleneck (slows down a document traverse).

    union {
        char IntrusiveData_[INTRUSIVE_SIZE];

        struct {
            const char* Data_;
            size_t Size_;
        };
    };

    struct {
        ui8 ShortSize_ : 5;
        ui8 IsNull_    : 1;
        // Reserved    : 2;
    };
};

/***************************************************************************//**
 * TTypeCodecs represents a set of codecs available to use with the IType
 * instance.
 * TODO: Use this class instead of TTypeSerializerRegistry.
 ******************************************************************************/
class TTypeCodecs {
public:
    TTypeCodecs(TTypeId typeId = 0);

    template <bool IsNullable>
    const ICodec* GetDefaultCodec() const {
        const ICodec* codec = IsNullable ? DefaultNullable : DefaultNonNullable;
        Y_ABORT_UNLESS(codec, "No default codec.");
        return codec;
    }

    const ICodec* GetCodec(TCodecSig sig) const {
        auto iter = Codecs.find(sig);
        Y_ABORT_UNLESS(iter != Codecs.end(), "Unregistered codec (%u).", ui16(sig));
        return iter->second;
    }

    bool Has(TCodecSig sig) const {
        return Codecs.find(sig) != Codecs.end();
    }

    template <typename TCodec>
    const ICodec* AddCodec() {
        auto codec = Singleton<TCodec>();
        auto inserted = Codecs.insert(std::make_pair(TCodec::Sig(), codec));
        Y_ABORT_UNLESS(inserted.second, "Codec signature collision (%u).", ui16(TCodec::Sig()));
        return codec;
    }

    const ICodec* AddAlias(TCodecSig from, TCodecSig to, bool force = false) {
        auto iter = Codecs.find(to);
        Y_ABORT_UNLESS(iter != Codecs.end(), "Aliasing an unregistered codec (%u -> %u).", ui16(from), ui16(to));
        return AddAlias(from, iter->second, force);
    }

    const ICodec* AddAlias(TCodecSig from, const ICodec* to, bool force = false) {
        Y_ABORT_UNLESS(to, "Aliasing an unregistered codec (%u -> nullptr).", ui16(from));
        auto& alias = Codecs[from];
        Y_ABORT_UNLESS(force || !alias, "Codec signature collision (%u).", ui16(from));
        alias = to;

        // Cache the default codecs.
        if (from.Type() == TCodecType::Default) {
            if (from.IsNullable())
                DefaultNullable = to;
            else
                DefaultNonNullable = to;
        }

        return to;
    }

private:
    using TIdToCodec = THashMap<ui16, const ICodec*>;
    TIdToCodec Codecs;

    const ICodec* DefaultNullable;
    const ICodec* DefaultNonNullable;
};

////////////////////////////////////////////////////////////////////////////////
inline IChunkDecoder::TPtr IChunkDecoder::ReadChunk(const TDataRef& data, const TTypeCodecs* codecs) {
    Y_DEBUG_ABORT_UNLESS(data.Size() >= sizeof(TCodecSig));
    const TCodecSig sig = ReadUnaligned<TCodecSig>(data.Data());
    auto codec = codecs->GetCodec(sig);
    Y_ABORT_UNLESS(codec, "Unregistered codec (%u).", ui16(sig));
    return codec->ReadChunk(data);
}

} // namespace NScheme
} // namespace NKikimr
