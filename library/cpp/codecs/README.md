This is a library of compression algorithms with a unified interface and serialization.
See also library/cpp/codecs/static, where a support for statically compiled dictionaries is implemented.

All algorithms have a common `ICodec` interface (described in codecs.h).

The `ICodec` interface has the following methods:\
    `virtual ui8 ICodec::Encode (TMemoryRegion, TBuffer&) const;`\
            - Input - memory region. Output - filled buffer and the rest of the last byte, if it was not filled to the end.\
    `virtual void ICodec::Decode (TMemoryRegion, TBuffer&) const;`\
            - Input - memory region. Output - filled buffer.\
    `virtual void Save (TOutputStream*) const;`\
            - Serialization.\
    `virtual void Load (TInputStream*);`\
            - Deserialization.\
    `virtual bool NeedsTraining() const;`\
            - Returns if it is necessary or not to teach this codec.\
    `virtual bool PaddingBit() const;`\
            - Returns which values should fill the free bits of the last byte.\
    `virtual size_t ApproximateSizeOnEncode(size_t sz) const;`\
            - Returns an approximate estimate of the size of the result after encoding.\
                    For example could be used for a more accurate preallocation of a buffer.\
    `virtual size_t ApproximateSizeOnDecode(size_t sz) const;`\
            - Returns an approximate estimate of the size of the result after decoding.\
                    For example could be used for a more accurate preallocation of a buffer.\
    `virtual TString GetName() const;`\
            - The name of the codec. It is required for registration of the codec in the system of serialization/deserialization.\
                    For example, it allows you to save information about which combination of codecs was in use (see below).\
    `virtual void Learn(ISequenceReader*);`\
            - The interface for teaching codecs that use information about the distribution of data.

In addition, the library has a number of utilities that allow a more flexible use of it.

In the `ICodec` class the following methods are available:\
    `static TCodecPtr GetInstance(const TString& name);`\
            - Creation of a codec instance by a symbolic name\
                    (See `GetName()` above)\
    `static void Store(TOutputStream*, TCodecPtr p);`\
            - Serialization of the codec along with its name\
                    Allows you to save information about which particular combination of codecs was used for encoding,\
                    and then reassemble the same combination for decoding\
    `static TCodecPtr Restore(TInputStream* in);`\
            - Serialization of the codec along with its name\
    `static TCodecPtr RestoreFromString(TStringBuf data);`\
            - Loads the codec instance from the string\
    `static TVector<TString> GetCodecsList();`\
            - The list of registered codecs
