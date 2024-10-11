#pragma once

#include <functional>
#include <memory>
#include <vector>

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>
#include <util/generic/yexception.h>

#include <contrib/libs/cxxsupp/libcxx/include/type_traits>

#include "kafka_records.h"
#include "kafka_consumer_protocol.h"
#include "kafka_log_impl.h"

namespace NKafka {
namespace NPrivate {

static constexpr bool DEBUG_ENABLED = false;

struct TWriteCollector {
    ui32 NumTaggedFields = 0;
};

struct TSizeCollector {
    ui32 Size = 0;
    ui32 NumTaggedFields = 0;
};

template<class T, typename U = std::make_unsigned_t<T>>
size_t SizeOfUnsignedVarint(T v) {
    static constexpr U Mask = Max<U>() - 0x7F;

    U value = v;
    size_t bytes = 1;
    while ((value & Mask) != 0L) {
        bytes += 1;
        value >>= 7;
    }
    return bytes;
}

template<class T>
size_t SizeOfVarint(T value) {
    return SizeOfUnsignedVarint(AsUnsigned<T>(value));
}

template<TKafkaVersion min, TKafkaVersion max>
constexpr bool VersionAll() {
    return 0 == min && max == Max<TKafkaVersion>();
}

template<TKafkaVersion min, TKafkaVersion max>
constexpr bool VersionNone() {
    return 0 == min && max == -1;
}

template<TKafkaVersion min, TKafkaVersion max>
inline bool VersionCheck(const TKafkaVersion value) {
    if constexpr (VersionNone<min, max>()) {
        return false;
    } else if constexpr (VersionAll<min, max>()) {
        return true;
    } else if constexpr (max == Max<TKafkaVersion>()) {
        return min <= value;
    } else if constexpr (min == 0) {
        return value <= max;
    } else {
        return min <= value && value <= max;
    }
}

template<typename Meta>
bool IsDefaultValue(const typename Meta::Type& value) {
    if constexpr (std::is_base_of_v<TMessage, typename Meta::Type>) {
        typename Meta::Type defValue;
        return defValue == value;
    } else if constexpr (Meta::TypeDesc::Default) {
        return Meta::Default == value;
    } else if constexpr (Meta::TypeDesc::Nullable) {
        return Meta::TypeDesc::IsNull(value);
    } else {
        return false;
    }
}


template <typename Meta, typename = ESizeFormat>
struct HasSizeFormat: std::false_type {};

template <typename Meta>
struct HasSizeFormat<Meta, decltype((void)Meta::SizeFormat, ESizeFormat::Default)>: std::true_type {};

template<typename Meta>
constexpr ESizeFormat SizeFormat() {
    if constexpr (HasSizeFormat<Meta>::value) {
        return Meta::SizeFormat;
    } else {
        return ESizeFormat::Default;
    }
}


template<typename Meta>
inline void WriteStringSize(TKafkaWritable& writable, TKafkaVersion version, TKafkaInt32 value) {
    if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
        writable.writeUnsignedVarint(value + 1);
    } else {
        writable << (TKafkaInt16)value;
    }
}

template<typename Meta>
inline TKafkaInt32 ReadStringSize(TKafkaReadable& readable, TKafkaVersion version) {
    if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
        return readable.readUnsignedVarint<ui32>() - 1;
    } else {
        TKafkaInt16 v;
        readable >> v;
        return v;
    }
}

template<typename Meta>
inline void WriteArraySize(TKafkaWritable& writable, TKafkaVersion version, TKafkaInt32 value) {
    if constexpr (SizeFormat<Meta>() == Varint) {
        return writable.writeVarint(value);
    } else if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
        writable.writeUnsignedVarint(value + 1);
    } else {
        writable << value;
    }
}

template<typename Meta>
inline TKafkaInt32 ReadArraySize(TKafkaReadable& readable, TKafkaVersion version) {
    if constexpr (SizeFormat<Meta>() == Varint) {
        return readable.readVarint<TKafkaInt32>();
    } else if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
        return readable.readUnsignedVarint<ui32>() - 1;
    } else {
        TKafkaInt32 v;
        readable >> v;
        return v;
    }
}

template<typename Meta>
inline TKafkaInt32 ArraySize(TKafkaVersion version, TKafkaInt32 size) {
    if constexpr (SizeFormat<Meta>() == Varint) {
        return SizeOfVarint(size);
    } else if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
        return SizeOfUnsignedVarint(size + 1);
    } else {
        return sizeof(TKafkaInt32);
    }
}


inline IOutputStream& operator <<(IOutputStream& out, const TKafkaUuid& /*value*/) {
    return out << "---";
}



//
// Common
//
template<typename Meta,
         typename TValueType = typename Meta::Type,
         typename TTypeDesc = typename Meta::TypeDesc>
class TypeStrategy {
public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion /*version*/, const TValueType& value) {
        writable << value;
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion /*version*/, const TValueType& value) {
        writable << value;
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion /*version*/, TValueType& value) {
        readable >> value;
    }

    inline static i64 DoSize(TKafkaVersion /*version*/, const TValueType& /*value*/) {
        static_assert(TTypeDesc::FixedLength, "Unsupported type: serialization length must be constant");
        return sizeof(TValueType);
    }

    inline static void DoLog(const TValueType& value) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' value " << value << Endl;
        }
    }
};


//
// TKafkaVarintDesc
//
template<typename Meta,
         typename TValueType>
class TypeStrategy<Meta, TValueType, TKafkaVarintDesc> {
public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion version, const TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            writable.writeVarint(value);
        } else {
            writable << value;
        }
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion version, const TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            writable.writeVarint(value);
        } else {
            writable << value;
        }
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion version, TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            value = readable.readVarint<TValueType>();
        } else {
            readable >> value;
        }
    }

    inline static i64 DoSize(TKafkaVersion version, const TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            return SizeOfVarint(value);
        } else {
            return sizeof(TValueType);
        }
    }

    inline static void DoLog(const TValueType& value) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' value " << value << Endl;
        }
    }
};


//
// TKafkaUnsignedVarintDesc
//
template<typename Meta,
         typename TValueType>
class TypeStrategy<Meta, TValueType, TKafkaUnsignedVarintDesc> {
public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion version, const TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            writable.writeUnsignedVarint(value + 1);
        } else {
            writable << value;
        }
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion version, const TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            writable.writeUnsignedVarint(value + 1);
        } else {
            writable << value;
        }
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion version, TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            value = readable.readUnsignedVarint<TValueType>() - 1;
        } else {
            readable >> value;
        }
    }

    inline static i64 DoSize(TKafkaVersion version, const TValueType& value) {
        if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
            return SizeOfUnsignedVarint(value);
        } else {
            return sizeof(TValueType);
        }
    }

    inline static void DoLog(const TValueType& value) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' value " << value << Endl;
        }
    }
};


//
// TKafkaStructDesc
//
template<typename Meta,
         typename TValueType>
class TypeStrategy<Meta, TValueType, TKafkaStructDesc> {
public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion version, const TValueType& value) {
        value.Write(writable, version);
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion version, const TValueType& value) {
        value.Write(writable, version);
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion version, TValueType& value) {
        value.Read(readable, version);
    }

    inline static i64 DoSize(TKafkaVersion version, const TValueType& value) {
        return value.Size(version);
    }

    inline static void DoLog(const TValueType& /*value*/) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' type struct" << Endl;
        }
    }
};


//
// TKafkaString
//
template<typename Meta>
class TypeStrategy<Meta, TKafkaString, TKafkaStringDesc> {
public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion version, const TKafkaString& value) {
        if (value) {
            const auto& v = *value;
            if (v.length() > Max<i16>()) {
                ythrow yexception() << "string field " << Meta::Name << " is too long to be serialized " << v.length();
            }
            WriteStringSize<Meta>(writable, version, v.length());
            writable << v;
        } else {
            if (VersionCheck<Meta::NullableVersions.Min, Meta::NullableVersions.Max>(version)) {
                WriteStringSize<Meta>(writable, version, -1);
            } else {
                ythrow yexception() << "non-nullable field " << Meta::Name << " serializing as null";
            }
        }
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion version, const TKafkaString& value) {
        const auto& v = *value;
        WriteStringSize<Meta>(writable, version, v.length());
        writable << v;
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion version, TKafkaString& value) {
        TKafkaInt32 length = ReadStringSize<Meta>(readable, version);
        if (length < 0) {
            if (VersionCheck<Meta::NullableVersions.Min, Meta::NullableVersions.Max>(version)) {
                value = std::nullopt;
            } else {
                ythrow yexception() << "non-nullable field " << Meta::Name << " was serialized as null";
            }
        } else if (length > Max<i16>()){
            ythrow yexception() << "string field " << Meta::Name << " had invalid length " << length;
        } else {
            value = TString();
            value->ReserveAndResize(length);
            readable.read(const_cast<char*>(value->data()), length);
        }
    }

    inline static i64 DoSize(TKafkaVersion version, const TKafkaString& value) {
        if (value) {
            const auto& v = *value;
            if (v.length() > Max<i16>()) {
                ythrow yexception() << "string field " << Meta::Name << " is too long to be serialized " << v.length();
            }
            if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
                return v.length() + SizeOfUnsignedVarint(v.length() + 1);
            } else {
                return v.length() + sizeof(TKafkaInt16);
            }
        } else {
            if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
                return sizeof(TKafkaInt8);
            } else {
                return sizeof(TKafkaInt16);
            }
        }
    }

    inline static void DoLog(const TKafkaString& value) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' type String. Size " << (value ? value->size() : 0) << " Value: " << (value ? *value : "null") << Endl;
        }
    }
};


//
// TKafkaBytes
//
template<typename Meta>
class TypeStrategy<Meta, TKafkaBytes, TKafkaBytesDesc> {
public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion version, const TKafkaBytes& value) {
        if (value) {
            const auto& v = *value;
            WriteArraySize<Meta>(writable, version, v.size());
            writable << v;
        } else {
            if (VersionCheck<Meta::NullableVersions.Min, Meta::NullableVersions.Max>(version)) {
                WriteArraySize<Meta>(writable, version, -1);
            } else {
                ythrow yexception() << "non-nullable field " << Meta::Name << " serializing as null";
            }
        }
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion version, const TKafkaBytes& value) {
        const auto& v = *value;
        WriteArraySize<Meta>(writable, version, v.size());
        writable << v;
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion version, TKafkaBytes& value) {
        TKafkaInt32 length = ReadArraySize<Meta>(readable, version);
        if (length < 0) {
            if (VersionCheck<Meta::NullableVersions.Min, Meta::NullableVersions.Max>(version)) {
                value = std::nullopt;
            } else {
                ythrow yexception() << "non-nullable field " << Meta::Name << " was serialized as null";
            }
        } else {
            value = readable.Bytes(length);
        }
    }

    inline static i64 DoSize(TKafkaVersion version, const TKafkaBytes& value) {
        if (value) {
            const auto& v = *value;
            return v.size() + ArraySize<Meta>(version, v.size());
        } else {
            if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
                return 1;
            } else {
                return sizeof(TKafkaInt32);
            }
        }
    }

    inline static void DoLog(const TKafkaBytes& value) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' type Bytes. Size " << (value ? value->size() : 0) << Endl;
        }
    }
};


//
// TKafkaRecords
//
template<typename Meta>
class TypeStrategy<Meta, TKafkaRecords, TKafkaRecordsDesc> {
    static constexpr TKafkaVersion CURRENT_RECORD_VERSION = 2;
public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion version, const TKafkaRecords& value) {
        if (value) {
            WriteArraySize<Meta>(writable, version, value->Size(CURRENT_RECORD_VERSION));
            (*value).Write(writable, CURRENT_RECORD_VERSION);
        } else {
            WriteArraySize<Meta>(writable, version, 0);
        }
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion version, const TKafkaRecords& value) {
        DoWrite(writable, version, value);
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion version, TKafkaRecords& value) {
        int length = ReadArraySize<Meta>(readable, version);
        if (length > 0) {
            char magic = readable.take(16);
            value.emplace();

            if (magic < CURRENT_RECORD_VERSION) {
                TKafkaRecordBatchV0 v0;
                v0.Read(readable, magic);

                value->Magic = v0.Record.Magic;
                value->Crc = v0.Record.Crc;
                value->Attributes = v0.Record.Attributes & 0x07;

                value->Records.resize(1);
                auto& record = value->Records.front();
                record.Length = v0.Record.MessageSize;
                record.OffsetDelta = v0.Offset;
                record.TimestampDelta = v0.Record.Timestamp;
                record.Key = v0.Record.Key;
                record.Value = v0.Record.Value;
            } else {
                (*value).Read(readable, magic);
            }
        } else {
            value = std::nullopt;
        }
    }

    inline static i64 DoSize(TKafkaVersion version, const TKafkaRecords& value) {
        if (value) {
            const auto& v = *value;
            const auto size = v.Size(CURRENT_RECORD_VERSION);
            return size + ArraySize<Meta>(version, size);
        } else {
            if (VersionCheck<Meta::FlexibleVersions.Min, Meta::FlexibleVersions.Max>(version)) {
                return 1;
            } else {
                return sizeof(TKafkaInt32);
            }
        }
    }

    inline static void DoLog(const TKafkaRecords& /*value*/) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' type Records." << Endl;
        }
    }
};


//
// KafkaArray
//
template<typename Meta,
         typename TValueType>
class TypeStrategy<Meta, std::vector<TValueType>, TKafkaArrayDesc> {
    using ItemStrategy = TypeStrategy<Meta, typename Meta::ItemType, typename Meta::ItemTypeDesc>;

public:
    inline static void DoWrite(TKafkaWritable& writable, TKafkaVersion version, const std::vector<TValueType>& value) {
        WriteArraySize<Meta>(writable, version, value.size());

        for(const auto& v : value) {
            ItemStrategy::DoWrite(writable, version, v);
        }
    }

    inline static void DoWriteTag(TKafkaWritable& writable, TKafkaVersion version, const std::vector<TValueType>& value) {
        WriteArraySize<Meta>(writable, version, value.size());

        for(const auto& v : value) {
            ItemStrategy::DoWrite(writable, version, v);
        }
    }

    inline static void DoRead(TKafkaReadable& readable, TKafkaVersion version, std::vector<TValueType>& value) {
        TKafkaInt32 length = ReadArraySize<Meta>(readable, version);
        if (length < 0) {
            if (VersionCheck<Meta::NullableVersions.Min, Meta::NullableVersions.Max>(version)) {
                return;
            } else {
                ythrow yexception() << "non-nullable field " << Meta::Name << " was serialized as null";
            }
        }
        value.resize(length);

        for (int i = 0; i < length; ++i) {
            ItemStrategy::DoRead(readable, version, value[i]);
        }
    }

    inline static i64 DoSize(TKafkaVersion version, const std::vector<TValueType>& value) {
        TKafkaInt32 size = 0;
        if constexpr (Meta::TypeDesc::FixedLength) {
            size = value.size() * sizeof(TValueType);
        } else {
            for(const auto& v : value) {
                size += ItemStrategy::DoSize(version, v);
            }
        }
        return size + ArraySize<Meta>(version, value.size());
    }

    inline static void DoLog(const std::vector<TValueType>& value) {
        if constexpr (DEBUG_ENABLED) {
            Cerr << "Was read field '" << Meta::Name << "' type Array. Size " <<  value.size() << Endl;
        }
    }
};



//
// Main fields function
//
template<typename Meta>
inline void Write(TWriteCollector& collector, TKafkaWritable& writable, TKafkaInt16 version, const typename Meta::Type& value) {
    if (VersionCheck<Meta::PresentVersions.Min, Meta::PresentVersions.Max>(version)) { 
        if (VersionCheck<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>(version)) {
            if (!IsDefaultValue<Meta>(value)) {
                ++collector.NumTaggedFields;
            }
        } else  {
            TypeStrategy<Meta, typename Meta::Type>::DoWrite(writable, version, value);
        }
    }
}

template<typename Meta>
inline void Read(TKafkaReadable& readable, TKafkaInt16 version, typename Meta::Type& value) {
    if (!VersionNone<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>() 
        && VersionCheck<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>(version)) {
        return;
    } else {
        if (VersionCheck<Meta::PresentVersions.Min, Meta::PresentVersions.Max>(version)) {
            try {
                TypeStrategy<Meta, typename Meta::Type>::DoRead(readable, version, value);
                TypeStrategy<Meta, typename Meta::Type>::DoLog(value);
            } catch (const yexception& e) {
                ythrow yexception() << "error on read field " << Meta::Name << ": " << e.what();
            }
        } else if constexpr (Meta::TypeDesc::Default) {
            value = Meta::Default;
        }
    }
}

template<typename Meta>
inline void Size(TSizeCollector& collector, TKafkaInt16 version, const typename Meta::Type& value) {
    if constexpr (!VersionNone<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>()) {
        if (VersionCheck<Meta::PresentVersions.Min, Meta::PresentVersions.Max>(version)) {
            if (VersionCheck<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>(version)) {
                if (!IsDefaultValue<Meta>(value)) {
                    ++collector.NumTaggedFields;

                    i64 size = TypeStrategy<Meta, typename Meta::Type>::DoSize(version, value);
                    collector.Size += size + SizeOfUnsignedVarint(Meta::Tag) + SizeOfUnsignedVarint(size); 
                    if constexpr (DEBUG_ENABLED) {
                        Cerr << "Size of field '" << Meta::Name << "' " << size << " + " << SizeOfUnsignedVarint(Meta::Tag) << " + " << SizeOfUnsignedVarint(size) << Endl;                    
                    }
                }
            } else {
                i64 size = TypeStrategy<Meta, typename Meta::Type>::DoSize(version, value);
                collector.Size += size;
                if constexpr (DEBUG_ENABLED) {
                    Cerr << "Size of field '" << Meta::Name << "' " << size << Endl;                    
                }
            }
        }
    } else {
        if (VersionCheck<Meta::PresentVersions.Min, Meta::PresentVersions.Max>(version)) {
            i64 size = TypeStrategy<Meta, typename Meta::Type>::DoSize(version, value);
            collector.Size += size;
            if constexpr (DEBUG_ENABLED) {
                Cerr << "Size of field '" << Meta::Name << "' " << size << Endl;                    
            }
        }
    }
}

template<typename Meta>
inline void WriteTag(TKafkaWritable& writable, TKafkaInt16 version, const typename Meta::Type& value) {
    if constexpr (!VersionNone<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>()) {
        if (VersionCheck<Meta::PresentVersions.Min, Meta::PresentVersions.Max>(version) &&
            VersionCheck<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>(version)) {
            if (!IsDefaultValue<Meta>(value)) {
                writable.writeUnsignedVarint(Meta::Tag);
                writable.writeUnsignedVarint(TypeStrategy<Meta, typename Meta::Type>::DoSize(version, value));
                TypeStrategy<Meta, typename Meta::Type>::DoWriteTag(writable, version, value);
            }
        }
    }
}

template<typename Meta>
inline void ReadTag(TKafkaReadable& readable, TKafkaInt16 version, typename Meta::Type& value) {
    if constexpr (!VersionNone<Meta::TaggedVersions.Min, Meta::TaggedVersions.Max>()) {
        TypeStrategy<Meta, typename Meta::Type>::DoRead(readable, version, value);
    }
}


} // NPrivate

} // namespace NKafka
