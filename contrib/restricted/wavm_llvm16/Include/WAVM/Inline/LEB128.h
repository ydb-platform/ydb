#pragma once

#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Serialization.h"

namespace WAVM { namespace Serialization {
	// LEB128 variable-length integer serialization.
	template<typename Value, Uptr maxBits>
	WAVM_FORCEINLINE void serializeVarInt(OutputStream& stream,
										  Value& inValue,
										  Value minValue,
										  Value maxValue)
	{
		Value value = inValue;

		if(value < minValue || value > maxValue)
		{
			throw FatalSerializationException(
				std::string("out-of-range value: ") + std::to_string(minValue)
				+ "<=" + std::to_string(value) + "<=" + std::to_string(maxValue));
		}

		bool more = true;
		while(more)
		{
			U8 outputByte = (U8)(value & 127);
			value >>= 7;
			more = std::is_signed<Value>::value
					   ? (value != 0 && value != Value(-1)) || (value >= 0 && (outputByte & 0x40))
							 || (value < 0 && !(outputByte & 0x40))
					   : (value != 0);
			if(more) { outputByte |= 0x80; }
			*stream.advance(1) = outputByte;
		};
	}

	template<typename Value, Uptr maxBits>
	WAVM_FORCEINLINE void serializeVarInt(InputStream& stream,
										  Value& value,
										  Value minValue,
										  Value maxValue)
	{
		// First, read the variable number of input bytes into a fixed size buffer.
		static constexpr Uptr maxBytes = (maxBits + 6) / 7;
		U8 bytes[maxBytes] = {0};
		Uptr numBytes = 0;
		I8 signExtendShift = (I8)sizeof(Value) * 8;
		while(numBytes < maxBytes)
		{
			U8 byte = *stream.advance(1);
			bytes[numBytes] = byte;
			++numBytes;
			signExtendShift -= 7;
			if(!(byte & 0x80)) { break; }
		};

		// Ensure that the input does not encode more than maxBits of data.
		static constexpr Uptr numUsedBitsInLastByte = maxBits - (maxBytes - 1) * 7;
		static constexpr Uptr numUnusedBitsInLast = 8 - numUsedBitsInLastByte;
		static constexpr U8 lastBitUsedMask = U8(1 << (numUsedBitsInLastByte - 1));
		static constexpr U8 lastByteUsedMask = U8(1 << numUsedBitsInLastByte) - U8(1);
		static constexpr U8 lastByteSignedMask = U8(~U8(lastByteUsedMask) & ~U8(0x80));
		const U8 lastByte = bytes[maxBytes - 1];
		if(!std::is_signed<Value>::value)
		{
			if((lastByte & ~lastByteUsedMask) != 0)
			{
				throw FatalSerializationException(
					"Invalid unsigned LEB encoding: unused bits in final byte must be 0");
			}
		}
		else
		{
			const I8 signBit = I8((lastByte & lastBitUsedMask) << numUnusedBitsInLast);
			const I8 signExtendedLastBit = signBit >> numUnusedBitsInLast;
			if((lastByte & ~lastByteUsedMask) != (signExtendedLastBit & lastByteSignedMask))
			{
				throw FatalSerializationException(
					"Invalid signed LEB encoding: unused bits in final byte must match the "
					"most-significant used bit");
			}
		}

		// Decode the buffer's bytes into the output integer.
		value = 0;
		for(Uptr byteIndex = 0; byteIndex < maxBytes; ++byteIndex)
		{ value |= Value(U64(bytes[byteIndex] & ~0x80) << U64(byteIndex * 7)); }

		// Sign extend the output integer to the full size of Value.
		if(std::is_signed<Value>::value && signExtendShift > 0)
		{ value = Value(value << signExtendShift) >> signExtendShift; }

		// Check that the output integer is in the expected range.
		if(value < minValue || value > maxValue)
		{
			throw FatalSerializationException(
				std::string("out-of-range value: ") + std::to_string(minValue)
				+ "<=" + std::to_string(value) + "<=" + std::to_string(maxValue));
		}
	}

	// Helpers for various common LEB128 parameters.
	template<typename Stream, typename Value> void serializeVarUInt1(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 1>(stream, value, 0, 1);
	}
	template<typename Stream, typename Value> void serializeVarUInt7(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 7>(stream, value, 0, 127);
	}
	template<typename Stream, typename Value> void serializeVarUInt8(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 8>(stream, value, 0, 255);
	}
	template<typename Stream, typename Value> void serializeVarUInt32(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 32>(stream, value, 0, UINT32_MAX);
	}
	template<typename Stream, typename Value> void serializeVarUInt64(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 64>(stream, value, 0, UINT64_MAX);
	}
	template<typename Stream, typename Value> void serializeVarInt7(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 7>(stream, value, -64, 63);
	}
	template<typename Stream, typename Value> void serializeVarInt32(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 32>(stream, value, INT32_MIN, INT32_MAX);
	}
	template<typename Stream, typename Value> void serializeVarInt64(Stream& stream, Value& value)
	{
		serializeVarInt<Value, 64>(stream, value, INT64_MIN, INT64_MAX);
	}
}}
