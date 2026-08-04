#pragma once

#include "BasicTypes.h"
#include "WAVM/Inline/Assert.h"

namespace WAVM { namespace Unicode {
	template<typename String> void encodeUTF8CodePoint(U32 codePoint, String& outString)
	{
		if(codePoint < 0x80) { outString += char(codePoint); }
		else if(codePoint < 0x800)
		{
			outString += char((codePoint >> 6) & 0x1F) | 0xC0;
			outString += char((codePoint)&0x3F) | 0x80;
		}
		else if(codePoint < 0x10000)
		{
			outString += char((codePoint >> 12) & 0x0F) | 0xE0;
			outString += char((codePoint >> 6) & 0x3F) | 0x80;
			outString += char((codePoint)&0x3F) | 0x80;
		}
		else
		{
			WAVM_ASSERT(codePoint < 0x200000);
			outString += char((codePoint >> 18) & 0x07) | 0xF0;
			outString += char((codePoint >> 12) & 0x3F) | 0x80;
			outString += char((codePoint >> 6) & 0x3F) | 0x80;
			outString += char((codePoint)&0x3F) | 0x80;
		}
	}

	inline bool decodeUTF8CodePoint(const U8*& nextChar, const U8* endChar, U32& outCodePoint)
	{
		// Decode a UTF-8 byte sequence to a Unicode codepoint.
		// The valid ranges are taken from table 3-7 in the Unicode Standard 9.0:
		// "Well-Formed UTF-8 Byte Sequences"
		if(*nextChar < 0x80)
		{
			outCodePoint = *nextChar;
			++nextChar;
			return true;
		}
		else if(*nextChar >= 0xc2 && *nextChar <= 0xdf)
		{
			if(nextChar + 1 >= endChar || nextChar[1] < 0x80 || nextChar[1] > 0xbf)
			{ return false; }

			outCodePoint = (U32(nextChar[0] & 0x1F) << 6) | U32(nextChar[1] & 0x3F);
			nextChar += 2;
			return true;
		}
		else if(*nextChar >= 0xe0 && *nextChar <= 0xef)
		{
			if(*nextChar == 0xe0)
			{
				if(nextChar + 2 >= endChar || nextChar[1] < 0xa0 || nextChar[1] > 0xbf
				   || nextChar[2] < 0x80 || nextChar[2] > 0xbf)
				{ return false; }
			}
			else if(*nextChar == 0xed)
			{
				if(nextChar + 2 >= endChar || nextChar[1] < 0x80 || nextChar[1] > 0x9f
				   || nextChar[2] < 0x80 || nextChar[2] > 0xbf)
				{ return false; }
			}
			else if(*nextChar >= 0xe1 && *nextChar <= 0xef)
			{
				if(nextChar + 2 >= endChar || nextChar[1] < 0x80 || nextChar[1] > 0xbf
				   || nextChar[2] < 0x80 || nextChar[2] > 0xbf)
				{ return false; }
			}
			outCodePoint = (U32(nextChar[0] & 0x0F) << 12) | (U32(nextChar[1] & 0x3F) << 6)
						   | U32(nextChar[2] & 0x3F);
			nextChar += 3;
			return true;
		}
		else if(*nextChar >= 0xf0 && *nextChar <= 0xf4)
		{
			if(*nextChar == 0xf0)
			{
				if(nextChar + 3 >= endChar || nextChar[1] < 0x90 || nextChar[1] > 0xbf
				   || nextChar[2] < 0x80 || nextChar[2] > 0xbf || nextChar[3] < 0x80
				   || nextChar[3] > 0xbf)
				{ return false; }
			}
			else if(*nextChar >= 0xf1 && *nextChar <= 0xf3)
			{
				if(nextChar + 3 >= endChar || nextChar[1] < 0x80 || nextChar[1] > 0xbf
				   || nextChar[2] < 0x80 || nextChar[2] > 0xbf || nextChar[3] < 0x80
				   || nextChar[3] > 0xbf)
				{ return false; }
			}
			else if(*nextChar == 0xf4)
			{
				if(nextChar + 3 >= endChar || nextChar[1] < 0x80 || nextChar[1] > 0x8f
				   || nextChar[2] < 0x80 || nextChar[2] > 0xbf || nextChar[3] < 0x80
				   || nextChar[3] > 0xbf)
				{ return false; }
			}

			outCodePoint = (U32(nextChar[0] & 0x07) << 18) | (U32(nextChar[1] & 0x3F) << 12)
						   | (U32(nextChar[2] & 0x3F) << 6) | U32(nextChar[3] & 0x3F);
			nextChar += 4;
			return true;
		}
		else
		{
			return false;
		}
	}

	inline bool decodeUTF16CodePoint(const U16*& nextChar16,
									 const U16* endChar16,
									 U32& outCodePoint)
	{
		// Decode a UTF-16 byte sequence to a Unicode codepoint.
		if(nextChar16[0] < 0xd800 || nextChar16[0] >= 0xe000)
		{
			outCodePoint = nextChar16[0];
			++nextChar16;
			return true;
		}
		else if(nextChar16[0] <= 0xdbff)
		{
			if(nextChar16[1] >= 0xdc00 && nextChar16[1] <= 0xdfff)
			{
				outCodePoint
					= ((nextChar16[0] - 0xd800) << 10) + (nextChar16[1] - 0xdc00) + 0x10000;
				nextChar16 += 2;
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}

	inline const U8* validateUTF8String(const U8* nextChar, const U8* endChar)
	{
		U32 codePoint;
		while(nextChar != endChar && decodeUTF8CodePoint(nextChar, endChar, codePoint)) {};
		return nextChar;
	}

	template<typename String> void encodeUTF16CodePoint(U32 codePoint, String& outString)
	{
		if(codePoint < 0x10000) { outString += U16(codePoint); }
		else
		{
			WAVM_ASSERT(codePoint < 0x110000);
			outString += U16(((codePoint - 0x10000) >> 10) & 0x3ff) | 0xd800;
			outString += U16((codePoint - 0x10000) & 0x3ff) | 0xdc00;
		}
	}

	template<typename String>
	const U8* transcodeUTF8ToUTF16(const U8* nextChar, const U8* endChar, String& outString)
	{
		U32 codePoint;
		while(nextChar != endChar && decodeUTF8CodePoint(nextChar, endChar, codePoint))
		{ encodeUTF16CodePoint(codePoint, outString); };
		return nextChar;
	}

	template<typename String>
	const U16* transcodeUTF16ToUTF8(const U16* nextChar, const U16* endChar, String& outString)
	{
		U32 codePoint;
		while(nextChar != endChar && decodeUTF16CodePoint(nextChar, endChar, codePoint))
		{ encodeUTF8CodePoint(codePoint, outString); };
		return nextChar;
	}
}}
