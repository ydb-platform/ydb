 #!/usr/bin/python
 # -*- coding: utf-8 -*-

from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from builtins import range
from builtins import int
from builtins import chr
from future import standard_library
standard_library.install_aliases()
from builtins import object
import math
import re


keyStrBase64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
keyStrUriSafe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-$"
baseReverseDic = {};

class Object(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def getBaseValue(alphabet, character):
    if alphabet not in baseReverseDic:
        baseReverseDic[alphabet] = {}
    for i in range(len(alphabet)):
        baseReverseDic[alphabet][alphabet[i]] = i
    return baseReverseDic[alphabet][character]


def _compress(uncompressed, bitsPerChar, getCharFromInt):
    if (uncompressed is None):
        return ""

    context_dictionary = {}
    context_dictionaryToCreate= {}
    context_c = ""
    context_wc = ""
    context_w = ""
    context_enlargeIn = 2 # Compensate for the first entry which should not count
    context_dictSize = 3
    context_numBits = 2
    context_data = []
    context_data_val = 0
    context_data_position = 0

    for ii in range(len(uncompressed)):
        context_c = uncompressed[ii]
        if context_c not in context_dictionary:
            context_dictionary[context_c] = context_dictSize
            context_dictSize += 1
            context_dictionaryToCreate[context_c] = True

        context_wc = context_w + context_c
        if context_wc in context_dictionary:
            context_w = context_wc
        else:
            if context_w in context_dictionaryToCreate:
                if ord(context_w[0]) < 256:
                    for i in range(context_numBits):
                        context_data_val = (context_data_val << 1)
                        if context_data_position == bitsPerChar-1:
                            context_data_position = 0
                            context_data.append(getCharFromInt(context_data_val))
                            context_data_val = 0
                        else:
                            context_data_position += 1
                    value = ord(context_w[0])
                    for i in range(8):
                        context_data_val = (context_data_val << 1) | (value & 1)
                        if context_data_position == bitsPerChar - 1:
                            context_data_position = 0
                            context_data.append(getCharFromInt(context_data_val))
                            context_data_val = 0
                        else:
                            context_data_position += 1
                        value = value >> 1

                else:
                    value = 1
                    for i in range(context_numBits):
                        context_data_val = (context_data_val << 1) | value
                        if context_data_position == bitsPerChar - 1:
                            context_data_position = 0
                            context_data.append(getCharFromInt(context_data_val))
                            context_data_val = 0
                        else:
                            context_data_position += 1
                        value = 0
                    value = ord(context_w[0])
                    for i in range(16):
                        context_data_val = (context_data_val << 1) | (value & 1)
                        if context_data_position == bitsPerChar - 1:
                            context_data_position = 0
                            context_data.append(getCharFromInt(context_data_val))
                            context_data_val = 0
                        else:
                            context_data_position += 1
                        value = value >> 1
                context_enlargeIn -= 1
                if context_enlargeIn == 0:
                    context_enlargeIn = math.pow(2, context_numBits)
                    context_numBits += 1
                del context_dictionaryToCreate[context_w]
            else:
                value = context_dictionary[context_w]
                for i in range(context_numBits):
                    context_data_val = (context_data_val << 1) | (value & 1)
                    if context_data_position == bitsPerChar - 1:
                        context_data_position = 0
                        context_data.append(getCharFromInt(context_data_val))
                        context_data_val = 0
                    else:
                        context_data_position += 1
                    value = value >> 1

            context_enlargeIn -= 1
            if context_enlargeIn == 0:
                context_enlargeIn = math.pow(2, context_numBits)
                context_numBits += 1
            
            # Add wc to the dictionary.
            context_dictionary[context_wc] = context_dictSize
            context_dictSize += 1
            context_w = str(context_c)

    # Output the code for w.
    if context_w != "":
        if context_w in context_dictionaryToCreate:
            if ord(context_w[0]) < 256:
                for i in range(context_numBits):
                    context_data_val = (context_data_val << 1)
                    if context_data_position == bitsPerChar-1:
                        context_data_position = 0
                        context_data.append(getCharFromInt(context_data_val))
                        context_data_val = 0
                    else:
                        context_data_position += 1
                value = ord(context_w[0])
                for i in range(8):
                    context_data_val = (context_data_val << 1) | (value & 1)
                    if context_data_position == bitsPerChar - 1:
                        context_data_position = 0
                        context_data.append(getCharFromInt(context_data_val))
                        context_data_val = 0
                    else:
                        context_data_position += 1
                    value = value >> 1
            else:
                value = 1
                for i in range(context_numBits):
                    context_data_val = (context_data_val << 1) | value
                    if context_data_position == bitsPerChar - 1:
                        context_data_position = 0
                        context_data.append(getCharFromInt(context_data_val))
                        context_data_val = 0
                    else:
                        context_data_position += 1
                    value = 0
                value = ord(context_w[0])
                for i in range(16):
                    context_data_val = (context_data_val << 1) | (value & 1)
                    if context_data_position == bitsPerChar - 1:
                        context_data_position = 0
                        context_data.append(getCharFromInt(context_data_val))
                        context_data_val = 0
                    else:
                        context_data_position += 1
                    value = value >> 1
            context_enlargeIn -= 1
            if context_enlargeIn == 0:
                context_enlargeIn = math.pow(2, context_numBits)
                context_numBits += 1
            del context_dictionaryToCreate[context_w]
        else:
            value = context_dictionary[context_w]
            for i in range(context_numBits):
                context_data_val = (context_data_val << 1) | (value & 1)
                if context_data_position == bitsPerChar - 1:
                    context_data_position = 0
                    context_data.append(getCharFromInt(context_data_val))
                    context_data_val = 0
                else:
                    context_data_position += 1
                value = value >> 1

    context_enlargeIn -= 1
    if context_enlargeIn == 0:
        context_enlargeIn = math.pow(2, context_numBits)
        context_numBits += 1

    # Mark the end of the stream
    value = 2
    for i in range(context_numBits):
        context_data_val = (context_data_val << 1) | (value & 1)
        if context_data_position == bitsPerChar - 1:
            context_data_position = 0
            context_data.append(getCharFromInt(context_data_val))
            context_data_val = 0
        else:
            context_data_position += 1
        value = value >> 1

    # Flush the last char
    while True:
        context_data_val = (context_data_val << 1)
        if context_data_position == bitsPerChar - 1:
            context_data.append(getCharFromInt(context_data_val))
            break
        else:
           context_data_position += 1

    return "".join(context_data)


def _decompress(length, resetValue, getNextValue):
    dictionary = {}
    enlargeIn = 4
    dictSize = 4
    numBits = 3
    entry = ""
    result = []

    data = Object(
        val=getNextValue(0),
        position=resetValue,
        index=1
    )

    for i in range(3):
        dictionary[i] = i

    bits = 0
    maxpower = math.pow(2, 2)
    power = 1

    while power != maxpower:
        resb = data.val & data.position
        data.position >>= 1
        if data.position == 0:
            data.position = resetValue
            data.val = getNextValue(data.index)
            data.index += 1

        bits |= power if resb > 0 else 0
        power <<= 1;

    next = bits
    if next == 0:
        bits = 0
        maxpower = math.pow(2, 8)
        power = 1
        while power != maxpower:
            resb = data.val & data.position
            data.position >>= 1
            if data.position == 0:
                data.position = resetValue
                data.val = getNextValue(data.index)
                data.index += 1
            bits |= power if resb > 0 else 0
            power <<= 1
        c = chr(bits)
    elif next == 1:
        bits = 0
        maxpower = math.pow(2, 16)
        power = 1
        while power != maxpower:
            resb = data.val & data.position
            data.position >>= 1
            if data.position == 0:
                data.position = resetValue;
                data.val = getNextValue(data.index)
                data.index += 1
            bits |= power if resb > 0 else 0
            power <<= 1
        c = chr(bits)
    elif next == 2:
        return ""

    dictionary[3] = c
    w = c
    result.append(c)
    counter = 0
    while True:
        counter += 1
        if data.index > length:
            return ""

        bits = 0
        maxpower = math.pow(2, numBits)
        power = 1
        while power != maxpower:
            resb = data.val & data.position
            data.position >>= 1
            if data.position == 0:
                data.position = resetValue;
                data.val = getNextValue(data.index)
                data.index += 1
            bits |= power if resb > 0 else 0
            power <<= 1

        c = bits
        if c == 0:
            bits = 0
            maxpower = math.pow(2, 8)
            power = 1
            while power != maxpower:
                resb = data.val & data.position
                data.position >>= 1
                if data.position == 0:
                    data.position = resetValue
                    data.val = getNextValue(data.index)
                    data.index += 1
                bits |= power if resb > 0 else 0
                power <<= 1

            dictionary[dictSize] = chr(bits)
            dictSize += 1
            c = dictSize - 1
            enlargeIn -= 1
        elif c == 1:
            bits = 0
            maxpower = math.pow(2, 16)
            power = 1
            while power != maxpower:
                resb = data.val & data.position
                data.position >>= 1
                if data.position == 0:
                    data.position = resetValue;
                    data.val = getNextValue(data.index)
                    data.index += 1
                bits |= power if resb > 0 else 0
                power <<= 1
            dictionary[dictSize] = chr(bits)
            dictSize += 1
            c = dictSize - 1
            enlargeIn -= 1
        elif c == 2:
            return "".join(result)


        if enlargeIn == 0:
            enlargeIn = math.pow(2, numBits)
            numBits += 1

        if c in dictionary:
            entry = dictionary[c]
        else:
            if c == dictSize:
                entry = w + w[0]
            else:
                return None
        result.append(entry)

        # Add w+entry[0] to the dictionary.
        dictionary[dictSize] = w + entry[0]
        dictSize += 1
        enlargeIn -= 1

        w = entry
        if enlargeIn == 0:
            enlargeIn = math.pow(2, numBits)
            numBits += 1


class LZString(object):
    @staticmethod
    def compress(uncompressed):
        return _compress(uncompressed, 16, chr)

    @staticmethod
    def compressToUTF16(uncompressed):
        if uncompressed is None:
            return ""
        return _compress(uncompressed, 15, lambda a: chr(a+32)) + " "

    @staticmethod
    def compressToBase64(uncompressed):
        if uncompressed is None:
            return ""
        res = _compress(uncompressed, 6, lambda a: keyStrBase64[a])
        # To produce valid Base64
        end = len(res) % 4
        if end > 0:
            res += "="*(4 - end)
        return res

    @staticmethod
    def compressToEncodedURIComponent(uncompressed):
        if uncompressed is None:
            return ""
        return _compress(uncompressed, 6, lambda a: keyStrUriSafe[a])

    @staticmethod
    def decompress(compressed):
        if compressed is None:
            return ""
        if compressed == "":
            return None
        return _decompress(len(compressed), 32768, lambda index: ord(compressed[index]))

    @staticmethod
    def decompressFromUTF16(compressed):
        if compressed is None:
            return ""
        if compressed == "":
            return None
        return _decompress(len(compressed), 16384, lambda index: compressed[index] - 32)

    @staticmethod
    def decompressFromBase64(compressed):
        if compressed is None:
            return ""
        if compressed == "":
            return None
        return _decompress(len(compressed), 32, lambda index: getBaseValue(keyStrBase64, compressed[index]))

    @staticmethod
    def decompressFromEncodedURIComponent(compressed):
        if compressed is None:
            return ""
        if compressed == "":
            return None
        compressed = compressed.replace(" ", "+")
        return _decompress(len(compressed), 32, lambda index: getBaseValue(keyStrUriSafe, compressed[index]))
