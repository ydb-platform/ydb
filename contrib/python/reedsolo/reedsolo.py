#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012-2015 Tomer Filiba <tomerfiliba@gmail.com>
# Copyright (c) 2015 rotorgit
# Copyright (c) 2015-2023 Stephen Larroque <LRQ3000@gmail.com>

'''
Reed Solomon
============

A pure-python `universal errors-and-erasures Reed-Solomon Codec <http://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction>`_
, based on the wonderful tutorial at
`wikiversity <http://en.wikiversity.org/wiki/Reed%E2%80%93Solomon_codes_for_coders>`_,
written by "Bobmath" and "LRQ3000".

The code of wikiversity is here consolidated into a nice API with exceptions handling.
The algorithm can correct up to 2*e+v <= nsym, where e is the number of errors,
v the number of erasures and nsym = n-k = the number of ECC (error correction code) symbols.
This means that you can either correct exactly floor(nsym/2) errors, or nsym erasures
(errors where you know the position), and a combination of both errors and erasures.
The code should work on pretty much any reasonable version of python (2.4-3.5),
but I'm only testing on 2.7 - 3.4.

.. note::
   The codec is universal, meaning that it can decode any message encoded by another RS encoder
   as long as you provide the correct parameters.
   Note however that if you use higher fields (ie, bigger c_exp), the algorithms will be slower, first because
   we cannot then use the optimized bytearray() structure but only array.array('i', ...), and also because
   Reed-Solomon's complexity is quadratic (both in encoding and decoding), so this means that the longer
   your messages, the longer it will take to encode/decode (quadratically!).

   The algorithm itself can handle messages up to (2^c_exp)-1 symbols, including the ECC symbols,
   and each symbol can have a value of up to (2^c_exp)-1 (indeed, both the message length and the maximum
   value for one character is constrained by the same mathematical reason). By default, we use the field GF(2^8),
   which means that you are limited to values between 0 and 255 (perfect to represent a single hexadecimal
   symbol on computers, so you can encode any binary stream) and limited to messages+ecc of maximum
   length 255. However, you can "chunk" longer messages to fit them into the message length limit.
   The ``RSCodec`` class will automatically apply chunking, by splitting longer messages into chunks and
   encode/decode them separately; it shouldn't make a difference from an API perspective (ie, from your POV).

::

    # Initialization
    >>> from reedsolo import RSCodec
    >>> rsc = RSCodec(10)  # 10 ecc symbols

    # Encoding
    >>> rsc.encode([1,2,3,4])
    b'\x01\x02\x03\x04,\x9d\x1c+=\xf8h\xfa\x98M'
    >>> rsc.encode(bytearray([1,2,3,4]))
    bytearray(b'\x01\x02\x03\x04,\x9d\x1c+=\xf8h\xfa\x98M')
    >>> rsc.encode(b'hello world')
    b'hello world\xed%T\xc4\xfd\xfd\x89\xf3\xa8\xaa'
    # Note that chunking is supported transparently to encode any string length.

    # Decoding (repairing)
    >>> rsc.decode(b'hello world\xed%T\xc4\xfd\xfd\x89\xf3\xa8\xaa')[0]
    b'hello world'
    >>> rsc.decode(b'heXlo worXd\xed%T\xc4\xfdX\x89\xf3\xa8\xaa')[0]     # 3 errors
    b'hello world'
    >>> rsc.decode(b'hXXXo worXd\xed%T\xc4\xfdX\x89\xf3\xa8\xaa')[0]     # 5 errors
    b'hello world'
    >>> rsc.decode(b'hXXXo worXd\xed%T\xc4\xfdXX\xf3\xa8\xaa')[0]        # 6 errors - fail
    Traceback (most recent call last):
      ...
    ReedSolomonError: Could not locate error

    >>> rsc = RSCodec(12)  # using 2 more ecc symbols (to correct max 6 errors or 12 erasures)
    >>> rsc.encode(b'hello world')
    b'hello world?Ay\xb2\xbc\xdc\x01q\xb9\xe3\xe2='
    >>> rsc.decode(b'hello worXXXXy\xb2XX\x01q\xb9\xe3\xe2=')[0]         # 6 errors - ok
    b'hello world'
    >>> rsc.decode(b'helXXXXXXXXXXy\xb2XX\x01q\xb9\xe3\xe2=', erase_pos=[3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16])[0]  # 12 erasures - OK
    b'hello world'

    # Checking
    >> rsc.check(b'hello worXXXXy\xb2XX\x01q\xb9\xe3\xe2=')
    [False]
    >> rmes, rmesecc = rsc.decode(b'hello worXXXXy\xb2XX\x01q\xb9\xe3\xe2=')
    >> rsc.check(rmesecc)
    [True]

    # To use longer chunks or bigger values than 255 (may be very slow)
    >> rsc = RSCodec(12, nsize=4095)  # always use a power of 2 minus 1
    >> rsc = RSCodec(12, c_exp=12)  # alternative way to set nsize=4095
    >> mes = 'a' * (4095-12)
    >> mesecc = rsc.encode(mes)
    >> mesecc[2] = 1
    >> mesecc[-1] = 1
    >> rmes, rmesecc = rsc.decode(mesecc)
    >> rsc.check(mesecc)
    [False]
    >> rsc.check(rmesecc)
    [True]

    If you want full control, you can skip the API and directly use the library as-is. Here's how:

    First you need to init the precomputed tables:
    >> import reedsolo as rs
    >> rs.init_tables(0x11d)
    Pro tip: if you get the error: ValueError: byte must be in range(0, 256), please check that your prime polynomial is correct for your field.
    Pro tip2: by default, you can only encode messages of max length and max symbol value = 256. If you want to encode bigger messages,
    please use the following (where c_exp is the exponent of your Galois Field, eg, 12 = max length 2^12 = 4096):
    >> prim = rs.find_prime_polys(c_exp=12, fast_primes=True, single=True)[0]
    >> rs.init_tables(c_exp=12, prim=prim)
    
    Let's define our RS message and ecc size:
    >> n = 255  # length of total message+ecc
    >> nsym = 12  # length of ecc
    >> mes = "a" * (n-nsym)  # generate a sample message

    To optimize, you can precompute the generator polynomial:
    >> gen = rs.rs_generator_poly_all(n)

    Then to encode:
    >> mesecc = rs.rs_encode_msg(mes, nsym, gen=gen[nsym])

    Let's tamper our message:
    >> mesecc[1] = 0

    To decode:
    >> rmes, recc, errata_pos = rs.rs_correct_msg(mesecc, nsym, erase_pos=erase_pos)
    Note that both the message and the ecc are corrected (if possible of course).
    Pro tip: if you know a few erasures positions, you can specify them in a list `erase_pos` to double the repair power. But you can also just specify an empty list.

    If the decoding fails, it will normally automatically check and raise a ReedSolomonError exception that you can handle.
    However if you want to manually check if the repaired message is correct, you can do so:
    >> rs.rs_check(rmes + recc, nsym)

    Note: if you want to use multiple reedsolomon with different parameters, you need to backup the globals and restore them before calling reedsolo functions:
    >> rs.init_tables()
    >> global gf_log, gf_exp, field_charac
    >> bak_gf_log, bak_gf_exp, bak_field_charac = gf_log, gf_exp, field_charac
    Then at anytime, you can do:
    >> global gf_log, gf_exp, field_charac
    >> gf_log, gf_exp, field_charac = bak_gf_log, bak_gf_exp, bak_field_charac
    >> mesecc = rs.rs_encode_msg(mes, nsym)
    >> rmes, recc, errata_pos = rs.rs_correct_msg(mesecc, nsym)
    The globals backup is not necessary if you use RSCodec, it will be automatically managed.

    Read the sourcecode's comments for more info about how it works, and for the various parameters you can setup if
    you need to interface with other RS codecs.

'''

# TODO IMPORTANT: try to keep the same convention for the ordering of polynomials inside lists throughout the code and functions (because for now there are a lot of list reversing in order to make it work, you never know the order of a polynomial, ie, if the first coefficient is the major degree or the constant term...).

import array
import math


################### INIT and stuff ###################

try:  # pragma: no cover
    bytearray
    _bytearray = bytearray
except NameError:  # pragma: no cover
    def _bytearray(obj = 0, encoding = "latin-1"):  # pragma: no cover
        '''Simple pure-python bytearray replacement if not implemented'''
        # always use Latin-1 and not UTF8 because Latin-1 maps the first 256 characters to their bytevalue equivalents. UTF8 may mangle your data (particularly at vale 128)
        if isinstance(obj, str):
            obj = [ord(ch) for ch in obj.encode(encoding)]
        elif isinstance(obj, int):
            obj = [0] * obj
        return array.array("B", obj)

try:  # pragma: no cover
    # compatibility with Python 2.7
    xrange
except NameError:  # pragma: no cover
    # compatibility with Python 3+
    xrange = range

class ReedSolomonError(Exception):
    pass

gf_exp = _bytearray([1] * 512) # For efficiency, gf_exp[] has size 2*GF_SIZE, so that a simple multiplication of two numbers can be resolved without calling % 255. For more infos on how to generate this extended exponentiation table, see paper: "Fast software implementation of finite field operations", Cheng Huang and Lihao Xu, Washington University in St. Louis, Tech. Rep (2003).
gf_log = _bytearray(256)
field_charac = int(2**8 - 1)

################### GALOIS FIELD ELEMENTS MATHS ###################
# General note: Galois Field maths essentially are all the standard math operations everybody learn in primary school,
# but with only integer AND they can wraparound (ie, we use a modulo), so that in practice this means that
# Galois Field math operations are bounded in a very specific range of values. This changes a lot how the maths are done,
# but not that much, so you can still wrap your head around if you are willing to spend some time.

def rwh_primes1(n):
    # http://stackoverflow.com/questions/2068372/fastest-way-to-list-all-primes-below-n-in-python/3035188#3035188
    ''' Returns a list of primes < n '''
    n_half = int(n/2)
    sieve = [True] * n_half
    for i in xrange(3,int(math.pow(n,0.5))+1,2):
        if sieve[int(i/2)]:
            sieve[int((i*i)/2)::i] = [False] * int((n-i*i-1)/(2*i)+1)
    return array.array('i', [2] + [2*i+1 for i in xrange(1,n_half) if sieve[i]])

def find_prime_polys(generator=2, c_exp=8, fast_primes=False, single=False):
    '''Compute the list of prime polynomials for the given generator and galois field characteristic exponent.'''
    # fast_primes will output less results but will be significantly faster.
    # single will output the first prime polynomial found, so if all you want is to just find one prime polynomial to generate the LUT for Reed-Solomon to work, then just use that.

    # A prime polynomial (necessarily irreducible) is necessary to reduce the multiplications in the Galois Field, so as to avoid overflows.
    # Why do we need a "prime polynomial"? Can't we just reduce modulo 255 (for GF(2^8) for example)? Because we need the values to be unique.
    # For example: if the generator (alpha) = 2 and c_exp = 8 (GF(2^8) == GF(256)), then the generated Galois Field (0, 1, α, α^1, α^2, ..., α^(p-1)) will be galois field it becomes 0, 1, 2, 4, 8, 16, etc. However, upon reaching 128, the next value will be doubled (ie, next power of 2), which will give 256. Then we must reduce, because we have overflowed above the maximum value of 255. But, if we modulo 255, this will generate 256 == 1. Then 2, 4, 8, 16, etc. giving us a repeating pattern of numbers. This is very bad, as it's then not anymore a bijection (ie, a non-zero value doesn't have a unique index). That's why we can't just modulo 255, but we need another number above 255, which is called the prime polynomial.
    # Why so much hassle? Because we are using precomputed look-up tables for multiplication: instead of multiplying a*b, we precompute alpha^a, alpha^b and alpha^(a+b), so that we can just use our lookup table at alpha^(a+b) and get our result. But just like in our original field we had 0,1,2,...,p-1 distinct unique values, in our "LUT" field using alpha we must have unique distinct values (we don't care that they are different from the original field as long as they are unique and distinct). That's why we need to avoid duplicated values, and to avoid duplicated values we need to use a prime irreducible polynomial.

    # Here is implemented a bruteforce approach to find all these prime polynomials, by generating every possible prime polynomials (ie, every integers between field_charac+1 and field_charac*2), and then we build the whole Galois Field, and we reject the candidate prime polynomial if it duplicates even one value or if it generates a value above field_charac (ie, cause an overflow).
    # Note that this algorithm is slow if the field is too big (above 12), because it's an exhaustive search algorithm. There are probabilistic approaches, and almost surely prime approaches, but there is no determistic polynomial time algorithm to find irreducible monic polynomials. More info can be found at: http://people.mpi-inf.mpg.de/~csaha/lectures/lec9.pdf
    # Another faster algorithm may be found at Adleman, Leonard M., and Hendrik W. Lenstra. "Finding irreducible polynomials over finite fields." Proceedings of the eighteenth annual ACM symposium on Theory of computing. ACM, 1986.

    # Prepare the finite field characteristic (2^p - 1), this also represent the maximum possible value in this field
    root_charac = 2 # we're in GF(2)
    field_charac = int(root_charac**c_exp - 1)
    field_charac_next = int(root_charac**(c_exp+1) - 1)

    if fast_primes:
        prim_candidates = rwh_primes1(field_charac_next) # generate maybe prime polynomials and check later if they really are irreducible
        prim_candidates = array.array('i', [x for x in prim_candidates if x > field_charac]) # filter out too small primes
    else:
        prim_candidates = array.array('i', xrange(field_charac+2, field_charac_next, root_charac)) # try each possible prime polynomial, but skip even numbers (because divisible by 2 so necessarily not irreducible)

    # Start of the main loop
    correct_primes = array.array('i', [])
    for prim in prim_candidates: # try potential candidates primitive irreducible polys
        seen = _bytearray(field_charac+1) # memory variable to indicate if a value was already generated in the field (value at index x is set to 1) or not (set to 0 by default)
        conflict = False # flag to know if there was at least one conflict

        # Second loop, build the whole Galois Field
        x = 1
        for i in xrange(field_charac):
            # Compute the next value in the field (ie, the next power of alpha/generator)
            x = gf_mult_noLUT(x, generator, prim, field_charac+1)

            # Rejection criterion: if the value overflowed (above field_charac) or is a duplicate of a previously generated power of alpha, then we reject this polynomial (not prime)
            if x > field_charac or seen[x] == 1:
                conflict = True
                break
            # Else we flag this value as seen (to maybe detect future duplicates), and we continue onto the next power of alpha
            else:
                seen[x] = 1

        # End of the second loop: if there's no conflict (no overflow nor duplicated value), this is a prime polynomial!
        if not conflict: 
            correct_primes.append(prim)
            if single: return array.array('i', [prim])  # for API consistency, we always return an array, but here with a single value

    # Return the list of all prime polynomials
    return correct_primes # you can use the following to print the hexadecimal representation of each prime polynomial: print [hex(i) for i in correct_primes]

def init_tables(prim=0x11d, generator=2, c_exp=8):
    '''Precompute the logarithm and anti-log tables for faster computation later, using the provided primitive polynomial.
    These tables are used for multiplication/division since addition/substraction are simple XOR operations inside GF of characteristic 2.
    The basic idea is quite simple: since b**(log_b(x), log_b(y)) == x * y given any number b (the base or generator of the logarithm), then we can use any number b to precompute logarithm and anti-log (exponentiation) tables to use for multiplying two numbers x and y.
    That's why when we use a different base/generator number, the log and anti-log tables are drastically different, but the resulting computations are the same given any such tables.
    For more infos, see https://en.wikipedia.org/wiki/Finite_field_arithmetic#Implementation_tricks
    '''
    # generator is the generator number (the "increment" that will be used to walk through the field by multiplication, this must be a prime number). This is basically the base of the logarithm/anti-log tables. Also often noted "alpha" in academic books.
    # prim is the primitive/prime (binary) polynomial and must be irreducible (ie, it can't represented as the product of two smaller polynomials). It's a polynomial in the binary sense: each bit is a coefficient, but in fact it's an integer between field_charac+1 and field_charac*2, and not a list of gf values. The prime polynomial will be used to reduce the overflows back into the range of the Galois Field without duplicating values (all values should be unique). See the function find_prime_polys() and: http://research.swtch.com/field and http://www.pclviewer.com/rs2/galois.html
    # note that the choice of generator or prime polynomial doesn't matter very much: any two finite fields of size p^n have identical structure, even if they give the individual elements different names (ie, the coefficients of the codeword will be different, but the final result will be the same: you can always correct as many errors/erasures with any choice for those parameters). That's why it makes sense to refer to all the finite fields, and all decoders based on Reed-Solomon, of size p^n as one concept: GF(p^n). It can however impact sensibly the speed (because some parameters will generate sparser tables).
    # c_exp is the exponent for the field's characteristic GF(2^c_exp)

    # Redefine _bytearray() in case we need to support integers or messages of length > 256
    global _bytearray
    if c_exp <= 8:
        _bytearray = bytearray
    else:
        def _bytearray(obj = 0, encoding = "latin-1"):
            '''Fake bytearray replacement, supporting int values above 255'''
            # always use Latin-1 and not UTF8 because Latin-1 maps the first 256 characters to their bytevalue equivalents. UTF8 may mangle your data (particularly at vale 128)
            if isinstance(obj, str):  # obj is a string, convert to list of ints
                obj = obj.encode(encoding)
                if isinstance(obj, str):  # Py2 str: convert to list of ascii ints
                    obj = [ord(chr) for chr in obj]
                elif isinstance(obj, bytes):  # Py3 bytes: characters are bytes, need to convert to int for array.array('i', obj)
                    obj = [int(chr) for chr in obj]
                else:
                    raise(ValueError, "Type of object not recognized!")
            elif isinstance(obj, int):  # compatibility with list preallocation bytearray(int)
                obj = [0] * obj
            elif isinstance(obj, bytes):
                obj = [int(b) for b in obj]
            # Else obj is a list of int, it's ok
            return array.array("i", obj)

    # Init global tables
    global gf_exp, gf_log, field_charac
    field_charac = int(2**c_exp - 1)
    gf_exp = _bytearray(field_charac * 2) # anti-log (exponential) table. The first two elements will always be [GF256int(1), generator]
    gf_log = _bytearray(field_charac+1) # log table, log[0] is impossible and thus unused

    # For each possible value in the galois field 2^8, we will pre-compute the logarithm and anti-logarithm (exponential) of this value
    # To do that, we generate the Galois Field F(2^p) by building a list starting with the element 0 followed by the (p-1) successive powers of the generator α : 1, α, α^1, α^2, ..., α^(p-1).
    x = 1
    for i in xrange(field_charac): # we could skip index 255 which is equal to index 0 because of modulo: g^255==g^0 but either way, this does not change the later outputs (ie, the ecc symbols will be the same either way)
        gf_exp[i] = x # compute anti-log for this value and store it in a table
        gf_log[x] = i # compute log at the same time
        x = gf_mult_noLUT(x, generator, prim, field_charac+1)

        # If you use only generator==2 or a power of 2, you can use the following which is faster than gf_mult_noLUT():
        #x <<= 1 # multiply by 2 (change 1 by another number y to multiply by a power of 2^y)
        #if x & 0x100: # similar to x >= 256, but a lot faster (because 0x100 == 256)
            #x ^= prim # substract the primary polynomial to the current value (instead of 255, so that we get a unique set made of coprime numbers), this is the core of the tables generation

    # Optimization: double the size of the anti-log table so that we don't need to mod 255 to stay inside the bounds (because we will mainly use this table for the multiplication of two GF numbers, no more).
    for i in xrange(field_charac, field_charac * 2):
        gf_exp[i] = gf_exp[i - field_charac]

    return gf_log, gf_exp, field_charac

def gf_add(x, y):
    '''Add two galois field integers'''
    return x ^ y

def gf_sub(x, y):
    '''Subtract two galois field integers'''
    return x ^ y # in binary galois field, substraction is just the same as addition (since we mod 2)

def gf_neg(x):
    '''Negate one galois field integer (does nothing)'''
    return x

def gf_inverse(x):
    '''Inverse of a galois field integer'''
    return gf_exp[field_charac - gf_log[x]] # gf_inverse(x) == gf_div(1, x)

def gf_mul(x, y):
    '''Multiply two galois field integers'''
    if x == 0 or y == 0:
        return 0
    return gf_exp[(gf_log[x] + gf_log[y]) % field_charac]

def gf_div(x, y):
    '''Divide x by y galois field integers'''
    if y == 0:
        raise ZeroDivisionError()
    if x == 0:
        return 0
    return gf_exp[(gf_log[x] + field_charac - gf_log[y]) % field_charac]

def gf_pow(x, power):
    '''Power of x galois field integer'''
    return gf_exp[(gf_log[x] * power) % field_charac]

def gf_mult_noLUT_slow(x, y, prim=0):
    '''Multiplication in Galois Fields on-the-fly without using a precomputed look-up table (and thus it's slower) by using the standard carry-less multiplication + modular reduction using an irreducible prime polynomial.'''

    ### Define bitwise carry-less operations as inner functions ###
    def cl_mult(x,y):
        '''Bitwise carry-less multiplication on integers'''
        z = 0
        i = 0
        while (y>>i) > 0:
            if y & (1<<i):
                z ^= x<<i
            i += 1
        return z

    def bit_length(n):
        '''Compute the position of the most significant bit (1) of an integer. Equivalent to int.bit_length()'''
        bits = 0
        while n >> bits: bits += 1
        return bits
 
    def cl_div(dividend, divisor=None):
        '''Bitwise carry-less long division on integers and returns the remainder'''
        # Compute the position of the most significant bit for each integers
        dl1 = bit_length(dividend)
        dl2 = bit_length(divisor)
        # If the dividend is smaller than the divisor, just exit
        if dl1 < dl2:
            return dividend
        # Else, align the most significant 1 of the divisor to the most significant 1 of the dividend (by shifting the divisor)
        for i in xrange(dl1-dl2,-1,-1):
            # Check that the dividend is divisible (useless for the first iteration but important for the next ones)
            if dividend & (1 << i+dl2-1):
                # If divisible, then shift the divisor to align the most significant bits and XOR (carry-less substraction)
                dividend ^= divisor << i
        return dividend
 
    ### Main GF multiplication routine ###
 
    # Multiply the gf numbers
    result = cl_mult(x,y)
    # Then do a modular reduction (ie, remainder from the division) with an irreducible primitive polynomial so that it stays inside GF bounds
    if prim > 0:
        result = cl_div(result, prim)
 
    return result

def gf_mult_noLUT(x, y, prim=0, field_charac_full=256, carryless=True):
    '''Galois Field integer multiplication on-the-fly without using a look-up table, using Russian Peasant Multiplication algorithm (faster than the standard multiplication + modular reduction). This is still slower than using a look-up table, but is the fastest alternative, and is often used in embedded circuits where storage space is limited (ie, no space for a look-up table).
    If prim is 0 and carryless=False, then the function produces the result for a standard integers multiplication (no carry-less arithmetics nor modular reduction).'''
    r = 0
    while y: # while y is above 0
        if y & 1: r = r ^ x if carryless else r + x # y is odd, then add the corresponding x to r (the sum of all x's corresponding to odd y's will give the final product). Note that since we're in GF(2), the addition is in fact an XOR (very important because in GF(2) the multiplication and additions are carry-less, thus it changes the result!).
        y = y >> 1 # equivalent to y // 2
        x = x << 1 # equivalent to x*2
        if prim > 0 and x & field_charac_full: x = x ^ prim # GF modulo: if x >= 256 then apply modular reduction using the primitive polynomial (we just substract, but since the primitive number can be above 256 then we directly XOR).

    return r


################### GALOIS FIELD POLYNOMIALS MATHS ###################

def gf_poly_scale(p, x):
    '''Scale a galois field polynomial with a factor x (an integer)'''
    #return _bytearray([gf_mul(p[i], x) for i in xrange(len(p))])  # unoptimized one-liner
    out = _bytearray(len(p))
    for i in range(len(p)):
        out[i] = gf_mul(p[i], x)
    return out

def gf_poly_add(p, q):
    '''Add two galois field polynomials'''
    q_len = len(q)
    r = _bytearray( max(len(p), q_len) )
    r[len(r)-len(p):len(r)] = p
    #for i in xrange(len(p)):
        #r[i + len(r) - len(p)] = p[i]
    for i in xrange(q_len):
        r[i + len(r) - q_len] ^= q[i]
    return r

def gf_poly_mul(p, q):
    '''Multiply two polynomials, inside Galois Field (but the procedure is generic). Optimized function by precomputation of log.'''
    # Pre-allocate the result array
    r = _bytearray(len(p) + len(q) - 1)
    # Precompute the logarithm of p
    lp = [gf_log[p[i]] for i in xrange(len(p))]
    # Compute the polynomial multiplication (just like the outer product of two vectors, we multiply each coefficients of p with all coefficients of q)
    for j in xrange(len(q)):
        qj = q[j] # optimization: load the coefficient once
        if qj != 0: # log(0) is undefined, we need to check that
            lq = gf_log[qj] # Optimization: precache the logarithm of the current coefficient of q
            for i in xrange(len(p)):
                if p[i] != 0: # log(0) is undefined, need to check that...
                    r[i + j] ^= gf_exp[lp[i] + lq] # equivalent to: r[i + j] = gf_add(r[i+j], gf_mul(p[i], q[j]))
    return r

def gf_poly_mul_simple(p, q): # simple equivalent way of multiplying two polynomials without precomputation, but thus it's slower
    '''Multiply two polynomials, inside Galois Field'''
    # Pre-allocate the result array
    r = _bytearray(len(p) + len(q) - 1)
    # Compute the polynomial multiplication (just like the outer product of two vectors, we multiply each coefficients of p with all coefficients of q)
    for j in xrange(len(q)):
        for i in xrange(len(p)):
            r[i + j] ^= gf_mul(p[i], q[j]) # equivalent to: r[i + j] = gf_add(r[i+j], gf_mul(p[i], q[j])) -- you can see it's your usual polynomial multiplication
    return r

def gf_poly_neg(poly):
    '''Returns the polynomial with all coefficients negated. In GF(2^p), negation does not change the coefficient, so we return the polynomial as-is.'''
    return poly

def gf_poly_div(dividend, divisor):
    '''Fast polynomial division by using Extended Synthetic Division and optimized for GF(2^p) computations (doesn't work with standard polynomials outside of this galois field).'''
    # CAUTION: this function expects polynomials to follow the opposite convention at decoding: the terms must go from the biggest to lowest degree (while most other functions here expect a list from lowest to biggest degree). eg: 1 + 2x + 5x^2 = [5, 2, 1], NOT [1, 2, 5]

    msg_out = _bytearray(dividend) # Copy the dividend list and pad with 0 where the ecc bytes will be computed
    # Cache lengths for faster access inside loops
    divisor_len = len(divisor)
    #normalizer = divisor[0] # precomputing for performance
    for i in xrange(len(dividend) - (divisor_len-1)):
        #msg_out[i] /= normalizer # for general polynomial division (when polynomials are non-monic), the usual way of using synthetic division is to divide the divisor g(x) with its leading coefficient (call it a). In this implementation, this means:we need to compute: coef = msg_out[i] / gen[0]. For more infos, see http://en.wikipedia.org/wiki/Synthetic_division
        coef = msg_out[i] # precaching
        if coef != 0: # log(0) is undefined, so we need to avoid that case explicitly (and it's also a good optimization). In fact if you remove it, it should still work because gf_mul() will take care of the condition. But it's still a good practice to put the condition here.
            for j in xrange(1, divisor_len): # in synthetic division, we always skip the first coefficient of the divisior, because it's only used to normalize the dividend coefficient
                if divisor[j] != 0: # log(0) is undefined
                    msg_out[i + j] ^= gf_mul(divisor[j], coef) # equivalent to the more mathematically correct (but xoring directly is faster): msg_out[i + j] += -divisor[j] * coef

    # The resulting msg_out contains both the quotient and the remainder, the remainder being the size of the divisor (the remainder has necessarily the same degree as the divisor -- not length but degree == length-1 -- since it's what we couldn't divide from the dividend), so we compute the index where this separation is, and return the quotient and remainder.
    separator = -(divisor_len-1)
    return msg_out[:separator], msg_out[separator:] # return quotient, remainder.

def gf_poly_square(poly):  # pragma: no cover
    '''Linear time implementation of polynomial squaring. For details, see paper: "A fast software implementation for arithmetic operations in GF (2n)". De Win, E., Bosselaers, A., Vandenberghe, S., De Gersem, P., & Vandewalle, J. (1996, January). In Advances in Cryptology - Asiacrypt'96 (pp. 65-76). Springer Berlin Heidelberg.'''
    length = len(poly)
    out = _bytearray(2*length - 1)
    for i in xrange(length-1):
        p = poly[i]
        k = 2*i
        if p != 0:
            #out[k] = gf_exp[(2*gf_log[p]) % field_charac] # not necessary to modulo (2^r)-1 since gf_exp is duplicated up to 510.
            out[k] = gf_exp[2*gf_log[p]]
        #else: # not necessary since the output is already initialized to an array of 0
            #out[k] = 0
    out[2*length-2] = gf_exp[2*gf_log[poly[length-1]]]
    if out[0] == 0: out[0] = 2*poly[1] - 1
    return out

def gf_poly_eval(poly, x):
    '''Evaluates a polynomial in GF(2^p) given the value for x. This is based on Horner's scheme for maximum efficiency.'''
    y = poly[0]
    for i in xrange(1, len(poly)):
        y = gf_mul(y, x) ^ poly[i]
    return y


################### REED-SOLOMON ENCODING ###################

def rs_generator_poly(nsym, fcr=0, generator=2):
    '''Generate an irreducible generator polynomial (necessary to encode a message into Reed-Solomon)'''
    g = _bytearray([1])
    for i in xrange(nsym):
        g = gf_poly_mul(g, [1, gf_pow(generator, i+fcr)])
    return g

def rs_generator_poly_all(max_nsym, fcr=0, generator=2):
    '''Generate all irreducible generator polynomials up to max_nsym (usually you can use n, the length of the message+ecc). Very useful to reduce processing time if you want to encode using variable schemes and nsym rates.'''
    #g_all = {}  # old approach using a dict
    #g_all[0] = g_all[1] = _bytearray([1])
    g_all = [[1]] * max_nsym  # a list of list is potentially faster than using a dict, since it is pre-allocated and a list has less overhead than a dict
    for nsym in xrange(max_nsym):
        g_all[nsym] = rs_generator_poly(nsym, fcr, generator)
    return g_all

def rs_simple_encode_msg(msg_in, nsym, fcr=0, generator=2, gen=None):
    '''Simple Reed-Solomon encoding (mainly an example for you to understand how it works, because it's slower than the inlined function below)'''
    global field_charac
    if (len(msg_in) + nsym) > field_charac: raise ValueError("Message is too long (%i when max is %i)" % (len(msg_in)+nsym, field_charac))
    if gen is None: gen = rs_generator_poly(nsym, fcr, generator)

    # Pad the message, then divide it by the irreducible generator polynomial
    _, remainder = gf_poly_div(msg_in + _bytearray(len(gen)-1), gen)
    # The remainder is our RS code! Just append it to our original message to get our full codeword (this represents a polynomial of max 256 terms)
    msg_out = msg_in + remainder
    # Return the codeword
    return msg_out

def rs_encode_msg(msg_in, nsym, fcr=0, generator=2, gen=None):
    '''Reed-Solomon main encoding function, using polynomial division (Extended Synthetic Division, the fastest algorithm available to my knowledge), better explained at http://research.swtch.com/field'''
    global field_charac
    if (len(msg_in) + nsym) > field_charac: raise ValueError("Message is too long (%i when max is %i)" % (len(msg_in)+nsym, field_charac))
    if gen is None: gen = rs_generator_poly(nsym, fcr, generator)

    msg_in = _bytearray(msg_in)
    msg_out = _bytearray(msg_in) + _bytearray(len(gen)-1) # init msg_out with the values inside msg_in and pad with len(gen)-1 bytes (which is the number of ecc symbols).

    # Precompute the logarithm of every items in the generator
    lgen = _bytearray([gf_log[gen[j]] for j in xrange(len(gen))])

    # Cache lengths for faster access inside loops
    msg_in_len = len(msg_in)
    gen_len = len(gen)

    # Extended synthetic division main loop
    # Fastest implementation with PyPy (but the Cython version in creedsolo.pyx is about 2x faster)
    for i in xrange(msg_in_len):
        coef = msg_out[i] # Note that it's msg_out here, not msg_in. Thus, we reuse the updated value at each iteration (this is how Synthetic Division works: instead of storing in a temporary register the intermediate values, we directly commit them to the output).
        # coef = gf_mul(msg_out[i], gf_inverse(gen[0]))  # for general polynomial division (when polynomials are non-monic), the usual way of using synthetic division is to divide the divisor g(x) with its leading coefficient (call it a). In this implementation, this means:we need to compute: coef = msg_out[i] / gen[0]
        if coef != 0: # log(0) is undefined, so we need to manually check for this case. There's no need to check the divisor here because we know it can't be 0 since we generated it.
            lcoef = gf_log[coef] # precaching

            for j in xrange(1, gen_len): # in synthetic division, we always skip the first coefficient of the divisior, because it's only used to normalize the dividend coefficient (which is here useless since the divisor, the generator polynomial, is always monic)
                #if gen[j] != 0: # log(0) is undefined so we need to check that, but it slow things down in fact and it's useless in our case (reed-solomon encoding) since we know that all coefficients in the generator are not 0
                msg_out[i + j] ^= gf_exp[lcoef + lgen[j]] # optimization, equivalent to gf_mul(gen[j], msg_out[i]) and we just substract it to msg_out[i+j] (but since we are in GF256, it's equivalent to an addition and to an XOR). In other words, this is simply a "multiply-accumulate operation"

    # Recopy the original message bytes (overwrites the part where the quotient was computed)
    msg_out[:msg_in_len] = msg_in # equivalent to c = mprime - b, where mprime is msg_in padded with [0]*nsym
    return msg_out


################### REED-SOLOMON DECODING ###################

def rs_calc_syndromes(msg, nsym, fcr=0, generator=2):
    '''Given the received codeword msg and the number of error correcting symbols (nsym), computes the syndromes polynomial.
    Mathematically, it's essentially equivalent to a Fourrier Transform (Chien search being the inverse).
    '''
    # Note the "[0] +" : we add a 0 coefficient for the lowest degree (the constant). This effectively shifts the syndrome, and will shift every computations depending on the syndromes (such as the errors locator polynomial, errors evaluator polynomial, etc. but not the errors positions).
    # This is not necessary as anyway syndromes are defined such as there are only non-zero coefficients (the only 0 is the shift of the constant here) and subsequent computations will/must account for the shift by skipping the first iteration (eg, the often seen range(1, n-k+1)), but you can also avoid prepending the 0 coeff and adapt every subsequent computations to start from 0 instead of 1.
    return [0] + [gf_poly_eval(msg, gf_pow(generator, i+fcr)) for i in xrange(nsym)]

def rs_correct_errata(msg_in, synd, err_pos, fcr=0, generator=2): # err_pos is a list of the positions of the errors/erasures/errata
    '''Forney algorithm, computes the values (error magnitude) to correct the input message.'''
    global field_charac
    msg = _bytearray(msg_in)
    # calculate errata locator polynomial to correct both errors and erasures (by combining the errors positions given by the error locator polynomial found by BM with the erasures positions given by caller)
    coef_pos = [len(msg) - 1 - p for p in err_pos] # need to convert the positions to coefficients degrees for the errata locator algo to work (eg: instead of [0, 1, 2] it will become [len(msg)-1, len(msg)-2, len(msg) -3])
    err_loc = rs_find_errata_locator(coef_pos, generator)
    # calculate errata evaluator polynomial (often called Omega or Gamma in academic papers)
    err_eval = rs_find_error_evaluator(synd[::-1], err_loc, len(err_loc)-1)[::-1]

    # Second part of Chien search to get the error location polynomial X from the error positions in err_pos (the roots of the error locator polynomial, ie, where it evaluates to 0)
    X = _bytearray(len(coef_pos)) # will store the position of the errors
    for i in xrange(len(coef_pos)):
        l = field_charac - coef_pos[i]
        X[i] = gf_pow(generator, -l)

    # Forney algorithm: compute the magnitudes
    E = _bytearray(len(msg)) # will store the values that need to be corrected (substracted) to the message containing errors. This is sometimes called the error magnitude polynomial.
    X_len = len(X)
    for i, Xi in enumerate(X):

        Xi_inv = gf_inverse(Xi)

        # Compute the formal derivative of the error locator polynomial (see Blahut, Algebraic codes for data transmission, pp 196-197).
        # the formal derivative of the errata locator is used as the denominator of the Forney Algorithm, which simply says that the ith error value is given by error_evaluator(gf_inverse(Xi)) / error_locator_derivative(gf_inverse(Xi)). See Blahut, Algebraic codes for data transmission, pp 196-197.
        #err_loc_prime_tmp = _bytearray()
        #for j in xrange(X_len):
        #    if j != i:
        #        err_loc_prime_tmp.append( gf_sub(1, gf_mul(Xi_inv, X[j])) )
        # compute the product, which is the denominator of the Forney algorithm (errata locator derivative)
        #err_loc_prime = 1
        #for coef in err_loc_prime_tmp:
        #    err_loc_prime = gf_mul(err_loc_prime, coef)
        # equivalent to: err_loc_prime = functools.reduce(gf_mul, err_loc_prime_tmp, 1)

        # Alternative but faster way to compute the formal derivative of the error locator polynomial
        err_loc_prime = 1
        for j in xrange(X_len):
            if j != i:
                err_loc_prime = gf_mul(err_loc_prime, gf_sub(1, gf_mul(Xi_inv, X[j])))

        # Test if we could find the errata locator, else we raise an Exception (because else since we divide y by err_loc_prime to compute the magnitude, we will get a ZeroDivisionError exception otherwise)
        if err_loc_prime == 0:
            raise ReedSolomonError("Decoding failed: Forney algorithm could not properly detect where the errors are located (errata locator prime is 0).")

        # Compute y (evaluation of the errata evaluator polynomial)
        # This is a more faithful translation of the theoretical equation contrary to the old forney method. Here it is exactly copy/pasted from the included presentation decoding_rs.pdf: Yl = omega(Xl.inverse()) / prod(1 - Xj*Xl.inverse()) for j in len(X) (in the paper it's for j in s, but it's useless when len(X) < s because we compute neutral terms 1 for nothing, and wrong when correcting more than s erasures or erasures+errors since it prevents computing all required terms).
        # Thus here this method works with erasures too because firstly we fixed the equation to be like the theoretical one (don't know why it was modified in _old_forney(), if it's an optimization, it doesn't enhance anything), and secondly because we removed the product bound on s, which prevented computing errors and erasures above the s=(n-k)//2 bound.
        y = gf_poly_eval(err_eval[::-1], Xi_inv) # numerator of the Forney algorithm (errata evaluator evaluated)
        y = gf_mul(gf_pow(Xi, 1-fcr), y) # adjust to fcr parameter
        
        # Compute the magnitude
        magnitude = gf_div(y, err_loc_prime) # magnitude value of the error, calculated by the Forney algorithm (an equation in fact): dividing the errata evaluator with the errata locator derivative gives us the errata magnitude (ie, value to repair) the ith symbol
        E[err_pos[i]] = magnitude # store the magnitude for this error into the magnitude polynomial

    # Apply the correction of values to get our message corrected! (note that the ecc bytes also gets corrected!)
    # (this isn't the Forney algorithm, we just apply the result of decoding here)
    msg = gf_poly_add(msg, E) # equivalent to Ci = Ri - Ei where Ci is the correct message, Ri the received (senseword) message, and Ei the errata magnitudes (minus is replaced by XOR since it's equivalent in GF(2^p)). So in fact here we substract from the received message the errors magnitude, which logically corrects the value to what it should be.
    return msg

def rs_find_error_locator(synd, nsym, erase_loc=None, erase_count=0):
    '''Find error/errata locator and evaluator polynomials with Berlekamp-Massey algorithm'''
    # The idea is that BM will iteratively estimate the error locator polynomial.
    # To do this, it will compute a Discrepancy term called Delta, which will tell us if the error locator polynomial needs an update or not
    # (hence why it's called discrepancy: it tells us when we are getting off board from the correct value).

    # Init the polynomials
    if erase_loc: # if the erasure locator polynomial is supplied, we init with its value, so that we include erasures in the final locator polynomial
        err_loc = _bytearray(erase_loc)
        old_loc = _bytearray(erase_loc)
    else:
        err_loc = _bytearray([1]) # This is the main variable we want to fill, also called Sigma in other notations or more formally the errors/errata locator polynomial.
        old_loc = _bytearray([1]) # BM is an iterative algorithm, and we need the errata locator polynomial of the previous iteration in order to update other necessary variables.
    #L = 0 # update flag variable, not needed here because we use an alternative equivalent way of checking if update is needed (but using the flag could potentially be faster depending on if using length(list) is taking linear time in your language, here in Python it's constant so it's as fast.

    # Fix the syndrome shifting: when computing the syndrome, some implementations may prepend a 0 coefficient for the lowest degree term (the constant). This is a case of syndrome shifting, thus the syndrome will be bigger than the number of ecc symbols (I don't know what purpose serves this shifting). If that's the case, then we need to account for the syndrome shifting when we use the syndrome such as inside BM, by skipping those prepended coefficients.
    # Another way to detect the shifting is to detect the 0 coefficients: by definition, a syndrome does not contain any 0 coefficient (except if there are no errors/erasures, in this case they are all 0). This however doesn't work with the modified Forney syndrome, which set to 0 the coefficients corresponding to erasures, leaving only the coefficients corresponding to errors.
    synd_shift = 0
    if len(synd) > nsym: synd_shift = len(synd) - nsym

    for i in xrange(nsym-erase_count): # generally: nsym-erase_count == len(synd), except when you input a partial erase_loc and using the full syndrome instead of the Forney syndrome, in which case nsym-erase_count is more correct (len(synd) will fail badly with IndexError).
        if erase_loc: # if an erasures locator polynomial was provided to init the errors locator polynomial, then we must skip the FIRST erase_count iterations (not the last iterations, this is very important!)
            K = erase_count+i+synd_shift
        else: # if erasures locator is not provided, then either there's no erasures to account or we use the Forney syndromes, so we don't need to use erase_count nor erase_loc (the erasures have been trimmed out of the Forney syndromes).
            K = i+synd_shift

        # Compute the discrepancy Delta
        # Here is the close-to-the-books operation to compute the discrepancy Delta: it's a simple polynomial multiplication of error locator with the syndromes, and then we get the Kth element.
        #delta = gf_poly_mul(err_loc[::-1], synd)[K] # theoretically it should be gf_poly_add(synd[::-1], [1])[::-1] instead of just synd, but it seems it's not absolutely necessary to correctly decode.
        # But this can be optimized: since we only need the Kth element, we don't need to compute the polynomial multiplication for any other element but the Kth. Thus to optimize, we compute the polymul only at the item we need, skipping the rest (avoiding a nested loop, thus we are linear time instead of quadratic).
        # This optimization is actually described in several figures of the book "Algebraic codes for data transmission", Blahut, Richard E., 2003, Cambridge university press.
        delta = synd[K]
        for j in xrange(1, len(err_loc)):
            delta ^= gf_mul(err_loc[-(j+1)], synd[K - j]) # delta is also called discrepancy. Here we do a partial polynomial multiplication (ie, we compute the polynomial multiplication only for the term of degree K). Should be equivalent to brownanrs.polynomial.mul_at().
        #print "delta", K, delta, list(gf_poly_mul(err_loc[::-1], synd)) # debugline

        # Shift polynomials to compute the next degree
        old_loc = old_loc + _bytearray([0])

        # Iteratively estimate the errata locator and evaluator polynomials
        if delta != 0: # Update only if there's a discrepancy
            if len(old_loc) > len(err_loc): # Rule B (rule A is implicitly defined because rule A just says that we skip any modification for this iteration)
            #if 2*L <= K+erase_count: # equivalent to len(old_loc) > len(err_loc), as long as L is correctly computed
                # Computing errata locator polynomial Sigma
                new_loc = gf_poly_scale(old_loc, delta)
                old_loc = gf_poly_scale(err_loc, gf_inverse(delta)) # effectively we are doing err_loc * 1/delta = err_loc // delta
                err_loc = new_loc
                # Update the update flag
                #L = K - L # the update flag L is tricky: in Blahut's schema, it's mandatory to use `L = K - L - erase_count` (and indeed in a previous draft of this function, if you forgot to do `- erase_count` it would lead to correcting only 2*(errors+erasures) <= (n-k) instead of 2*errors+erasures <= (n-k)), but in this latest draft, this will lead to a wrong decoding in some cases where it should correctly decode! Thus you should try with and without `- erase_count` to update L on your own implementation and see which one works OK without producing wrong decoding failures.

            # Update with the discrepancy
            err_loc = gf_poly_add(err_loc, gf_poly_scale(old_loc, delta))

    # Check if the result is correct, that there's not too many errors to correct
    for i, x in enumerate(err_loc):  #drop leading 0s, else errs will not be of the correct size. This does not use functional closures (ie, lambdas), which Cython and JIT compilers cannot optimize, such as `err_loc = list(itertools.dropwhile(lambda x: x == 0, err_loc))`
        if x != 0:
            err_loc = err_loc[i:]
            break
    errs = len(err_loc) - 1
    if (errs-erase_count) * 2 + erase_count > nsym:
        raise ReedSolomonError("Too many errors to correct")

    # Return result
    return err_loc

def rs_find_errata_locator(e_pos, generator=2):
    '''Compute the erasures/errors/errata locator polynomial from the erasures/errors/errata positions (the positions must be relative to the x coefficient, eg: "hello worldxxxxxxxxx" is tampered to "h_ll_ worldxxxxxxxxx" with xxxxxxxxx being the ecc of length n-k=9, here the string positions are [1, 4], but the coefficients are reversed since the ecc characters are placed as the first coefficients of the polynomial, thus the coefficients of the erased characters are n-1 - [1, 4] = [18, 15] = erasures_loc to be specified as an argument.'''
    # See: http://ocw.usu.edu/Electrical_and_Computer_Engineering/Error_Control_Coding/lecture7.pdf and Blahut, Richard E. "Transform techniques for error control codes." IBM Journal of Research and development 23.3 (1979): 299-315. http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.92.600&rep=rep1&type=pdf and also a MatLab implementation here: http://www.mathworks.com/matlabcentral/fileexchange/23567-reed-solomon-errors-and-erasures-decoder/content//RS_E_E_DEC.m
    e_loc = [1] # just to init because we will multiply, so it must be 1 so that the multiplication starts correctly without nulling any term
    # erasures_loc is very simple to compute: erasures_loc = prod(1 - x*alpha**i) for i in erasures_pos and where alpha is the alpha chosen to evaluate polynomials (here in this library it's gf(3)). To generate c*x where c is a constant, we simply generate a Polynomial([c, 0]) where 0 is the constant and c is positionned to be the coefficient for x^1.
    for i in e_pos:
        e_loc = gf_poly_mul( e_loc, gf_poly_add(_bytearray([1]), [gf_pow(generator, i), 0]) )
    return e_loc

def rs_find_error_evaluator(synd, err_loc, nsym):
    '''Compute the error (or erasures if you supply sigma=erasures locator polynomial, or errata) evaluator polynomial Omega from the syndrome and the error/erasures/errata locator Sigma. Omega is already computed at the same time as Sigma inside the Berlekamp-Massey implemented above, but in case you modify Sigma, you can recompute Omega afterwards using this method, or just ensure that Omega computed by BM is correct given Sigma.'''
    # Omega(x) = [ Synd(x) * Error_loc(x) ] mod x^(n-k+1)
    #_, remainder = gf_poly_div( gf_poly_mul(synd, err_loc), ([1] + [0]*(nsym+1)) ) # first multiply syndromes * errata_locator, then do a polynomial division to truncate the polynomial to the required length

    # Faster way that is equivalent
    remainder = gf_poly_mul(synd, err_loc) # first multiply the syndromes with the errata locator polynomial
    remainder = remainder[len(remainder)-(nsym+1):] # then divide by a polynomial of the length we want, which is equivalent to slicing the list (which represents the polynomial)

    return remainder

def rs_find_errors(err_loc, nmess, generator=2):
    '''Find the roots (ie, where evaluation = zero) of error polynomial by bruteforce trial, this is a sort of Chien's search (but less efficient, Chien's search is a way to evaluate the polynomial such that each evaluation only takes constant time).'''
    # nmess = length of whole codeword (message + ecc symbols)
    errs = len(err_loc) - 1
    err_pos = []
    for i in xrange(nmess): # normally we should try all 2^8 possible values, but here we optimize to just check the interesting symbols
        if gf_poly_eval(err_loc, gf_pow(generator, i)) == 0: # It's a 0? Bingo, it's a root of the error locator polynomial, in other terms this is the location of an error
            err_pos.append(nmess - 1 - i)
    # Sanity check: the number of errors/errata positions found should be exactly the same as the length of the errata locator polynomial
    if len(err_pos) != errs:
        # TODO: to decode messages+ecc with length n > 255, we may try to use a bruteforce approach: the correct positions ARE in the final array j, but the problem is because we are above the Galois Field's range, there is a wraparound so that for example if j should be [0, 1, 2, 3], we will also get [255, 256, 257, 258] (because 258 % 255 == 3, same for the other values), so we can't discriminate. The issue is that fixing any errs_nb errors among those will always give a correct output message (in the sense that the syndrome will be all 0), so we may not even be able to check if that's correct or not, so I'm not sure the bruteforce approach may even be possible.
        raise ReedSolomonError("Too many (or few) errors found by Chien Search for the errata locator polynomial!")
    return _bytearray(err_pos)

def rs_forney_syndromes(synd, pos, nmess, generator=2):
    # Compute Forney syndromes, which computes a modified syndromes to compute only errors (erasures are trimmed out). Do not confuse this with Forney algorithm, which allows to correct the message based on the location of errors.
    erase_pos_reversed = [nmess-1-p for p in pos] # prepare the coefficient degree positions (instead of the erasures positions)

    # Optimized method, all operations are inlined
    fsynd = list(synd[1:])      # make a copy and trim the first coefficient which is always 0 by definition
    for i in xrange(len(pos)):
        x = gf_pow(generator, erase_pos_reversed[i])
        for j in xrange(len(fsynd) - 1):
            fsynd[j] = gf_mul(fsynd[j], x) ^ fsynd[j + 1]
        #fsynd.pop() # useless? it doesn't change the results of computations to leave it there

    # Theoretical way of computing the modified Forney syndromes: fsynd = (erase_loc * synd) % x^(n-k) -- although the trimming by using x^(n-k) is maybe not necessary as many books do not even mention it (and it works without trimming)
    # See Shao, H. M., Truong, T. K., Deutsch, L. J., & Reed, I. S. (1986, April). A single chip VLSI Reed-Solomon decoder. In Acoustics, Speech, and Signal Processing, IEEE International Conference on ICASSP'86. (Vol. 11, pp. 2151-2154). IEEE.ISO 690
    #erase_loc = rs_find_errata_locator(erase_pos_reversed, generator=generator) # computing the erasures locator polynomial
    #fsynd = gf_poly_mul(erase_loc[::-1], synd[1:]) # then multiply with the syndrome to get the untrimmed forney syndrome
    #fsynd = fsynd[len(pos):] # then trim the first erase_pos coefficients which are useless. Seems to be not necessary, but this reduces the computation time later in BM (thus it's an optimization).

    return fsynd

def rs_correct_msg(msg_in, nsym, fcr=0, generator=2, erase_pos=None, only_erasures=False):
    '''Reed-Solomon main decoding function'''
    global field_charac
    if len(msg_in) > field_charac:
        # Note that it is in fact possible to encode/decode messages that are longer than field_charac, but because this will be above the field, this will generate more error positions during Chien Search than it should, because this will generate duplicate values, which should normally be prevented thank's to the prime polynomial reduction (eg, because it can't discriminate between error at position 1 or 256, both being exactly equal under galois field 2^8). So it's really not advised to do it, but it's possible (but then you're not guaranted to be able to correct any error/erasure on symbols with a position above the length of field_charac -- if you really need a bigger message without chunking, then you should better enlarge c_exp so that you get a bigger field).
        raise ValueError("Message is too long (%i when max is %i)" % (len(msg_in), field_charac))

    msg_out = _bytearray(msg_in)     # copy of message
    # erasures: set them to null bytes for easier decoding (but this is not necessary, they will be corrected anyway, but debugging will be easier with null bytes because the error locator polynomial values will only depend on the errors locations, not their values)
    if erase_pos is None:
        erase_pos = _bytearray()
    else:
        if isinstance(erase_pos, list):  # ensure we use a bytearray, not a list
            erase_pos = _bytearray(erase_pos)
        for e_pos in erase_pos:
            msg_out[e_pos] = 0
    # check if there are too many erasures to correct (beyond the Singleton bound)
    if len(erase_pos) > nsym: raise ReedSolomonError("Too many erasures to correct")
    # prepare the syndrome polynomial using only errors (ie: errors = characters that were either replaced by null byte or changed to another character, but we don't know their positions)
    synd = rs_calc_syndromes(msg_out, nsym, fcr, generator)
    # check if there's any error/erasure in the input codeword. If not (all syndromes coefficients are 0), then just return the codeword as-is.
    if max(synd) == 0:
        return msg_out[:-nsym], msg_out[-nsym:], erase_pos  # no errors

    # Find errors locations
    if only_erasures:
        err_pos = _bytearray()
    else:
        # compute the Forney syndromes, which hide the erasures from the original syndrome (so that BM will just have to deal with errors, not erasures)
        fsynd = rs_forney_syndromes(synd, erase_pos, len(msg_out), generator)
        # compute the error locator polynomial using Berlekamp-Massey
        err_loc = rs_find_error_locator(fsynd, nsym, erase_count=len(erase_pos))
        # locate the message errors using Chien search (or bruteforce search)
        err_pos = rs_find_errors(err_loc[::-1], len(msg_out), generator)
        if err_pos is None:
            raise ReedSolomonError("Could not locate error")

    # Find errors values and apply them to correct the message
    # compute errata evaluator and errata magnitude polynomials, then correct errors and erasures
    msg_out = rs_correct_errata(msg_out, synd, erase_pos + err_pos, fcr, generator) # note that we here use the original syndrome, not the forney syndrome (because we will correct both errors and erasures, so we need the full syndrome)
    # check if the final message is fully repaired
    synd = rs_calc_syndromes(msg_out, nsym, fcr, generator)
    if max(synd) > 0:
        raise ReedSolomonError("Could not correct message")
    # return the successfully decoded message
    return msg_out[:-nsym], msg_out[-nsym:], erase_pos + err_pos # also return the corrected ecc block so that the user can check(), and the position of errors to allow for adaptive bitrate algorithm to check how the number of errors vary

def rs_correct_msg_nofsynd(msg_in, nsym, fcr=0, generator=2, erase_pos=None, only_erasures=False):
    '''Reed-Solomon main decoding function, without using the modified Forney syndromes'''
    global field_charac
    if len(msg_in) > field_charac:
        raise ValueError("Message is too long (%i when max is %i)" % (len(msg_in), field_charac))

    msg_out = _bytearray(msg_in)     # copy of message
    # erasures: set them to null bytes for easier decoding (but this is not necessary, they will be corrected anyway, but debugging will be easier with null bytes because the error locator polynomial values will only depend on the errors locations, not their values)
    if erase_pos is None:
        erase_pos = _bytearray()
    else:
        if isinstance(erase_pos, list):  # ensure we use a bytearray, not a list
            erase_pos = _bytearray(erase_pos)
        for e_pos in erase_pos:
            msg_out[e_pos] = 0
    # check if there are too many erasures
    if len(erase_pos) > nsym: raise ReedSolomonError("Too many erasures to correct")
    # prepare the syndrome polynomial using only errors (ie: errors = characters that were either replaced by null byte or changed to another character, but we don't know their positions)
    synd = rs_calc_syndromes(msg_out, nsym, fcr, generator)
    # check if there's any error/erasure in the input codeword. If not (all syndromes coefficients are 0), then just return the codeword as-is.
    if max(synd) == 0:
        return msg_out[:-nsym], msg_out[-nsym:], []  # no errors

    # prepare erasures locator and evaluator polynomials
    erase_loc = None
    #erase_eval = None
    erase_count = 0
    if erase_pos:
        erase_count = len(erase_pos)
        msg_out_len = len(msg_out)  # cache to avoid recalculations inside loop
        erase_pos_reversed = [msg_out_len-1-eras for eras in erase_pos]
        erase_loc = rs_find_errata_locator(erase_pos_reversed, generator=generator)
        #erase_eval = rs_find_error_evaluator(synd[::-1], erase_loc, len(erase_loc)-1)

    # prepare errors/errata locator polynomial
    if only_erasures:
        err_loc = erase_loc[::-1]
        #err_eval = erase_eval[::-1]
    else:
        err_loc = rs_find_error_locator(synd, nsym, erase_loc=erase_loc, erase_count=erase_count)
        err_loc = err_loc[::-1]
        #err_eval = rs_find_error_evaluator(synd[::-1], err_loc[::-1], len(err_loc)-1)[::-1] # find error/errata evaluator polynomial (not really necessary since we already compute it at the same time as the error locator poly in BM)

    # locate the message errors
    err_pos = rs_find_errors(err_loc, len(msg_out), generator) # find the roots of the errata locator polynomial (ie: the positions of the errors/errata)
    if err_pos is None:
        raise ReedSolomonError("Could not locate error")

    # compute errata evaluator and errata magnitude polynomials, then correct errors and erasures
    msg_out = rs_correct_errata(msg_out, synd, err_pos, fcr=fcr, generator=generator)
    # check if the final message is fully repaired
    synd = rs_calc_syndromes(msg_out, nsym, fcr, generator)
    if max(synd) > 0:
        raise ReedSolomonError("Could not correct message")
    # return the successfully decoded message
    return msg_out[:-nsym], msg_out[-nsym:], erase_pos + err_pos # also return the corrected ecc block so that the user can check(), and the position of errors to allow for adaptive bitrate algorithm to check how the number of errors vary

def rs_check(msg, nsym, fcr=0, generator=2):
    '''Returns true if the message + ecc has no error of false otherwise (may not always catch a wrong decoding or a wrong message, particularly if there are too many errors -- above the Singleton bound --, but it usually does)'''
    return ( max(rs_calc_syndromes(msg, nsym, fcr, generator)) == 0 )

######################## end of REED SOLOMON DECODING ###############

#===================================================================================================
# API
#===================================================================================================
class RSCodec(object):
    '''
    A Reed Solomon encoder/decoder. After initializing the object, use ``encode`` to encode a
    (byte)string to include the RS correction code, and pass such an encoded (byte)string to
    ``decode`` to extract the original message (if the number of errors allows for correct decoding).
    The ``nsym`` argument is the length of the correction code, and it determines the number of
    error bytes (if I understand this correctly, half of ``nsym`` is correctable)
    '''
    '''
    Modifications by rotorgit 2/3/2015:
    Added support for US FAA ADSB UAT RS FEC, by allowing user to specify
    different primitive polynomial and non-zero first consecutive root (fcr).
    For UAT/ADSB use, set fcr=120 and prim=0x187 when instantiating
    the class; leaving them out will default for previous values (0 and
    0x11d)
    '''

    def __init__(self, nsym=10, nsize=255, fcr=0, prim=0x11d, generator=2, c_exp=8, single_gen=True):
        '''Initialize the Reed-Solomon codec. Note that different parameters change the internal values (the ecc symbols, look-up table values, etc) but not the output result (whether your message can be repaired or not, there is no influence of the parameters).
        nsym : number of ecc symbols (you can repair nsym/2 errors and nsym erasures.
        nsize : maximum length of each chunk. If higher than 255, will use a higher Galois Field, but the algorithm's complexity and computational cost will raise quadratically...
        single_gen : if you want to use the same RSCodec for different nsym parameters (but nsize the same), then set single_gen=False. This is only required for encoding with various number of ecc symbols, as for decoding this is always possible even if single_gen=True.
        '''

        # Auto-setup if galois field or message length is different than default (exponent 8)
        if nsize > 255 and c_exp <= 8:  # nsize (chunksize) is larger than the galois field, we resize the galois field
            # Get the next closest power of two
            c_exp = int(math.log(2 ** (math.floor(math.log(nsize) / math.log(2)) + 1), 2))
        if c_exp != 8 and prim == 0x11d:  # prim was not correctly defined, find one
            prim = find_prime_polys(generator=generator, c_exp=c_exp, fast_primes=True, single=True)[0]
            if nsize == 255:  # resize chunk size if not set
                nsize = int(2**c_exp - 1)
        if nsym >= nsize:
            raise ValueError('ECC symbols must be strictly less than the total message length (nsym < nsize).')

        # Memorize variables
        self.nsym = nsym # number of ecc symbols (ie, the repairing rate will be r=(nsym/2)/nsize, so for example if you have nsym=5 and nsize=10, you have a rate r=0.25, so you can correct up to 0.25% errors (or exactly 2 symbols out of 10), and 0.5% erasures (5 symbols out of 10).
        self.nsize = nsize # maximum length of one chunk (ie, message + ecc symbols after encoding, for the message alone it's nsize-nsym)
        self.fcr = fcr # first consecutive root, can be any value between 0 and (2**c_exp)-1
        self.prim = prim # prime irreducible polynomial, use find_prime_polys() to find a prime poly
        self.generator = generator # generator integer, must be prime
        self.c_exp = c_exp # exponent of the field's characteristic. This both defines the maximum value per symbol and the maximum length of one chunk. By default it's GF(2^8), do not change if you're not sure what it means.

        # Initialize the look-up tables for easy and quick multiplication/division
        self.gf_log, self.gf_exp, self.field_charac = init_tables(prim, generator, c_exp)
        # Precompute the generator polynomials
        if single_gen:
            self.gen = {}
            self.gen[nsym] = rs_generator_poly(nsym, fcr=fcr, generator=generator)
        else:
            self.gen = rs_generator_poly_all(nsize, fcr=fcr, generator=generator)

    def chunk(self, data, chunk_size):
        '''Split a long message into chunks
        DEPRECATED: inlined alternate form so that we can preallocate arrays and hence get faster results with JIT compilers such as PyPy.'''
        for i in xrange(0, len(data), chunk_size):
            # alternative chunking form: j = i*chunk_size; chunk = data[j:j+chunk_size]
            # Split the long message in a chunk
            chunk = data[i:i+chunk_size]
            yield chunk

    def encode(self, data, nsym=None):
        '''Encode a message (ie, add the ecc symbols) using Reed-Solomon, whatever the length of the message because we use chunking
        Optionally, can set nsym to encode with a different number of error correction symbols, but RSCodec must be initialized with single_gen=False first.
        slice_assign=True allows to speed up the loop quite significantly in JIT compilers such as PyPy by preallocating the output bytearray and slice assigning into it, instead of constantly extending an empty bytearray, but this only works in Python 3, not Python 2, hence is disabled by default for retrocompatibility.
        '''
        # Restore precomputed tables (allow to use multiple RSCodec in one script)
        global gf_log, gf_exp, field_charac
        gf_log, gf_exp, field_charac = self.gf_log, self.gf_exp, self.field_charac
        nsize, fcr, generator = self.nsize, self.fcr, self.generator

        if not nsym:
            nsym = self.nsym
        gen = self.gen[nsym]

        if isinstance(data, str):
            data = _bytearray(data)

        # Calculate chunk size and total number of chunks for looping
        chunk_size = int(nsize - nsym)
        total_chunks = int(math.ceil(float(len(data)) / float(chunk_size)))  # need to convert to floats first to get an accurate floating division, or else we assume implicit conversion and it will cause an error on Python 2

        # Preallocate output array
        enc = _bytearray(total_chunks * nsize)  # pre-allocate array and we will overwrite data in it, much faster than extending
        # Chunking loop
        for i in xrange(0, total_chunks):
            # Encode this chunk and update a slice of the output bytearray, much more efficient than extending an array constantly
            enc[i*nsize:(i+1)*nsize] = rs_encode_msg(data[i*chunk_size:(i+1)*chunk_size], nsym, fcr=fcr, generator=generator, gen=gen)
        return enc

    def decode(self, data, nsym=None, erase_pos=None, only_erasures=False):
        '''Repair a message, whatever its size is, by using chunking. May return a wrong result if number of errors > nsym because then too many errors to be corrected.
        Note that it returns a couple of vars: the repaired messages, and the repaired messages+ecc (useful for checking).
        Usage: rmes, rmesecc = RSCodec.decode(data).
        Optionally: can specify nsym to decode messages of different parameters, erase_pos with a list of erasures positions to double the number of erasures that can be corrected compared to unlocalized errors, only_erasures boolean to specify if we should only look for erasures, which speeds up and doubles the total correction power.
        '''
        # erase_pos is a list of positions where you know (or greatly suspect at least) there is an erasure (ie, wrong character but you know it's at this position). Just input the list of all positions you know there are errors, and this method will automatically split the erasures positions to attach to the corresponding data chunk.

        # Restore precomputed tables (allow to use multiple RSCodec in one script)
        global gf_log, gf_exp, field_charac
        gf_log, gf_exp, field_charac = self.gf_log, self.gf_exp, self.field_charac

        if isinstance(data, str):
            data = _bytearray(data)
        if isinstance(erase_pos, list):
            erase_pos = _bytearray(erase_pos)

        # Precache class attributes into local variables
        if not nsym:
            nsym = self.nsym
        nsize = self.nsize
        fcr = self.fcr
        generator = self.generator

        # Calculate chunk size and total number of chunks for looping
        chunk_size = nsize
        total_chunks = int(math.ceil(float(len(data)) / float(chunk_size)))  # need to convert to floats first to get an accurate floating division, or else we assume implicit conversion and it will cause an error on Python 2
        nmes = int(nsize-nsym)

        # Initialize output array
        dec = _bytearray(total_chunks * nmes)  # pre-allocate array and we will overwrite data in it, much faster than extending
        dec_full = _bytearray(total_chunks * nsize)
        errata_pos_all = _bytearray()
        # Chunking loop
        for i in xrange(0, total_chunks):  # Split the long message in a chunk
            # Extract the erasures for this chunk
            if erase_pos is not None:
                # First extract the erasures for this chunk (all erasures below the maximum chunk length)
                e_pos = [x for x in erase_pos if x < nsize]
                # Then remove the extract erasures from the big list and also decrement all subsequent positions values by nsize (the current chunk's size) so as to prepare the correct alignment for the next iteration
                erase_pos = [x - nsize for x in erase_pos if x >= nsize]
            else:
                e_pos = _bytearray()
            # Decode/repair this chunk!
            rmes, recc, errata_pos = rs_correct_msg(data[i*chunk_size:(i+1)*chunk_size], nsym, fcr=fcr, generator=generator, erase_pos=e_pos, only_erasures=only_erasures)
            dec[i*nmes:(i+1)*nmes] = rmes
            dec_full[i*nsize:(i+1)*nsize] = rmes
            dec_full[i*nsize + nmes:(i+1)*nsize + nmes] = recc  # append corrected ecc just after corrected message. The two lines are equivalent to rmes + recc but here we don't need to concatenate both arrays first (and create a third one for nothing) before storing in the output array
            errata_pos_all.extend(errata_pos)
        return dec, dec_full, errata_pos_all

    def check(self, data, nsym=None):
        '''Check if a message+ecc stream is not corrupted (or fully repaired). Note: may return a wrong result if number of errors > nsym.'''
        if not nsym:
            nsym = self.nsym
        if isinstance(data, str):
            data = _bytearray(data)

        # Precache class attributes into local variables
        nsize = self.nsize
        fcr = self.fcr
        generator = self.generator

        # Calculate chunksize
        chunk_size = nsize
        total_chunks = int(math.ceil(float(len(data)) / float(chunk_size)))  # need to convert to floats first to get an accurate floating division, or else we assume implicit conversion and it will cause an error on Python 2

        # Pre-allocate output array
        check = [False] * total_chunks  # TODO: could use a bitarray but this creates an external dependency and it's not compatible with cython https://pypi.org/project/bitarray/ and https://www.noveltech.dev/booleans-python-numpy/
        # Chunking loop
        for i in xrange(0, total_chunks):  # Split the long message in a chunk
            check[i] = rs_check(data[i*chunk_size:(i+1)*chunk_size], nsym, fcr=fcr, generator=generator)
        return check

    def maxerrata(self, nsym=None, errors=None, erasures=None, verbose=False):
        '''Return the Singleton Bound for the current codec, which is the max number of errata (errors and erasures) that the codec can decode/correct.
        Beyond the Singleton Bound (too many errors/erasures), the algorithm will try to raise an exception, but it may also not detect any problem with the message and return 0 errors.
        Hence why you should use checksums if your goal is to detect errors (as opposed to correcting them), as checksums have no bounds on the number of errors, the only limitation being the probability of collisions.
        By default, return a tuple wth the maximum number of errors (2nd output) OR erasures (2nd output) that can be corrected.
        If errors or erasures (not both) is specified as argument, computes the remaining **simultaneous** correction capacity (eg, if errors specified, compute the number of erasures that can be simultaneously corrected).
        Set verbose to True to get print a report.'''
        # Fetch nsym from class attributes if not overriden by a function call
        if not nsym:
            nsym = self.nsym
        # Compute the maximum number of errors OR erasures
        maxerrors = int(nsym/2)  # always floor the number, we can't correct half a symbol, it's all or nothing
        maxerasures = nsym
        # Compute the maximum of simultaneous errors AND erasures
        if erasures is not None and erasures >= 0:
            # We know the erasures count, we want to know how many errors we can correct simultaneously
            if erasures > maxerasures:
                raise ReedSolomonError("Specified number of errors or erasures exceeding the Singleton Bound!")
            maxerrors = int((nsym-erasures)/2)
            if verbose:
                print('This codec can correct up to %i errors and %i erasures simultaneously' % (maxerrors, erasures))
            # Return a tuple with the maximum number of simultaneously corrected errors and erasures
            return maxerrors, erasures
        if errors is not None and errors >= 0:
            # We know the errors count, we want to know how many erasures we can correct simultaneously
            if errors > maxerrors:
                raise ReedSolomonError("Specified number of errors or erasures exceeding the Singleton Bound!")
            maxerasures = int(nsym-(errors*2))
            if verbose:
                print('This codec can correct up to %i errors and %i erasures simultaneously' % (errors, maxerasures))
            # Return a tuple with the maximum number of simultaneously corrected errors and erasures
            return errors, maxerasures
        # Return a tuple with the maximum number of errors and erasures (independently corrected)
        if verbose:
            print('This codec can correct up to %i errors and %i erasures independently' % (maxerrors, maxerasures))
        return maxerrors, maxerasures
