# vim: set et ai ts=4 sts=4 sw=4:
import os
import hashlib
from .util import *

SUN_JKS_ALGO_ID = (1,3,6,1,4,1,42,2,17,1,1) # JavaSoft proprietary key-protection algorithm
SUN_JCE_ALGO_ID = (1,3,6,1,4,1,42,2,19,1)   # PBE_WITH_MD5_AND_DES3_CBC_OID (non-published, modified version of PKCS#5 PBEWithMD5AndDES)

def jks_pkey_encrypt(key, password_str):
    """
    Encrypts the private key with password protection algorithm used by JKS keystores.
    """
    password_bytes = password_str.encode('utf-16be') # Java chars are UTF-16BE code units
    iv = os.urandom(20)

    key = bytearray(key)
    xoring = zip(key, _jks_keystream(iv, password_bytes))
    data = bytearray([d^k for d,k in xoring])

    check = hashlib.sha1(bytes(password_bytes + key)).digest()
    return bytes(iv + data + check)

def jks_pkey_decrypt(data, password_str):
    """
    Decrypts the private key password protection algorithm used by JKS keystores.
    The JDK sources state that 'the password is expected to be in printable ASCII', though this does not appear to be enforced;
    the password is converted into bytes simply by taking each individual Java char and appending its raw 2-byte representation.
    See sun/security/provider/KeyProtector.java in the JDK sources.
    """
    password_bytes = password_str.encode('utf-16be') # Java chars are UTF-16BE code units

    data = bytearray(data)
    iv, data, check = data[:20], data[20:-20], data[-20:]
    xoring = zip(data, _jks_keystream(iv, password_bytes))
    key = bytearray([d^k for d,k in xoring])

    if hashlib.sha1(bytes(password_bytes + key)).digest() != check:
        raise BadHashCheckException("Bad hash check on private key; wrong password?")
    key = bytes(key)
    return key

def _jks_keystream(iv, password):
    """Helper keystream generator for _jks_pkey_decrypt"""
    cur = iv
    while 1:
        xhash = hashlib.sha1(bytes(password + cur)) # hashlib.sha1 in python 2.6 does not accept a bytearray argument
        cur = bytearray(xhash.digest()) # make sure we iterate over ints in both Py2 and Py3
        for byte in cur:
            yield byte

def jce_pbe_decrypt(data, password, salt, iteration_count):
    """
    Decrypts Sun's custom PBEWithMD5AndTripleDES password-based encryption scheme.
    It is based on password-based encryption as defined by the PKCS #5 standard, except that it uses triple DES instead of DES.
    Here's how this algorithm works:
      1. Create random salt and split it in two halves. If the two halves are identical, invert(*) the first half.
      2. Concatenate password with each of the halves.
      3. Digest each concatenation with c iterations, where c is the iterationCount. Concatenate the output from each digest round with the password,
         and use the result as the input to the next digest operation. The digest algorithm is MD5.
      4. After c iterations, use the 2 resulting digests as follows: The 16 bytes of the first digest and the 1st 8 bytes of the 2nd digest
         form the triple DES key, and the last 8 bytes of the 2nd digest form the IV.

    (*) Not actually an inversion operation due to an implementation bug in com.sun.crypto.provider.PBECipherCore. See _jce_invert_salt_half for details.
    See http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/6-b27/com/sun/crypto/provider/PBECipherCore.java#PBECipherCore.deriveCipherKey%28java.security.Key%29
    """
    key, iv = _jce_pbe_derive_key_and_iv(password, salt, iteration_count)

    from Crypto.Cipher import DES3
    des3 = DES3.new(key, DES3.MODE_CBC, IV=iv)
    padded = des3.decrypt(data)

    result = strip_pkcs5_padding(padded)
    return result

def _jce_pbe_derive_key_and_iv(password, salt, iteration_count):
    if len(salt) != 8:
        raise ValueError("Expected 8-byte salt for PBEWithMD5AndTripleDES (OID %s), found %d bytes" % (".".join(str(i) for i in SUN_JCE_ALGO_ID), len(salt)))

    # Note: unlike JKS, the PBEWithMD5AndTripleDES algorithm as implemented for JCE keystores uses an ASCII string for the password, not a regular Java/UTF-16BE string.
    # It validates this explicitly and will throw an InvalidKeySpecException if non-ASCII byte codes are present in the password.
    # See PBEKey's constructor in com/sun/crypto/provider/PBEKey.java.
    try:
        password_bytes = password.encode('ascii')
    except (UnicodeDecodeError, UnicodeEncodeError):
        raise ValueError("Key password contains non-ASCII characters")

    salt_halves = [salt[0:4], salt[4:8]]
    if salt_halves[0] == salt_halves[1]:
        salt_halves[0] = _jce_invert_salt_half(salt_halves[0])

    derived = b""
    for i in range(2):
        to_be_hashed = salt_halves[i]
        for k in range(iteration_count):
            to_be_hashed = hashlib.md5(to_be_hashed + password_bytes).digest()
        derived += to_be_hashed

    key = derived[:-8] # = 24 bytes
    iv = derived[-8:]
    return key, iv

def _jce_invert_salt_half(salt_half):
    """
    JCE's proprietary PBEWithMD5AndTripleDES algorithm as described in the OpenJDK sources calls for inverting the first salt half if the two halves are equal.
    However, there appears to be a bug in the original JCE implementation of com.sun.crypto.provider.PBECipherCore causing it to perform a different operation:

      for (i=0; i<2; i++) {
          byte tmp = salt[i];
          salt[i] = salt[3-i];
          salt[3-1] = tmp;     // <-- typo '1' instead of 'i'
      }

    The result is transforming [a,b,c,d] into [d,a,b,d] instead of [d,c,b,a] (verified going back to the original JCE 1.2.2 release for JDK 1.2).
    See source (or bytecode) of com.sun.crypto.provider.PBECipherCore (JRE <= 7) and com.sun.crypto.provider.PBES1Core (JRE 8+):
    """
    salt = bytearray(salt_half)
    salt[2] = salt[1]
    salt[1] = salt[0]
    salt[0] = salt[3]
    return bytes(salt)

