#pragma once

#include "common.h"

#include <util/generic/ptr.h>
#include <util/system/defaults.h>

namespace NArgonish {
    /**
     * Type of Argon2 algorithm
     */
    enum class EArgon2Type : ui32 {
        Argon2d = 0, /// Data dependent version of Argon2
        Argon2i = 1, /// Data independent version of Argon2
        Argon2id = 2 /// Mixed version of Argon2
    };

    /**
     * Interface of all Argon2 instances
     */
    class IArgon2Base {
    public:
        virtual ~IArgon2Base() {
        }
        /**
         * Applies Argon2 algorithm
         * @param pwd password
         * @param pwdlen password length
         * @param salt salt
         * @param saltlen salt length
         * @param out output
         * @param outlen output length
         * @param aad additional authenticated data (optional)
         * @param aadlen additional authenticated data length (optional)
         */
        virtual void Hash(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,
                          ui8* out, ui32 outlen, const ui8* aad = nullptr, ui32 aadlen = 0) const = 0;

        /**
         * Applies Argon2 algorithm to a password and compares the result with the hash data
         * @param pwd password
         * @param pwdlen password length
         * @param salt salt
         * @param saltlen salt length
         * @param hash hash value to compare with the result
         * @param hashlen hash value length
         * @param aad additional authenticated data (optional)
         * @param adadlen additional authenticated data length (optional)
         * @return true if the Argon2 result equals to the value in hash
         */
        virtual bool Verify(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,
                            const ui8* hash, ui32 hashlen, const ui8* aad = nullptr, ui32 adadlen = 0) const = 0;

        /**
         * Applies Argon2 algorithms but allows to pass memory buffer for work.
         * This allows to use external memory allocator or reuse already allocated memory buffer.
         * @param memory memory buffer for Argon2 calculations
         * @param mlen memory buffer len (must be at least the value returned by the GetMemorySize method)
         * @param pwd password to hash
         * @param pwdlen password length
         * @param salt salt
         * @param saltlen salt length
         * @param out output buffer
         * @param outlen output length
         * @param aad additional authenticated data (optional)
         * @param aadlen additional authenticated data length (optional)
         */
        virtual void HashWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,
                                          const ui8* salt, ui32 saltlen, ui8* out, ui32 outlen,
                                          const ui8* aad = nullptr, ui32 aadlen = 0) const = 0;
        /**
         * Applies Argon2 algorithm to a password and compares the result with the hash data.
         * This method allows to use a custom memory allocator or reuse already allocated memory buffer.
         * @param memory memory buffer for Argon2 calculations
         * @param mlen memory buffer length
         * @param pwd password to hash
         * @param pwdlen password length
         * @param salt salt
         * @param saltlen salt length
         * @param hash hash value to compare with the result
         * @param hashlen hash value length
         * @param aad additional authenticated data (optional)
         * @param aadlen additional authenticated data length (optional)
         * @return true if the Argon2 result equals to the value in hash
         */
        virtual bool VerifyWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,
                                            const ui8* salt, ui32 saltlen, const ui8* hash, ui32 hashlen,
                                            const ui8* aad = nullptr, ui32 aadlen = 0) const = 0;

        /**
         * The function calculates the size of memory required by Argon2 algorithm
         * @return memory buffer size
         */
        virtual size_t GetMemorySize() const = 0;
    };

    /**
     * A factory to create Argon2 instances depending on instruction set, tcost, mcost, the number of threads etc.
     */
    class TArgon2Factory {
    public:
        /**
         * Constructs a factory object
         * @param skipTest if true then a simple runtime test will be skipped in the constructor (optional)
         */
        TArgon2Factory(bool skipTest = false);

        /**
         * Creates an instance of Argon2 algorithm.
         * The particular optimization is chosen automatically based on the cpuid instruction output.
         * @param atype the type of Argon2 algorithm
         * @param tcost the number of passes over memory block, must be at least 1
         * @param mcost the size in kilobytes of memory block used by Argon2
         * @param threads the number of threads for parallel version of Argon2 (must be 1,2 or 4)
         * @param key a secret key to use for password hashing (optional)
         * @param keylen the length of the key (optional)
         * @return unique_ptr to Argon2 instance. In case of error std::runtime_excetion is thrown
         */
        THolder<IArgon2Base> Create(EArgon2Type atype = EArgon2Type::Argon2d, ui32 tcost = 1, ui32 mcost = 1024,
                                    ui32 threads = 1, const ui8* key = nullptr, ui32 keylen = 0) const;

        /**
         * Creates an instance of Argon2 algorithm optimized for the provided instruction set
         * @param instructionSet instruction set
         * @param atype the type of Argon2 algorithm
         * @param tcost the number of passes over memory block, must be at least 1
         * @param mcost the size in kilobytes of memory block used by Argon2
         * @param threads the number of threads for parallel version of Argon2 (must be 1,2 or 4)
         * @param key a secret key to use for password hashing (optional)
         * @param keylen the length of the key (optional)
         * @return unique_ptr to Argon2 instance. In case of error std::runtime_excetion is thrown
         */
        THolder<IArgon2Base> Create(EInstructionSet instructionSet, EArgon2Type atype = EArgon2Type::Argon2d, ui32 tcost = 1,
                                    ui32 mcost = 1024, ui32 threads = 1, const ui8* key = nullptr,
                                    ui32 keylen = 0) const;

        /**
         * The function returns the best instruction set available on the current CPU
         * @return InstructionSet value
         */
        EInstructionSet GetInstructionSet() const;

    protected:
        EInstructionSet InstructionSet_ = EInstructionSet::REF;
        void QuickTest_() const;
    };
}
