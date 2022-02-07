#pragma once

#include "common.h"

#include <util/generic/ptr.h>

namespace NArgonish {
    /**
     * Interface for all Blake2B instances
     */
    class IBlake2Base {
    public:
        virtual ~IBlake2Base() {
        }
        /**
         * Updates intermediate hash with an ui32 value
         * @param in integer to hash
         */
        virtual void Update(ui32 in) = 0;

        /**
         * Updates intermediate hash with an array of bytes
         * @param pin input
         * @param inlen input length
         */
        virtual void Update(const void* pin, size_t inlen) = 0;

        /**
         * Finalizes the hash calculation and returns the hash value
         * @param out output buffer
         * @param outlen output buffer length
         */
        virtual void Final(void* out, size_t outlen) = 0;
    };

    /**
     * A factory that creates Blake2B instances optimized for different instruction sets
     */
    class TBlake2BFactory {
    public:
        /**
         * Constructs the factory object
         * @param skipTest if true then the constructor skips runtime Blake2B test
         */
        TBlake2BFactory(bool skipTest = false);

        /**
         * Creates an instance of Blake2B hash algorithm.
         * The optimisation is selected automatically based on the cpuid instruction output.
         * @param outlen the output buffer length, this value takes part in hashing
         * @param key a secret key to make Blake2B work as a keyed hash function
         * @param keylen the secret key length
         * @return returns an unique_ptr containing Blake2B instance
         */
        THolder<IBlake2Base> Create(size_t outlen = 32, const ui8* key = nullptr, size_t keylen = 0) const;

        /**
         * Creates an instance of Blake2B hash algorithm optimized for the particular instruction set
         * @param instructionSet instruction set
         * @param outlen the output buffer length, this value takes part in hashing
         * @param key a secret key to make Blake2B work as a keyed hash function
         * @param keylen the secret key length
         * @return returns an unique_ptr containing Blake2B instance
         */
        THolder<IBlake2Base> Create(EInstructionSet instructionSet, size_t outlen = 32,
                                    const ui8* key = nullptr, size_t keylen = 0) const;

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
