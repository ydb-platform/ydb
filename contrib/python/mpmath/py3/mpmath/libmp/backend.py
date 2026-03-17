import fractions
import os


#----------------------------------------------------------------------------#
# Support GMPY for high-speed large integer arithmetic.                      #
#                                                                            #
# To allow an external module to handle arithmetic, we need to make sure     #
# that all high-precision variables are declared of the correct type. MPZ    #
# is the constructor for the high-precision type. It defaults to Python's    #
# long type but can be assinged another type, typically gmpy.mpz.            #
#                                                                            #
# MPZ must be used for the mantissa component of an mpf and must be used     #
# for internal fixed-point operations.                                       #
#                                                                            #
# Side-effects:                                                              #
# * "is" cannot be used to test for special values.  Must use "==".          #
#----------------------------------------------------------------------------#

gmpy = None
BACKEND = 'python'
MPZ = int
MPQ = fractions.Fraction

if 'MPMATH_NOGMPY' not in os.environ:
    try:
        import gmpy2 as gmpy
        BACKEND = 'gmpy'
        MPQ = gmpy.mpq
    except ImportError:
        try:
            import gmp as gmpy
            BACKEND = 'gmp'
        except ImportError:
            pass

    if gmpy:
        MPZ = gmpy.mpz

MPZ_ZERO = MPZ(0)
MPZ_ONE = MPZ(1)
MPZ_TWO = MPZ(2)
MPZ_THREE = MPZ(3)
MPZ_FIVE = MPZ(5)

int_types = (int,) if BACKEND == 'python' else (int, MPZ)
