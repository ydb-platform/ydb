from ctypes import (
    Structure,
    addressof,
    c_int,
    c_ubyte,
    c_ushort,
    c_void_p,
    sizeof,
    string_at,
)
from enum import IntEnum


class sock_filter(Structure):
    _fields_ = [
        ('code', c_ushort),  # u16
        ('jt', c_ubyte),  # u8
        ('jf', c_ubyte),  # u8
        ('k', c_int),  # can be signed or unsigned
    ]


class sock_fprog(Structure):
    _fields_ = [('len', c_ushort), ('filter', c_void_p)]


def compile(code: list[list[int]]):
    ProgramType = sock_filter * len(code)
    program = ProgramType(*[sock_filter(*line) for line in code])
    sfp = sock_fprog(len(code), addressof(program[0]))
    return string_at(addressof(sfp), sizeof(sfp)), program


class BPF(IntEnum):
    '''BPF constants.

    See:
    - https://www.kernel.org/doc/Documentation/networking/filter.txt
    - https://github.com/iovisor/bpf-docs/blob/master/eBPF.md
    '''

    # Operations
    LD = 0x00  # Load
    LDX = 0x01  # Load Index
    ST = 0x02  # Store
    STX = 0x03  # Store Index
    ALU = 0x04  # Arithmetic Logic Unit
    JMP = 0x05  # Jump
    RET = 0x06  # Return
    MISC = 0x07  # Miscellaneous

    # Sizes
    W = 0x00  # Word (4 bytes)
    H = 0x08  # Half-word (2 bytes)
    B = 0x10  # Byte (1 byte)

    # Offsets
    IMM = 0x00  # Immediate
    ABS = 0x20  # Absolute
    IND = 0x40  # Indirect
    MEM = 0x60  # Memory
    LEN = 0x80  # Packet Length
    MSH = 0xA0  # Masked

    # gotos
    JEQ = 0x10  # Jump if Equal
    JGT = 0x20  # Jump if Greater
    JGE = 0x30  # Jump if Greater or Equal
    JSET = 0x40  # Jump if Bit is Set

    # Sources
    K = 0x00  # Constant
    X = 0x08  # Register

    # Operators
    ADD = 0x00
    SUB = 0x10
    MUL = 0x20
    DIV = 0x30
    OR = 0x40
    AND = 0x50
    LSH = 0x60
    RSH = 0x70
    NEG = 0x80
    MOD = 0x90
    XOR = 0xA0
    MOV = 0xB0
    ARSH = 0xC0
    END = 0xD0
