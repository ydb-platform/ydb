#-------------------------------------------------------------------------------
# elftools: elf/enums.py
#
# Mappings of enum names to values
#
# Eli Bendersky (eliben@gmail.com)
# This code is in the public domain
#-------------------------------------------------------------------------------
from ..common.utils import merge_dicts
from ..construct import Pass


# e_ident[EI_CLASS] in the ELF header
ENUM_EI_CLASS = dict(
    ELFCLASSNONE=0,
    ELFCLASS32=1,
    ELFCLASS64=2
)

# e_ident[EI_DATA] in the ELF header
ENUM_EI_DATA = dict(
    ELFDATANONE=0,
    ELFDATA2LSB=1,
    ELFDATA2MSB=2
)

# e_version in the ELF header
ENUM_E_VERSION = dict(
    EV_NONE=0,
    EV_CURRENT=1,
    _default_=Pass,
)

# e_ident[EI_OSABI] in the ELF header
ENUM_EI_OSABI = dict(
    ELFOSABI_SYSV=0,
    ELFOSABI_HPUX=1,
    ELFOSABI_NETBSD=2,
    ELFOSABI_LINUX=3,
    ELFOSABI_HURD=4,
    ELFOSABI_SOLARIS=6,
    ELFOSABI_AIX=7,
    ELFOSABI_IRIX=8,
    ELFOSABI_FREEBSD=9,
    ELFOSABI_TRU64=10,
    ELFOSABI_MODESTO=11,
    ELFOSABI_OPENBSD=12,
    ELFOSABI_OPENVMS=13,
    ELFOSABI_NSK=14,
    ELFOSABI_AROS=15,
    ELFOSABI_FENIXOS=16,
    ELFOSABI_CLOUD=17,
    ELFOSABI_SORTIX=53,
    ELFOSABI_ARM_AEABI=64,
    ELFOSABI_ARM=97,
    ELFOSABI_CELL_LV2=102,
    ELFOSABI_STANDALONE=255,
    _default_=Pass,
)

# e_type in the ELF header
ENUM_E_TYPE = dict(
    ET_NONE=0,
    ET_REL=1,
    ET_EXEC=2,
    ET_DYN=3,
    ET_CORE=4,
    ET_LOPROC=0xff00,
    ET_HIPROC=0xffff,
    _default_=Pass,
)

# e_machine in the ELF header
ENUM_E_MACHINE = dict(
    EM_NONE          = 0,   # No machine
    EM_M32           = 1,   # AT&T WE 32100
    EM_SPARC         = 2,   # SPARC
    EM_386           = 3,   # Intel 80386
    EM_68K           = 4,   # Motorola 68000
    EM_88K           = 5,   # Motorola 88000
    EM_IAMCU         = 6,   # Intel MCU
    EM_860           = 7,   # Intel 80860
    EM_MIPS          = 8,   # MIPS I Architecture
    EM_S370          = 9,   # IBM System/370 Processor
    EM_MIPS_RS3_LE   = 10,  # MIPS RS3000 Little-endian
    EM_PARISC        = 15,  # Hewlett-Packard PA-RISC
    EM_VPP500        = 17,  # Fujitsu VPP500
    EM_SPARC32PLUS   = 18,  # Enhanced instruction set SPARC
    EM_960           = 19,  # Intel 80960
    EM_PPC           = 20,  # PowerPC
    EM_PPC64         = 21,  # 64-bit PowerPC
    EM_S390          = 22,  # IBM System/390 Processor
    EM_SPU           = 23,  # IBM SPU/SPC
    EM_V800          = 36,  # NEC V800
    EM_FR20          = 37,  # Fujitsu FR20
    EM_RH32          = 38,  # TRW RH-32
    EM_RCE           = 39,  # Motorola RCE
    EM_ARM           = 40,  # ARM 32-bit architecture (AARCH32)
    EM_ALPHA         = 41,  # Digital Alpha
    EM_SH            = 42,  # Hitachi SH
    EM_SPARCV9       = 43,  # SPARC Version 9
    EM_TRICORE       = 44,  # Siemens TriCore embedded processor
    EM_ARC           = 45,  # Argonaut RISC Core, Argonaut Technologies Inc.
    EM_H8_300        = 46,  # Hitachi H8/300
    EM_H8_300H       = 47,  # Hitachi H8/300H
    EM_H8S           = 48,  # Hitachi H8S
    EM_H8_500        = 49,  # Hitachi H8/500
    EM_IA_64         = 50,  # Intel IA-64 processor architecture
    EM_MIPS_X        = 51,  # Stanford MIPS-X
    EM_COLDFIRE      = 52,  # Motorola ColdFire
    EM_68HC12        = 53,  # Motorola M68HC12
    EM_MMA           = 54,  # Fujitsu MMA Multimedia Accelerator
    EM_PCP           = 55,  # Siemens PCP
    EM_NCPU          = 56,  # Sony nCPU embedded RISC processor
    EM_NDR1          = 57,  # Denso NDR1 microprocessor
    EM_STARCORE      = 58,  # Motorola Star*Core processor
    EM_ME16          = 59,  # Toyota ME16 processor
    EM_ST100         = 60,  # STMicroelectronics ST100 processor
    EM_TINYJ         = 61,  # Advanced Logic Corp. TinyJ embedded processor family
    EM_X86_64        = 62,  # AMD x86-64 architecture
    EM_PDSP          = 63,  # Sony DSP Processor
    EM_PDP10         = 64,  # Digital Equipment Corp. PDP-10
    EM_PDP11         = 65,  # Digital Equipment Corp. PDP-11
    EM_FX66          = 66,  # Siemens FX66 microcontroller
    EM_ST9PLUS       = 67,  # STMicroelectronics ST9+ 8/16 bit microcontroller
    EM_ST7           = 68,  # STMicroelectronics ST7 8-bit microcontroller
    EM_68HC16        = 69,  # Motorola MC68HC16 Microcontroller
    EM_68HC11        = 70,  # Motorola MC68HC11 Microcontroller
    EM_68HC08        = 71,  # Motorola MC68HC08 Microcontroller
    EM_68HC05        = 72,  # Motorola MC68HC05 Microcontroller
    EM_SVX           = 73,  # Silicon Graphics SVx
    EM_ST19          = 74,  # STMicroelectronics ST19 8-bit microcontroller
    EM_VAX           = 75,  # Digital VAX
    EM_CRIS          = 76,  # Axis Communications 32-bit embedded processor
    EM_JAVELIN       = 77,  # Infineon Technologies 32-bit embedded processor
    EM_FIREPATH      = 78,  # Element 14 64-bit DSP Processor
    EM_ZSP           = 79,  # LSI Logic 16-bit DSP Processor
    EM_MMIX          = 80,  # Donald Knuth's educational 64-bit processor
    EM_HUANY         = 81,  # Harvard University machine-independent object files
    EM_PRISM         = 82,  # SiTera Prism
    EM_AVR           = 83,  # Atmel AVR 8-bit microcontroller
    EM_FR30          = 84,  # Fujitsu FR30
    EM_D10V          = 85,  # Mitsubishi D10V
    EM_D30V          = 86,  # Mitsubishi D30V
    EM_V850          = 87,  # NEC v850
    EM_M32R          = 88,  # Mitsubishi M32R
    EM_MN10300       = 89,  # Matsushita MN10300
    EM_MN10200       = 90,  # Matsushita MN10200
    EM_PJ            = 91,  # picoJava
    EM_OPENRISC      = 92,  # OpenRISC 32-bit embedded processor
    EM_ARC_COMPACT   = 93,  # ARC International ARCompact processor (old spelling/synonym: EM_ARC_A5)
    EM_XTENSA        = 94,  # Tensilica Xtensa Architecture
    EM_VIDEOCORE     = 95,  # Alphamosaic VideoCore processor
    EM_TMM_GPP       = 96,  # Thompson Multimedia General Purpose Processor
    EM_NS32K         = 97,  # National Semiconductor 32000 series
    EM_TPC           = 98,  # Tenor Network TPC processor
    EM_SNP1K         = 99,  # Trebia SNP 1000 processor
    EM_ST200         = 100, # STMicroelectronics (www.st.com) ST200 microcontroller
    EM_IP2K          = 101, # Ubicom IP2xxx microcontroller family
    EM_MAX           = 102, # MAX Processor
    EM_CR            = 103, # National Semiconductor CompactRISC microprocessor
    EM_F2MC16        = 104, # Fujitsu F2MC16
    EM_MSP430        = 105, # Texas Instruments embedded microcontroller msp430
    EM_BLACKFIN      = 106, # Analog Devices Blackfin (DSP) processor
    EM_SE_C33        = 107, # S1C33 Family of Seiko Epson processors
    EM_SEP           = 108, # Sharp embedded microprocessor
    EM_ARCA          = 109, # Arca RISC Microprocessor
    EM_UNICORE       = 110, # Microprocessor series from PKU-Unity Ltd. and MPRC of Peking University
    EM_EXCESS        = 111, # eXcess: 16/32/64-bit configurable embedded CPU
    EM_DXP           = 112, # Icera Semiconductor Inc. Deep Execution Processor
    EM_ALTERA_NIOS2  = 113, # Altera Nios II soft-core processor
    EM_CRX           = 114, # National Semiconductor CompactRISC CRX microprocessor
    EM_XGATE         = 115, # Motorola XGATE embedded processor
    EM_C166          = 116, # Infineon C16x/XC16x processor
    EM_M16C          = 117, # Renesas M16C series microprocessors
    EM_DSPIC30F      = 118, # Microchip Technology dsPIC30F Digital Signal Controller
    EM_CE            = 119, # Freescale Communication Engine RISC core
    EM_M32C          = 120, # Renesas M32C series microprocessors
    EM_TSK3000       = 131, # Altium TSK3000 core
    EM_RS08          = 132, # Freescale RS08 embedded processor
    EM_SHARC         = 133, # Analog Devices SHARC family of 32-bit DSP processors
    EM_ECOG2         = 134, # Cyan Technology eCOG2 microprocessor
    EM_SCORE7        = 135, # Sunplus S+core7 RISC processor
    EM_DSP24         = 136, # New Japan Radio (NJR) 24-bit DSP Processor
    EM_VIDEOCORE3    = 137, # Broadcom VideoCore III processor
    EM_LATTICEMICO32 = 138, # RISC processor for Lattice FPGA architecture
    EM_SE_C17        = 139, # Seiko Epson C17 family
    EM_TI_C6000      = 140, # The Texas Instruments TMS320C6000 DSP family
    EM_TI_C2000      = 141, # The Texas Instruments TMS320C2000 DSP family
    EM_TI_C5500      = 142, # The Texas Instruments TMS320C55x DSP family
    EM_TI_ARP32      = 143, # Texas Instruments Application Specific RISC Processor, 32bit fetch
    EM_TI_PRU        = 144, # Texas Instruments Programmable Realtime Unit
    EM_MMDSP_PLUS    = 160, # STMicroelectronics 64bit VLIW Data Signal Processor
    EM_CYPRESS_M8C   = 161, # Cypress M8C microprocessor
    EM_R32C          = 162, # Renesas R32C series microprocessors
    EM_TRIMEDIA      = 163, # NXP Semiconductors TriMedia architecture family
    EM_QDSP6         = 164, # QUALCOMM DSP6 Processor
    EM_8051          = 165, # Intel 8051 and variants
    EM_STXP7X        = 166, # STMicroelectronics STxP7x family of configurable and extensible RISC processors
    EM_NDS32         = 167, # Andes Technology compact code size embedded RISC processor family
    EM_ECOG1         = 168, # Cyan Technology eCOG1X family
    EM_ECOG1X        = 168, # Cyan Technology eCOG1X family
    EM_MAXQ30        = 169, # Dallas Semiconductor MAXQ30 Core Micro-controllers
    EM_XIMO16        = 170, # New Japan Radio (NJR) 16-bit DSP Processor
    EM_MANIK         = 171, # M2000 Reconfigurable RISC Microprocessor
    EM_CRAYNV2       = 172, # Cray Inc. NV2 vector architecture
    EM_RX            = 173, # Renesas RX family
    EM_METAG         = 174, # Imagination Technologies META processor architecture
    EM_MCST_ELBRUS   = 175, # MCST Elbrus general purpose hardware architecture
    EM_ECOG16        = 176, # Cyan Technology eCOG16 family
    EM_CR16          = 177, # National Semiconductor CompactRISC CR16 16-bit microprocessor
    EM_ETPU          = 178, # Freescale Extended Time Processing Unit
    EM_SLE9X         = 179, # Infineon Technologies SLE9X core
    EM_L10M          = 180, # Intel L10M
    EM_K10M          = 181, # Intel K10M
    EM_AARCH64       = 183, # ARM 64-bit architecture (AARCH64)
    EM_AVR32         = 185, # Atmel Corporation 32-bit microprocessor family
    EM_STM8          = 186, # STMicroeletronics STM8 8-bit microcontroller
    EM_TILE64        = 187, # Tilera TILE64 multicore architecture family
    EM_TILEPRO       = 188, # Tilera TILEPro multicore architecture family
    EM_MICROBLAZE    = 189, # Xilinx MicroBlaze 32-bit RISC soft processor core
    EM_CUDA          = 190, # NVIDIA CUDA architecture
    EM_TILEGX        = 191, # Tilera TILE-Gx multicore architecture family
    EM_CLOUDSHIELD   = 192, # CloudShield architecture family
    EM_COREA_1ST     = 193, # KIPO-KAIST Core-A 1st generation processor family
    EM_COREA_2ND     = 194, # KIPO-KAIST Core-A 2nd generation processor family
    EM_ARC_COMPACT2  = 195, # Synopsys ARCompact V2
    EM_OPEN8         = 196, # Open8 8-bit RISC soft processor core
    EM_RL78          = 197, # Renesas RL78 family
    EM_VIDEOCORE5    = 198, # Broadcom VideoCore V processor
    EM_78KOR         = 199, # Renesas 78KOR family
    EM_56800EX       = 200, # Freescale 56800EX Digital Signal Controller (DSC)
    EM_BA1           = 201, # Beyond BA1 CPU architecture
    EM_BA2           = 202, # Beyond BA2 CPU architecture
    EM_XCORE         = 203, # XMOS xCORE processor family
    EM_MCHP_PIC      = 204, # Microchip 8-bit PIC(r) family
    EM_INTEL205      = 205, # Reserved by Intel
    EM_INTEL206      = 206, # Reserved by Intel
    EM_INTEL207      = 207, # Reserved by Intel
    EM_INTEL208      = 208, # Reserved by Intel
    EM_INTEL209      = 209, # Reserved by Intel
    EM_KM32          = 210, # KM211 KM32 32-bit processor
    EM_KMX32         = 211, # KM211 KMX32 32-bit processor
    EM_KMX16         = 212, # KM211 KMX16 16-bit processor
    EM_KMX8          = 213, # KM211 KMX8 8-bit processor
    EM_KVARC         = 214, # KM211 KVARC processor
    EM_CDP           = 215, # Paneve CDP architecture family
    EM_COGE          = 216, # Cognitive Smart Memory Processor
    EM_COOL          = 217, # Bluechip Systems CoolEngine
    EM_NORC          = 218, # Nanoradio Optimized RISC
    EM_CSR_KALIMBA   = 219, # CSR Kalimba architecture family
    EM_Z80           = 220, # Zilog Z80
    EM_VISIUM        = 221, # Controls and Data Services VISIUMcore processor
    EM_FT32          = 222, # FTDI Chip FT32 high performance 32-bit RISC architecture
    EM_MOXIE         = 223, # Moxie processor family
    EM_AMDGPU        = 224, # AMD GPU architecture
    EM_RISCV         = 243, # RISC-V
    EM_BPF           = 247,	# Linux BPF - in-kernel virtual machine
    EM_CSKY          = 252,	# C-SKY
    EM_FRV           = 0x5441, # Fujitsu FR-V
    # Reservations
    # reserved  11-14   Reserved for future use
    # reserved  16      Reserved for future use
    # reserved  24-35   Reserved for future use
    # reserved  121-130 Reserved for future use
    # reserved  145-159 Reserved for future use
    # reserved  145-159 Reserved for future use
    # reserved  182     Reserved for future Intel use
    # reserved  184     Reserved for future ARM use
    # unknown/reserve?  225 - 242
    _default_=Pass,
)

# sh_type in the section header
#
# This is the "base" dict that doesn't hold processor-specific values; from it
# we later create per-processor dicts that use the LOPROC...HIPROC range to
# define processor-specific values. The proper dict should be used based on the
# machine the ELF header refers to.
ENUM_SH_TYPE_BASE = dict(
    SHT_NULL=0,
    SHT_PROGBITS=1,
    SHT_SYMTAB=2,
    SHT_STRTAB=3,
    SHT_RELA=4,
    SHT_HASH=5,
    SHT_DYNAMIC=6,
    SHT_NOTE=7,
    SHT_NOBITS=8,
    SHT_REL=9,
    SHT_SHLIB=10,
    SHT_DYNSYM=11,
    SHT_INIT_ARRAY=14,
    SHT_FINI_ARRAY=15,
    SHT_PREINIT_ARRAY=16,
    SHT_GROUP=17,
    SHT_SYMTAB_SHNDX=18,
    SHT_RELR=19,
    SHT_NUM=20,
    SHT_LOOS=0x60000000,
    SHT_GNU_ATTRIBUTES=0x6ffffff5,
    SHT_GNU_HASH=0x6ffffff6,
    SHT_GNU_LIBLIST=0x6ffffff7,
    SHT_GNU_verdef=0x6ffffffd,  # also SHT_SUNW_verdef
    SHT_GNU_verneed=0x6ffffffe, # also SHT_SUNW_verneed
    SHT_GNU_versym=0x6fffffff,  # also SHT_SUNW_versym, SHT_HIOS

    # These are commented out because they carry no semantic meaning in
    # themselves and may be overridden by target-specific enums.
    #SHT_LOPROC=0x70000000,
    #SHT_HIPROC=0x7fffffff,

    SHT_LOUSER=0x80000000,
    SHT_HIUSER=0xffffffff,
    SHT_SUNW_LDYNSYM=0x6ffffff3,
    SHT_SUNW_syminfo=0x6ffffffc,
    _default_=Pass,
)

ENUM_SH_TYPE_AMD64 = merge_dicts(
        ENUM_SH_TYPE_BASE,
        dict(SHT_AMD64_UNWIND=0x70000001))

ENUM_SH_TYPE_ARM = merge_dicts(
        ENUM_SH_TYPE_BASE,
        dict(
            SHT_ARM_EXIDX=0x70000001,
            SHT_ARM_PREEMPTMAP=0x70000002,
            SHT_ARM_ATTRIBUTES=0x70000003,
            SHT_ARM_DEBUGOVERLAY=0x70000004))

ENUM_SH_TYPE_MIPS = merge_dicts(
        ENUM_SH_TYPE_BASE,
        dict(
            SHT_MIPS_LIBLIST=0x70000000,
            SHT_MIPS_DEBUG=0x70000005,
            SHT_MIPS_REGINFO=0x70000006,
            SHT_MIPS_PACKAGE=0x70000007,
            SHT_MIPS_PACKSYM=0x70000008,
            SHT_MIPS_RELD=0x70000009,
            SHT_MIPS_IFACE=0x7000000b,
            SHT_MIPS_CONTENT=0x7000000c,
            SHT_MIPS_OPTIONS=0x7000000d,
            SHT_MIPS_SHDR=0x70000010,
            SHT_MIPS_FDESC=0x70000011,
            SHT_MIPS_EXTSYM=0x70000012,
            SHT_MIPS_DENSE=0x70000013,
            SHT_MIPS_PDESC=0x70000014,
            SHT_MIPS_LOCSYM=0x70000015,
            SHT_MIPS_AUXSYM=0x70000016,
            SHT_MIPS_OPTSYM=0x70000017,
            SHT_MIPS_LOCSTR=0x70000018,
            SHT_MIPS_LINE=0x70000019,
            SHT_MIPS_RFDESC=0x7000001a,
            SHT_MIPS_DELTASYM=0x7000001b,
            SHT_MIPS_DELTAINST=0x7000001c,
            SHT_MIPS_DELTACLASS=0x7000001d,
            SHT_MIPS_DWARF=0x7000001e,
            SHT_MIPS_DELTADECL=0x7000001f,
            SHT_MIPS_SYMBOL_LIB=0x70000020,
            SHT_MIPS_EVENTS=0x70000021,
            SHT_MIPS_TRANSLATE=0x70000022,
            SHT_MIPS_PIXIE=0x70000023,
            SHT_MIPS_XLATE=0x70000024,
            SHT_MIPS_XLATE_DEBUG=0x70000025,
            SHT_MIPS_WHIRL=0x70000026,
            SHT_MIPS_EH_REGION=0x70000027,
            SHT_MIPS_XLATE_OLD=0x70000028,
            SHT_MIPS_PDR_EXCEPTION=0x70000029,
            SHT_MIPS_ABIFLAGS=0x7000002a))

ENUM_ELFCOMPRESS_TYPE = dict(
    ELFCOMPRESS_ZLIB=1,
    ELFCOMPRESS_LOOS=0x60000000,
    ELFCOMPRESS_HIOS=0x6fffffff,
    ELFCOMPRESS_LOPROC=0x70000000,
    ELFCOMPRESS_HIPROC=0x7fffffff,
    _default_=Pass,
)

# p_type in the program header
# some values scavenged from the ELF headers in binutils-2.21
#
# Using the same base + per-processor augmentation technique as in sh_type.
ENUM_P_TYPE_BASE = dict(
    PT_NULL=0,
    PT_LOAD=1,
    PT_DYNAMIC=2,
    PT_INTERP=3,
    PT_NOTE=4,
    PT_SHLIB=5,
    PT_PHDR=6,
    PT_TLS=7,
    PT_LOOS=0x60000000,
    PT_HIOS=0x6fffffff,

    # These are commented out because they carry no semantic meaning in
    # themselves and may be overridden by target-specific enums.
    #PT_LOPROC=0x70000000,
    #PT_HIPROC=0x7fffffff,

    PT_GNU_EH_FRAME=0x6474e550,
    PT_GNU_STACK=0x6474e551,
    PT_GNU_RELRO=0x6474e552,
    PT_GNU_PROPERTY=0x6474e553,
    _default_=Pass,
)

ENUM_P_TYPE_ARM = merge_dicts(
        ENUM_P_TYPE_BASE,
        dict(
            PT_ARM_ARCHEXT=0x70000000,
            PT_ARM_EXIDX=0x70000001))

ENUM_P_TYPE_AARCH64 = merge_dicts(
        ENUM_P_TYPE_BASE,
        dict(
            PT_AARCH64_ARCHEXT=0x70000000,
            PT_AARCH64_UNWIND=0x70000001))

ENUM_P_TYPE_MIPS = merge_dicts(
        ENUM_P_TYPE_BASE,
        dict(PT_MIPS_ABIFLAGS=0x70000003))

# st_info bindings in the symbol header
ENUM_ST_INFO_BIND = dict(
    STB_LOCAL=0,
    STB_GLOBAL=1,
    STB_WEAK=2,
    STB_NUM=3,
    STB_LOOS=10,
    STB_HIOS=12,
    STB_LOPROC=13,
    STB_HIPROC=15,
    _default_=Pass,
)

# st_info type in the symbol header
ENUM_ST_INFO_TYPE = dict(
    STT_NOTYPE=0,
    STT_OBJECT=1,
    STT_FUNC=2,
    STT_SECTION=3,
    STT_FILE=4,
    STT_COMMON=5,
    STT_TLS=6,
    STT_NUM=7,
    STT_RELC=8,
    STT_SRELC=9,
    STT_LOOS=10,
    STT_HIOS=12,
    STT_LOPROC=13,
    STT_HIPROC=15,
    _default_=Pass,
)

# visibility from st_other
ENUM_ST_VISIBILITY = dict(
    STV_DEFAULT=0,
    STV_INTERNAL=1,
    STV_HIDDEN=2,
    STV_PROTECTED=3,
    STV_EXPORTED=4,
    STV_SINGLETON=5,
    STV_ELIMINATE=6,
    _default_=Pass,
)

ENUM_ST_LOCAL = dict(
    _default_=Pass,
)

# st_shndx
ENUM_ST_SHNDX = dict(
    SHN_UNDEF=0,
    SHN_ABS=0xfff1,
    SHN_COMMON=0xfff2,
    _default_=Pass,
)

# d_tag
ENUM_D_TAG_COMMON = dict(
    DT_NULL=0,
    DT_NEEDED=1,
    DT_PLTRELSZ=2,
    DT_PLTGOT=3,
    DT_HASH=4,
    DT_STRTAB=5,
    DT_SYMTAB=6,
    DT_RELA=7,
    DT_RELASZ=8,
    DT_RELAENT=9,
    DT_STRSZ=10,
    DT_SYMENT=11,
    DT_INIT=12,
    DT_FINI=13,
    DT_SONAME=14,
    DT_RPATH=15,
    DT_SYMBOLIC=16,
    DT_REL=17,
    DT_RELSZ=18,
    DT_RELENT=19,
    DT_PLTREL=20,
    DT_DEBUG=21,
    DT_TEXTREL=22,
    DT_JMPREL=23,
    DT_BIND_NOW=24,
    DT_INIT_ARRAY=25,
    DT_FINI_ARRAY=26,
    DT_INIT_ARRAYSZ=27,
    DT_FINI_ARRAYSZ=28,
    DT_RUNPATH=29,
    DT_FLAGS=30,
    DT_ENCODING=32,
    DT_PREINIT_ARRAY=32,
    DT_PREINIT_ARRAYSZ=33,
    DT_SYMTAB_SHNDX=34,
    DT_RELRSZ=35,
    DT_RELR=36,
    DT_RELRENT=37,
    DT_NUM=38,
    DT_LOOS=0x6000000d,
    DT_ANDROID_REL=0x6000000f,
    DT_ANDROID_RELSZ=0x60000010,
    DT_ANDROID_RELA=0x60000011,
    DT_ANDROID_RELASZ=0x60000012,
    DT_ANDROID_RELR=0x6fffe000,
    DT_ANDROID_RELRSZ=0x6fffe001,
    DT_ANDROID_RELRENT=0x6fffe003,
    DT_ANDROID_RELRCOUNT=0x6fffe005,
    DT_HIOS=0x6ffff000,
    DT_LOPROC=0x70000000,
    DT_HIPROC=0x7fffffff,
    DT_PROCNUM=0x35,
    DT_VALRNGLO=0x6ffffd00,
    DT_GNU_PRELINKED=0x6ffffdf5,
    DT_GNU_CONFLICTSZ=0x6ffffdf6,
    DT_GNU_LIBLISTSZ=0x6ffffdf7,
    DT_CHECKSUM=0x6ffffdf8,
    DT_PLTPADSZ=0x6ffffdf9,
    DT_MOVEENT=0x6ffffdfa,
    DT_MOVESZ=0x6ffffdfb,
    DT_SYMINSZ=0x6ffffdfe,
    DT_SYMINENT=0x6ffffdff,
    DT_GNU_HASH=0x6ffffef5,
    DT_TLSDESC_PLT=0x6ffffef6,
    DT_TLSDESC_GOT=0x6ffffef7,
    DT_GNU_CONFLICT=0x6ffffef8,
    DT_GNU_LIBLIST=0x6ffffef9,
    DT_CONFIG=0x6ffffefa,
    DT_DEPAUDIT=0x6ffffefb,
    DT_AUDIT=0x6ffffefc,
    DT_PLTPAD=0x6ffffefd,
    DT_MOVETAB=0x6ffffefe,
    DT_SYMINFO=0x6ffffeff,
    DT_VERSYM=0x6ffffff0,
    DT_RELACOUNT=0x6ffffff9,
    DT_RELCOUNT=0x6ffffffa,
    DT_FLAGS_1=0x6ffffffb,
    DT_VERDEF=0x6ffffffc,
    DT_VERDEFNUM=0x6ffffffd,
    DT_VERNEED=0x6ffffffe,
    DT_VERNEEDNUM=0x6fffffff,
    DT_AUXILIARY=0x7ffffffd,
    DT_FILTER=0x7fffffff,
    _default_=Pass,
)

# Above are the dynamic tags which are valid always.
# Below are the dynamic tags which are only valid in certain contexts.

ENUM_D_TAG_SOLARIS = dict(
    DT_SUNW_AUXILIARY=0x6000000d,
    DT_SUNW_RTLDINF=0x6000000e,
    DT_SUNW_FILTER=0x6000000f,
    DT_SUNW_CAP=0x60000010,
    DT_SUNW_SYMTAB=0x60000011,
    DT_SUNW_SYMSZ=0x60000012,
    DT_SUNW_ENCODING=0x60000013,
    DT_SUNW_SORTENT=0x60000013,
    DT_SUNW_SYMSORT=0x60000014,
    DT_SUNW_SYMSORTSZ=0x60000015,
    DT_SUNW_TLSSORT=0x60000016,
    DT_SUNW_TLSSORTSZ=0x60000017,
    DT_SUNW_CAPINFO=0x60000018,
    DT_SUNW_STRPAD=0x60000019,
    DT_SUNW_CAPCHAIN=0x6000001a,
    DT_SUNW_LDMACH=0x6000001b,
    DT_SUNW_CAPCHAINENT=0x6000001d,
    DT_SUNW_CAPCHAINSZ=0x6000001f,
)

ENUM_D_TAG_MIPS = dict(
    DT_MIPS_RLD_VERSION=0x70000001,
    DT_MIPS_TIME_STAMP=0x70000002,
    DT_MIPS_ICHECKSUM=0x70000003,
    DT_MIPS_IVERSION=0x70000004,
    DT_MIPS_FLAGS=0x70000005,
    DT_MIPS_BASE_ADDRESS=0x70000006,
    DT_MIPS_CONFLICT=0x70000008,
    DT_MIPS_LIBLIST=0x70000009,
    DT_MIPS_LOCAL_GOTNO=0x7000000a,
    DT_MIPS_CONFLICTNO=0x7000000b,
    DT_MIPS_LIBLISTNO=0x70000010,
    DT_MIPS_SYMTABNO=0x70000011,
    DT_MIPS_UNREFEXTNO=0x70000012,
    DT_MIPS_GOTSYM=0x70000013,
    DT_MIPS_HIPAGENO=0x70000014,
    DT_MIPS_RLD_MAP=0x70000016,
    DT_MIPS_RLD_MAP_REL=0x70000035,
)

# Here is the mapping from e_machine enum to the extra dynamic tags which it
# validates. Solaris is missing from this list because its inclusion is not
# controlled by e_machine but rather e_ident[EI_OSABI].
# TODO: add the rest of the machine-specific dynamic tags, not just mips and
# solaris

ENUMMAP_EXTRA_D_TAG_MACHINE = dict(
    EM_MIPS=ENUM_D_TAG_MIPS,
    EM_MIPS_RS3_LE=ENUM_D_TAG_MIPS,
)

# Here is the full combined mapping from tag name to value

ENUM_D_TAG = dict(ENUM_D_TAG_COMMON)
ENUM_D_TAG.update(ENUM_D_TAG_SOLARIS)
for k in ENUMMAP_EXTRA_D_TAG_MACHINE:
    ENUM_D_TAG.update(ENUMMAP_EXTRA_D_TAG_MACHINE[k])

ENUM_DT_FLAGS = dict(
    DF_ORIGIN=0x1,
    DF_SYMBOLIC=0x2,
    DF_TEXTREL=0x4,
    DF_BIND_NOW=0x8,
    DF_STATIC_TLS=0x10,
)

ENUM_DT_FLAGS_1 = dict(
    DF_1_NOW=0x1,
    DF_1_GLOBAL=0x2,
    DF_1_GROUP=0x4,
    DF_1_NODELETE=0x8,
    DF_1_LOADFLTR=0x10,
    DF_1_INITFIRST=0x20,
    DF_1_NOOPEN=0x40,
    DF_1_ORIGIN=0x80,
    DF_1_DIRECT=0x100,
    DF_1_TRANS=0x200,
    DF_1_INTERPOSE=0x400,
    DF_1_NODEFLIB=0x800,
    DF_1_NODUMP=0x1000,
    DF_1_CONFALT=0x2000,
    DF_1_ENDFILTEE=0x4000,
    DF_1_DISPRELDNE=0x8000,
    DF_1_DISPRELPND=0x10000,
    DF_1_NODIRECT=0x20000,
    DF_1_IGNMULDEF=0x40000,
    DF_1_NOKSYMS=0x80000,
    DF_1_NOHDR=0x100000,
    DF_1_EDITED=0x200000,
    DF_1_NORELOC=0x400000,
    DF_1_SYMINTPOSE=0x800000,
    DF_1_GLOBAUDIT=0x1000000,
    DF_1_SINGLETON=0x2000000,
    DF_1_STUB=0x4000000,
    DF_1_PIE=0x8000000,
)

ENUM_RELOC_TYPE_MIPS = dict(
    R_MIPS_NONE=0,
    R_MIPS_16=1,
    R_MIPS_32=2,
    R_MIPS_REL32=3,
    R_MIPS_26=4,
    R_MIPS_HI16=5,
    R_MIPS_LO16=6,
    R_MIPS_GPREL16=7,
    R_MIPS_LITERAL=8,
    R_MIPS_GOT16=9,
    R_MIPS_PC16=10,
    R_MIPS_CALL16=11,
    R_MIPS_GPREL32=12,
    R_MIPS_SHIFT5=16,
    R_MIPS_SHIFT6=17,
    R_MIPS_64=18,
    R_MIPS_GOT_DISP=19,
    R_MIPS_GOT_PAGE=20,
    R_MIPS_GOT_OFST=21,
    R_MIPS_GOT_HI16=22,
    R_MIPS_GOT_LO16=23,
    R_MIPS_SUB=24,
    R_MIPS_INSERT_A=25,
    R_MIPS_INSERT_B=26,
    R_MIPS_DELETE=27,
    R_MIPS_HIGHER=28,
    R_MIPS_HIGHEST=29,
    R_MIPS_CALL_HI16=30,
    R_MIPS_CALL_LO16=31,
    R_MIPS_SCN_DISP=32,
    R_MIPS_REL16=33,
    R_MIPS_ADD_IMMEDIATE=34,
    R_MIPS_PJUMP=35,
    R_MIPS_RELGOT=36,
    R_MIPS_JALR=37,
    R_MIPS_TLS_DTPMOD32=38,
    R_MIPS_TLS_DTPREL32=39,
    R_MIPS_TLS_DTPMOD64=40,
    R_MIPS_TLS_DTPREL64=41,
    R_MIPS_TLS_GD=42,
    R_MIPS_TLS_LDM=43,
    R_MIPS_TLS_DTPREL_HI16=44,
    R_MIPS_TLS_DTPREL_LO16=45,
    R_MIPS_TLS_GOTTPREL=46,
    R_MIPS_TLS_TPREL32=47,
    R_MIPS_TLS_TPREL64=48,
    R_MIPS_TLS_TPREL_HI16=49,
    R_MIPS_TLS_TPREL_LO16=50,
    R_MIPS_GLOB_DAT=51,
    R_MIPS_COPY=126,
    R_MIPS_JUMP_SLOT=127,
    _default_=Pass,
)

ENUM_RELOC_TYPE_i386 = dict(
    R_386_NONE=0,
    R_386_32=1,
    R_386_PC32=2,
    R_386_GOT32=3,
    R_386_PLT32=4,
    R_386_COPY=5,
    R_386_GLOB_DAT=6,
    R_386_JUMP_SLOT=7,
    R_386_RELATIVE=8,
    R_386_GOTOFF=9,
    R_386_GOTPC=10,
    R_386_32PLT=11,
    R_386_TLS_TPOFF=14,
    R_386_TLS_IE=15,
    R_386_TLS_GOTIE=16,
    R_386_TLS_LE=17,
    R_386_TLS_GD=18,
    R_386_TLS_LDM=19,
    R_386_16=20,
    R_386_PC16=21,
    R_386_8=22,
    R_386_PC8=23,
    R_386_TLS_GD_32=24,
    R_386_TLS_GD_PUSH=25,
    R_386_TLS_GD_CALL=26,
    R_386_TLS_GD_POP=27,
    R_386_TLS_LDM_32=28,
    R_386_TLS_LDM_PUSH=29,
    R_386_TLS_LDM_CALL=30,
    R_386_TLS_LDM_POP=31,
    R_386_TLS_LDO_32=32,
    R_386_TLS_IE_32=33,
    R_386_TLS_LE_32=34,
    R_386_TLS_DTPMOD32=35,
    R_386_TLS_DTPOFF32=36,
    R_386_TLS_TPOFF32=37,
    R_386_TLS_GOTDESC=39,
    R_386_TLS_DESC_CALL=40,
    R_386_TLS_DESC=41,
    R_386_IRELATIVE=42,
    R_386_USED_BY_INTEL_200=200,
    R_386_GNU_VTINHERIT=250,
    R_386_GNU_VTENTRY=251,
    _default_=Pass,
)

ENUM_RELOC_TYPE_x64 = dict(
    R_X86_64_NONE=0,
    R_X86_64_64=1,
    R_X86_64_PC32=2,
    R_X86_64_GOT32=3,
    R_X86_64_PLT32=4,
    R_X86_64_COPY=5,
    R_X86_64_GLOB_DAT=6,
    R_X86_64_JUMP_SLOT=7,
    R_X86_64_RELATIVE=8,
    R_X86_64_GOTPCREL=9,
    R_X86_64_32=10,
    R_X86_64_32S=11,
    R_X86_64_16=12,
    R_X86_64_PC16=13,
    R_X86_64_8=14,
    R_X86_64_PC8=15,
    R_X86_64_DTPMOD64=16,
    R_X86_64_DTPOFF64=17,
    R_X86_64_TPOFF64=18,
    R_X86_64_TLSGD=19,
    R_X86_64_TLSLD=20,
    R_X86_64_DTPOFF32=21,
    R_X86_64_GOTTPOFF=22,
    R_X86_64_TPOFF32=23,
    R_X86_64_PC64=24,
    R_X86_64_GOTOFF64=25,
    R_X86_64_GOTPC32=26,
    R_X86_64_GOT64=27,
    R_X86_64_GOTPCREL64=28,
    R_X86_64_GOTPC64=29,
    R_X86_64_GOTPLT64=30,
    R_X86_64_PLTOFF64=31,
    R_X86_64_GOTPC32_TLSDESC=34,
    R_X86_64_TLSDESC_CALL=35,
    R_X86_64_TLSDESC=36,
    R_X86_64_IRELATIVE=37,
    R_X86_64_REX_GOTPCRELX=42,
    R_X86_64_GNU_VTINHERIT=250,
    R_X86_64_GNU_VTENTRY=251,
    _default_=Pass,
)

# Sunw Syminfo Bound To special values
ENUM_SUNW_SYMINFO_BOUNDTO = dict(
    SYMINFO_BT_SELF=0xffff,
    SYMINFO_BT_PARENT=0xfffe,
    SYMINFO_BT_NONE=0xfffd,
    SYMINFO_BT_EXTERN=0xfffc,
    _default_=Pass,
)

# Versym section, version dependency index
ENUM_VERSYM = dict(
    VER_NDX_LOCAL=0,
    VER_NDX_GLOBAL=1,
    VER_NDX_LORESERVE=0xff00,
    VER_NDX_ELIMINATE=0xff01,
    _default_=Pass,
)

# Sunw Syminfo Bound To special values
ENUM_SUNW_SYMINFO_BOUNDTO = dict(
    SYMINFO_BT_SELF=0xffff,
    SYMINFO_BT_PARENT=0xfffe,
    SYMINFO_BT_NONE=0xfffd,
    SYMINFO_BT_EXTERN=0xfffc,
    _default_=Pass,
)

# PT_NOTE section types for all ELF types except ET_CORE
ENUM_NOTE_N_TYPE = dict(
    NT_GNU_ABI_TAG=1,
    NT_GNU_HWCAP=2,
    NT_GNU_BUILD_ID=3,
    NT_GNU_GOLD_VERSION=4,
    NT_GNU_PROPERTY_TYPE_0=5,
    _default_=Pass,
)

# PT_NOTE section types for ET_CORE
ENUM_CORE_NOTE_N_TYPE = dict(
    NT_PRSTATUS=1,
    NT_FPREGSET=2,
    NT_PRPSINFO=3,
    NT_TASKSTRUCT=4,
    NT_AUXV=6,
    NT_SIGINFO=0x53494749,
    NT_FILE=0x46494c45,
    _default_=Pass,
)

# Values in GNU .note.ABI-tag notes (n_type=='NT_GNU_ABI_TAG')
ENUM_NOTE_ABI_TAG_OS = dict(
    ELF_NOTE_OS_LINUX=0,
    ELF_NOTE_OS_GNU=1,
    ELF_NOTE_OS_SOLARIS2=2,
    ELF_NOTE_OS_FREEBSD=3,
    ELF_NOTE_OS_NETBSD=4,
    ELF_NOTE_OS_SYLLABLE=5,
    _default_=Pass,
)

# Values in GNU .note.gnu.property notes (n_type=='NT_GNU_PROPERTY_TYPE_0')
ENUM_NOTE_GNU_PROPERTY_TYPE = dict(
    GNU_PROPERTY_STACK_SIZE=1,
    GNU_PROPERTY_NO_COPY_ON_PROTECTED=2,
    GNU_PROPERTY_X86_FEATURE_1_AND=0xc0000002,
    GNU_PROPERTY_X86_ISA_1_NEEDED=0xc0008002,
    GNU_PROPERTY_X86_FEATURE_2_USED=0xc0010001,
    GNU_PROPERTY_X86_ISA_1_USED=0xc0010002,
    _default_=Pass,
)

ENUM_GNU_PROPERTY_X86_FEATURE_1_FLAGS = dict(
    GNU_PROPERTY_X86_FEATURE_1_IBT=1,
    GNU_PROPERTY_X86_FEATURE_1_SHSTK=2,
    GNU_PROPERTY_X86_FEATURE_1_LAM_U48=4,
    GNU_PROPERTY_X86_FEATURE_1_LAM_U57=8,
    _default_=Pass
)

ENUM_RELOC_TYPE_ARM = dict(
    R_ARM_NONE=0,
    R_ARM_PC24=1,
    R_ARM_ABS32=2,
    R_ARM_REL32=3,
    R_ARM_LDR_PC_G0=4,
    R_ARM_ABS16=5,
    R_ARM_ABS12=6,
    R_ARM_THM_ABS5=7,
    R_ARM_ABS8=8,
    R_ARM_SBREL32=9,
    R_ARM_THM_CALL=10,
    R_ARM_THM_PC8=11,
    R_ARM_BREL_ADJ=12,
    R_ARM_SWI24=13,
    R_ARM_THM_SWI8=14,
    R_ARM_XPC25=15,
    R_ARM_THM_XPC22=16,
    R_ARM_TLS_DTPMOD32=17,
    R_ARM_TLS_DTPOFF32=18,
    R_ARM_TLS_TPOFF32=19,
    R_ARM_COPY=20,
    R_ARM_GLOB_DAT=21,
    R_ARM_JUMP_SLOT=22,
    R_ARM_RELATIVE=23,
    R_ARM_GOTOFF32=24,
    R_ARM_BASE_PREL=25,
    R_ARM_GOT_BREL=26,
    R_ARM_PLT32=27,
    R_ARM_CALL=28,
    R_ARM_JUMP24=29,
    R_ARM_THM_JUMP24=30,
    R_ARM_BASE_ABS=31,
    R_ARM_ALU_PCREL_7_0=32,
    R_ARM_ALU_PCREL_15_8=33,
    R_ARM_ALU_PCREL_23_15=34,
    R_ARM_LDR_SBREL_11_0_NC=35,
    R_ARM_ALU_SBREL_19_12_NC=36,
    R_ARM_ALU_SBREL_27_20_CK=37,
    R_ARM_TARGET1=38,
    R_ARM_SBREL31=39,
    R_ARM_V4BX=40,
    R_ARM_TARGET2=41,
    R_ARM_PREL31=42,
    R_ARM_MOVW_ABS_NC=43,
    R_ARM_MOVT_ABS=44,
    R_ARM_MOVW_PREL_NC=45,
    R_ARM_MOVT_PREL=46,
    R_ARM_THM_MOVW_ABS_NC=47,
    R_ARM_THM_MOVT_ABS=48,
    R_ARM_THM_MOVW_PREL_NC=49,
    R_ARM_THM_MOVT_PREL=50,
    R_ARM_THM_JUMP19=51,
    R_ARM_THM_JUMP6=52,
    R_ARM_THM_ALU_PREL_11_0=53,
    R_ARM_THM_PC12=54,
    R_ARM_ABS32_NOI=55,
    R_ARM_REL32_NOI=56,
    R_ARM_ALU_PC_G0_NC=57,
    R_ARM_ALU_PC_G0=58,
    R_ARM_ALU_PC_G1_NC=59,
    R_ARM_ALU_PC_G1=60,
    R_ARM_ALU_PC_G2=61,
    R_ARM_LDR_PC_G1=62,
    R_ARM_LDR_PC_G2=63,
    R_ARM_LDRS_PC_G0=64,
    R_ARM_LDRS_PC_G1=65,
    R_ARM_LDRS_PC_G2=66,
    R_ARM_LDC_PC_G0=67,
    R_ARM_LDC_PC_G1=68,
    R_ARM_LDC_PC_G2=69,
    R_ARM_ALU_SB_G0_NC=70,
    R_ARM_ALU_SB_G0=71,
    R_ARM_ALU_SB_G1_NC=72,
    R_ARM_ALU_SB_G1=73,
    R_ARM_ALU_SB_G2=74,
    R_ARM_LDR_SB_G0=75,
    R_ARM_LDR_SB_G1=76,
    R_ARM_LDR_SB_G2=77,
    R_ARM_LDRS_SB_G0=78,
    R_ARM_LDRS_SB_G1=79,
    R_ARM_LDRS_SB_G2=80,
    R_ARM_LDC_SB_G0=81,
    R_ARM_LDC_SB_G1=82,
    R_ARM_LDC_SB_G2=83,
    R_ARM_MOVW_BREL_NC=84,
    R_ARM_MOVT_BREL=85,
    R_ARM_MOVW_BREL=86,
    R_ARM_THM_MOVW_BREL_NC=87,
    R_ARM_THM_MOVT_BREL=88,
    R_ARM_THM_MOVW_BREL=89,
    R_ARM_PLT32_ABS=94,
    R_ARM_GOT_ABS=95,
    R_ARM_GOT_PREL=96,
    R_ARM_GOT_BREL12=97,
    R_ARM_GOTOFF12=98,
    R_ARM_GOTRELAX=99,
    R_ARM_GNU_VTENTRY=100,
    R_ARM_GNU_VTINHERIT=101,
    R_ARM_THM_JUMP11=102,
    R_ARM_THM_JUMP8=103,
    R_ARM_TLS_GD32=104,
    R_ARM_TLS_LDM32=105,
    R_ARM_TLS_LDO32=106,
    R_ARM_TLS_IE32=107,
    R_ARM_TLS_LE32=108,
    R_ARM_TLS_LDO12=109,
    R_ARM_TLS_LE12=110,
    R_ARM_TLS_IE12GP=111,
    R_ARM_PRIVATE_0=112,
    R_ARM_PRIVATE_1=113,
    R_ARM_PRIVATE_2=114,
    R_ARM_PRIVATE_3=115,
    R_ARM_PRIVATE_4=116,
    R_ARM_PRIVATE_5=117,
    R_ARM_PRIVATE_6=118,
    R_ARM_PRIVATE_7=119,
    R_ARM_PRIVATE_8=120,
    R_ARM_PRIVATE_9=121,
    R_ARM_PRIVATE_10=122,
    R_ARM_PRIVATE_11=123,
    R_ARM_PRIVATE_12=124,
    R_ARM_PRIVATE_13=125,
    R_ARM_PRIVATE_14=126,
    R_ARM_PRIVATE_15=127,
    R_ARM_ME_TOO=128,
    R_ARM_THM_TLS_DESCSEQ16=129,
    R_ARM_THM_TLS_DESCSEQ32=130,
    R_ARM_THM_GOT_BREL12=131,
    R_ARM_IRELATIVE=140,
)

ENUM_RELOC_TYPE_AARCH64 = dict(
    R_AARCH64_NONE=256,
    R_AARCH64_ABS64=257,
    R_AARCH64_ABS32=258,
    R_AARCH64_ABS16=259,
    R_AARCH64_PREL64=260,
    R_AARCH64_PREL32=261,
    R_AARCH64_PREL16=262,
    R_AARCH64_MOVW_UABS_G0=263,
    R_AARCH64_MOVW_UABS_G0_NC=264,
    R_AARCH64_MOVW_UABS_G1=265,
    R_AARCH64_MOVW_UABS_G1_NC=266,
    R_AARCH64_MOVW_UABS_G2=267,
    R_AARCH64_MOVW_UABS_G2_NC=268,
    R_AARCH64_MOVW_UABS_G3=269,
    R_AARCH64_MOVW_SABS_G0=270,
    R_AARCH64_MOVW_SABS_G1=271,
    R_AARCH64_MOVW_SABS_G2=272,
    R_AARCH64_LD_PREL_LO19=273,
    R_AARCH64_ADR_PREL_LO21=274,
    R_AARCH64_ADR_PREL_PG_HI21=275,
    R_AARCH64_ADR_PREL_PG_HI21_NC=276,
    R_AARCH64_ADD_ABS_LO12_NC=277,
    R_AARCH64_LDST8_ABS_LO12_NC=278,
    R_AARCH64_TSTBR14=279,
    R_AARCH64_CONDBR19=280,
    R_AARCH64_JUMP26=282,
    R_AARCH64_CALL26=283,
    R_AARCH64_LDST16_ABS_LO12_NC=284,
    R_AARCH64_LDST32_ABS_LO12_NC=285,
    R_AARCH64_LDST64_ABS_LO12_NC=286,
    R_AARCH64_MOVW_PREL_G0=287,
    R_AARCH64_MOVW_PREL_G0_NC=288,
    R_AARCH64_MOVW_PREL_G1=289,
    R_AARCH64_MOVW_PREL_G1_NC=290,
    R_AARCH64_MOVW_PREL_G2=291,
    R_AARCH64_MOVW_PREL_G2_NC=292,
    R_AARCH64_MOVW_PREL_G3=293,
    R_AARCH64_MOVW_GOTOFF_G0=300,
    R_AARCH64_MOVW_GOTOFF_G0_NC=301,
    R_AARCH64_MOVW_GOTOFF_G1=302,
    R_AARCH64_MOVW_GOTOFF_G1_NC=303,
    R_AARCH64_MOVW_GOTOFF_G2=304,
    R_AARCH64_MOVW_GOTOFF_G2_NC=305,
    R_AARCH64_MOVW_GOTOFF_G3=306,
    R_AARCH64_GOTREL64=307,
    R_AARCH64_GOTREL32=308,
    R_AARCH64_GOT_LD_PREL19=309,
    R_AARCH64_LD64_GOTOFF_LO15=310,
    R_AARCH64_ADR_GOT_PAGE=311,
    R_AARCH64_LD64_GOT_LO12_NC=312,
    R_AARCH64_TLSGD_ADR_PREL21=512,
    R_AARCH64_TLSGD_ADR_PAGE21=513,
    R_AARCH64_TLSGD_ADD_LO12_NC=514,
    R_AARCH64_TLSGD_MOVW_G1=515,
    R_AARCH64_TLSGD_MOVW_G0_NC=516,
    R_AARCH64_TLSLD_ADR_PREL21=517,
    R_AARCH64_TLSLD_ADR_PAGE21=518,
    R_AARCH64_TLSLD_ADD_LO12_NC=519,
    R_AARCH64_TLSLD_MOVW_G1=520,
    R_AARCH64_TLSLD_MOVW_G0_NC=521,
    R_AARCH64_TLSLD_LD_PREL19=522,
    R_AARCH64_TLSLD_MOVW_DTPREL_G2=523,
    R_AARCH64_TLSLD_MOVW_DTPREL_G1=524,
    R_AARCH64_TLSLD_MOVW_DTPREL_G1_NC=525,
    R_AARCH64_TLSLD_MOVW_DTPREL_G0=526,
    R_AARCH64_TLSLD_MOVW_DTPREL_G0_NC=527,
    R_AARCH64_TLSLD_ADD_DTPREL_HI12=528,
    R_AARCH64_TLSLD_ADD_DTPREL_LO12=529,
    R_AARCH64_TLSLD_ADD_DTPREL_LO12_NC=530,
    R_AARCH64_TLSLD_LDST8_DTPREL_LO12=531,
    R_AARCH64_TLSLD_LDST8_DTPREL_LO12_NC=532,
    R_AARCH64_TLSLD_LDST16_DTPREL_LO12=533,
    R_AARCH64_TLSLD_LDST16_DTPREL_LO12_NC=534,
    R_AARCH64_TLSLD_LDST32_DTPREL_LO12=535,
    R_AARCH64_TLSLD_LDST32_DTPREL_LO12_NC=536,
    R_AARCH64_TLSLD_LDST64_DTPREL_LO12=537,
    R_AARCH64_TLSLD_LDST64_DTPREL_LO12_NC=538,
    R_AARCH64_TLSIE_MOVW_GOTTPREL_G1=539,
    R_AARCH64_TLSIE_MOVW_GOTTPREL_G0_NC=540,
    R_AARCH64_TLSIE_ADR_GOTTPREL_PAGE21=541,
    R_AARCH64_TLSIE_LD64_GOTTPREL_LO12_NC=542,
    R_AARCH64_TLSIE_LD_GOTTPREL_PREL19=543,
    R_AARCH64_TLSLE_MOVW_TPREL_G2=544,
    R_AARCH64_TLSLE_MOVW_TPREL_G1=545,
    R_AARCH64_TLSLE_MOVW_TPREL_G1_NC=546,
    R_AARCH64_TLSLE_MOVW_TPREL_G0=547,
    R_AARCH64_TLSLE_MOVW_TPREL_G0_NC=548,
    R_AARCH64_TLSLE_ADD_TPREL_HI12=549,
    R_AARCH64_TLSLE_ADD_TPREL_LO12=550,
    R_AARCH64_TLSLE_ADD_TPREL_LO12_NC=551,
    R_AARCH64_TLSLE_LDST8_TPREL_LO12=552,
    R_AARCH64_TLSLE_LDST8_TPREL_LO12_NC=553,
    R_AARCH64_TLSLE_LDST16_TPREL_LO12=554,
    R_AARCH64_TLSLE_LDST16_TPREL_LO12_NC=555,
    R_AARCH64_TLSLE_LDST32_TPREL_LO12=556,
    R_AARCH64_TLSLE_LDST32_TPREL_LO12_NC=557,
    R_AARCH64_TLSLE_LDST64_TPREL_LO12=558,
    R_AARCH64_TLSLE_LDST64_TPREL_LO12_NC=559,
    R_AARCH64_COPY=1024,
    R_AARCH64_GLOB_DAT=1025,
    R_AARCH64_JUMP_SLOT=1026,
    R_AARCH64_RELATIVE=1027,
    R_AARCH64_TLS_DTPREL64=1028,
    R_AARCH64_TLS_DTPMOD64=1029,
    R_AARCH64_TLS_TPREL64=1030,
    R_AARCH64_TLS_DTPREL32=1031,
    R_AARCH64_TLS_DTPMOD32=1032,
    R_AARCH64_TLS_TPREL32=1033,
)

ENUM_ATTR_TAG_ARM = dict(
    TAG_FILE=1,
    TAG_SECTION=2,
    TAG_SYMBOL=3,
    TAG_CPU_RAW_NAME=4,
    TAG_CPU_NAME=5,
    TAG_CPU_ARCH=6,
    TAG_CPU_ARCH_PROFILE=7,
    TAG_ARM_ISA_USE=8,
    TAG_THUMB_ISA_USE=9,
    TAG_FP_ARCH=10,
    TAG_WMMX_ARCH=11,
    TAG_ADVANCED_SIMD_ARCH=12,
    TAG_PCS_CONFIG=13,
    TAG_ABI_PCS_R9_USE=14,
    TAG_ABI_PCS_RW_DATA=15,
    TAG_ABI_PCS_RO_DATA=16,
    TAG_ABI_PCS_GOT_USE=17,
    TAG_ABI_PCS_WCHAR_T=18,
    TAG_ABI_FP_ROUNDING=19,
    TAG_ABI_FP_DENORMAL=20,
    TAG_ABI_FP_EXCEPTIONS=21,
    TAG_ABI_FP_USER_EXCEPTIONS=22,
    TAG_ABI_FP_NUMBER_MODEL=23,
    TAG_ABI_ALIGN_NEEDED=24,
    TAG_ABI_ALIGN_PRESERVED=25,
    TAG_ABI_ENUM_SIZE=26,
    TAG_ABI_HARDFP_USE=27,
    TAG_ABI_VFP_ARGS=28,
    TAG_ABI_WMMX_ARGS=29,
    TAG_ABI_OPTIMIZATION_GOALS=30,
    TAG_ABI_FP_OPTIMIZATION_GOALS=31,
    TAG_COMPATIBILITY=32,
    TAG_CPU_UNALIGNED_ACCESS=34,
    TAG_FP_HP_EXTENSION=36,
    TAG_ABI_FP_16BIT_FORMAT=38,
    TAG_MPEXTENSION_USE=42,
    TAG_DIV_USE=44,
    TAG_NODEFAULTS=64,
    TAG_ALSO_COMPATIBLE_WITH=65,
    TAG_T2EE_USE=66,
    TAG_CONFORMANCE=67,
    TAG_VIRTUALIZATION_USE=68,
    TAG_MPEXTENSION_USE_OLD=70,
)

# https://openpowerfoundation.org/wp-content/uploads/2016/03/ABI64BitOpenPOWERv1.1_16July2015_pub4.pdf
# See 3.5.3 Relocation Types Table.
ENUM_RELOC_TYPE_PPC64 = dict(
    R_PPC64_NONE=0,
    R_PPC64_ADDR32=1,
    R_PPC64_ADDR24=2,
    R_PPC64_ADDR16=3,
    R_PPC64_ADDR16_LO=4,
    R_PPC64_ADDR16_HI=5,
    R_PPC64_ADDR16_HA=6,
    R_PPC64_ADDR14=7,
    R_PPC64_ADDR14_BRTAKEN=8,
    R_PPC64_ADDR14_BRNTAKEN=9,
    R_PPC64_REL24=10,
    R_PPC64_REL14=11,
    R_PPC64_REL14_BRTAKEN=12,
    R_PPC64_REL14_BRNTAKEN=13,
    R_PPC64_GOT16=14,
    R_PPC64_GOT16_LO=15,
    R_PPC64_GOT16_HI=16,
    R_PPC64_GOT16_HA=17,
    R_PPC64_COPY=19,
    R_PPC64_GLOB_DAT=20,
    R_PPC64_JMP_SLOT=21,
    R_PPC64_RELATIVE=22,
    R_PPC64_UADDR32=24,
    R_PPC64_UADDR16=25,
    R_PPC64_REL32=26,
    R_PPC64_PLT32=27,
    R_PPC64_PLTREL32=28,
    R_PPC64_PLT16_LO=29,
    R_PPC64_PLT16_HI=30,
    R_PPC64_PLT16_HA=31,
    R_PPC64_SECTOFF=33,
    R_PPC64_SECTOFF_LO=34,
    R_PPC64_SECTOFF_HI=35,
    R_PPC64_SECTOFF_HA=36,
    R_PPC64_ADDR30=37,
    R_PPC64_ADDR64=38,
    R_PPC64_ADDR16_HIGHER=39,
    R_PPC64_ADDR16_HIGHERA=40,
    R_PPC64_ADDR16_HIGHEST=41,
    R_PPC64_ADDR16_HIGHESTA=42,
    R_PPC64_UADDR64=43,
    R_PPC64_REL64=44,
    R_PPC64_PLT64=45,
    R_PPC64_PLTREL64=46,
    R_PPC64_TOC16=47,
    R_PPC64_TOC16_LO=48,
    R_PPC64_TOC16_HI=49,
    R_PPC64_TOC16_HA=50,
    R_PPC64_TOC=51,
    R_PPC64_PLTGOT16=52,
    R_PPC64_PLTGOT16_LO=53,
    R_PPC64_PLTGOT16_HI=54,
    R_PPC64_PLTGOT16_HA=55,
    R_PPC64_ADDR16_DS=56,
    R_PPC64_ADDR16_LO_DS=57,
    R_PPC64_GOT16_DS=58,
    R_PPC64_GOT16_LO_DS=59,
    R_PPC64_PLT16_LO_DS=60,
    R_PPC64_SECTOFF_DS=61,
    R_PPC64_SECTOFF_LO_DS=62,
    R_PPC64_TOC16_DS=63,
    R_PPC64_TOC16_LO_DS=64,
    R_PPC64_PLTGOT16_DS=65,
    R_PPC64_PLTGOT16_LO_DS=66,
    R_PPC64_TLS=67,
    R_PPC64_DTPMOD64=68,
    R_PPC64_TPREL16=69,
    R_PPC64_TPREL16_LO=70,
    R_PPC64_TPREL16_HI=71,
    R_PPC64_TPREL16_HA=72,
    R_PPC64_TPREL64=73,
    R_PPC64_DTPREL16=74,
    R_PPC64_DTPREL16_LO=75,
    R_PPC64_DTPREL16_HI=76,
    R_PPC64_DTPREL16_HA=77,
    R_PPC64_DTPREL64=78,
    R_PPC64_GOT_TLSGD16=79,
    R_PPC64_GOT_TLSGD16_LO=80,
    R_PPC64_GOT_TLSGD16_HI=81,
    R_PPC64_GOT_TLSGD16_HA=82,
    R_PPC64_GOT_TLSLD16=83,
    R_PPC64_GOT_TLSLD16_LO=84,
    R_PPC64_GOT_TLSLD16_HI=85,
    R_PPC64_GOT_TLSLD16_HA=86,
    R_PPC64_GOT_TPREL16_DS=87,
    R_PPC64_GOT_TPREL16_LO_DS=88,
    R_PPC64_GOT_TPREL16_HI=89,
    R_PPC64_GOT_TPREL16_HA=90,
    R_PPC64_GOT_DTPREL16_DS=91,
    R_PPC64_GOT_DTPREL16_LO_DS=92,
    R_PPC64_GOT_DTPREL16_HI=93,
    R_PPC64_GOT_DTPREL16_HA=94,
    R_PPC64_TPREL16_DS=95,
    R_PPC64_TPREL16_LO_DS=96,
    R_PPC64_TPREL16_HIGHER=97,
    R_PPC64_TPREL16_HIGHERA=98,
    R_PPC64_TPREL16_HIGHEST=99,
    R_PPC64_TPREL16_HIGHESTA=100,
    R_PPC64_DTPREL16_DS=101,
    R_PPC64_DTPREL16_LO_DS=102,
    R_PPC64_DTPREL16_HIGHER=103,
    R_PPC64_DTPREL16_HIGHERA=104,
    R_PPC64_DTPREL16_HIGHEST=105,
    R_PPC64_DTPREL16_HIGHESTA=106,
    R_PPC64_TLSGD=107,
    R_PPC64_TLSLD=108,
    R_PPC64_TOCSAVE=109,
    R_PPC64_ADDR16_HIGH=110,
    R_PPC64_ADDR16_HIGHA=111,
    R_PPC64_TPREL16_HIGH=112,
    R_PPC64_TPREL16_HIGHA=113,
    R_PPC64_DTPREL16_HIGH=114,
    R_PPC64_DTPREL16_HIGHA=115,
    R_PPC64_REL24_NOTOC=116,
    R_PPC64_ADDR64_LOCAL=117,
    R_PPC64_IRELATIVE=248,
    R_PPC64_REL16=249,
    R_PPC64_REL16_LO=250,
    R_PPC64_REL16_HI=251,
    R_PPC64_REL16_HA=252,
    R_PPC64_GNU_VTINHERIT=253,
    R_PPC64_GNU_VTENTRY=254,
)
