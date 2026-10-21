#ifndef PROC
#define PROC(ENUM, NAME, DEFAULT_MARCH, FAST_UNALIGNED_ACCESS)
#endif

PROC(GENERIC_RV32, {"generic-rv32"}, {"rv32i2p1"}, 0)
PROC(GENERIC_RV64, {"generic-rv64"}, {"rv64i2p1"}, 0)
PROC(ROCKET_RV32, {"rocket-rv32"}, {"rv32i2p1_zicsr2p0_zifencei2p0"}, 0)
PROC(ROCKET_RV64, {"rocket-rv64"}, {"rv64i2p1_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_E20, {"sifive-e20"}, {"rv32i2p1_m2p0_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_E21, {"sifive-e21"}, {"rv32i2p1_m2p0_a2p1_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_E24, {"sifive-e24"}, {"rv32i2p1_m2p0_a2p1_f2p2_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_E31, {"sifive-e31"}, {"rv32i2p1_m2p0_a2p1_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_E34, {"sifive-e34"}, {"rv32i2p1_m2p0_a2p1_f2p2_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_E76, {"sifive-e76"}, {"rv32i2p1_m2p0_a2p1_f2p2_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_P450, {"sifive-p450"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_zic64b1p0_zicbom1p0_zicbop1p0_zicboz1p0_ziccamoa1p0_ziccif1p0_zicclsm1p0_ziccrse1p0_zicsr2p0_zifencei2p0_zihintntl1p0_zihintpause2p0_zihpm2p0_za64rs1p0_zfhmin1p0_zba1p0_zbb1p0_zbs1p0"}, 1)
PROC(SIFIVE_P670, {"sifive-p670"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_v1p0_zic64b1p0_zicbom1p0_zicbop1p0_zicboz1p0_ziccamoa1p0_ziccif1p0_zicclsm1p0_ziccrse1p0_zicsr2p0_zifencei2p0_zihintntl1p0_zihintpause2p0_zihpm2p0_za64rs1p0_zfhmin1p0_zba1p0_zbb1p0_zbs1p0_zvbb1p0_zvbc1p0_zve32f1p0_zve32x1p0_zve64d1p0_zve64f1p0_zve64x1p0_zvkb1p0_zvkg1p0_zvkn1p0_zvknc1p0_zvkned1p0_zvkng1p0_zvknhb1p0_zvks1p0_zvksc1p0_zvksed1p0_zvksg1p0_zvksh1p0_zvkt1p0_zvl128b1p0_zvl32b1p0_zvl64b1p0"}, 1)
PROC(SIFIVE_S21, {"sifive-s21"}, {"rv64i2p1_m2p0_a2p1_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_S51, {"sifive-s51"}, {"rv64i2p1_m2p0_a2p1_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_S54, {"sifive-s54"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_S76, {"sifive-s76"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_zicsr2p0_zifencei2p0_zihintpause2p0"}, 0)
PROC(SIFIVE_U54, {"sifive-u54"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_U74, {"sifive-u74"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SIFIVE_X280, {"sifive-x280"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_v1p0_zicsr2p0_zifencei2p0_zfh1p0_zfhmin1p0_zba1p0_zbb1p0_zve32f1p0_zve32x1p0_zve64d1p0_zve64f1p0_zve64x1p0_zvfh1p0_zvfhmin1p0_zvl128b1p0_zvl256b1p0_zvl32b1p0_zvl512b1p0_zvl64b1p0"}, 0)
PROC(SYNTACORE_SCR1_BASE, {"syntacore-scr1-base"}, {"rv32i2p1_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(SYNTACORE_SCR1_MAX, {"syntacore-scr1-max"}, {"rv32i2p1_m2p0_c2p0_zicsr2p0_zifencei2p0"}, 0)
PROC(VENTANA_VEYRON_V1, {"veyron-v1"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_zicbom1p0_zicbop1p0_zicboz1p0_zicntr2p0_zicsr2p0_zifencei2p0_zihintpause2p0_zihpm2p0_zba1p0_zbb1p0_zbc1p0_zbs1p0_xventanacondops1p0"}, 0)
PROC(XIANGSHAN_NANHU, {"xiangshan-nanhu"}, {"rv64i2p1_m2p0_a2p1_f2p2_d2p2_c2p0_zicbom1p0_zicboz1p0_zicsr2p0_zifencei2p0_zba1p0_zbb1p0_zbc1p0_zbkb1p0_zbkc1p0_zbkx1p0_zbs1p0_zkn1p0_zknd1p0_zkne1p0_zknh1p0_zks1p0_zksed1p0_zksh1p0_svinval1p0"}, 0)

#undef PROC

#ifndef TUNE_PROC
#define TUNE_PROC(ENUM, NAME)
#endif

TUNE_PROC(GENERIC, "generic")
TUNE_PROC(ROCKET, "rocket")
TUNE_PROC(SIFIVE_7, "sifive-7-series")

#undef TUNE_PROC
