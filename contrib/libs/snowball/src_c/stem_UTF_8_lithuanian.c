/* Generated from lithuanian.sbl by Snowball 3.0.1 - https://snowballstem.org/ */

#include "../runtime/header.h"

static int r_fix_conflicts(struct SN_env * z);
static int r_fix_gd(struct SN_env * z);
static int r_fix_chdz(struct SN_env * z);
static int r_step1(struct SN_env * z);
static int r_step2(struct SN_env * z);
#ifdef __cplusplus
extern "C" {
#endif
extern int lithuanian_UTF_8_stem(struct SN_env * z);
#ifdef __cplusplus
}
#endif
#ifdef __cplusplus
extern "C" {
#endif


extern struct SN_env * lithuanian_UTF_8_create_env(void);
extern void lithuanian_UTF_8_close_env(struct SN_env * z);


#ifdef __cplusplus
}
#endif
static const symbol s_0_0[1] = { 'a' };
static const symbol s_0_1[2] = { 'i', 'a' };
static const symbol s_0_2[4] = { 'e', 'r', 'i', 'a' };
static const symbol s_0_3[4] = { 'o', 's', 'n', 'a' };
static const symbol s_0_4[5] = { 'i', 'o', 's', 'n', 'a' };
static const symbol s_0_5[5] = { 'u', 'o', 's', 'n', 'a' };
static const symbol s_0_6[6] = { 'i', 'u', 'o', 's', 'n', 'a' };
static const symbol s_0_7[4] = { 'y', 's', 'n', 'a' };
static const symbol s_0_8[5] = { 0xC4, 0x97, 's', 'n', 'a' };
static const symbol s_0_9[1] = { 'e' };
static const symbol s_0_10[2] = { 'i', 'e' };
static const symbol s_0_11[4] = { 'e', 'n', 'i', 'e' };
static const symbol s_0_12[4] = { 'e', 'r', 'i', 'e' };
static const symbol s_0_13[3] = { 'o', 'j', 'e' };
static const symbol s_0_14[4] = { 'i', 'o', 'j', 'e' };
static const symbol s_0_15[3] = { 'u', 'j', 'e' };
static const symbol s_0_16[4] = { 'i', 'u', 'j', 'e' };
static const symbol s_0_17[3] = { 'y', 'j', 'e' };
static const symbol s_0_18[5] = { 'e', 'n', 'y', 'j', 'e' };
static const symbol s_0_19[5] = { 'e', 'r', 'y', 'j', 'e' };
static const symbol s_0_20[4] = { 0xC4, 0x97, 'j', 'e' };
static const symbol s_0_21[3] = { 'a', 'm', 'e' };
static const symbol s_0_22[4] = { 'i', 'a', 'm', 'e' };
static const symbol s_0_23[4] = { 's', 'i', 'm', 'e' };
static const symbol s_0_24[3] = { 'o', 'm', 'e' };
static const symbol s_0_25[4] = { 0xC4, 0x97, 'm', 'e' };
static const symbol s_0_26[7] = { 't', 'u', 'm', 0xC4, 0x97, 'm', 'e' };
static const symbol s_0_27[3] = { 'o', 's', 'e' };
static const symbol s_0_28[4] = { 'i', 'o', 's', 'e' };
static const symbol s_0_29[4] = { 'u', 'o', 's', 'e' };
static const symbol s_0_30[5] = { 'i', 'u', 'o', 's', 'e' };
static const symbol s_0_31[3] = { 'y', 's', 'e' };
static const symbol s_0_32[5] = { 'e', 'n', 'y', 's', 'e' };
static const symbol s_0_33[5] = { 'e', 'r', 'y', 's', 'e' };
static const symbol s_0_34[4] = { 0xC4, 0x97, 's', 'e' };
static const symbol s_0_35[3] = { 'a', 't', 'e' };
static const symbol s_0_36[4] = { 'i', 'a', 't', 'e' };
static const symbol s_0_37[3] = { 'i', 't', 'e' };
static const symbol s_0_38[4] = { 'k', 'i', 't', 'e' };
static const symbol s_0_39[4] = { 's', 'i', 't', 'e' };
static const symbol s_0_40[3] = { 'o', 't', 'e' };
static const symbol s_0_41[4] = { 't', 'u', 't', 'e' };
static const symbol s_0_42[4] = { 0xC4, 0x97, 't', 'e' };
static const symbol s_0_43[7] = { 't', 'u', 'm', 0xC4, 0x97, 't', 'e' };
static const symbol s_0_44[1] = { 'i' };
static const symbol s_0_45[2] = { 'a', 'i' };
static const symbol s_0_46[3] = { 'i', 'a', 'i' };
static const symbol s_0_47[5] = { 'e', 'r', 'i', 'a', 'i' };
static const symbol s_0_48[2] = { 'e', 'i' };
static const symbol s_0_49[5] = { 't', 'u', 'm', 'e', 'i' };
static const symbol s_0_50[2] = { 'k', 'i' };
static const symbol s_0_51[3] = { 'i', 'm', 'i' };
static const symbol s_0_52[5] = { 'e', 'r', 'i', 'm', 'i' };
static const symbol s_0_53[3] = { 'u', 'm', 'i' };
static const symbol s_0_54[4] = { 'i', 'u', 'm', 'i' };
static const symbol s_0_55[2] = { 's', 'i' };
static const symbol s_0_56[3] = { 'a', 's', 'i' };
static const symbol s_0_57[4] = { 'i', 'a', 's', 'i' };
static const symbol s_0_58[3] = { 'e', 's', 'i' };
static const symbol s_0_59[4] = { 'i', 'e', 's', 'i' };
static const symbol s_0_60[5] = { 's', 'i', 'e', 's', 'i' };
static const symbol s_0_61[3] = { 'i', 's', 'i' };
static const symbol s_0_62[4] = { 'a', 'i', 's', 'i' };
static const symbol s_0_63[4] = { 'e', 'i', 's', 'i' };
static const symbol s_0_64[7] = { 't', 'u', 'm', 'e', 'i', 's', 'i' };
static const symbol s_0_65[4] = { 'u', 'i', 's', 'i' };
static const symbol s_0_66[3] = { 'o', 's', 'i' };
static const symbol s_0_67[6] = { 0xC4, 0x97, 'j', 'o', 's', 'i' };
static const symbol s_0_68[4] = { 'u', 'o', 's', 'i' };
static const symbol s_0_69[5] = { 'i', 'u', 'o', 's', 'i' };
static const symbol s_0_70[6] = { 's', 'i', 'u', 'o', 's', 'i' };
static const symbol s_0_71[3] = { 'u', 's', 'i' };
static const symbol s_0_72[4] = { 'a', 'u', 's', 'i' };
static const symbol s_0_73[7] = { 0xC4, 0x8D, 'i', 'a', 'u', 's', 'i' };
static const symbol s_0_74[4] = { 0xC4, 0x85, 's', 'i' };
static const symbol s_0_75[4] = { 0xC4, 0x97, 's', 'i' };
static const symbol s_0_76[4] = { 0xC5, 0xB3, 's', 'i' };
static const symbol s_0_77[5] = { 't', 0xC5, 0xB3, 's', 'i' };
static const symbol s_0_78[2] = { 't', 'i' };
static const symbol s_0_79[4] = { 'e', 'n', 't', 'i' };
static const symbol s_0_80[4] = { 'i', 'n', 't', 'i' };
static const symbol s_0_81[3] = { 'o', 't', 'i' };
static const symbol s_0_82[4] = { 'i', 'o', 't', 'i' };
static const symbol s_0_83[4] = { 'u', 'o', 't', 'i' };
static const symbol s_0_84[5] = { 'i', 'u', 'o', 't', 'i' };
static const symbol s_0_85[4] = { 'a', 'u', 't', 'i' };
static const symbol s_0_86[5] = { 'i', 'a', 'u', 't', 'i' };
static const symbol s_0_87[3] = { 'y', 't', 'i' };
static const symbol s_0_88[4] = { 0xC4, 0x97, 't', 'i' };
static const symbol s_0_89[7] = { 't', 'e', 'l', 0xC4, 0x97, 't', 'i' };
static const symbol s_0_90[6] = { 'i', 'n', 0xC4, 0x97, 't', 'i' };
static const symbol s_0_91[7] = { 't', 'e', 'r', 0xC4, 0x97, 't', 'i' };
static const symbol s_0_92[2] = { 'u', 'i' };
static const symbol s_0_93[3] = { 'i', 'u', 'i' };
static const symbol s_0_94[5] = { 'e', 'n', 'i', 'u', 'i' };
static const symbol s_0_95[2] = { 'o', 'j' };
static const symbol s_0_96[3] = { 0xC4, 0x97, 'j' };
static const symbol s_0_97[1] = { 'k' };
static const symbol s_0_98[2] = { 'a', 'm' };
static const symbol s_0_99[3] = { 'i', 'a', 'm' };
static const symbol s_0_100[3] = { 'i', 'e', 'm' };
static const symbol s_0_101[2] = { 'i', 'm' };
static const symbol s_0_102[3] = { 's', 'i', 'm' };
static const symbol s_0_103[2] = { 'o', 'm' };
static const symbol s_0_104[3] = { 't', 'u', 'm' };
static const symbol s_0_105[3] = { 0xC4, 0x97, 'm' };
static const symbol s_0_106[6] = { 't', 'u', 'm', 0xC4, 0x97, 'm' };
static const symbol s_0_107[2] = { 'a', 'n' };
static const symbol s_0_108[2] = { 'o', 'n' };
static const symbol s_0_109[3] = { 'i', 'o', 'n' };
static const symbol s_0_110[2] = { 'u', 'n' };
static const symbol s_0_111[3] = { 'i', 'u', 'n' };
static const symbol s_0_112[3] = { 0xC4, 0x97, 'n' };
static const symbol s_0_113[1] = { 'o' };
static const symbol s_0_114[2] = { 'i', 'o' };
static const symbol s_0_115[4] = { 'e', 'n', 'i', 'o' };
static const symbol s_0_116[4] = { 0xC4, 0x97, 'j', 'o' };
static const symbol s_0_117[2] = { 'u', 'o' };
static const symbol s_0_118[1] = { 's' };
static const symbol s_0_119[2] = { 'a', 's' };
static const symbol s_0_120[3] = { 'i', 'a', 's' };
static const symbol s_0_121[2] = { 'e', 's' };
static const symbol s_0_122[3] = { 'i', 'e', 's' };
static const symbol s_0_123[2] = { 'i', 's' };
static const symbol s_0_124[3] = { 'a', 'i', 's' };
static const symbol s_0_125[4] = { 'i', 'a', 'i', 's' };
static const symbol s_0_126[6] = { 't', 'u', 'm', 'e', 'i', 's' };
static const symbol s_0_127[4] = { 'i', 'm', 'i', 's' };
static const symbol s_0_128[6] = { 'e', 'n', 'i', 'm', 'i', 's' };
static const symbol s_0_129[4] = { 'o', 'm', 'i', 's' };
static const symbol s_0_130[5] = { 'i', 'o', 'm', 'i', 's' };
static const symbol s_0_131[4] = { 'u', 'm', 'i', 's' };
static const symbol s_0_132[5] = { 0xC4, 0x97, 'm', 'i', 's' };
static const symbol s_0_133[4] = { 'e', 'n', 'i', 's' };
static const symbol s_0_134[4] = { 'a', 's', 'i', 's' };
static const symbol s_0_135[4] = { 'y', 's', 'i', 's' };
static const symbol s_0_136[3] = { 'a', 'm', 's' };
static const symbol s_0_137[4] = { 'i', 'a', 'm', 's' };
static const symbol s_0_138[4] = { 'i', 'e', 'm', 's' };
static const symbol s_0_139[3] = { 'i', 'm', 's' };
static const symbol s_0_140[5] = { 'e', 'n', 'i', 'm', 's' };
static const symbol s_0_141[5] = { 'e', 'r', 'i', 'm', 's' };
static const symbol s_0_142[3] = { 'o', 'm', 's' };
static const symbol s_0_143[4] = { 'i', 'o', 'm', 's' };
static const symbol s_0_144[3] = { 'u', 'm', 's' };
static const symbol s_0_145[4] = { 0xC4, 0x97, 'm', 's' };
static const symbol s_0_146[3] = { 'e', 'n', 's' };
static const symbol s_0_147[2] = { 'o', 's' };
static const symbol s_0_148[3] = { 'i', 'o', 's' };
static const symbol s_0_149[3] = { 'u', 'o', 's' };
static const symbol s_0_150[4] = { 'i', 'u', 'o', 's' };
static const symbol s_0_151[3] = { 'e', 'r', 's' };
static const symbol s_0_152[2] = { 'u', 's' };
static const symbol s_0_153[3] = { 'a', 'u', 's' };
static const symbol s_0_154[4] = { 'i', 'a', 'u', 's' };
static const symbol s_0_155[3] = { 'i', 'u', 's' };
static const symbol s_0_156[2] = { 'y', 's' };
static const symbol s_0_157[4] = { 'e', 'n', 'y', 's' };
static const symbol s_0_158[4] = { 'e', 'r', 'y', 's' };
static const symbol s_0_159[3] = { 0xC4, 0x85, 's' };
static const symbol s_0_160[4] = { 'i', 0xC4, 0x85, 's' };
static const symbol s_0_161[3] = { 0xC4, 0x97, 's' };
static const symbol s_0_162[5] = { 'a', 'm', 0xC4, 0x97, 's' };
static const symbol s_0_163[6] = { 'i', 'a', 'm', 0xC4, 0x97, 's' };
static const symbol s_0_164[5] = { 'i', 'm', 0xC4, 0x97, 's' };
static const symbol s_0_165[6] = { 'k', 'i', 'm', 0xC4, 0x97, 's' };
static const symbol s_0_166[6] = { 's', 'i', 'm', 0xC4, 0x97, 's' };
static const symbol s_0_167[5] = { 'o', 'm', 0xC4, 0x97, 's' };
static const symbol s_0_168[6] = { 0xC4, 0x97, 'm', 0xC4, 0x97, 's' };
static const symbol s_0_169[9] = { 't', 'u', 'm', 0xC4, 0x97, 'm', 0xC4, 0x97, 's' };
static const symbol s_0_170[5] = { 'a', 't', 0xC4, 0x97, 's' };
static const symbol s_0_171[6] = { 'i', 'a', 't', 0xC4, 0x97, 's' };
static const symbol s_0_172[6] = { 's', 'i', 't', 0xC4, 0x97, 's' };
static const symbol s_0_173[5] = { 'o', 't', 0xC4, 0x97, 's' };
static const symbol s_0_174[6] = { 0xC4, 0x97, 't', 0xC4, 0x97, 's' };
static const symbol s_0_175[9] = { 't', 'u', 'm', 0xC4, 0x97, 't', 0xC4, 0x97, 's' };
static const symbol s_0_176[3] = { 0xC5, 0xAB, 's' };
static const symbol s_0_177[3] = { 0xC4, 0xAF, 's' };
static const symbol s_0_178[4] = { 't', 0xC5, 0xB3, 's' };
static const symbol s_0_179[2] = { 'a', 't' };
static const symbol s_0_180[3] = { 'i', 'a', 't' };
static const symbol s_0_181[2] = { 'i', 't' };
static const symbol s_0_182[3] = { 's', 'i', 't' };
static const symbol s_0_183[2] = { 'o', 't' };
static const symbol s_0_184[3] = { 0xC4, 0x97, 't' };
static const symbol s_0_185[6] = { 't', 'u', 'm', 0xC4, 0x97, 't' };
static const symbol s_0_186[1] = { 'u' };
static const symbol s_0_187[2] = { 'a', 'u' };
static const symbol s_0_188[3] = { 'i', 'a', 'u' };
static const symbol s_0_189[5] = { 0xC4, 0x8D, 'i', 'a', 'u' };
static const symbol s_0_190[2] = { 'i', 'u' };
static const symbol s_0_191[4] = { 'e', 'n', 'i', 'u' };
static const symbol s_0_192[3] = { 's', 'i', 'u' };
static const symbol s_0_193[1] = { 'y' };
static const symbol s_0_194[2] = { 0xC4, 0x85 };
static const symbol s_0_195[3] = { 'i', 0xC4, 0x85 };
static const symbol s_0_196[2] = { 0xC4, 0x97 };
static const symbol s_0_197[2] = { 0xC4, 0x99 };
static const symbol s_0_198[2] = { 0xC4, 0xAF };
static const symbol s_0_199[4] = { 'e', 'n', 0xC4, 0xAF };
static const symbol s_0_200[4] = { 'e', 'r', 0xC4, 0xAF };
static const symbol s_0_201[2] = { 0xC5, 0xB3 };
static const symbol s_0_202[3] = { 'i', 0xC5, 0xB3 };
static const symbol s_0_203[4] = { 'e', 'r', 0xC5, 0xB3 };
static const struct among a_0[204] = {
{ 1, s_0_0, 0, -1, 0},
{ 2, s_0_1, -1, -1, 0},
{ 4, s_0_2, -1, -1, 0},
{ 4, s_0_3, -3, -1, 0},
{ 5, s_0_4, -1, -1, 0},
{ 5, s_0_5, -2, -1, 0},
{ 6, s_0_6, -1, -1, 0},
{ 4, s_0_7, -7, -1, 0},
{ 5, s_0_8, -8, -1, 0},
{ 1, s_0_9, 0, -1, 0},
{ 2, s_0_10, -1, -1, 0},
{ 4, s_0_11, -1, -1, 0},
{ 4, s_0_12, -2, -1, 0},
{ 3, s_0_13, -4, -1, 0},
{ 4, s_0_14, -1, -1, 0},
{ 3, s_0_15, -6, -1, 0},
{ 4, s_0_16, -1, -1, 0},
{ 3, s_0_17, -8, -1, 0},
{ 5, s_0_18, -1, -1, 0},
{ 5, s_0_19, -2, -1, 0},
{ 4, s_0_20, -11, -1, 0},
{ 3, s_0_21, -12, -1, 0},
{ 4, s_0_22, -1, -1, 0},
{ 4, s_0_23, -14, -1, 0},
{ 3, s_0_24, -15, -1, 0},
{ 4, s_0_25, -16, -1, 0},
{ 7, s_0_26, -1, -1, 0},
{ 3, s_0_27, -18, -1, 0},
{ 4, s_0_28, -1, -1, 0},
{ 4, s_0_29, -2, -1, 0},
{ 5, s_0_30, -1, -1, 0},
{ 3, s_0_31, -22, -1, 0},
{ 5, s_0_32, -1, -1, 0},
{ 5, s_0_33, -2, -1, 0},
{ 4, s_0_34, -25, -1, 0},
{ 3, s_0_35, -26, -1, 0},
{ 4, s_0_36, -1, -1, 0},
{ 3, s_0_37, -28, -1, 0},
{ 4, s_0_38, -1, -1, 0},
{ 4, s_0_39, -2, -1, 0},
{ 3, s_0_40, -31, -1, 0},
{ 4, s_0_41, -32, -1, 0},
{ 4, s_0_42, -33, -1, 0},
{ 7, s_0_43, -1, -1, 0},
{ 1, s_0_44, 0, -1, 0},
{ 2, s_0_45, -1, -1, 0},
{ 3, s_0_46, -1, -1, 0},
{ 5, s_0_47, -1, -1, 0},
{ 2, s_0_48, -4, -1, 0},
{ 5, s_0_49, -1, -1, 0},
{ 2, s_0_50, -6, -1, 0},
{ 3, s_0_51, -7, -1, 0},
{ 5, s_0_52, -1, -1, 0},
{ 3, s_0_53, -9, -1, 0},
{ 4, s_0_54, -1, -1, 0},
{ 2, s_0_55, -11, -1, 0},
{ 3, s_0_56, -1, -1, 0},
{ 4, s_0_57, -1, -1, 0},
{ 3, s_0_58, -3, -1, 0},
{ 4, s_0_59, -1, -1, 0},
{ 5, s_0_60, -1, -1, 0},
{ 3, s_0_61, -6, -1, 0},
{ 4, s_0_62, -1, -1, 0},
{ 4, s_0_63, -2, -1, 0},
{ 7, s_0_64, -1, -1, 0},
{ 4, s_0_65, -4, -1, 0},
{ 3, s_0_66, -11, -1, 0},
{ 6, s_0_67, -1, -1, 0},
{ 4, s_0_68, -2, -1, 0},
{ 5, s_0_69, -1, -1, 0},
{ 6, s_0_70, -1, -1, 0},
{ 3, s_0_71, -16, -1, 0},
{ 4, s_0_72, -1, -1, 0},
{ 7, s_0_73, -1, -1, 0},
{ 4, s_0_74, -19, -1, 0},
{ 4, s_0_75, -20, -1, 0},
{ 4, s_0_76, -21, -1, 0},
{ 5, s_0_77, -1, -1, 0},
{ 2, s_0_78, -34, -1, 0},
{ 4, s_0_79, -1, -1, 0},
{ 4, s_0_80, -2, -1, 0},
{ 3, s_0_81, -3, -1, 0},
{ 4, s_0_82, -1, -1, 0},
{ 4, s_0_83, -2, -1, 0},
{ 5, s_0_84, -1, -1, 0},
{ 4, s_0_85, -7, -1, 0},
{ 5, s_0_86, -1, -1, 0},
{ 3, s_0_87, -9, -1, 0},
{ 4, s_0_88, -10, -1, 0},
{ 7, s_0_89, -1, -1, 0},
{ 6, s_0_90, -2, -1, 0},
{ 7, s_0_91, -3, -1, 0},
{ 2, s_0_92, -48, -1, 0},
{ 3, s_0_93, -1, -1, 0},
{ 5, s_0_94, -1, -1, 0},
{ 2, s_0_95, 0, -1, 0},
{ 3, s_0_96, 0, -1, 0},
{ 1, s_0_97, 0, -1, 0},
{ 2, s_0_98, 0, -1, 0},
{ 3, s_0_99, -1, -1, 0},
{ 3, s_0_100, 0, -1, 0},
{ 2, s_0_101, 0, -1, 0},
{ 3, s_0_102, -1, -1, 0},
{ 2, s_0_103, 0, -1, 0},
{ 3, s_0_104, 0, -1, 0},
{ 3, s_0_105, 0, -1, 0},
{ 6, s_0_106, -1, -1, 0},
{ 2, s_0_107, 0, -1, 0},
{ 2, s_0_108, 0, -1, 0},
{ 3, s_0_109, -1, -1, 0},
{ 2, s_0_110, 0, -1, 0},
{ 3, s_0_111, -1, -1, 0},
{ 3, s_0_112, 0, -1, 0},
{ 1, s_0_113, 0, -1, 0},
{ 2, s_0_114, -1, -1, 0},
{ 4, s_0_115, -1, -1, 0},
{ 4, s_0_116, -3, -1, 0},
{ 2, s_0_117, -4, -1, 0},
{ 1, s_0_118, 0, -1, 0},
{ 2, s_0_119, -1, -1, 0},
{ 3, s_0_120, -1, -1, 0},
{ 2, s_0_121, -3, -1, 0},
{ 3, s_0_122, -1, -1, 0},
{ 2, s_0_123, -5, -1, 0},
{ 3, s_0_124, -1, -1, 0},
{ 4, s_0_125, -1, -1, 0},
{ 6, s_0_126, -3, -1, 0},
{ 4, s_0_127, -4, -1, 0},
{ 6, s_0_128, -1, -1, 0},
{ 4, s_0_129, -6, -1, 0},
{ 5, s_0_130, -1, -1, 0},
{ 4, s_0_131, -8, -1, 0},
{ 5, s_0_132, -9, -1, 0},
{ 4, s_0_133, -10, -1, 0},
{ 4, s_0_134, -11, -1, 0},
{ 4, s_0_135, -12, -1, 0},
{ 3, s_0_136, -18, -1, 0},
{ 4, s_0_137, -1, -1, 0},
{ 4, s_0_138, -20, -1, 0},
{ 3, s_0_139, -21, -1, 0},
{ 5, s_0_140, -1, -1, 0},
{ 5, s_0_141, -2, -1, 0},
{ 3, s_0_142, -24, -1, 0},
{ 4, s_0_143, -1, -1, 0},
{ 3, s_0_144, -26, -1, 0},
{ 4, s_0_145, -27, -1, 0},
{ 3, s_0_146, -28, -1, 0},
{ 2, s_0_147, -29, -1, 0},
{ 3, s_0_148, -1, -1, 0},
{ 3, s_0_149, -2, -1, 0},
{ 4, s_0_150, -1, -1, 0},
{ 3, s_0_151, -33, -1, 0},
{ 2, s_0_152, -34, -1, 0},
{ 3, s_0_153, -1, -1, 0},
{ 4, s_0_154, -1, -1, 0},
{ 3, s_0_155, -3, -1, 0},
{ 2, s_0_156, -38, -1, 0},
{ 4, s_0_157, -1, -1, 0},
{ 4, s_0_158, -2, -1, 0},
{ 3, s_0_159, -41, -1, 0},
{ 4, s_0_160, -1, -1, 0},
{ 3, s_0_161, -43, -1, 0},
{ 5, s_0_162, -1, -1, 0},
{ 6, s_0_163, -1, -1, 0},
{ 5, s_0_164, -3, -1, 0},
{ 6, s_0_165, -1, -1, 0},
{ 6, s_0_166, -2, -1, 0},
{ 5, s_0_167, -6, -1, 0},
{ 6, s_0_168, -7, -1, 0},
{ 9, s_0_169, -1, -1, 0},
{ 5, s_0_170, -9, -1, 0},
{ 6, s_0_171, -1, -1, 0},
{ 6, s_0_172, -11, -1, 0},
{ 5, s_0_173, -12, -1, 0},
{ 6, s_0_174, -13, -1, 0},
{ 9, s_0_175, -1, -1, 0},
{ 3, s_0_176, -58, -1, 0},
{ 3, s_0_177, -59, -1, 0},
{ 4, s_0_178, -60, -1, 0},
{ 2, s_0_179, 0, -1, 0},
{ 3, s_0_180, -1, -1, 0},
{ 2, s_0_181, 0, -1, 0},
{ 3, s_0_182, -1, -1, 0},
{ 2, s_0_183, 0, -1, 0},
{ 3, s_0_184, 0, -1, 0},
{ 6, s_0_185, -1, -1, 0},
{ 1, s_0_186, 0, -1, 0},
{ 2, s_0_187, -1, -1, 0},
{ 3, s_0_188, -1, -1, 0},
{ 5, s_0_189, -1, -1, 0},
{ 2, s_0_190, -4, -1, 0},
{ 4, s_0_191, -1, -1, 0},
{ 3, s_0_192, -2, -1, 0},
{ 1, s_0_193, 0, -1, 0},
{ 2, s_0_194, 0, -1, 0},
{ 3, s_0_195, -1, -1, 0},
{ 2, s_0_196, 0, -1, 0},
{ 2, s_0_197, 0, -1, 0},
{ 2, s_0_198, 0, -1, 0},
{ 4, s_0_199, -1, -1, 0},
{ 4, s_0_200, -2, -1, 0},
{ 2, s_0_201, 0, -1, 0},
{ 3, s_0_202, -1, -1, 0},
{ 4, s_0_203, -2, -1, 0}
};

static const symbol s_1_0[3] = { 'i', 'n', 'g' };
static const symbol s_1_1[2] = { 'a', 'j' };
static const symbol s_1_2[3] = { 'i', 'a', 'j' };
static const symbol s_1_3[3] = { 'i', 'e', 'j' };
static const symbol s_1_4[2] = { 'o', 'j' };
static const symbol s_1_5[3] = { 'i', 'o', 'j' };
static const symbol s_1_6[3] = { 'u', 'o', 'j' };
static const symbol s_1_7[4] = { 'i', 'u', 'o', 'j' };
static const symbol s_1_8[3] = { 'a', 'u', 'j' };
static const symbol s_1_9[3] = { 0xC4, 0x85, 'j' };
static const symbol s_1_10[4] = { 'i', 0xC4, 0x85, 'j' };
static const symbol s_1_11[3] = { 0xC4, 0x97, 'j' };
static const symbol s_1_12[3] = { 0xC5, 0xB3, 'j' };
static const symbol s_1_13[4] = { 'i', 0xC5, 0xB3, 'j' };
static const symbol s_1_14[2] = { 'o', 'k' };
static const symbol s_1_15[3] = { 'i', 'o', 'k' };
static const symbol s_1_16[3] = { 'i', 'u', 'k' };
static const symbol s_1_17[5] = { 'u', 'l', 'i', 'u', 'k' };
static const symbol s_1_18[6] = { 'u', 0xC4, 0x8D, 'i', 'u', 'k' };
static const symbol s_1_19[4] = { 'i', 0xC5, 0xA1, 'k' };
static const symbol s_1_20[3] = { 'i', 'u', 'l' };
static const symbol s_1_21[2] = { 'y', 'l' };
static const symbol s_1_22[3] = { 0xC4, 0x97, 'l' };
static const symbol s_1_23[2] = { 'a', 'm' };
static const symbol s_1_24[3] = { 'd', 'a', 'm' };
static const symbol s_1_25[3] = { 'j', 'a', 'm' };
static const symbol s_1_26[4] = { 'z', 'g', 'a', 'n' };
static const symbol s_1_27[3] = { 'a', 'i', 'n' };
static const symbol s_1_28[3] = { 'e', 's', 'n' };
static const symbol s_1_29[2] = { 'o', 'p' };
static const symbol s_1_30[3] = { 'i', 'o', 'p' };
static const symbol s_1_31[3] = { 'i', 'a', 's' };
static const symbol s_1_32[3] = { 'i', 'e', 's' };
static const symbol s_1_33[3] = { 'a', 'i', 's' };
static const symbol s_1_34[4] = { 'i', 'a', 'i', 's' };
static const symbol s_1_35[2] = { 'o', 's' };
static const symbol s_1_36[3] = { 'i', 'o', 's' };
static const symbol s_1_37[3] = { 'u', 'o', 's' };
static const symbol s_1_38[4] = { 'i', 'u', 'o', 's' };
static const symbol s_1_39[3] = { 'a', 'u', 's' };
static const symbol s_1_40[4] = { 'i', 'a', 'u', 's' };
static const symbol s_1_41[3] = { 0xC4, 0x85, 's' };
static const symbol s_1_42[4] = { 'i', 0xC4, 0x85, 's' };
static const symbol s_1_43[3] = { 0xC4, 0x99, 's' };
static const symbol s_1_44[7] = { 'u', 't', 0xC4, 0x97, 'a', 'i', 't' };
static const symbol s_1_45[3] = { 'a', 'n', 't' };
static const symbol s_1_46[4] = { 'i', 'a', 'n', 't' };
static const symbol s_1_47[5] = { 's', 'i', 'a', 'n', 't' };
static const symbol s_1_48[3] = { 'i', 'n', 't' };
static const symbol s_1_49[2] = { 'o', 't' };
static const symbol s_1_50[3] = { 'u', 'o', 't' };
static const symbol s_1_51[4] = { 'i', 'u', 'o', 't' };
static const symbol s_1_52[2] = { 'y', 't' };
static const symbol s_1_53[3] = { 0xC4, 0x97, 't' };
static const symbol s_1_54[5] = { 'y', 'k', 0xC5, 0xA1, 't' };
static const symbol s_1_55[3] = { 'i', 'a', 'u' };
static const symbol s_1_56[3] = { 'd', 'a', 'v' };
static const symbol s_1_57[2] = { 's', 'v' };
static const symbol s_1_58[3] = { 0xC5, 0xA1, 'v' };
static const symbol s_1_59[6] = { 'y', 'k', 0xC5, 0xA1, 0xC4, 0x8D };
static const symbol s_1_60[2] = { 0xC4, 0x99 };
static const symbol s_1_61[5] = { 0xC4, 0x97, 'j', 0xC4, 0x99 };
static const struct among a_1[62] = {
{ 3, s_1_0, 0, -1, 0},
{ 2, s_1_1, 0, -1, 0},
{ 3, s_1_2, -1, -1, 0},
{ 3, s_1_3, 0, -1, 0},
{ 2, s_1_4, 0, -1, 0},
{ 3, s_1_5, -1, -1, 0},
{ 3, s_1_6, -2, -1, 0},
{ 4, s_1_7, -1, -1, 0},
{ 3, s_1_8, 0, -1, 0},
{ 3, s_1_9, 0, -1, 0},
{ 4, s_1_10, -1, -1, 0},
{ 3, s_1_11, 0, -1, 0},
{ 3, s_1_12, 0, -1, 0},
{ 4, s_1_13, -1, -1, 0},
{ 2, s_1_14, 0, -1, 0},
{ 3, s_1_15, -1, -1, 0},
{ 3, s_1_16, 0, -1, 0},
{ 5, s_1_17, -1, -1, 0},
{ 6, s_1_18, -2, -1, 0},
{ 4, s_1_19, 0, -1, 0},
{ 3, s_1_20, 0, -1, 0},
{ 2, s_1_21, 0, -1, 0},
{ 3, s_1_22, 0, -1, 0},
{ 2, s_1_23, 0, -1, 0},
{ 3, s_1_24, -1, -1, 0},
{ 3, s_1_25, -2, -1, 0},
{ 4, s_1_26, 0, -1, 0},
{ 3, s_1_27, 0, -1, 0},
{ 3, s_1_28, 0, -1, 0},
{ 2, s_1_29, 0, -1, 0},
{ 3, s_1_30, -1, -1, 0},
{ 3, s_1_31, 0, -1, 0},
{ 3, s_1_32, 0, -1, 0},
{ 3, s_1_33, 0, -1, 0},
{ 4, s_1_34, -1, -1, 0},
{ 2, s_1_35, 0, -1, 0},
{ 3, s_1_36, -1, -1, 0},
{ 3, s_1_37, -2, -1, 0},
{ 4, s_1_38, -1, -1, 0},
{ 3, s_1_39, 0, -1, 0},
{ 4, s_1_40, -1, -1, 0},
{ 3, s_1_41, 0, -1, 0},
{ 4, s_1_42, -1, -1, 0},
{ 3, s_1_43, 0, -1, 0},
{ 7, s_1_44, 0, -1, 0},
{ 3, s_1_45, 0, -1, 0},
{ 4, s_1_46, -1, -1, 0},
{ 5, s_1_47, -1, -1, 0},
{ 3, s_1_48, 0, -1, 0},
{ 2, s_1_49, 0, -1, 0},
{ 3, s_1_50, -1, -1, 0},
{ 4, s_1_51, -1, -1, 0},
{ 2, s_1_52, 0, -1, 0},
{ 3, s_1_53, 0, -1, 0},
{ 5, s_1_54, 0, -1, 0},
{ 3, s_1_55, 0, -1, 0},
{ 3, s_1_56, 0, -1, 0},
{ 2, s_1_57, 0, -1, 0},
{ 3, s_1_58, 0, -1, 0},
{ 6, s_1_59, 0, -1, 0},
{ 2, s_1_60, 0, -1, 0},
{ 5, s_1_61, -1, -1, 0}
};

static const symbol s_2_0[5] = { 'o', 'j', 'i', 'm', 'e' };
static const symbol s_2_1[6] = { 0xC4, 0x97, 'j', 'i', 'm', 'e' };
static const symbol s_2_2[5] = { 'a', 'v', 'i', 'm', 'e' };
static const symbol s_2_3[5] = { 'o', 'k', 'a', 't', 'e' };
static const symbol s_2_4[4] = { 'a', 'i', 't', 'e' };
static const symbol s_2_5[4] = { 'u', 'o', 't', 'e' };
static const symbol s_2_6[5] = { 'a', 's', 'i', 'u', 's' };
static const symbol s_2_7[7] = { 'o', 'k', 'a', 't', 0xC4, 0x97, 's' };
static const symbol s_2_8[6] = { 'a', 'i', 't', 0xC4, 0x97, 's' };
static const symbol s_2_9[6] = { 'u', 'o', 't', 0xC4, 0x97, 's' };
static const symbol s_2_10[4] = { 'e', 's', 'i', 'u' };
static const struct among a_2[11] = {
{ 5, s_2_0, 0, 7, 0},
{ 6, s_2_1, 0, 3, 0},
{ 5, s_2_2, 0, 6, 0},
{ 5, s_2_3, 0, 8, 0},
{ 4, s_2_4, 0, 1, 0},
{ 4, s_2_5, 0, 2, 0},
{ 5, s_2_6, 0, 5, 0},
{ 7, s_2_7, 0, 8, 0},
{ 6, s_2_8, 0, 1, 0},
{ 6, s_2_9, 0, 2, 0},
{ 4, s_2_10, 0, 4, 0}
};

static const symbol s_3_0[2] = { 0xC4, 0x8D };
static const symbol s_3_1[3] = { 'd', 0xC5, 0xBE };
static const struct among a_3[2] = {
{ 2, s_3_0, 0, 1, 0},
{ 3, s_3_1, 0, 2, 0}
};

static const symbol s_4_0[2] = { 'g', 'd' };
static const struct among a_4[1] = {
{ 2, s_4_0, 0, 1, 0}
};

static const unsigned char g_v[] = { 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 64, 1, 0, 64, 0, 0, 0, 0, 0, 0, 0, 4, 4 };

static const symbol s_0[] = { 'a', 'i', 't', 0xC4, 0x97 };
static const symbol s_1[] = { 'u', 'o', 't', 0xC4, 0x97 };
static const symbol s_2[] = { 0xC4, 0x97, 'j', 'i', 'm', 'a', 's' };
static const symbol s_3[] = { 'e', 's', 'y', 's' };
static const symbol s_4[] = { 'a', 's', 'y', 's' };
static const symbol s_5[] = { 'a', 'v', 'i', 'm', 'a', 's' };
static const symbol s_6[] = { 'o', 'j', 'i', 'm', 'a', 's' };
static const symbol s_7[] = { 'o', 'k', 'a', 't', 0xC4, 0x97 };
static const symbol s_8[] = { 't' };
static const symbol s_9[] = { 'd' };
static const symbol s_10[] = { 'g' };

static int r_step1(struct SN_env * z) {

    {
        int v_1;
        if (z->c < z->I[0]) return 0;
        v_1 = z->lb; z->lb = z->I[0];
        z->ket = z->c;
        if (!find_among_b(z, a_0, 204)) { z->lb = v_1; return 0; }
        z->bra = z->c;
        z->lb = v_1;
    }
    {
        int ret = slice_del(z);
        if (ret < 0) return ret;
    }
    return 1;
}

static int r_step2(struct SN_env * z) {
    while (1) {
        int v_1 = z->l - z->c;

        {
            int v_2;
            if (z->c < z->I[0]) goto lab0;
            v_2 = z->lb; z->lb = z->I[0];
            z->ket = z->c;
            if (!find_among_b(z, a_1, 62)) { z->lb = v_2; goto lab0; }
            z->bra = z->c;
            z->lb = v_2;
        }
        {
            int ret = slice_del(z);
            if (ret < 0) return ret;
        }
        continue;
    lab0:
        z->c = z->l - v_1;
        break;
    }
    return 1;
}

static int r_fix_conflicts(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    if (z->c - 3 <= z->lb || z->p[z->c - 1] >> 5 != 3 || !((2621472 >> (z->p[z->c - 1] & 0x1f)) & 1)) return 0;
    among_var = find_among_b(z, a_2, 11);
    if (!among_var) return 0;
    z->bra = z->c;
    switch (among_var) {
        case 1:
            {
                int ret = slice_from_s(z, 5, s_0);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_from_s(z, 5, s_1);
                if (ret < 0) return ret;
            }
            break;
        case 3:
            {
                int ret = slice_from_s(z, 7, s_2);
                if (ret < 0) return ret;
            }
            break;
        case 4:
            {
                int ret = slice_from_s(z, 4, s_3);
                if (ret < 0) return ret;
            }
            break;
        case 5:
            {
                int ret = slice_from_s(z, 4, s_4);
                if (ret < 0) return ret;
            }
            break;
        case 6:
            {
                int ret = slice_from_s(z, 6, s_5);
                if (ret < 0) return ret;
            }
            break;
        case 7:
            {
                int ret = slice_from_s(z, 6, s_6);
                if (ret < 0) return ret;
            }
            break;
        case 8:
            {
                int ret = slice_from_s(z, 6, s_7);
                if (ret < 0) return ret;
            }
            break;
    }
    return 1;
}

static int r_fix_chdz(struct SN_env * z) {
    int among_var;
    z->ket = z->c;
    if (z->c - 1 <= z->lb || (z->p[z->c - 1] != 141 && z->p[z->c - 1] != 190)) return 0;
    among_var = find_among_b(z, a_3, 2);
    if (!among_var) return 0;
    z->bra = z->c;
    switch (among_var) {
        case 1:
            {
                int ret = slice_from_s(z, 1, s_8);
                if (ret < 0) return ret;
            }
            break;
        case 2:
            {
                int ret = slice_from_s(z, 1, s_9);
                if (ret < 0) return ret;
            }
            break;
    }
    return 1;
}

static int r_fix_gd(struct SN_env * z) {
    z->ket = z->c;
    if (z->c - 1 <= z->lb || z->p[z->c - 1] != 100) return 0;
    if (!find_among_b(z, a_4, 1)) return 0;
    z->bra = z->c;
    {
        int ret = slice_from_s(z, 1, s_10);
        if (ret < 0) return ret;
    }
    return 1;
}

extern int lithuanian_UTF_8_stem(struct SN_env * z) {
    z->I[0] = z->l;
    {
        int v_1 = z->c;
        {
            int v_2 = z->c;
            {
                int v_3 = z->c;
                if (z->c == z->l || z->p[z->c] != 'a') { z->c = v_2; goto lab1; }
                z->c++;
                z->c = v_3;
            }
            if (len_utf8(z->p) <= 6) { z->c = v_2; goto lab1; }
            {
                int ret = skip_utf8(z->p, z->c, z->l, 1);
                if (ret < 0) { z->c = v_2; goto lab1; }
                z->c = ret;
            }
        lab1:
            ;
        }
        {
            int ret = out_grouping_U(z, g_v, 97, 371, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        {
            int ret = in_grouping_U(z, g_v, 97, 371, 1);
            if (ret < 0) goto lab0;
            z->c += ret;
        }
        z->I[0] = z->c;
    lab0:
        z->c = v_1;
    }
    z->lb = z->c; z->c = z->l;
    {
        int v_4 = z->l - z->c;
        {
            int ret = r_fix_conflicts(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_4;
    }
    {
        int v_5 = z->l - z->c;
        {
            int ret = r_step1(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_5;
    }
    {
        int v_6 = z->l - z->c;
        {
            int ret = r_fix_chdz(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_6;
    }
    {
        int v_7 = z->l - z->c;
        {
            int ret = r_step2(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_7;
    }
    {
        int v_8 = z->l - z->c;
        {
            int ret = r_fix_chdz(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_8;
    }
    {
        int v_9 = z->l - z->c;
        {
            int ret = r_fix_gd(z);
            if (ret < 0) return ret;
        }
        z->c = z->l - v_9;
    }
    z->c = z->lb;
    return 1;
}

extern struct SN_env * lithuanian_UTF_8_create_env(void) { return SN_create_env(0, 1); }

extern void lithuanian_UTF_8_close_env(struct SN_env * z) { SN_close_env(z, 0); }

