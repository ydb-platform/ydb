-- CREATE TABLE `_temp/_pool_19967` (
--                                      `_q_000_f_000_type` String,
--                                      `_q_000_f_001_type` String,
--                                      `_q_000_f_002` Utf8,
--                                      `_q_000_f_002_upper` Utf8,
--                                      `_q_000_f_003` Utf8,
--                                      `_q_000_f_003_upper` Utf8,
--                                      `_q_000_f_004` Decimal(1, 0),
--                                      `_q_000_f_005` Bool,
--                                      `_ydb_pk` BigSerial NOT NULL,
--                                      PRIMARY KEY (`_ydb_pk`)
-- )

Pragma TablePathPrefix("/Root/pg-data/ydb_zup");

$const_0
= AsList(CAST("\x5c\x32\x35\x34\x5c\x30\x31\x31\x5c\x33\x35\x33\x30\x5c\x33\x37\x32\x56\x28\x22\x45\x3b\x2d\x5c\x33\x36\x34\x5c\x30\x31\x35\x5c\x30\x33\x32\x6b\x5c\x33\x32\x34" AS String), CAST("\x5c\x32\x36\x33\x5c\x33\x32\x37\x5c\x33\x30\x35\x5c\x33\x32\x32\x54\x5d\x3e\x5c\x32\x37\x36\x4f\x5c\x32\x34\x33\x5c\x33\x31\x34\x2c\x5c\x32\x34\x34\x5c\x33\x34\x34\x2c\x5c\x32\x31\x35" AS String));
 $const_1
= AsList(CAST("\x5c\x32\x35\x34\x5c\x30\x31\x31\x5c\x33\x35\x33\x30\x5c\x33\x37\x32\x56\x28\x22\x45\x3b\x2d\x5c\x33\x36\x34\x5c\x30\x31\x35\x5c\x30\x33\x32\x6b\x5c\x33\x32\x34" AS String), CAST("\x5c\x32\x36\x33\x5c\x33\x32\x37\x5c\x33\x30\x35\x5c\x33\x32\x32\x54\x5d\x3e\x5c\x32\x37\x36\x4f\x5c\x32\x34\x33\x5c\x33\x31\x34\x2c\x5c\x32\x34\x34\x5c\x33\x34\x34\x2c\x5c\x32\x31\x35" AS String));
 $const_2
= CAST("\x5c\x32\x32\x33\x73\x5c\x30\x30\x30\x5c\x30\x33\x33\x5c\x30\x32\x31\x5c\x32\x36\x32\x55\x5c\x32\x32\x30\x5c\x30\x32\x31\x5c\x33\x34\x32\x5c\x32\x30\x36\x36\x5c\x32\x36\x31\x5c\x30\x32\x35\x5c\x32\x36\x30\x6e" AS String);
 $const_3
= CAST("0" AS Decimal(7, 0));
 $const_4
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30" AS String?);
 $const_5
= CAST(NULL AS String?);
 $const_6
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30" AS String?);
 $const_7
= CAST(NULL AS String?);
 $const_8
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30" AS String?);
 $const_9
= CAST(NULL AS String?);
 $const_10
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x30\x30\x30" AS String?);
 $const_11
= CAST(NULL AS String?);
 $const_12
= CAST("0" AS Decimal(7, 0));
 $const_13
= CAST("0" AS Decimal(7, 0));
 $const_14
= CAST("0" AS Decimal(7, 0));
 $const_15
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_16
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x34\x34" AS String?);
 $const_17
= CAST("0" AS Decimal(7, 0));
 $const_18
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_19
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x32" AS String?);
 $const_20
= CAST("0" AS Decimal(7, 0));
 $const_21
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_22
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x6f\x5c\x33\x35\x33" AS String?);
 $const_23
= CAST("0" AS Decimal(7, 0));
 $const_24
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_25
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x7e\x5c\x33\x34\x36" AS String?);
 $const_26
= CAST("0" AS Decimal(7, 0));
 $const_27
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_28
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x31\x5c\x33\x31\x33" AS String?);
 $const_29
= CAST("0" AS Decimal(7, 0));
 $const_30
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_31
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x34\x35" AS String?);
 $const_32
= CAST("0" AS Decimal(7, 0));
 $const_33
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_34
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x2f" AS String?);
 $const_35
= CAST("0" AS Decimal(7, 0));
 $const_36
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_37
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x43" AS String?);
 $const_38
= CAST("0" AS Decimal(7, 0));
 $const_39
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_40
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x33" AS String?);
 $const_41
= CAST("0" AS Decimal(7, 0));
 $const_42
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_43
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x5c\x32\x32\x30" AS String?);
 $const_44
= CAST("0" AS Decimal(7, 0));
 $const_45
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_46
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x78\x5c\x32\x33\x35" AS String?);
 $const_47
= CAST("0" AS Decimal(7, 0));
 $const_48
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_49
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x70\x4b" AS String?);
 $const_50
= CAST("0" AS Decimal(7, 0));
 $const_51
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_52
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x77" AS String?);
 $const_53
= CAST("0" AS Decimal(7, 0));
 $const_54
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_55
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x58" AS String?);
 $const_56
= CAST("0" AS Decimal(7, 0));
 $const_57
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_58
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x37" AS String?);
 $const_59
= CAST("0" AS Decimal(7, 0));
 $const_60
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_61
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x31\x33" AS String?);
 $const_62
= CAST("0" AS Decimal(7, 0));
 $const_63
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_64
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x31\x34" AS String?);
 $const_65
= CAST("0" AS Decimal(7, 0));
 $const_66
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_67
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x34\x30" AS String?);
 $const_68
= CAST("0" AS Decimal(7, 0));
 $const_69
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_70
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x44" AS String?);
 $const_71
= CAST("0" AS Decimal(7, 0));
 $const_72
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_73
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x3f" AS String?);
 $const_74
= CAST("0" AS Decimal(7, 0));
 $const_75
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_76
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x31\x34\x5c\x33\x36\x30" AS String?);
 $const_77
= CAST("0" AS Decimal(7, 0));
 $const_78
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_79
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x40" AS String?);
 $const_80
= CAST("0" AS Decimal(7, 0));
 $const_81
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_82
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x31\x30" AS String?);
 $const_83
= CAST("0" AS Decimal(7, 0));
 $const_84
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_85
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x33\x34" AS String?);
 $const_86
= CAST("0" AS Decimal(7, 0));
 $const_87
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_88
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x6e\x5c\x32\x32\x33" AS String?);
 $const_89
= CAST("0" AS Decimal(7, 0));
 $const_90
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_91
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x5c\x32\x32\x36" AS String?);
 $const_92
= CAST("0" AS Decimal(7, 0));
 $const_93
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_94
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x33\x33\x5c\x30\x31\x30" AS String?);
 $const_95
= CAST("0" AS Decimal(7, 0));
 $const_96
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_97
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x51" AS String?);
 $const_98
= CAST("0" AS Decimal(7, 0));
 $const_99
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_100
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x33" AS String?);
 $const_101
= CAST("0" AS Decimal(7, 0));
 $const_102
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_103
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x5c\x32\x30\x34" AS String?);
 $const_104
= CAST("0" AS Decimal(7, 0));
 $const_105
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_106
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x38" AS String?);
 $const_107
= CAST("0" AS Decimal(7, 0));
 $const_108
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_109
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x5c\x33\x30\x32" AS String?);
 $const_110
= CAST("0" AS Decimal(7, 0));
 $const_111
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_112
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x31\x36" AS String?);
 $const_113
= CAST("0" AS Decimal(7, 0));
 $const_114
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_115
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x36" AS String?);
 $const_116
= CAST("0" AS Decimal(7, 0));
 $const_117
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_118
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x34\x31" AS String?);
 $const_119
= CAST("0" AS Decimal(7, 0));
 $const_120
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_121
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x20\x50" AS String?);
 $const_122
= CAST("0" AS Decimal(7, 0));
 $const_123
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_124
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x31\x32" AS String?);
 $const_125
= CAST("0" AS Decimal(7, 0));
 $const_126
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_127
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x33\x36" AS String?);
 $const_128
= CAST("0" AS Decimal(7, 0));
 $const_129
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_130
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x31" AS String?);
 $const_131
= CAST("0" AS Decimal(7, 0));
 $const_132
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_133
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x31\x35" AS String?);
 $const_134
= CAST("0" AS Decimal(7, 0));
 $const_135
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_136
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x54" AS String?);
 $const_137
= CAST("0" AS Decimal(7, 0));
 $const_138
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_139
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x4d" AS String?);
 $const_140
= CAST("0" AS Decimal(7, 0));
 $const_141
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_142
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x20\x51" AS String?);
 $const_143
= CAST("0" AS Decimal(7, 0));
 $const_144
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_145
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x35" AS String?);
 $const_146
= CAST("0" AS Decimal(7, 0));
 $const_147
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_148
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x53" AS String?);
 $const_149
= CAST("0" AS Decimal(7, 0));
 $const_150
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_151
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x5b" AS String?);
 $const_152
= CAST("0" AS Decimal(7, 0));
 $const_153
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_154
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x78\x5c\x32\x34\x30" AS String?);
 $const_155
= CAST("0" AS Decimal(7, 0));
 $const_156
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_157
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x28" AS String?);
 $const_158
= CAST("0" AS Decimal(7, 0));
 $const_159
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_160
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x27" AS String?);
 $const_161
= CAST("0" AS Decimal(7, 0));
 $const_162
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_163
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x31\x32\x5c\x32\x30\x32" AS String?);
 $const_164
= CAST("0" AS Decimal(7, 0));
 $const_165
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_166
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x30\x37" AS String?);
 $const_167
= CAST("0" AS Decimal(7, 0));
 $const_168
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_169
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x7d" AS String?);
 $const_170
= CAST("0" AS Decimal(7, 0));
 $const_171
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_172
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x7e" AS String?);
 $const_173
= CAST("0" AS Decimal(7, 0));
 $const_174
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_175
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x5c\x32\x31\x37" AS String?);
 $const_176
= CAST("0" AS Decimal(7, 0));
 $const_177
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_178
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x5c\x32\x37\x32" AS String?);
 $const_179
= CAST("0" AS Decimal(7, 0));
 $const_180
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_181
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x25" AS String?);
 $const_182
= CAST("0" AS Decimal(7, 0));
 $const_183
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_184
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x34" AS String?);
 $const_185
= CAST("0" AS Decimal(7, 0));
 $const_186
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_187
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x31\x31" AS String?);
 $const_188
= CAST("0" AS Decimal(7, 0));
 $const_189
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_190
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x3a" AS String?);
 $const_191
= CAST("0" AS Decimal(7, 0));
 $const_192
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_193
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x73" AS String?);
 $const_194
= CAST("0" AS Decimal(7, 0));
 $const_195
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_196
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x2b\x5c\x33\x32\x33" AS String?);
 $const_197
= CAST("0" AS Decimal(7, 0));
 $const_198
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_199
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x36\x31\x5c\x30\x33\x30" AS String?);
 $const_200
= CAST("0" AS Decimal(7, 0));
 $const_201
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_202
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x5c\x30\x33\x35" AS String?);
 $const_203
= CAST("0" AS Decimal(7, 0));
 $const_204
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_205
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x30\x35" AS String?);
 $const_206
= CAST("0" AS Decimal(7, 0));
 $const_207
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_208
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x76" AS String?);
 $const_209
= CAST("0" AS Decimal(7, 0));
 $const_210
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_211
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x34\x32" AS String?);
 $const_212
= CAST("0" AS Decimal(7, 0));
 $const_213
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_214
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x48" AS String?);
 $const_215
= CAST("0" AS Decimal(7, 0));
 $const_216
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_217
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x34\x33" AS String?);
 $const_218
= CAST("0" AS Decimal(7, 0));
 $const_219
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_220
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x36\x31\x5c\x30\x33\x37" AS String?);
 $const_221
= CAST("0" AS Decimal(7, 0));
 $const_222
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_223
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x20\x4e" AS String?);
 $const_224
= CAST("0" AS Decimal(7, 0));
 $const_225
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_226
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x31" AS String?);
 $const_227
= CAST("0" AS Decimal(7, 0));
 $const_228
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_229
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x34" AS String?);
 $const_230
= CAST("0" AS Decimal(7, 0));
 $const_231
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_232
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x50" AS String?);
 $const_233
= CAST("0" AS Decimal(7, 0));
 $const_234
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_235
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x38" AS String?);
 $const_236
= CAST("0" AS Decimal(7, 0));
 $const_237
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_238
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x3c" AS String?);
 $const_239
= CAST("0" AS Decimal(7, 0));
 $const_240
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_241
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x36\x31\x5c\x30\x33\x34" AS String?);
 $const_242
= CAST("0" AS Decimal(7, 0));
 $const_243
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_244
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x32\x35\x34\x5c\x33\x30\x35" AS String?);
 $const_245
= CAST("0" AS Decimal(7, 0));
 $const_246
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_247
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x39" AS String?);
 $const_248
= CAST("0" AS Decimal(7, 0));
 $const_249
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_250
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x3e" AS String?);
 $const_251
= CAST("0" AS Decimal(7, 0));
 $const_252
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_253
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x5c\x30\x32\x35\x3b" AS String?);
 $const_254
= CAST("0" AS Decimal(7, 0));
 $const_255
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_256
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x33\x35" AS String?);
 $const_257
= CAST("0" AS Decimal(7, 0));
 $const_258
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_259
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x32" AS String?);
 $const_260
= CAST("0" AS Decimal(7, 0));
 $const_261
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_262
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x32\x31\x5c\x32\x34\x36" AS String?);
 $const_263
= CAST("0" AS Decimal(7, 0));
 $const_264
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_265
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x32\x31\x5c\x32\x34\x35" AS String?);
 $const_266
= CAST("0" AS Decimal(7, 0));
 $const_267
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_268
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x31\x24\x5c\x32\x30\x36" AS String?);
 $const_269
= CAST("0" AS Decimal(7, 0));
 $const_270
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_271
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x4b" AS String?);
 $const_272
= CAST("0" AS Decimal(7, 0));
 $const_273
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_274
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x29" AS String?);
 $const_275
= CAST("0" AS Decimal(7, 0));
 $const_276
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_277
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x4c" AS String?);
 $const_278
= CAST("0" AS Decimal(7, 0));
 $const_279
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_280
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x42" AS String?);
 $const_281
= CAST("0" AS Decimal(7, 0));
 $const_282
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_283
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x5a" AS String?);
 $const_284
= CAST("0" AS Decimal(7, 0));
 $const_285
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_286
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x5c\x30\x33\x36" AS String?);
 $const_287
= CAST("0" AS Decimal(7, 0));
 $const_288
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_289
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x5c\x33\x33\x37\x5c\x32\x33\x37" AS String?);
 $const_290
= CAST("0" AS Decimal(7, 0));
 $const_291
= CAST("\x5c\x30\x31\x30" AS String?);
 $const_292
= CAST("\x5c\x30\x30\x30\x5c\x30\x30\x30\x57\x30" AS String?);


SELECT `t1`.`_q_000_f_000_type`,
       `t6`.`_fld56959`,
       `t6`.`_fld56959_upper`,
       `t2`.`_fld56948_type`,
       `t2`.`_fld56948_rtref`,
       `t8`.`_fld57526rref`,
       `t10`.`_fld71031rref`,
       `t12`.`_fld28655rref`,
       `t14`.`_fld32490rref`,
       `t16`.`_fld56136rref`,
       `t18`.`_fld57551rref`,
       `t20`.`_fld22889rref`,
       `t22`.`_fld23710rref`,
       `t24`.`_fld71071rref`,
       `t26`.`_fld46735rref`,
       `t28`.`_fld30918rref`,
       `t30`.`_fld28755rref`,
       `t32`.`_fld45797rref`,
       `t34`.`_fld24528rref`,
       `t36`.`_fld71227rref`,
       `t38`.`_fld75120rref`,
       `t40`.`_fld75142rref`,
       `t42`.`_fld57430rref`,
       `t44`.`_fld23756rref`,
       `t46`.`_fld23540rref`,
       `t48`.`_fld69090rref`,
       `t50`.`_fld23586rref`,
       `t52`.`_fld75023rref`,
       `t54`.`_fld57320rref`,
       `t56`.`_fld28309rref`,
       `t58`.`_fld46957rref`,
       `t60`.`_fld72490rref`,
       `t62`.`_fld24247rref`,
       `t64`.`_fld23074rref`,
       `t66`.`_fld46283rref`,
       `t68`.`_fld71266rref`,
       `t70`.`_fld48516rref`,
       `t72`.`_fld75201rref`,
       `t74`.`_fld71188rref`,
       `t76`.`_fld57454rref`,
       `t78`.`_fld73916rref`,
       `t80`.`_fld75084rref`,
       `t82`.`_fld57382rref`,
       `t84`.`_fld22981rref`,
       `t86`.`_fld75178rref`,
       `t88`.`_fld24387rref`,
       `t90`.`_fld24104rref`,
       `t92`.`_fld73955rref`,
       `t94`.`_fld71149rref`,
       `t96`.`_fld24341rref`,
       `t98`.`_fld24663rref`,
       `t100`.`_fld30981rref`,
       `t102`.`_fld22645rref`,
       `t104`.`_fld22599rref`,
       `t106`.`_fld68232rref`,
       `t108`.`_fld74984rref`,
       `t110`.`_fld46021rref`,
       `t112`.`_fld46059rref`,
       `t114`.`_fld46697rref`,
       `t116`.`_fld48220rref`,
       `t118`.`_fld22551rref`,
       `t120`.`_fld23121rref`,
       `t122`.`_fld75062rref`,
       `t124`.`_fld71345rref`,
       `t126`.`_fld45647rref`,
       `t128`.`_fld76767rref`,
       `t130`.`_fld62179rref`,
       `t132`.`_fld22427rref`,
       `t134`.`_fld74909rref`,
       `t136`.`_fld45759rref`,
       `t138`.`_fld57478rref`,
       `t140`.`_fld23937rref`,
       `t142`.`_fld57502rref`,
       `t144`.`_fld62350rref`,
       `t146`.`_fld73855rref`,
       `t148`.`_fld70992rref`,
       `t150`.`_fld71110rref`,
       `t152`.`_fld24201rref`,
       `t154`.`_fld23268rref`,
       `t156`.`_fld71424rref`,
       `t158`.`_fld62267rref`,
       `t160`.`_fld48630rref`,
       `t162`.`_fld71305rref`,
       `t164`.`_fld23494rref`,
       `t166`.`_fld71385rref`,
       `t168`.`_fld57358rref`,
       `t170`.`_fld23028rref`,
       `t172`.`_fld53873rref`,
       `t174`.`_fld53851rref`,
       `t176`.`_fld74945rref`,
       `t178`.`_fld24012rref`,
       `t180`.`_fld22691rref`,
       `t182`.`_fld24058rref`,
       `t184`.`_fld23664rref`,
       `t186`.`_fld24617rref`,
       `t188`.`_fld22473rref`,
       `t190`.`_fld57406rref`,
       `t192`.`_fld22935rref`,
       `t2`.`_fld56950rref`,
       `t2`.`_fld56951`,
       `t6`.`_fld56958_type`,
       `t6`.`_fld56958_rtref`,
       `t6`.`_fld56958_rrref`,
       `t6`.`_fld56960`,
       `t6`.`_fld56962`,
       `t6`.`_fld56962_upper`,
       `t6`.`_fld56963rref`,
       `t6`.`_fld56964rref`,
       `t6`.`_fld56965rref`,
       `t6`.`_fld56966`,
       `t6`.`_fld56966_upper`,
       `t6`.`_fld56967`
FROM (SELECT `t1_src`.`_q_000_f_000_type`, `t1_src`.`_q_000_f_001_type`, `t1_src`.`_q_000_f_002`, `t1_src`.`_q_000_f_002_upper`, `t1_src`.`_q_000_f_003`, `t1_src`.`_q_000_f_003_upper`, `t1_src`.`_q_000_f_004`, `t1_src`.`_q_000_f_005`, `t1_src`.`_ydb_pk`,
             (CASE WHEN `t1_src`.`_q_000_f_000_type` IS NOT NULL THEN $const_4 ELSE $const_5 END)   AS `join_const_2`,
             (CASE WHEN `t1_src`.`_q_000_f_000_type` IS NOT NULL THEN $const_6 ELSE $const_7 END)   AS `join_const_3`,
             (CASE WHEN `t1_src`.`_q_000_f_000_type` IS NOT NULL THEN $const_8 ELSE $const_9 END)   AS `join_const_0`,
             (CASE WHEN `t1_src`.`_q_000_f_000_type` IS NOT NULL THEN $const_10 ELSE $const_11 END) AS `join_const_1`
      FROM `_temp/_pool_19967` AS `t1_src`) AS `t1`
         INNER JOIN (SELECT *
                     FROM `public/_inforg56947` AS `t2_src`
                     WHERE (`t2_src`.`_fld56950rref` IN $const_0)
                       AND (`t2_src`.`_fld56950rref` IN $const_1)
                       AND ((`t2_src`.`_fld56949rref` = $const_2))
                       AND ((`t2_src`.`_fld543` = $const_3))) AS `t2`
                    ON ((`t1`.`_q_000_f_000_type` = `t2`.`_fld56948_type`)) AND
                       (`t1`.`join_const_0` = `t2`.`_fld56948_rtref`) AND
                       (`t1`.`join_const_1` = `t2`.`_fld56948_rrref`) LEFT JOIN (SELECT * FROM `public/_inforg56957` AS `t6_src` WHERE ((`t6_src`.`_fld543` = $const_13))) AS `t6` ON ((`t1`.`_q_000_f_000_type` = `t6`.`_fld56958_type`)) AND (`t1`.`join_const_2` = `t6`.`_fld56958_rtref`) AND (`t1`.`join_const_3` = `t6`.`_fld56958_rrref`) LEFT JOIN (SELECT `t8_src`.`_fld543`, `t8_src`.`_fld57526rref`, `t8_src`.`_idrref`, $const_15 AS `join_const_4`, $const_16 AS `join_const_5` FROM `public/_reference57252` AS `t8_src` WHERE ((`t8_src`.`_fld543` = $const_14))) AS `t8` ON (`t2`.`_fld56948_type` = `t8`.`join_const_4`) AND (`t2`.`_fld56948_rtref` = `t8`.`join_const_5`) AND ((`t2`.`_fld56948_rrref` = `t8`.`_idrref`)) LEFT JOIN (SELECT `t10_src`.`_fld543`, `t10_src`.`_fld71031rref`, `t10_src`.`_idrref`, $const_18 AS `join_const_6`, $const_19 AS `join_const_7` FROM `public/_reference70962` AS `t10_src` WHERE ((`t10_src`.`_fld543` = $const_17))) AS `t10` ON (`t2`.`_fld56948_type` = `t10`.`join_const_6`) AND (`t2`.`_fld56948_rtref` = `t10`.`join_const_7`) AND ((`t2`.`_fld56948_rrref` = `t10`.`_idrref`)) LEFT JOIN (SELECT `t12_src`.`_fld28655rref`, `t12_src`.`_fld543`, `t12_src`.`_idrref`, $const_21 AS `join_const_8`, $const_22 AS `join_const_9` FROM `public/_reference28651` AS `t12_src` WHERE ((`t12_src`.`_fld543` = $const_20))) AS `t12` ON (`t2`.`_fld56948_type` = `t12`.`join_const_8`) AND (`t2`.`_fld56948_rtref` = `t12`.`join_const_9`) AND ((`t2`.`_fld56948_rrref` = `t12`.`_idrref`)) LEFT JOIN (SELECT `t14_src`.`_fld32490rref`, `t14_src`.`_fld543`, `t14_src`.`_idrref`, $const_24 AS `join_const_10`, $const_25 AS `join_const_11` FROM `public/_reference32486` AS `t14_src` WHERE ((`t14_src`.`_fld543` = $const_23))) AS `t14` ON (`t2`.`_fld56948_type` = `t14`.`join_const_10`) AND (`t2`.`_fld56948_rtref` = `t14`.`join_const_11`) AND ((`t2`.`_fld56948_rrref` = `t14`.`_idrref`)) LEFT JOIN (SELECT `t16_src`.`_fld543`, `t16_src`.`_fld56136rref`, `t16_src`.`_idrref`, $const_27 AS `join_const_12`, $const_28 AS `join_const_13` FROM `public/_reference55755` AS `t16_src` WHERE ((`t16_src`.`_fld543` = $const_26))) AS `t16` ON (`t2`.`_fld56948_type` = `t16`.`join_const_12`) AND (`t2`.`_fld56948_rtref` = `t16`.`join_const_13`) AND ((`t2`.`_fld56948_rrref` = `t16`.`_idrref`)) LEFT JOIN (SELECT `t18_src`.`_fld543`, `t18_src`.`_fld57551rref`, `t18_src`.`_idrref`, $const_30 AS `join_const_14`, $const_31 AS `join_const_15` FROM `public/_reference57253` AS `t18_src` WHERE ((`t18_src`.`_fld543` = $const_29))) AS `t18` ON (`t2`.`_fld56948_type` = `t18`.`join_const_14`) AND (`t2`.`_fld56948_rtref` = `t18`.`join_const_15`) AND ((`t2`.`_fld56948_rrref` = `t18`.`_idrref`)) LEFT JOIN (SELECT `t20_src`.`_fld22889rref`, `t20_src`.`_fld543`, `t20_src`.`_idrref`, $const_33 AS `join_const_16`, $const_34 AS `join_const_17` FROM `public/_reference22319` AS `t20_src` WHERE ((`t20_src`.`_fld543` = $const_32))) AS `t20` ON (`t2`.`_fld56948_type` = `t20`.`join_const_16`) AND (`t2`.`_fld56948_rtref` = `t20`.`join_const_17`) AND ((`t2`.`_fld56948_rrref` = `t20`.`_idrref`)) LEFT JOIN (SELECT `t22_src`.`_fld23710rref`, `t22_src`.`_fld543`, `t22_src`.`_idrref`, $const_36 AS `join_const_18`, $const_37 AS `join_const_19` FROM `public/_reference22339` AS `t22_src` WHERE ((`t22_src`.`_fld543` = $const_35))) AS `t22` ON (`t2`.`_fld56948_type` = `t22`.`join_const_18`) AND (`t2`.`_fld56948_rtref` = `t22`.`join_const_19`) AND ((`t2`.`_fld56948_rrref` = `t22`.`_idrref`)) LEFT JOIN (SELECT `t24_src`.`_fld543`, `t24_src`.`_fld71071rref`, `t24_src`.`_idrref`, $const_39 AS `join_const_20`, $const_40 AS `join_const_21` FROM `public/_reference70963` AS `t24_src` WHERE ((`t24_src`.`_fld543` = $const_38))) AS `t24` ON (`t2`.`_fld56948_type` = `t24`.`join_const_20`) AND (`t2`.`_fld56948_rtref` = `t24`.`join_const_21`) AND ((`t2`.`_fld56948_rrref` = `t24`.`_idrref`)) LEFT JOIN (SELECT `t26_src`.`_fld46735rref`, `t26_src`.`_fld543`, `t26_src`.`_idrref`, $const_42 AS `join_const_22`, $const_43 AS `join_const_23` FROM `public/_reference44176` AS `t26_src` WHERE ((`t26_src`.`_fld543` = $const_41))) AS `t26` ON (`t2`.`_fld56948_type` = `t26`.`join_const_22`) AND (`t2`.`_fld56948_rtref` = `t26`.`join_const_23`) AND ((`t2`.`_fld56948_rrref` = `t26`.`_idrref`)) LEFT JOIN (SELECT `t28_src`.`_fld30918rref`, `t28_src`.`_fld543`, `t28_src`.`_idrref`, $const_45 AS `join_const_24`, $const_46 AS `join_const_25` FROM `public/_reference30877` AS `t28_src` WHERE ((`t28_src`.`_fld543` = $const_44))) AS `t28` ON (`t2`.`_fld56948_type` = `t28`.`join_const_24`) AND (`t2`.`_fld56948_rtref` = `t28`.`join_const_25`) AND ((`t2`.`_fld56948_rrref` = `t28`.`_idrref`)) LEFT JOIN (SELECT `t30_src`.`_fld28755rref`, `t30_src`.`_fld543`, `t30_src`.`_idrref`, $const_48 AS `join_const_26`, $const_49 AS `join_const_27` FROM `public/_reference28747` AS `t30_src` WHERE ((`t30_src`.`_fld543` = $const_47))) AS `t30` ON (`t2`.`_fld56948_type` = `t30`.`join_const_26`) AND (`t2`.`_fld56948_rtref` = `t30`.`join_const_27`) AND ((`t2`.`_fld56948_rrref` = `t30`.`_idrref`)) LEFT JOIN (SELECT `t32_src`.`_fld45797rref`, `t32_src`.`_fld543`, `t32_src`.`_idrref`, $const_51 AS `join_const_28`, $const_52 AS `join_const_29` FROM `public/_reference44151` AS `t32_src` WHERE ((`t32_src`.`_fld543` = $const_50))) AS `t32` ON (`t2`.`_fld56948_type` = `t32`.`join_const_28`) AND (`t2`.`_fld56948_rtref` = `t32`.`join_const_29`) AND ((`t2`.`_fld56948_rrref` = `t32`.`_idrref`)) LEFT JOIN (SELECT `t34_src`.`_fld24528rref`, `t34_src`.`_fld543`, `t34_src`.`_idrref`, $const_54 AS `join_const_30`, $const_55 AS `join_const_31` FROM `public/_reference22360` AS `t34_src` WHERE ((`t34_src`.`_fld543` = $const_53))) AS `t34` ON (`t2`.`_fld56948_type` = `t34`.`join_const_30`) AND (`t2`.`_fld56948_rtref` = `t34`.`join_const_31`) AND ((`t2`.`_fld56948_rrref` = `t34`.`_idrref`)) LEFT JOIN (SELECT `t36_src`.`_fld543`, `t36_src`.`_fld71227rref`, `t36_src`.`_idrref`, $const_57 AS `join_const_32`, $const_58 AS `join_const_33` FROM `public/_reference70967` AS `t36_src` WHERE ((`t36_src`.`_fld543` = $const_56))) AS `t36` ON (`t2`.`_fld56948_type` = `t36`.`join_const_32`) AND (`t2`.`_fld56948_rtref` = `t36`.`join_const_33`) AND ((`t2`.`_fld56948_rrref` = `t36`.`_idrref`)) LEFT JOIN (SELECT `t38_src`.`_fld543`, `t38_src`.`_fld75120rref`, `t38_src`.`_idrref`, $const_60 AS `join_const_34`, $const_61 AS `join_const_35` FROM `public/_reference74891` AS `t38_src` WHERE ((`t38_src`.`_fld543` = $const_59))) AS `t38` ON (`t2`.`_fld56948_type` = `t38`.`join_const_34`) AND (`t2`.`_fld56948_rtref` = `t38`.`join_const_35`) AND ((`t2`.`_fld56948_rrref` = `t38`.`_idrref`)) LEFT JOIN (SELECT `t40_src`.`_fld543`, `t40_src`.`_fld75142rref`, `t40_src`.`_idrref`, $const_63 AS `join_const_36`, $const_64 AS `join_const_37` FROM `public/_reference74892` AS `t40_src` WHERE ((`t40_src`.`_fld543` = $const_62))) AS `t40` ON (`t2`.`_fld56948_type` = `t40`.`join_const_36`) AND (`t2`.`_fld56948_rtref` = `t40`.`join_const_37`) AND ((`t2`.`_fld56948_rrref` = `t40`.`_idrref`)) LEFT JOIN (SELECT `t42_src`.`_fld543`, `t42_src`.`_fld57430rref`, `t42_src`.`_idrref`, $const_66 AS `join_const_38`, $const_67 AS `join_const_39` FROM `public/_reference57248` AS `t42_src` WHERE ((`t42_src`.`_fld543` = $const_65))) AS `t42` ON (`t2`.`_fld56948_type` = `t42`.`join_const_38`) AND (`t2`.`_fld56948_rtref` = `t42`.`join_const_39`) AND ((`t2`.`_fld56948_rrref` = `t42`.`_idrref`)) LEFT JOIN (SELECT `t44_src`.`_fld23756rref`, `t44_src`.`_fld543`, `t44_src`.`_idrref`, $const_69 AS `join_const_40`, $const_70 AS `join_const_41` FROM `public/_reference22340` AS `t44_src` WHERE ((`t44_src`.`_fld543` = $const_68))) AS `t44` ON (`t2`.`_fld56948_type` = `t44`.`join_const_40`) AND (`t2`.`_fld56948_rtref` = `t44`.`join_const_41`) AND ((`t2`.`_fld56948_rrref` = `t44`.`_idrref`)) LEFT JOIN (SELECT `t46_src`.`_fld23540rref`, `t46_src`.`_fld543`, `t46_src`.`_idrref`, $const_72 AS `join_const_42`, $const_73 AS `join_const_43` FROM `public/_reference22335` AS `t46_src` WHERE ((`t46_src`.`_fld543` = $const_71))) AS `t46` ON (`t2`.`_fld56948_type` = `t46`.`join_const_42`) AND (`t2`.`_fld56948_rtref` = `t46`.`join_const_43`) AND ((`t2`.`_fld56948_rrref` = `t46`.`_idrref`)) LEFT JOIN (SELECT `t48_src`.`_fld543`, `t48_src`.`_fld69090rref`, `t48_src`.`_idrref`, $const_75 AS `join_const_44`, $const_76 AS `join_const_45` FROM `public/_reference68848` AS `t48_src` WHERE ((`t48_src`.`_fld543` = $const_74))) AS `t48` ON (`t2`.`_fld56948_type` = `t48`.`join_const_44`) AND (`t2`.`_fld56948_rtref` = `t48`.`join_const_45`) AND ((`t2`.`_fld56948_rrref` = `t48`.`_idrref`)) LEFT JOIN (SELECT `t50_src`.`_fld23586rref`, `t50_src`.`_fld543`, `t50_src`.`_idrref`, $const_78 AS `join_const_46`, $const_79 AS `join_const_47` FROM `public/_reference22336` AS `t50_src` WHERE ((`t50_src`.`_fld543` = $const_77))) AS `t50` ON (`t2`.`_fld56948_type` = `t50`.`join_const_46`) AND (`t2`.`_fld56948_rtref` = `t50`.`join_const_47`) AND ((`t2`.`_fld56948_rrref` = `t50`.`_idrref`)) LEFT JOIN (SELECT `t52_src`.`_fld543`, `t52_src`.`_fld75023rref`, `t52_src`.`_idrref`, $const_81 AS `join_const_48`, $const_82 AS `join_const_49` FROM `public/_reference74888` AS `t52_src` WHERE ((`t52_src`.`_fld543` = $const_80))) AS `t52` ON (`t2`.`_fld56948_type` = `t52`.`join_const_48`) AND (`t2`.`_fld56948_rtref` = `t52`.`join_const_49`) AND ((`t2`.`_fld56948_rrref` = `t52`.`_idrref`)) LEFT JOIN (SELECT `t54_src`.`_fld543`, `t54_src`.`_fld57320rref`, `t54_src`.`_idrref`, $const_84 AS `join_const_50`, $const_85 AS `join_const_51` FROM `public/_reference57244` AS `t54_src` WHERE ((`t54_src`.`_fld543` = $const_83))) AS `t54` ON (`t2`.`_fld56948_type` = `t54`.`join_const_50`) AND (`t2`.`_fld56948_rtref` = `t54`.`join_const_51`) AND ((`t2`.`_fld56948_rrref` = `t54`.`_idrref`)) LEFT JOIN (SELECT `t56_src`.`_fld28309rref`, `t56_src`.`_fld543`, `t56_src`.`_idrref`, $const_87 AS `join_const_52`, $const_88 AS `join_const_53` FROM `public/_reference28307` AS `t56_src` WHERE ((`t56_src`.`_fld543` = $const_86))) AS `t56` ON (`t2`.`_fld56948_type` = `t56`.`join_const_52`) AND (`t2`.`_fld56948_rtref` = `t56`.`join_const_53`) AND ((`t2`.`_fld56948_rrref` = `t56`.`_idrref`)) LEFT JOIN (SELECT `t58_src`.`_fld46957rref`, `t58_src`.`_fld543`, `t58_src`.`_idrref`, $const_90 AS `join_const_54`, $const_91 AS `join_const_55` FROM `public/_reference44182` AS `t58_src` WHERE ((`t58_src`.`_fld543` = $const_89))) AS `t58` ON (`t2`.`_fld56948_type` = `t58`.`join_const_54`) AND (`t2`.`_fld56948_rtref` = `t58`.`join_const_55`) AND ((`t2`.`_fld56948_rrref` = `t58`.`_idrref`)) LEFT JOIN (SELECT `t60_src`.`_fld543`, `t60_src`.`_fld72490rref`, `t60_src`.`_idrref`, $const_93 AS `join_const_56`, $const_94 AS `join_const_57` FROM `public/_reference72456` AS `t60_src` WHERE ((`t60_src`.`_fld543` = $const_92))) AS `t60` ON (`t2`.`_fld56948_type` = `t60`.`join_const_56`) AND (`t2`.`_fld56948_rtref` = `t60`.`join_const_57`) AND ((`t2`.`_fld56948_rrref` = `t60`.`_idrref`)) LEFT JOIN (SELECT `t62_src`.`_fld24247rref`, `t62_src`.`_fld543`, `t62_src`.`_idrref`, $const_96 AS `join_const_58`, $const_97 AS `join_const_59` FROM `public/_reference22353` AS `t62_src` WHERE ((`t62_src`.`_fld543` = $const_95))) AS `t62` ON (`t2`.`_fld56948_type` = `t62`.`join_const_58`) AND (`t2`.`_fld56948_rtref` = `t62`.`join_const_59`) AND ((`t2`.`_fld56948_rrref` = `t62`.`_idrref`)) LEFT JOIN (SELECT `t64_src`.`_fld23074rref`, `t64_src`.`_fld543`, `t64_src`.`_idrref`, $const_99 AS `join_const_60`, $const_100 AS `join_const_61` FROM `public/_reference22323` AS `t64_src` WHERE ((`t64_src`.`_fld543` = $const_98))) AS `t64` ON (`t2`.`_fld56948_type` = `t64`.`join_const_60`) AND (`t2`.`_fld56948_rtref` = `t64`.`join_const_61`) AND ((`t2`.`_fld56948_rrref` = `t64`.`_idrref`)) LEFT JOIN (SELECT `t66_src`.`_fld46283rref`, `t66_src`.`_fld543`, `t66_src`.`_idrref`, $const_102 AS `join_const_62`, $const_103 AS `join_const_63` FROM `public/_reference44164` AS `t66_src` WHERE ((`t66_src`.`_fld543` = $const_101))) AS `t66` ON (`t2`.`_fld56948_type` = `t66`.`join_const_62`) AND (`t2`.`_fld56948_rtref` = `t66`.`join_const_63`) AND ((`t2`.`_fld56948_rrref` = `t66`.`_idrref`)) LEFT JOIN (SELECT `t68_src`.`_fld543`, `t68_src`.`_fld71266rref`, `t68_src`.`_idrref`, $const_105 AS `join_const_64`, $const_106 AS `join_const_65` FROM `public/_reference70968` AS `t68_src` WHERE ((`t68_src`.`_fld543` = $const_104))) AS `t68` ON (`t2`.`_fld56948_type` = `t68`.`join_const_64`) AND (`t2`.`_fld56948_rtref` = `t68`.`join_const_65`) AND ((`t2`.`_fld56948_rrref` = `t68`.`_idrref`)) LEFT JOIN (SELECT `t70_src`.`_fld48516rref`, `t70_src`.`_fld543`, `t70_src`.`_idrref`, $const_108 AS `join_const_66`, $const_109 AS `join_const_67` FROM `public/_reference44226` AS `t70_src` WHERE ((`t70_src`.`_fld543` = $const_107))) AS `t70` ON (`t2`.`_fld56948_type` = `t70`.`join_const_66`) AND (`t2`.`_fld56948_rtref` = `t70`.`join_const_67`) AND ((`t2`.`_fld56948_rrref` = `t70`.`_idrref`)) LEFT JOIN (SELECT `t72_src`.`_fld543`, `t72_src`.`_fld75201rref`, `t72_src`.`_idrref`, $const_111 AS `join_const_68`, $const_112 AS `join_const_69` FROM `public/_reference74894` AS `t72_src` WHERE ((`t72_src`.`_fld543` = $const_110))) AS `t72` ON (`t2`.`_fld56948_type` = `t72`.`join_const_68`) AND (`t2`.`_fld56948_rtref` = `t72`.`join_const_69`) AND ((`t2`.`_fld56948_rrref` = `t72`.`_idrref`)) LEFT JOIN (SELECT `t74_src`.`_fld543`, `t74_src`.`_fld71188rref`, `t74_src`.`_idrref`, $const_114 AS `join_const_70`, $const_115 AS `join_const_71` FROM `public/_reference70966` AS `t74_src` WHERE ((`t74_src`.`_fld543` = $const_113))) AS `t74` ON (`t2`.`_fld56948_type` = `t74`.`join_const_70`) AND (`t2`.`_fld56948_rtref` = `t74`.`join_const_71`) AND ((`t2`.`_fld56948_rrref` = `t74`.`_idrref`)) LEFT JOIN (SELECT `t76_src`.`_fld543`, `t76_src`.`_fld57454rref`, `t76_src`.`_idrref`, $const_117 AS `join_const_72`, $const_118 AS `join_const_73` FROM `public/_reference57249` AS `t76_src` WHERE ((`t76_src`.`_fld543` = $const_116))) AS `t76` ON (`t2`.`_fld56948_type` = `t76`.`join_const_72`) AND (`t2`.`_fld56948_rtref` = `t76`.`join_const_73`) AND ((`t2`.`_fld56948_rrref` = `t76`.`_idrref`)) LEFT JOIN (SELECT `t78_src`.`_fld543`, `t78_src`.`_fld73916rref`, `t78_src`.`_idrref`, $const_120 AS `join_const_74`, $const_121 AS `join_const_75` FROM `public/_reference73808` AS `t78_src` WHERE ((`t78_src`.`_fld543` = $const_119))) AS `t78` ON (`t2`.`_fld56948_type` = `t78`.`join_const_74`) AND (`t2`.`_fld56948_rtref` = `t78`.`join_const_75`) AND ((`t2`.`_fld56948_rrref` = `t78`.`_idrref`)) LEFT JOIN (SELECT `t80_src`.`_fld543`, `t80_src`.`_fld75084rref`, `t80_src`.`_idrref`, $const_123 AS `join_const_76`, $const_124 AS `join_const_77` FROM `public/_reference74890` AS `t80_src` WHERE ((`t80_src`.`_fld543` = $const_122))) AS `t80` ON (`t2`.`_fld56948_type` = `t80`.`join_const_76`) AND (`t2`.`_fld56948_rtref` = `t80`.`join_const_77`) AND ((`t2`.`_fld56948_rrref` = `t80`.`_idrref`)) LEFT JOIN (SELECT `t82_src`.`_fld543`, `t82_src`.`_fld57382rref`, `t82_src`.`_idrref`, $const_126 AS `join_const_78`, $const_127 AS `join_const_79` FROM `public/_reference57246` AS `t82_src` WHERE ((`t82_src`.`_fld543` = $const_125))) AS `t82` ON (`t2`.`_fld56948_type` = `t82`.`join_const_78`) AND (`t2`.`_fld56948_rtref` = `t82`.`join_const_79`) AND ((`t2`.`_fld56948_rrref` = `t82`.`_idrref`)) LEFT JOIN (SELECT `t84_src`.`_fld22981rref`, `t84_src`.`_fld543`, `t84_src`.`_idrref`, $const_129 AS `join_const_80`, $const_130 AS `join_const_81` FROM `public/_reference22321` AS `t84_src` WHERE ((`t84_src`.`_fld543` = $const_128))) AS `t84` ON (`t2`.`_fld56948_type` = `t84`.`join_const_80`) AND (`t2`.`_fld56948_rtref` = `t84`.`join_const_81`) AND ((`t2`.`_fld56948_rrref` = `t84`.`_idrref`)) LEFT JOIN (SELECT `t86_src`.`_fld543`, `t86_src`.`_fld75178rref`, `t86_src`.`_idrref`, $const_132 AS `join_const_82`, $const_133 AS `join_const_83` FROM `public/_reference74893` AS `t86_src` WHERE ((`t86_src`.`_fld543` = $const_131))) AS `t86` ON (`t2`.`_fld56948_type` = `t86`.`join_const_82`) AND (`t2`.`_fld56948_rtref` = `t86`.`join_const_83`) AND ((`t2`.`_fld56948_rrref` = `t86`.`_idrref`)) LEFT JOIN (SELECT `t88_src`.`_fld24387rref`, `t88_src`.`_fld543`, `t88_src`.`_idrref`, $const_135 AS `join_const_84`, $const_136 AS `join_const_85` FROM `public/_reference22356` AS `t88_src` WHERE ((`t88_src`.`_fld543` = $const_134))) AS `t88` ON (`t2`.`_fld56948_type` = `t88`.`join_const_84`) AND (`t2`.`_fld56948_rtref` = `t88`.`join_const_85`) AND ((`t2`.`_fld56948_rrref` = `t88`.`_idrref`)) LEFT JOIN (SELECT `t90_src`.`_fld24104rref`, `t90_src`.`_fld543`, `t90_src`.`_idrref`, $const_138 AS `join_const_86`, $const_139 AS `join_const_87` FROM `public/_reference22349` AS `t90_src` WHERE ((`t90_src`.`_fld543` = $const_137))) AS `t90` ON (`t2`.`_fld56948_type` = `t90`.`join_const_86`) AND (`t2`.`_fld56948_rtref` = `t90`.`join_const_87`) AND ((`t2`.`_fld56948_rrref` = `t90`.`_idrref`)) LEFT JOIN (SELECT `t92_src`.`_fld543`, `t92_src`.`_fld73955rref`, `t92_src`.`_idrref`, $const_141 AS `join_const_88`, $const_142 AS `join_const_89` FROM `public/_reference73809` AS `t92_src` WHERE ((`t92_src`.`_fld543` = $const_140))) AS `t92` ON (`t2`.`_fld56948_type` = `t92`.`join_const_88`) AND (`t2`.`_fld56948_rtref` = `t92`.`join_const_89`) AND ((`t2`.`_fld56948_rrref` = `t92`.`_idrref`)) LEFT JOIN (SELECT `t94_src`.`_fld543`, `t94_src`.`_fld71149rref`, `t94_src`.`_idrref`, $const_144 AS `join_const_90`, $const_145 AS `join_const_91` FROM `public/_reference70965` AS `t94_src` WHERE ((`t94_src`.`_fld543` = $const_143))) AS `t94` ON (`t2`.`_fld56948_type` = `t94`.`join_const_90`) AND (`t2`.`_fld56948_rtref` = `t94`.`join_const_91`) AND ((`t2`.`_fld56948_rrref` = `t94`.`_idrref`)) LEFT JOIN (SELECT `t96_src`.`_fld24341rref`, `t96_src`.`_fld543`, `t96_src`.`_idrref`, $const_147 AS `join_const_92`, $const_148 AS `join_const_93` FROM `public/_reference22355` AS `t96_src` WHERE ((`t96_src`.`_fld543` = $const_146))) AS `t96` ON (`t2`.`_fld56948_type` = `t96`.`join_const_92`) AND (`t2`.`_fld56948_rtref` = `t96`.`join_const_93`) AND ((`t2`.`_fld56948_rrref` = `t96`.`_idrref`)) LEFT JOIN (SELECT `t98_src`.`_fld24663rref`, `t98_src`.`_fld543`, `t98_src`.`_idrref`, $const_150 AS `join_const_94`, $const_151 AS `join_const_95` FROM `public/_reference22363` AS `t98_src` WHERE ((`t98_src`.`_fld543` = $const_149))) AS `t98` ON (`t2`.`_fld56948_type` = `t98`.`join_const_94`) AND (`t2`.`_fld56948_rtref` = `t98`.`join_const_95`) AND ((`t2`.`_fld56948_rrref` = `t98`.`_idrref`)) LEFT JOIN (SELECT `t100_src`.`_fld30981rref`, `t100_src`.`_fld543`, `t100_src`.`_idrref`, $const_153 AS `join_const_96`, $const_154 AS `join_const_97` FROM `public/_reference30880` AS `t100_src` WHERE ((`t100_src`.`_fld543` = $const_152))) AS `t100` ON (`t2`.`_fld56948_type` = `t100`.`join_const_96`) AND (`t2`.`_fld56948_rtref` = `t100`.`join_const_97`) AND ((`t2`.`_fld56948_rrref` = `t100`.`_idrref`)) LEFT JOIN (SELECT `t102_src`.`_fld22645rref`, `t102_src`.`_fld543`, `t102_src`.`_idrref`, $const_156 AS `join_const_98`, $const_157 AS `join_const_99` FROM `public/_reference22312` AS `t102_src` WHERE ((`t102_src`.`_fld543` = $const_155))) AS `t102` ON (`t2`.`_fld56948_type` = `t102`.`join_const_98`) AND (`t2`.`_fld56948_rtref` = `t102`.`join_const_99`) AND ((`t2`.`_fld56948_rrref` = `t102`.`_idrref`)) LEFT JOIN (SELECT `t104_src`.`_fld22599rref`, `t104_src`.`_fld543`, `t104_src`.`_idrref`, $const_159 AS `join_const_100`, $const_160 AS `join_const_101` FROM `public/_reference22311` AS `t104_src` WHERE ((`t104_src`.`_fld543` = $const_158))) AS `t104` ON (`t2`.`_fld56948_type` = `t104`.`join_const_100`) AND (`t2`.`_fld56948_rtref` = `t104`.`join_const_101`) AND ((`t2`.`_fld56948_rrref` = `t104`.`_idrref`)) LEFT JOIN (SELECT `t106_src`.`_fld543`, `t106_src`.`_fld68232rref`, `t106_src`.`_idrref`, $const_162 AS `join_const_102`, $const_163 AS `join_const_103` FROM `public/_reference68226` AS `t106_src` WHERE ((`t106_src`.`_fld543` = $const_161))) AS `t106` ON (`t2`.`_fld56948_type` = `t106`.`join_const_102`) AND (`t2`.`_fld56948_rtref` = `t106`.`join_const_103`) AND ((`t2`.`_fld56948_rrref` = `t106`.`_idrref`)) LEFT JOIN (SELECT `t108_src`.`_fld543`, `t108_src`.`_fld74984rref`, `t108_src`.`_idrref`, $const_165 AS `join_const_104`, $const_166 AS `join_const_105` FROM `public/_reference74887` AS `t108_src` WHERE ((`t108_src`.`_fld543` = $const_164))) AS `t108` ON (`t2`.`_fld56948_type` = `t108`.`join_const_104`) AND (`t2`.`_fld56948_rtref` = `t108`.`join_const_105`) AND ((`t2`.`_fld56948_rrref` = `t108`.`_idrref`)) LEFT JOIN (SELECT `t110_src`.`_fld46021rref`, `t110_src`.`_fld543`, `t110_src`.`_idrref`, $const_168 AS `join_const_106`, $const_169 AS `join_const_107` FROM `public/_reference44157` AS `t110_src` WHERE ((`t110_src`.`_fld543` = $const_167))) AS `t110` ON (`t2`.`_fld56948_type` = `t110`.`join_const_106`) AND (`t2`.`_fld56948_rtref` = `t110`.`join_const_107`) AND ((`t2`.`_fld56948_rrref` = `t110`.`_idrref`)) LEFT JOIN (SELECT `t112_src`.`_fld46059rref`, `t112_src`.`_fld543`, `t112_src`.`_idrref`, $const_171 AS `join_const_108`, $const_172 AS `join_const_109` FROM `public/_reference44158` AS `t112_src` WHERE ((`t112_src`.`_fld543` = $const_170))) AS `t112` ON (`t2`.`_fld56948_type` = `t112`.`join_const_108`) AND (`t2`.`_fld56948_rtref` = `t112`.`join_const_109`) AND ((`t2`.`_fld56948_rrref` = `t112`.`_idrref`)) LEFT JOIN (SELECT `t114_src`.`_fld46697rref`, `t114_src`.`_fld543`, `t114_src`.`_idrref`, $const_174 AS `join_const_110`, $const_175 AS `join_const_111` FROM `public/_reference44175` AS `t114_src` WHERE ((`t114_src`.`_fld543` = $const_173))) AS `t114` ON (`t2`.`_fld56948_type` = `t114`.`join_const_110`) AND (`t2`.`_fld56948_rtref` = `t114`.`join_const_111`) AND ((`t2`.`_fld56948_rrref` = `t114`.`_idrref`)) LEFT JOIN (SELECT `t116_src`.`_fld48220rref`, `t116_src`.`_fld543`, `t116_src`.`_idrref`, $const_177 AS `join_const_112`, $const_178 AS `join_const_113` FROM `public/_reference44218` AS `t116_src` WHERE ((`t116_src`.`_fld543` = $const_176))) AS `t116` ON (`t2`.`_fld56948_type` = `t116`.`join_const_112`) AND (`t2`.`_fld56948_rtref` = `t116`.`join_const_113`) AND ((`t2`.`_fld56948_rrref` = `t116`.`_idrref`)) LEFT JOIN (SELECT `t118_src`.`_fld22551rref`, `t118_src`.`_fld543`, `t118_src`.`_idrref`, $const_180 AS `join_const_114`, $const_181 AS `join_const_115` FROM `public/_reference22309` AS `t118_src` WHERE ((`t118_src`.`_fld543` = $const_179))) AS `t118` ON (`t2`.`_fld56948_type` = `t118`.`join_const_114`) AND (`t2`.`_fld56948_rtref` = `t118`.`join_const_115`) AND ((`t2`.`_fld56948_rrref` = `t118`.`_idrref`)) LEFT JOIN (SELECT `t120_src`.`_fld23121rref`, `t120_src`.`_fld543`, `t120_src`.`_idrref`, $const_183 AS `join_const_116`, $const_184 AS `join_const_117` FROM `public/_reference22324` AS `t120_src` WHERE ((`t120_src`.`_fld543` = $const_182))) AS `t120` ON (`t2`.`_fld56948_type` = `t120`.`join_const_116`) AND (`t2`.`_fld56948_rtref` = `t120`.`join_const_117`) AND ((`t2`.`_fld56948_rrref` = `t120`.`_idrref`)) LEFT JOIN (SELECT `t122_src`.`_fld543`, `t122_src`.`_fld75062rref`, `t122_src`.`_idrref`, $const_186 AS `join_const_118`, $const_187 AS `join_const_119` FROM `public/_reference74889` AS `t122_src` WHERE ((`t122_src`.`_fld543` = $const_185))) AS `t122` ON (`t2`.`_fld56948_type` = `t122`.`join_const_118`) AND (`t2`.`_fld56948_rtref` = `t122`.`join_const_119`) AND ((`t2`.`_fld56948_rrref` = `t122`.`_idrref`)) LEFT JOIN (SELECT `t124_src`.`_fld543`, `t124_src`.`_fld71345rref`, `t124_src`.`_idrref`, $const_189 AS `join_const_120`, $const_190 AS `join_const_121` FROM `public/_reference70970` AS `t124_src` WHERE ((`t124_src`.`_fld543` = $const_188))) AS `t124` ON (`t2`.`_fld56948_type` = `t124`.`join_const_120`) AND (`t2`.`_fld56948_rtref` = `t124`.`join_const_121`) AND ((`t2`.`_fld56948_rrref` = `t124`.`_idrref`)) LEFT JOIN (SELECT `t126_src`.`_fld45647rref`, `t126_src`.`_fld543`, `t126_src`.`_idrref`, $const_192 AS `join_const_122`, $const_193 AS `join_const_123` FROM `public/_reference44147` AS `t126_src` WHERE ((`t126_src`.`_fld543` = $const_191))) AS `t126` ON (`t2`.`_fld56948_type` = `t126`.`join_const_122`) AND (`t2`.`_fld56948_rtref` = `t126`.`join_const_123`) AND ((`t2`.`_fld56948_rrref` = `t126`.`_idrref`)) LEFT JOIN (SELECT `t128_src`.`_fld543`, `t128_src`.`_fld76767rref`, `t128_src`.`_idrref`, $const_195 AS `join_const_124`, $const_196 AS `join_const_125` FROM `public/_reference76755` AS `t128_src` WHERE ((`t128_src`.`_fld543` = $const_194))) AS `t128` ON (`t2`.`_fld56948_type` = `t128`.`join_const_124`) AND (`t2`.`_fld56948_rtref` = `t128`.`join_const_125`) AND ((`t2`.`_fld56948_rrref` = `t128`.`_idrref`)) LEFT JOIN (SELECT `t130_src`.`_fld543`, `t130_src`.`_fld62179rref`, `t130_src`.`_idrref`, $const_198 AS `join_const_126`, $const_199 AS `join_const_127` FROM `public/_reference61720` AS `t130_src` WHERE ((`t130_src`.`_fld543` = $const_197))) AS `t130` ON (`t2`.`_fld56948_type` = `t130`.`join_const_126`) AND (`t2`.`_fld56948_rtref` = `t130`.`join_const_127`) AND ((`t2`.`_fld56948_rrref` = `t130`.`_idrref`)) LEFT JOIN (SELECT `t132_src`.`_fld22427rref`, `t132_src`.`_fld543`, `t132_src`.`_idrref`, $const_201 AS `join_const_128`, $const_202 AS `join_const_129` FROM `public/_reference22301` AS `t132_src` WHERE ((`t132_src`.`_fld543` = $const_200))) AS `t132` ON (`t2`.`_fld56948_type` = `t132`.`join_const_128`) AND (`t2`.`_fld56948_rtref` = `t132`.`join_const_129`) AND ((`t2`.`_fld56948_rrref` = `t132`.`_idrref`)) LEFT JOIN (SELECT `t134_src`.`_fld543`, `t134_src`.`_fld74909rref`, `t134_src`.`_idrref`, $const_204 AS `join_const_130`, $const_205 AS `join_const_131` FROM `public/_reference74885` AS `t134_src` WHERE ((`t134_src`.`_fld543` = $const_203))) AS `t134` ON (`t2`.`_fld56948_type` = `t134`.`join_const_130`) AND (`t2`.`_fld56948_rtref` = `t134`.`join_const_131`) AND ((`t2`.`_fld56948_rrref` = `t134`.`_idrref`)) LEFT JOIN (SELECT `t136_src`.`_fld45759rref`, `t136_src`.`_fld543`, `t136_src`.`_idrref`, $const_207 AS `join_const_132`, $const_208 AS `join_const_133` FROM `public/_reference44150` AS `t136_src` WHERE ((`t136_src`.`_fld543` = $const_206))) AS `t136` ON (`t2`.`_fld56948_type` = `t136`.`join_const_132`) AND (`t2`.`_fld56948_rtref` = `t136`.`join_const_133`) AND ((`t2`.`_fld56948_rrref` = `t136`.`_idrref`)) LEFT JOIN (SELECT `t138_src`.`_fld543`, `t138_src`.`_fld57478rref`, `t138_src`.`_idrref`, $const_210 AS `join_const_134`, $const_211 AS `join_const_135` FROM `public/_reference57250` AS `t138_src` WHERE ((`t138_src`.`_fld543` = $const_209))) AS `t138` ON (`t2`.`_fld56948_type` = `t138`.`join_const_134`) AND (`t2`.`_fld56948_rtref` = `t138`.`join_const_135`) AND ((`t2`.`_fld56948_rrref` = `t138`.`_idrref`)) LEFT JOIN (SELECT `t140_src`.`_fld23937rref`, `t140_src`.`_fld543`, `t140_src`.`_idrref`, $const_213 AS `join_const_136`, $const_214 AS `join_const_137` FROM `public/_reference22344` AS `t140_src` WHERE ((`t140_src`.`_fld543` = $const_212))) AS `t140` ON (`t2`.`_fld56948_type` = `t140`.`join_const_136`) AND (`t2`.`_fld56948_rtref` = `t140`.`join_const_137`) AND ((`t2`.`_fld56948_rrref` = `t140`.`_idrref`)) LEFT JOIN (SELECT `t142_src`.`_fld543`, `t142_src`.`_fld57502rref`, `t142_src`.`_idrref`, $const_216 AS `join_const_138`, $const_217 AS `join_const_139` FROM `public/_reference57251` AS `t142_src` WHERE ((`t142_src`.`_fld543` = $const_215))) AS `t142` ON (`t2`.`_fld56948_type` = `t142`.`join_const_138`) AND (`t2`.`_fld56948_rtref` = `t142`.`join_const_139`) AND ((`t2`.`_fld56948_rrref` = `t142`.`_idrref`)) LEFT JOIN (SELECT `t144_src`.`_fld543`, `t144_src`.`_fld62350rref`, `t144_src`.`_idrref`, $const_219 AS `join_const_140`, $const_220 AS `join_const_141` FROM `public/_reference61727` AS `t144_src` WHERE ((`t144_src`.`_fld543` = $const_218))) AS `t144` ON (`t2`.`_fld56948_type` = `t144`.`join_const_140`) AND (`t2`.`_fld56948_rtref` = `t144`.`join_const_141`) AND ((`t2`.`_fld56948_rrref` = `t144`.`_idrref`)) LEFT JOIN (SELECT `t146_src`.`_fld543`, `t146_src`.`_fld73855rref`, `t146_src`.`_idrref`, $const_222 AS `join_const_142`, $const_223 AS `join_const_143` FROM `public/_reference73806` AS `t146_src` WHERE ((`t146_src`.`_fld543` = $const_221))) AS `t146` ON (`t2`.`_fld56948_type` = `t146`.`join_const_142`) AND (`t2`.`_fld56948_rtref` = `t146`.`join_const_143`) AND ((`t2`.`_fld56948_rrref` = `t146`.`_idrref`)) LEFT JOIN (SELECT `t148_src`.`_fld543`, `t148_src`.`_fld70992rref`, `t148_src`.`_idrref`, $const_225 AS `join_const_144`, $const_226 AS `join_const_145` FROM `public/_reference70961` AS `t148_src` WHERE ((`t148_src`.`_fld543` = $const_224))) AS `t148` ON (`t2`.`_fld56948_type` = `t148`.`join_const_144`) AND (`t2`.`_fld56948_rtref` = `t148`.`join_const_145`) AND ((`t2`.`_fld56948_rrref` = `t148`.`_idrref`)) LEFT JOIN (SELECT `t150_src`.`_fld543`, `t150_src`.`_fld71110rref`, `t150_src`.`_idrref`, $const_228 AS `join_const_146`, $const_229 AS `join_const_147` FROM `public/_reference70964` AS `t150_src` WHERE ((`t150_src`.`_fld543` = $const_227))) AS `t150` ON (`t2`.`_fld56948_type` = `t150`.`join_const_146`) AND (`t2`.`_fld56948_rtref` = `t150`.`join_const_147`) AND ((`t2`.`_fld56948_rrref` = `t150`.`_idrref`)) LEFT JOIN (SELECT `t152_src`.`_fld24201rref`, `t152_src`.`_fld543`, `t152_src`.`_idrref`, $const_231 AS `join_const_148`, $const_232 AS `join_const_149` FROM `public/_reference22352` AS `t152_src` WHERE ((`t152_src`.`_fld543` = $const_230))) AS `t152` ON (`t2`.`_fld56948_type` = `t152`.`join_const_148`) AND (`t2`.`_fld56948_rtref` = `t152`.`join_const_149`) AND ((`t2`.`_fld56948_rrref` = `t152`.`_idrref`)) LEFT JOIN (SELECT `t154_src`.`_fld23268rref`, `t154_src`.`_fld543`, `t154_src`.`_idrref`, $const_234 AS `join_const_150`, $const_235 AS `join_const_151` FROM `public/_reference22328` AS `t154_src` WHERE ((`t154_src`.`_fld543` = $const_233))) AS `t154` ON (`t2`.`_fld56948_type` = `t154`.`join_const_150`) AND (`t2`.`_fld56948_rtref` = `t154`.`join_const_151`) AND ((`t2`.`_fld56948_rrref` = `t154`.`_idrref`)) LEFT JOIN (SELECT `t156_src`.`_fld543`, `t156_src`.`_fld71424rref`, `t156_src`.`_idrref`, $const_237 AS `join_const_152`, $const_238 AS `join_const_153` FROM `public/_reference70972` AS `t156_src` WHERE ((`t156_src`.`_fld543` = $const_236))) AS `t156` ON (`t2`.`_fld56948_type` = `t156`.`join_const_152`) AND (`t2`.`_fld56948_rtref` = `t156`.`join_const_153`) AND ((`t2`.`_fld56948_rrref` = `t156`.`_idrref`)) LEFT JOIN (SELECT `t158_src`.`_fld543`, `t158_src`.`_fld62267rref`, `t158_src`.`_idrref`, $const_240 AS `join_const_154`, $const_241 AS `join_const_155` FROM `public/_reference61724` AS `t158_src` WHERE ((`t158_src`.`_fld543` = $const_239))) AS `t158` ON (`t2`.`_fld56948_type` = `t158`.`join_const_154`) AND (`t2`.`_fld56948_rtref` = `t158`.`join_const_155`) AND ((`t2`.`_fld56948_rrref` = `t158`.`_idrref`)) LEFT JOIN (SELECT `t160_src`.`_fld48630rref`, `t160_src`.`_fld543`, `t160_src`.`_idrref`, $const_243 AS `join_const_156`, $const_244 AS `join_const_157` FROM `public/_reference44229` AS `t160_src` WHERE ((`t160_src`.`_fld543` = $const_242))) AS `t160` ON (`t2`.`_fld56948_type` = `t160`.`join_const_156`) AND (`t2`.`_fld56948_rtref` = `t160`.`join_const_157`) AND ((`t2`.`_fld56948_rrref` = `t160`.`_idrref`)) LEFT JOIN (SELECT `t162_src`.`_fld543`, `t162_src`.`_fld71305rref`, `t162_src`.`_idrref`, $const_246 AS `join_const_158`, $const_247 AS `join_const_159` FROM `public/_reference70969` AS `t162_src` WHERE ((`t162_src`.`_fld543` = $const_245))) AS `t162` ON (`t2`.`_fld56948_type` = `t162`.`join_const_158`) AND (`t2`.`_fld56948_rtref` = `t162`.`join_const_159`) AND ((`t2`.`_fld56948_rrref` = `t162`.`_idrref`)) LEFT JOIN (SELECT `t164_src`.`_fld23494rref`, `t164_src`.`_fld543`, `t164_src`.`_idrref`, $const_249 AS `join_const_160`, $const_250 AS `join_const_161` FROM `public/_reference22334` AS `t164_src` WHERE ((`t164_src`.`_fld543` = $const_248))) AS `t164` ON (`t2`.`_fld56948_type` = `t164`.`join_const_160`) AND (`t2`.`_fld56948_rtref` = `t164`.`join_const_161`) AND ((`t2`.`_fld56948_rrref` = `t164`.`_idrref`)) LEFT JOIN (SELECT `t166_src`.`_fld543`, `t166_src`.`_fld71385rref`, `t166_src`.`_idrref`, $const_252 AS `join_const_162`, $const_253 AS `join_const_163` FROM `public/_reference70971` AS `t166_src` WHERE ((`t166_src`.`_fld543` = $const_251))) AS `t166` ON (`t2`.`_fld56948_type` = `t166`.`join_const_162`) AND (`t2`.`_fld56948_rtref` = `t166`.`join_const_163`) AND ((`t2`.`_fld56948_rrref` = `t166`.`_idrref`)) LEFT JOIN (SELECT `t168_src`.`_fld543`, `t168_src`.`_fld57358rref`, `t168_src`.`_idrref`, $const_255 AS `join_const_164`, $const_256 AS `join_const_165` FROM `public/_reference57245` AS `t168_src` WHERE ((`t168_src`.`_fld543` = $const_254))) AS `t168` ON (`t2`.`_fld56948_type` = `t168`.`join_const_164`) AND (`t2`.`_fld56948_rtref` = `t168`.`join_const_165`) AND ((`t2`.`_fld56948_rrref` = `t168`.`_idrref`)) LEFT JOIN (SELECT `t170_src`.`_fld23028rref`, `t170_src`.`_fld543`, `t170_src`.`_idrref`, $const_258 AS `join_const_166`, $const_259 AS `join_const_167` FROM `public/_reference22322` AS `t170_src` WHERE ((`t170_src`.`_fld543` = $const_257))) AS `t170` ON (`t2`.`_fld56948_type` = `t170`.`join_const_166`) AND (`t2`.`_fld56948_rtref` = `t170`.`join_const_167`) AND ((`t2`.`_fld56948_rrref` = `t170`.`_idrref`)) LEFT JOIN (SELECT `t172_src`.`_fld53873rref`, `t172_src`.`_fld543`, `t172_src`.`_idrref`, $const_261 AS `join_const_168`, $const_262 AS `join_const_169` FROM `public/_reference53670` AS `t172_src` WHERE ((`t172_src`.`_fld543` = $const_260))) AS `t172` ON (`t2`.`_fld56948_type` = `t172`.`join_const_168`) AND (`t2`.`_fld56948_rtref` = `t172`.`join_const_169`) AND ((`t2`.`_fld56948_rrref` = `t172`.`_idrref`)) LEFT JOIN (SELECT `t174_src`.`_fld53851rref`, `t174_src`.`_fld543`, `t174_src`.`_idrref`, $const_264 AS `join_const_170`, $const_265 AS `join_const_171` FROM `public/_reference53669` AS `t174_src` WHERE ((`t174_src`.`_fld543` = $const_263))) AS `t174` ON (`t2`.`_fld56948_type` = `t174`.`join_const_170`) AND (`t2`.`_fld56948_rtref` = `t174`.`join_const_171`) AND ((`t2`.`_fld56948_rrref` = `t174`.`_idrref`)) LEFT JOIN (SELECT `t176_src`.`_fld543`, `t176_src`.`_fld74945rref`, `t176_src`.`_idrref`, $const_267 AS `join_const_172`, $const_268 AS `join_const_173` FROM `public/_reference74886` AS `t176_src` WHERE ((`t176_src`.`_fld543` = $const_266))) AS `t176` ON (`t2`.`_fld56948_type` = `t176`.`join_const_172`) AND (`t2`.`_fld56948_rtref` = `t176`.`join_const_173`) AND ((`t2`.`_fld56948_rrref` = `t176`.`_idrref`)) LEFT JOIN (SELECT `t178_src`.`_fld24012rref`, `t178_src`.`_fld543`, `t178_src`.`_idrref`, $const_270 AS `join_const_174`, $const_271 AS `join_const_175` FROM `public/_reference22347` AS `t178_src` WHERE ((`t178_src`.`_fld543` = $const_269))) AS `t178` ON (`t2`.`_fld56948_type` = `t178`.`join_const_174`) AND (`t2`.`_fld56948_rtref` = `t178`.`join_const_175`) AND ((`t2`.`_fld56948_rrref` = `t178`.`_idrref`)) LEFT JOIN (SELECT `t180_src`.`_fld22691rref`, `t180_src`.`_fld543`, `t180_src`.`_idrref`, $const_273 AS `join_const_176`, $const_274 AS `join_const_177` FROM `public/_reference22313` AS `t180_src` WHERE ((`t180_src`.`_fld543` = $const_272))) AS `t180` ON (`t2`.`_fld56948_type` = `t180`.`join_const_176`) AND (`t2`.`_fld56948_rtref` = `t180`.`join_const_177`) AND ((`t2`.`_fld56948_rrref` = `t180`.`_idrref`)) LEFT JOIN (SELECT `t182_src`.`_fld24058rref`, `t182_src`.`_fld543`, `t182_src`.`_idrref`, $const_276 AS `join_const_178`, $const_277 AS `join_const_179` FROM `public/_reference22348` AS `t182_src` WHERE ((`t182_src`.`_fld543` = $const_275))) AS `t182` ON (`t2`.`_fld56948_type` = `t182`.`join_const_178`) AND (`t2`.`_fld56948_rtref` = `t182`.`join_const_179`) AND ((`t2`.`_fld56948_rrref` = `t182`.`_idrref`)) LEFT JOIN (SELECT `t184_src`.`_fld23664rref`, `t184_src`.`_fld543`, `t184_src`.`_idrref`, $const_279 AS `join_const_180`, $const_280 AS `join_const_181` FROM `public/_reference22338` AS `t184_src` WHERE ((`t184_src`.`_fld543` = $const_278))) AS `t184` ON (`t2`.`_fld56948_type` = `t184`.`join_const_180`) AND (`t2`.`_fld56948_rtref` = `t184`.`join_const_181`) AND ((`t2`.`_fld56948_rrref` = `t184`.`_idrref`)) LEFT JOIN (SELECT `t186_src`.`_fld24617rref`, `t186_src`.`_fld543`, `t186_src`.`_idrref`, $const_282 AS `join_const_182`, $const_283 AS `join_const_183` FROM `public/_reference22362` AS `t186_src` WHERE ((`t186_src`.`_fld543` = $const_281))) AS `t186` ON (`t2`.`_fld56948_type` = `t186`.`join_const_182`) AND (`t2`.`_fld56948_rtref` = `t186`.`join_const_183`) AND ((`t2`.`_fld56948_rrref` = `t186`.`_idrref`)) LEFT JOIN (SELECT `t188_src`.`_fld22473rref`, `t188_src`.`_fld543`, `t188_src`.`_idrref`, $const_285 AS `join_const_184`, $const_286 AS `join_const_185` FROM `public/_reference22302` AS `t188_src` WHERE ((`t188_src`.`_fld543` = $const_284))) AS `t188` ON (`t2`.`_fld56948_type` = `t188`.`join_const_184`) AND (`t2`.`_fld56948_rtref` = `t188`.`join_const_185`) AND ((`t2`.`_fld56948_rrref` = `t188`.`_idrref`)) LEFT JOIN (SELECT `t190_src`.`_fld543`, `t190_src`.`_fld57406rref`, `t190_src`.`_idrref`, $const_288 AS `join_const_186`, $const_289 AS `join_const_187` FROM `public/_reference57247` AS `t190_src` WHERE ((`t190_src`.`_fld543` = $const_287))) AS `t190` ON (`t2`.`_fld56948_type` = `t190`.`join_const_186`) AND (`t2`.`_fld56948_rtref` = `t190`.`join_const_187`) AND ((`t2`.`_fld56948_rrref` = `t190`.`_idrref`)) LEFT JOIN (SELECT `t192_src`.`_fld22935rref`, `t192_src`.`_fld543`, `t192_src`.`_idrref`, $const_291 AS `join_const_188`, $const_292 AS `join_const_189` FROM `public/_reference22320` AS `t192_src` WHERE ((`t192_src`.`_fld543` = $const_290))) AS `t192` ON (`t2`.`_fld56948_type` = `t192`.`join_const_188`) AND (`t2`.`_fld56948_rtref` = `t192`.`join_const_189`) AND ((`t2`.`_fld56948_rrref` = `t192`.`_idrref`))
WHERE NOT EXISTS (
    SELECT 1
    FROM `public/_inforg56947` AS `t4_src`
    WHERE (`t4_src`.`_fld543` = $const_12)
      AND (`t2`.`_fld56948_type` = `t4_src`.`_fld56948_type`)
      AND (`t2`.`_fld56948_rtref` = `t4_src`.`_fld56948_rtref`)
      AND (`t2`.`_fld56948_rrref` = `t4_src`.`_fld56948_rrref`)
      AND (`t2`.`_fld62760rref` = `t4_src`.`_fld56949rref`)
      AND (`t2`.`_fld56950rref` = `t4_src`.`_fld56950rref`)
);
