#include "device.h"

extern int const ncclDevFuncIdCount = 670;
extern int const ncclDevFuncRowToId[] = {
/*   0*/ 669, // SendRecv
/*   1*/ 3, // AllGather RING LL
/*   2*/ 4, // AllGather RING LL128
/*   3*/ 5, // AllGather RING SIMPLE
/*   4*/ -1,
/*   5*/ -1,
/*   6*/ 0, // AllGather COLLNET_DIRECT SIMPLE
/*   7*/ -1,
/*   8*/ -1,
/*   9*/ 1, // AllGather NVLS SIMPLE
/*  10*/ -1,
/*  11*/ -1,
/*  12*/ 2, // AllGather PAT SIMPLE
/*  13*/ 342, // Broadcast RING LL
/*  14*/ 343, // Broadcast RING LL128
/*  15*/ 344, // Broadcast RING SIMPLE
/*  16*/ 315, // AllReduce Sum i8 TREE LL
/*  17*/ 316, // AllReduce Sum i8 TREE LL128
/*  18*/ 317, // AllReduce Sum i8 TREE SIMPLE
/*  19*/ 312, // AllReduce Sum i8 RING LL
/*  20*/ 313, // AllReduce Sum i8 RING LL128
/*  21*/ 314, // AllReduce Sum i8 RING SIMPLE
/*  22*/ -1,
/*  23*/ -1,
/*  24*/ 311, // AllReduce Sum i8 COLLNET_DIRECT SIMPLE
/*  25*/ -1,
/*  26*/ -1,
/*  27*/ 310, // AllReduce Sum i8 COLLNET_CHAIN SIMPLE
/*  28*/ -1,
/*  29*/ -1,
/*  30*/ -1,
/*  31*/ -1,
/*  32*/ -1,
/*  33*/ -1,
/*  34*/ 315, // AllReduce Sum u8 TREE LL
/*  35*/ 316, // AllReduce Sum u8 TREE LL128
/*  36*/ 317, // AllReduce Sum u8 TREE SIMPLE
/*  37*/ 312, // AllReduce Sum u8 RING LL
/*  38*/ 313, // AllReduce Sum u8 RING LL128
/*  39*/ 314, // AllReduce Sum u8 RING SIMPLE
/*  40*/ -1,
/*  41*/ -1,
/*  42*/ 311, // AllReduce Sum u8 COLLNET_DIRECT SIMPLE
/*  43*/ -1,
/*  44*/ -1,
/*  45*/ 310, // AllReduce Sum u8 COLLNET_CHAIN SIMPLE
/*  46*/ -1,
/*  47*/ -1,
/*  48*/ -1,
/*  49*/ -1,
/*  50*/ -1,
/*  51*/ -1,
/*  52*/ 297, // AllReduce Sum i32 TREE LL
/*  53*/ 298, // AllReduce Sum i32 TREE LL128
/*  54*/ 299, // AllReduce Sum i32 TREE SIMPLE
/*  55*/ 294, // AllReduce Sum i32 RING LL
/*  56*/ 295, // AllReduce Sum i32 RING LL128
/*  57*/ 296, // AllReduce Sum i32 RING SIMPLE
/*  58*/ -1,
/*  59*/ -1,
/*  60*/ 291, // AllReduce Sum i32 COLLNET_DIRECT SIMPLE
/*  61*/ -1,
/*  62*/ -1,
/*  63*/ 290, // AllReduce Sum i32 COLLNET_CHAIN SIMPLE
/*  64*/ -1,
/*  65*/ -1,
/*  66*/ 292, // AllReduce Sum i32 NVLS SIMPLE
/*  67*/ -1,
/*  68*/ -1,
/*  69*/ 293, // AllReduce Sum i32 NVLS_TREE SIMPLE
/*  70*/ 297, // AllReduce Sum u32 TREE LL
/*  71*/ 298, // AllReduce Sum u32 TREE LL128
/*  72*/ 299, // AllReduce Sum u32 TREE SIMPLE
/*  73*/ 294, // AllReduce Sum u32 RING LL
/*  74*/ 295, // AllReduce Sum u32 RING LL128
/*  75*/ 296, // AllReduce Sum u32 RING SIMPLE
/*  76*/ -1,
/*  77*/ -1,
/*  78*/ 291, // AllReduce Sum u32 COLLNET_DIRECT SIMPLE
/*  79*/ -1,
/*  80*/ -1,
/*  81*/ 290, // AllReduce Sum u32 COLLNET_CHAIN SIMPLE
/*  82*/ -1,
/*  83*/ -1,
/*  84*/ 292, // AllReduce Sum u32 NVLS SIMPLE
/*  85*/ -1,
/*  86*/ -1,
/*  87*/ 293, // AllReduce Sum u32 NVLS_TREE SIMPLE
/*  88*/ 307, // AllReduce Sum i64 TREE LL
/*  89*/ 308, // AllReduce Sum i64 TREE LL128
/*  90*/ 309, // AllReduce Sum i64 TREE SIMPLE
/*  91*/ 304, // AllReduce Sum i64 RING LL
/*  92*/ 305, // AllReduce Sum i64 RING LL128
/*  93*/ 306, // AllReduce Sum i64 RING SIMPLE
/*  94*/ -1,
/*  95*/ -1,
/*  96*/ 301, // AllReduce Sum i64 COLLNET_DIRECT SIMPLE
/*  97*/ -1,
/*  98*/ -1,
/*  99*/ 300, // AllReduce Sum i64 COLLNET_CHAIN SIMPLE
/* 100*/ -1,
/* 101*/ -1,
/* 102*/ 302, // AllReduce Sum i64 NVLS SIMPLE
/* 103*/ -1,
/* 104*/ -1,
/* 105*/ 303, // AllReduce Sum i64 NVLS_TREE SIMPLE
/* 106*/ 307, // AllReduce Sum u64 TREE LL
/* 107*/ 308, // AllReduce Sum u64 TREE LL128
/* 108*/ 309, // AllReduce Sum u64 TREE SIMPLE
/* 109*/ 304, // AllReduce Sum u64 RING LL
/* 110*/ 305, // AllReduce Sum u64 RING LL128
/* 111*/ 306, // AllReduce Sum u64 RING SIMPLE
/* 112*/ -1,
/* 113*/ -1,
/* 114*/ 301, // AllReduce Sum u64 COLLNET_DIRECT SIMPLE
/* 115*/ -1,
/* 116*/ -1,
/* 117*/ 300, // AllReduce Sum u64 COLLNET_CHAIN SIMPLE
/* 118*/ -1,
/* 119*/ -1,
/* 120*/ 302, // AllReduce Sum u64 NVLS SIMPLE
/* 121*/ -1,
/* 122*/ -1,
/* 123*/ 303, // AllReduce Sum u64 NVLS_TREE SIMPLE
/* 124*/ 251, // AllReduce Sum f16 TREE LL
/* 125*/ 252, // AllReduce Sum f16 TREE LL128
/* 126*/ 253, // AllReduce Sum f16 TREE SIMPLE
/* 127*/ 248, // AllReduce Sum f16 RING LL
/* 128*/ 249, // AllReduce Sum f16 RING LL128
/* 129*/ 250, // AllReduce Sum f16 RING SIMPLE
/* 130*/ -1,
/* 131*/ -1,
/* 132*/ 245, // AllReduce Sum f16 COLLNET_DIRECT SIMPLE
/* 133*/ -1,
/* 134*/ -1,
/* 135*/ 244, // AllReduce Sum f16 COLLNET_CHAIN SIMPLE
/* 136*/ -1,
/* 137*/ -1,
/* 138*/ 246, // AllReduce Sum f16 NVLS SIMPLE
/* 139*/ -1,
/* 140*/ -1,
/* 141*/ 247, // AllReduce Sum f16 NVLS_TREE SIMPLE
/* 142*/ 261, // AllReduce Sum f32 TREE LL
/* 143*/ 262, // AllReduce Sum f32 TREE LL128
/* 144*/ 263, // AllReduce Sum f32 TREE SIMPLE
/* 145*/ 258, // AllReduce Sum f32 RING LL
/* 146*/ 259, // AllReduce Sum f32 RING LL128
/* 147*/ 260, // AllReduce Sum f32 RING SIMPLE
/* 148*/ -1,
/* 149*/ -1,
/* 150*/ 255, // AllReduce Sum f32 COLLNET_DIRECT SIMPLE
/* 151*/ -1,
/* 152*/ -1,
/* 153*/ 254, // AllReduce Sum f32 COLLNET_CHAIN SIMPLE
/* 154*/ -1,
/* 155*/ -1,
/* 156*/ 256, // AllReduce Sum f32 NVLS SIMPLE
/* 157*/ -1,
/* 158*/ -1,
/* 159*/ 257, // AllReduce Sum f32 NVLS_TREE SIMPLE
/* 160*/ 271, // AllReduce Sum f64 TREE LL
/* 161*/ 272, // AllReduce Sum f64 TREE LL128
/* 162*/ 273, // AllReduce Sum f64 TREE SIMPLE
/* 163*/ 268, // AllReduce Sum f64 RING LL
/* 164*/ 269, // AllReduce Sum f64 RING LL128
/* 165*/ 270, // AllReduce Sum f64 RING SIMPLE
/* 166*/ -1,
/* 167*/ -1,
/* 168*/ 265, // AllReduce Sum f64 COLLNET_DIRECT SIMPLE
/* 169*/ -1,
/* 170*/ -1,
/* 171*/ 264, // AllReduce Sum f64 COLLNET_CHAIN SIMPLE
/* 172*/ -1,
/* 173*/ -1,
/* 174*/ 266, // AllReduce Sum f64 NVLS SIMPLE
/* 175*/ -1,
/* 176*/ -1,
/* 177*/ 267, // AllReduce Sum f64 NVLS_TREE SIMPLE
/* 178*/ 241, // AllReduce Sum bf16 TREE LL
/* 179*/ 242, // AllReduce Sum bf16 TREE LL128
/* 180*/ 243, // AllReduce Sum bf16 TREE SIMPLE
/* 181*/ 238, // AllReduce Sum bf16 RING LL
/* 182*/ 239, // AllReduce Sum bf16 RING LL128
/* 183*/ 240, // AllReduce Sum bf16 RING SIMPLE
/* 184*/ -1,
/* 185*/ -1,
/* 186*/ 235, // AllReduce Sum bf16 COLLNET_DIRECT SIMPLE
/* 187*/ -1,
/* 188*/ -1,
/* 189*/ 234, // AllReduce Sum bf16 COLLNET_CHAIN SIMPLE
/* 190*/ -1,
/* 191*/ -1,
/* 192*/ 236, // AllReduce Sum bf16 NVLS SIMPLE
/* 193*/ -1,
/* 194*/ -1,
/* 195*/ 237, // AllReduce Sum bf16 NVLS_TREE SIMPLE
/* 196*/ 279, // AllReduce Sum f8e4m3 TREE LL
/* 197*/ 280, // AllReduce Sum f8e4m3 TREE LL128
/* 198*/ 281, // AllReduce Sum f8e4m3 TREE SIMPLE
/* 199*/ 276, // AllReduce Sum f8e4m3 RING LL
/* 200*/ 277, // AllReduce Sum f8e4m3 RING LL128
/* 201*/ 278, // AllReduce Sum f8e4m3 RING SIMPLE
/* 202*/ -1,
/* 203*/ -1,
/* 204*/ 275, // AllReduce Sum f8e4m3 COLLNET_DIRECT SIMPLE
/* 205*/ -1,
/* 206*/ -1,
/* 207*/ 274, // AllReduce Sum f8e4m3 COLLNET_CHAIN SIMPLE
/* 208*/ -1,
/* 209*/ -1,
/* 210*/ -1,
/* 211*/ -1,
/* 212*/ -1,
/* 213*/ -1,
/* 214*/ 287, // AllReduce Sum f8e5m2 TREE LL
/* 215*/ 288, // AllReduce Sum f8e5m2 TREE LL128
/* 216*/ 289, // AllReduce Sum f8e5m2 TREE SIMPLE
/* 217*/ 284, // AllReduce Sum f8e5m2 RING LL
/* 218*/ 285, // AllReduce Sum f8e5m2 RING LL128
/* 219*/ 286, // AllReduce Sum f8e5m2 RING SIMPLE
/* 220*/ -1,
/* 221*/ -1,
/* 222*/ 283, // AllReduce Sum f8e5m2 COLLNET_DIRECT SIMPLE
/* 223*/ -1,
/* 224*/ -1,
/* 225*/ 282, // AllReduce Sum f8e5m2 COLLNET_CHAIN SIMPLE
/* 226*/ -1,
/* 227*/ -1,
/* 228*/ -1,
/* 229*/ -1,
/* 230*/ -1,
/* 231*/ -1,
/* 232*/ 231, // AllReduce Prod i8 TREE LL
/* 233*/ 232, // AllReduce Prod i8 TREE LL128
/* 234*/ 233, // AllReduce Prod i8 TREE SIMPLE
/* 235*/ 228, // AllReduce Prod i8 RING LL
/* 236*/ 229, // AllReduce Prod i8 RING LL128
/* 237*/ 230, // AllReduce Prod i8 RING SIMPLE
/* 238*/ -1,
/* 239*/ -1,
/* 240*/ 227, // AllReduce Prod i8 COLLNET_DIRECT SIMPLE
/* 241*/ -1,
/* 242*/ -1,
/* 243*/ 226, // AllReduce Prod i8 COLLNET_CHAIN SIMPLE
/* 244*/ -1,
/* 245*/ -1,
/* 246*/ -1,
/* 247*/ -1,
/* 248*/ -1,
/* 249*/ -1,
/* 250*/ 231, // AllReduce Prod u8 TREE LL
/* 251*/ 232, // AllReduce Prod u8 TREE LL128
/* 252*/ 233, // AllReduce Prod u8 TREE SIMPLE
/* 253*/ 228, // AllReduce Prod u8 RING LL
/* 254*/ 229, // AllReduce Prod u8 RING LL128
/* 255*/ 230, // AllReduce Prod u8 RING SIMPLE
/* 256*/ -1,
/* 257*/ -1,
/* 258*/ 227, // AllReduce Prod u8 COLLNET_DIRECT SIMPLE
/* 259*/ -1,
/* 260*/ -1,
/* 261*/ 226, // AllReduce Prod u8 COLLNET_CHAIN SIMPLE
/* 262*/ -1,
/* 263*/ -1,
/* 264*/ -1,
/* 265*/ -1,
/* 266*/ -1,
/* 267*/ -1,
/* 268*/ 215, // AllReduce Prod i32 TREE LL
/* 269*/ 216, // AllReduce Prod i32 TREE LL128
/* 270*/ 217, // AllReduce Prod i32 TREE SIMPLE
/* 271*/ 212, // AllReduce Prod i32 RING LL
/* 272*/ 213, // AllReduce Prod i32 RING LL128
/* 273*/ 214, // AllReduce Prod i32 RING SIMPLE
/* 274*/ -1,
/* 275*/ -1,
/* 276*/ 211, // AllReduce Prod i32 COLLNET_DIRECT SIMPLE
/* 277*/ -1,
/* 278*/ -1,
/* 279*/ 210, // AllReduce Prod i32 COLLNET_CHAIN SIMPLE
/* 280*/ -1,
/* 281*/ -1,
/* 282*/ -1,
/* 283*/ -1,
/* 284*/ -1,
/* 285*/ -1,
/* 286*/ 215, // AllReduce Prod u32 TREE LL
/* 287*/ 216, // AllReduce Prod u32 TREE LL128
/* 288*/ 217, // AllReduce Prod u32 TREE SIMPLE
/* 289*/ 212, // AllReduce Prod u32 RING LL
/* 290*/ 213, // AllReduce Prod u32 RING LL128
/* 291*/ 214, // AllReduce Prod u32 RING SIMPLE
/* 292*/ -1,
/* 293*/ -1,
/* 294*/ 211, // AllReduce Prod u32 COLLNET_DIRECT SIMPLE
/* 295*/ -1,
/* 296*/ -1,
/* 297*/ 210, // AllReduce Prod u32 COLLNET_CHAIN SIMPLE
/* 298*/ -1,
/* 299*/ -1,
/* 300*/ -1,
/* 301*/ -1,
/* 302*/ -1,
/* 303*/ -1,
/* 304*/ 223, // AllReduce Prod i64 TREE LL
/* 305*/ 224, // AllReduce Prod i64 TREE LL128
/* 306*/ 225, // AllReduce Prod i64 TREE SIMPLE
/* 307*/ 220, // AllReduce Prod i64 RING LL
/* 308*/ 221, // AllReduce Prod i64 RING LL128
/* 309*/ 222, // AllReduce Prod i64 RING SIMPLE
/* 310*/ -1,
/* 311*/ -1,
/* 312*/ 219, // AllReduce Prod i64 COLLNET_DIRECT SIMPLE
/* 313*/ -1,
/* 314*/ -1,
/* 315*/ 218, // AllReduce Prod i64 COLLNET_CHAIN SIMPLE
/* 316*/ -1,
/* 317*/ -1,
/* 318*/ -1,
/* 319*/ -1,
/* 320*/ -1,
/* 321*/ -1,
/* 322*/ 223, // AllReduce Prod u64 TREE LL
/* 323*/ 224, // AllReduce Prod u64 TREE LL128
/* 324*/ 225, // AllReduce Prod u64 TREE SIMPLE
/* 325*/ 220, // AllReduce Prod u64 RING LL
/* 326*/ 221, // AllReduce Prod u64 RING LL128
/* 327*/ 222, // AllReduce Prod u64 RING SIMPLE
/* 328*/ -1,
/* 329*/ -1,
/* 330*/ 219, // AllReduce Prod u64 COLLNET_DIRECT SIMPLE
/* 331*/ -1,
/* 332*/ -1,
/* 333*/ 218, // AllReduce Prod u64 COLLNET_CHAIN SIMPLE
/* 334*/ -1,
/* 335*/ -1,
/* 336*/ -1,
/* 337*/ -1,
/* 338*/ -1,
/* 339*/ -1,
/* 340*/ 175, // AllReduce Prod f16 TREE LL
/* 341*/ 176, // AllReduce Prod f16 TREE LL128
/* 342*/ 177, // AllReduce Prod f16 TREE SIMPLE
/* 343*/ 172, // AllReduce Prod f16 RING LL
/* 344*/ 173, // AllReduce Prod f16 RING LL128
/* 345*/ 174, // AllReduce Prod f16 RING SIMPLE
/* 346*/ -1,
/* 347*/ -1,
/* 348*/ 171, // AllReduce Prod f16 COLLNET_DIRECT SIMPLE
/* 349*/ -1,
/* 350*/ -1,
/* 351*/ 170, // AllReduce Prod f16 COLLNET_CHAIN SIMPLE
/* 352*/ -1,
/* 353*/ -1,
/* 354*/ -1,
/* 355*/ -1,
/* 356*/ -1,
/* 357*/ -1,
/* 358*/ 183, // AllReduce Prod f32 TREE LL
/* 359*/ 184, // AllReduce Prod f32 TREE LL128
/* 360*/ 185, // AllReduce Prod f32 TREE SIMPLE
/* 361*/ 180, // AllReduce Prod f32 RING LL
/* 362*/ 181, // AllReduce Prod f32 RING LL128
/* 363*/ 182, // AllReduce Prod f32 RING SIMPLE
/* 364*/ -1,
/* 365*/ -1,
/* 366*/ 179, // AllReduce Prod f32 COLLNET_DIRECT SIMPLE
/* 367*/ -1,
/* 368*/ -1,
/* 369*/ 178, // AllReduce Prod f32 COLLNET_CHAIN SIMPLE
/* 370*/ -1,
/* 371*/ -1,
/* 372*/ -1,
/* 373*/ -1,
/* 374*/ -1,
/* 375*/ -1,
/* 376*/ 191, // AllReduce Prod f64 TREE LL
/* 377*/ 192, // AllReduce Prod f64 TREE LL128
/* 378*/ 193, // AllReduce Prod f64 TREE SIMPLE
/* 379*/ 188, // AllReduce Prod f64 RING LL
/* 380*/ 189, // AllReduce Prod f64 RING LL128
/* 381*/ 190, // AllReduce Prod f64 RING SIMPLE
/* 382*/ -1,
/* 383*/ -1,
/* 384*/ 187, // AllReduce Prod f64 COLLNET_DIRECT SIMPLE
/* 385*/ -1,
/* 386*/ -1,
/* 387*/ 186, // AllReduce Prod f64 COLLNET_CHAIN SIMPLE
/* 388*/ -1,
/* 389*/ -1,
/* 390*/ -1,
/* 391*/ -1,
/* 392*/ -1,
/* 393*/ -1,
/* 394*/ 167, // AllReduce Prod bf16 TREE LL
/* 395*/ 168, // AllReduce Prod bf16 TREE LL128
/* 396*/ 169, // AllReduce Prod bf16 TREE SIMPLE
/* 397*/ 164, // AllReduce Prod bf16 RING LL
/* 398*/ 165, // AllReduce Prod bf16 RING LL128
/* 399*/ 166, // AllReduce Prod bf16 RING SIMPLE
/* 400*/ -1,
/* 401*/ -1,
/* 402*/ 163, // AllReduce Prod bf16 COLLNET_DIRECT SIMPLE
/* 403*/ -1,
/* 404*/ -1,
/* 405*/ 162, // AllReduce Prod bf16 COLLNET_CHAIN SIMPLE
/* 406*/ -1,
/* 407*/ -1,
/* 408*/ -1,
/* 409*/ -1,
/* 410*/ -1,
/* 411*/ -1,
/* 412*/ 199, // AllReduce Prod f8e4m3 TREE LL
/* 413*/ 200, // AllReduce Prod f8e4m3 TREE LL128
/* 414*/ 201, // AllReduce Prod f8e4m3 TREE SIMPLE
/* 415*/ 196, // AllReduce Prod f8e4m3 RING LL
/* 416*/ 197, // AllReduce Prod f8e4m3 RING LL128
/* 417*/ 198, // AllReduce Prod f8e4m3 RING SIMPLE
/* 418*/ -1,
/* 419*/ -1,
/* 420*/ 195, // AllReduce Prod f8e4m3 COLLNET_DIRECT SIMPLE
/* 421*/ -1,
/* 422*/ -1,
/* 423*/ 194, // AllReduce Prod f8e4m3 COLLNET_CHAIN SIMPLE
/* 424*/ -1,
/* 425*/ -1,
/* 426*/ -1,
/* 427*/ -1,
/* 428*/ -1,
/* 429*/ -1,
/* 430*/ 207, // AllReduce Prod f8e5m2 TREE LL
/* 431*/ 208, // AllReduce Prod f8e5m2 TREE LL128
/* 432*/ 209, // AllReduce Prod f8e5m2 TREE SIMPLE
/* 433*/ 204, // AllReduce Prod f8e5m2 RING LL
/* 434*/ 205, // AllReduce Prod f8e5m2 RING LL128
/* 435*/ 206, // AllReduce Prod f8e5m2 RING SIMPLE
/* 436*/ -1,
/* 437*/ -1,
/* 438*/ 203, // AllReduce Prod f8e5m2 COLLNET_DIRECT SIMPLE
/* 439*/ -1,
/* 440*/ -1,
/* 441*/ 202, // AllReduce Prod f8e5m2 COLLNET_CHAIN SIMPLE
/* 442*/ -1,
/* 443*/ -1,
/* 444*/ -1,
/* 445*/ -1,
/* 446*/ -1,
/* 447*/ -1,
/* 448*/ 87, // AllReduce MinMax i8 TREE LL
/* 449*/ 88, // AllReduce MinMax i8 TREE LL128
/* 450*/ 89, // AllReduce MinMax i8 TREE SIMPLE
/* 451*/ 84, // AllReduce MinMax i8 RING LL
/* 452*/ 85, // AllReduce MinMax i8 RING LL128
/* 453*/ 86, // AllReduce MinMax i8 RING SIMPLE
/* 454*/ -1,
/* 455*/ -1,
/* 456*/ 83, // AllReduce MinMax i8 COLLNET_DIRECT SIMPLE
/* 457*/ -1,
/* 458*/ -1,
/* 459*/ 82, // AllReduce MinMax i8 COLLNET_CHAIN SIMPLE
/* 460*/ -1,
/* 461*/ -1,
/* 462*/ -1,
/* 463*/ -1,
/* 464*/ -1,
/* 465*/ -1,
/* 466*/ 87, // AllReduce MinMax u8 TREE LL
/* 467*/ 88, // AllReduce MinMax u8 TREE LL128
/* 468*/ 89, // AllReduce MinMax u8 TREE SIMPLE
/* 469*/ 84, // AllReduce MinMax u8 RING LL
/* 470*/ 85, // AllReduce MinMax u8 RING LL128
/* 471*/ 86, // AllReduce MinMax u8 RING SIMPLE
/* 472*/ -1,
/* 473*/ -1,
/* 474*/ 83, // AllReduce MinMax u8 COLLNET_DIRECT SIMPLE
/* 475*/ -1,
/* 476*/ -1,
/* 477*/ 82, // AllReduce MinMax u8 COLLNET_CHAIN SIMPLE
/* 478*/ -1,
/* 479*/ -1,
/* 480*/ -1,
/* 481*/ -1,
/* 482*/ -1,
/* 483*/ -1,
/* 484*/ 69, // AllReduce MinMax i32 TREE LL
/* 485*/ 70, // AllReduce MinMax i32 TREE LL128
/* 486*/ 71, // AllReduce MinMax i32 TREE SIMPLE
/* 487*/ 66, // AllReduce MinMax i32 RING LL
/* 488*/ 67, // AllReduce MinMax i32 RING LL128
/* 489*/ 68, // AllReduce MinMax i32 RING SIMPLE
/* 490*/ -1,
/* 491*/ -1,
/* 492*/ 63, // AllReduce MinMax i32 COLLNET_DIRECT SIMPLE
/* 493*/ -1,
/* 494*/ -1,
/* 495*/ 62, // AllReduce MinMax i32 COLLNET_CHAIN SIMPLE
/* 496*/ -1,
/* 497*/ -1,
/* 498*/ 58, // AllReduce MinMax i32 NVLS SIMPLE
/* 499*/ -1,
/* 500*/ -1,
/* 501*/ 59, // AllReduce MinMax i32 NVLS_TREE SIMPLE
/* 502*/ 69, // AllReduce MinMax u32 TREE LL
/* 503*/ 70, // AllReduce MinMax u32 TREE LL128
/* 504*/ 71, // AllReduce MinMax u32 TREE SIMPLE
/* 505*/ 66, // AllReduce MinMax u32 RING LL
/* 506*/ 67, // AllReduce MinMax u32 RING LL128
/* 507*/ 68, // AllReduce MinMax u32 RING SIMPLE
/* 508*/ -1,
/* 509*/ -1,
/* 510*/ 63, // AllReduce MinMax u32 COLLNET_DIRECT SIMPLE
/* 511*/ -1,
/* 512*/ -1,
/* 513*/ 62, // AllReduce MinMax u32 COLLNET_CHAIN SIMPLE
/* 514*/ -1,
/* 515*/ -1,
/* 516*/ 64, // AllReduce MinMax u32 NVLS SIMPLE
/* 517*/ -1,
/* 518*/ -1,
/* 519*/ 65, // AllReduce MinMax u32 NVLS_TREE SIMPLE
/* 520*/ 79, // AllReduce MinMax i64 TREE LL
/* 521*/ 80, // AllReduce MinMax i64 TREE LL128
/* 522*/ 81, // AllReduce MinMax i64 TREE SIMPLE
/* 523*/ 76, // AllReduce MinMax i64 RING LL
/* 524*/ 77, // AllReduce MinMax i64 RING LL128
/* 525*/ 78, // AllReduce MinMax i64 RING SIMPLE
/* 526*/ -1,
/* 527*/ -1,
/* 528*/ 73, // AllReduce MinMax i64 COLLNET_DIRECT SIMPLE
/* 529*/ -1,
/* 530*/ -1,
/* 531*/ 72, // AllReduce MinMax i64 COLLNET_CHAIN SIMPLE
/* 532*/ -1,
/* 533*/ -1,
/* 534*/ 60, // AllReduce MinMax i64 NVLS SIMPLE
/* 535*/ -1,
/* 536*/ -1,
/* 537*/ 61, // AllReduce MinMax i64 NVLS_TREE SIMPLE
/* 538*/ 79, // AllReduce MinMax u64 TREE LL
/* 539*/ 80, // AllReduce MinMax u64 TREE LL128
/* 540*/ 81, // AllReduce MinMax u64 TREE SIMPLE
/* 541*/ 76, // AllReduce MinMax u64 RING LL
/* 542*/ 77, // AllReduce MinMax u64 RING LL128
/* 543*/ 78, // AllReduce MinMax u64 RING SIMPLE
/* 544*/ -1,
/* 545*/ -1,
/* 546*/ 73, // AllReduce MinMax u64 COLLNET_DIRECT SIMPLE
/* 547*/ -1,
/* 548*/ -1,
/* 549*/ 72, // AllReduce MinMax u64 COLLNET_CHAIN SIMPLE
/* 550*/ -1,
/* 551*/ -1,
/* 552*/ 74, // AllReduce MinMax u64 NVLS SIMPLE
/* 553*/ -1,
/* 554*/ -1,
/* 555*/ 75, // AllReduce MinMax u64 NVLS_TREE SIMPLE
/* 556*/ 23, // AllReduce MinMax f16 TREE LL
/* 557*/ 24, // AllReduce MinMax f16 TREE LL128
/* 558*/ 25, // AllReduce MinMax f16 TREE SIMPLE
/* 559*/ 20, // AllReduce MinMax f16 RING LL
/* 560*/ 21, // AllReduce MinMax f16 RING LL128
/* 561*/ 22, // AllReduce MinMax f16 RING SIMPLE
/* 562*/ -1,
/* 563*/ -1,
/* 564*/ 17, // AllReduce MinMax f16 COLLNET_DIRECT SIMPLE
/* 565*/ -1,
/* 566*/ -1,
/* 567*/ 16, // AllReduce MinMax f16 COLLNET_CHAIN SIMPLE
/* 568*/ -1,
/* 569*/ -1,
/* 570*/ 18, // AllReduce MinMax f16 NVLS SIMPLE
/* 571*/ -1,
/* 572*/ -1,
/* 573*/ 19, // AllReduce MinMax f16 NVLS_TREE SIMPLE
/* 574*/ 31, // AllReduce MinMax f32 TREE LL
/* 575*/ 32, // AllReduce MinMax f32 TREE LL128
/* 576*/ 33, // AllReduce MinMax f32 TREE SIMPLE
/* 577*/ 28, // AllReduce MinMax f32 RING LL
/* 578*/ 29, // AllReduce MinMax f32 RING LL128
/* 579*/ 30, // AllReduce MinMax f32 RING SIMPLE
/* 580*/ -1,
/* 581*/ -1,
/* 582*/ 27, // AllReduce MinMax f32 COLLNET_DIRECT SIMPLE
/* 583*/ -1,
/* 584*/ -1,
/* 585*/ 26, // AllReduce MinMax f32 COLLNET_CHAIN SIMPLE
/* 586*/ -1,
/* 587*/ -1,
/* 588*/ -1,
/* 589*/ -1,
/* 590*/ -1,
/* 591*/ -1,
/* 592*/ 39, // AllReduce MinMax f64 TREE LL
/* 593*/ 40, // AllReduce MinMax f64 TREE LL128
/* 594*/ 41, // AllReduce MinMax f64 TREE SIMPLE
/* 595*/ 36, // AllReduce MinMax f64 RING LL
/* 596*/ 37, // AllReduce MinMax f64 RING LL128
/* 597*/ 38, // AllReduce MinMax f64 RING SIMPLE
/* 598*/ -1,
/* 599*/ -1,
/* 600*/ 35, // AllReduce MinMax f64 COLLNET_DIRECT SIMPLE
/* 601*/ -1,
/* 602*/ -1,
/* 603*/ 34, // AllReduce MinMax f64 COLLNET_CHAIN SIMPLE
/* 604*/ -1,
/* 605*/ -1,
/* 606*/ -1,
/* 607*/ -1,
/* 608*/ -1,
/* 609*/ -1,
/* 610*/ 13, // AllReduce MinMax bf16 TREE LL
/* 611*/ 14, // AllReduce MinMax bf16 TREE LL128
/* 612*/ 15, // AllReduce MinMax bf16 TREE SIMPLE
/* 613*/ 10, // AllReduce MinMax bf16 RING LL
/* 614*/ 11, // AllReduce MinMax bf16 RING LL128
/* 615*/ 12, // AllReduce MinMax bf16 RING SIMPLE
/* 616*/ -1,
/* 617*/ -1,
/* 618*/ 7, // AllReduce MinMax bf16 COLLNET_DIRECT SIMPLE
/* 619*/ -1,
/* 620*/ -1,
/* 621*/ 6, // AllReduce MinMax bf16 COLLNET_CHAIN SIMPLE
/* 622*/ -1,
/* 623*/ -1,
/* 624*/ 8, // AllReduce MinMax bf16 NVLS SIMPLE
/* 625*/ -1,
/* 626*/ -1,
/* 627*/ 9, // AllReduce MinMax bf16 NVLS_TREE SIMPLE
/* 628*/ 47, // AllReduce MinMax f8e4m3 TREE LL
/* 629*/ 48, // AllReduce MinMax f8e4m3 TREE LL128
/* 630*/ 49, // AllReduce MinMax f8e4m3 TREE SIMPLE
/* 631*/ 44, // AllReduce MinMax f8e4m3 RING LL
/* 632*/ 45, // AllReduce MinMax f8e4m3 RING LL128
/* 633*/ 46, // AllReduce MinMax f8e4m3 RING SIMPLE
/* 634*/ -1,
/* 635*/ -1,
/* 636*/ 43, // AllReduce MinMax f8e4m3 COLLNET_DIRECT SIMPLE
/* 637*/ -1,
/* 638*/ -1,
/* 639*/ 42, // AllReduce MinMax f8e4m3 COLLNET_CHAIN SIMPLE
/* 640*/ -1,
/* 641*/ -1,
/* 642*/ -1,
/* 643*/ -1,
/* 644*/ -1,
/* 645*/ -1,
/* 646*/ 55, // AllReduce MinMax f8e5m2 TREE LL
/* 647*/ 56, // AllReduce MinMax f8e5m2 TREE LL128
/* 648*/ 57, // AllReduce MinMax f8e5m2 TREE SIMPLE
/* 649*/ 52, // AllReduce MinMax f8e5m2 RING LL
/* 650*/ 53, // AllReduce MinMax f8e5m2 RING LL128
/* 651*/ 54, // AllReduce MinMax f8e5m2 RING SIMPLE
/* 652*/ -1,
/* 653*/ -1,
/* 654*/ 51, // AllReduce MinMax f8e5m2 COLLNET_DIRECT SIMPLE
/* 655*/ -1,
/* 656*/ -1,
/* 657*/ 50, // AllReduce MinMax f8e5m2 COLLNET_CHAIN SIMPLE
/* 658*/ -1,
/* 659*/ -1,
/* 660*/ -1,
/* 661*/ -1,
/* 662*/ -1,
/* 663*/ -1,
/* 664*/ 159, // AllReduce PreMulSum i8 TREE LL
/* 665*/ 160, // AllReduce PreMulSum i8 TREE LL128
/* 666*/ 161, // AllReduce PreMulSum i8 TREE SIMPLE
/* 667*/ 156, // AllReduce PreMulSum i8 RING LL
/* 668*/ 157, // AllReduce PreMulSum i8 RING LL128
/* 669*/ 158, // AllReduce PreMulSum i8 RING SIMPLE
/* 670*/ -1,
/* 671*/ -1,
/* 672*/ 155, // AllReduce PreMulSum i8 COLLNET_DIRECT SIMPLE
/* 673*/ -1,
/* 674*/ -1,
/* 675*/ 154, // AllReduce PreMulSum i8 COLLNET_CHAIN SIMPLE
/* 676*/ -1,
/* 677*/ -1,
/* 678*/ -1,
/* 679*/ -1,
/* 680*/ -1,
/* 681*/ -1,
/* 682*/ 159, // AllReduce PreMulSum u8 TREE LL
/* 683*/ 160, // AllReduce PreMulSum u8 TREE LL128
/* 684*/ 161, // AllReduce PreMulSum u8 TREE SIMPLE
/* 685*/ 156, // AllReduce PreMulSum u8 RING LL
/* 686*/ 157, // AllReduce PreMulSum u8 RING LL128
/* 687*/ 158, // AllReduce PreMulSum u8 RING SIMPLE
/* 688*/ -1,
/* 689*/ -1,
/* 690*/ 155, // AllReduce PreMulSum u8 COLLNET_DIRECT SIMPLE
/* 691*/ -1,
/* 692*/ -1,
/* 693*/ 154, // AllReduce PreMulSum u8 COLLNET_CHAIN SIMPLE
/* 694*/ -1,
/* 695*/ -1,
/* 696*/ -1,
/* 697*/ -1,
/* 698*/ -1,
/* 699*/ -1,
/* 700*/ 143, // AllReduce PreMulSum i32 TREE LL
/* 701*/ 144, // AllReduce PreMulSum i32 TREE LL128
/* 702*/ 145, // AllReduce PreMulSum i32 TREE SIMPLE
/* 703*/ 140, // AllReduce PreMulSum i32 RING LL
/* 704*/ 141, // AllReduce PreMulSum i32 RING LL128
/* 705*/ 142, // AllReduce PreMulSum i32 RING SIMPLE
/* 706*/ -1,
/* 707*/ -1,
/* 708*/ 139, // AllReduce PreMulSum i32 COLLNET_DIRECT SIMPLE
/* 709*/ -1,
/* 710*/ -1,
/* 711*/ 138, // AllReduce PreMulSum i32 COLLNET_CHAIN SIMPLE
/* 712*/ -1,
/* 713*/ -1,
/* 714*/ -1,
/* 715*/ -1,
/* 716*/ -1,
/* 717*/ -1,
/* 718*/ 143, // AllReduce PreMulSum u32 TREE LL
/* 719*/ 144, // AllReduce PreMulSum u32 TREE LL128
/* 720*/ 145, // AllReduce PreMulSum u32 TREE SIMPLE
/* 721*/ 140, // AllReduce PreMulSum u32 RING LL
/* 722*/ 141, // AllReduce PreMulSum u32 RING LL128
/* 723*/ 142, // AllReduce PreMulSum u32 RING SIMPLE
/* 724*/ -1,
/* 725*/ -1,
/* 726*/ 139, // AllReduce PreMulSum u32 COLLNET_DIRECT SIMPLE
/* 727*/ -1,
/* 728*/ -1,
/* 729*/ 138, // AllReduce PreMulSum u32 COLLNET_CHAIN SIMPLE
/* 730*/ -1,
/* 731*/ -1,
/* 732*/ -1,
/* 733*/ -1,
/* 734*/ -1,
/* 735*/ -1,
/* 736*/ 151, // AllReduce PreMulSum i64 TREE LL
/* 737*/ 152, // AllReduce PreMulSum i64 TREE LL128
/* 738*/ 153, // AllReduce PreMulSum i64 TREE SIMPLE
/* 739*/ 148, // AllReduce PreMulSum i64 RING LL
/* 740*/ 149, // AllReduce PreMulSum i64 RING LL128
/* 741*/ 150, // AllReduce PreMulSum i64 RING SIMPLE
/* 742*/ -1,
/* 743*/ -1,
/* 744*/ 147, // AllReduce PreMulSum i64 COLLNET_DIRECT SIMPLE
/* 745*/ -1,
/* 746*/ -1,
/* 747*/ 146, // AllReduce PreMulSum i64 COLLNET_CHAIN SIMPLE
/* 748*/ -1,
/* 749*/ -1,
/* 750*/ -1,
/* 751*/ -1,
/* 752*/ -1,
/* 753*/ -1,
/* 754*/ 151, // AllReduce PreMulSum u64 TREE LL
/* 755*/ 152, // AllReduce PreMulSum u64 TREE LL128
/* 756*/ 153, // AllReduce PreMulSum u64 TREE SIMPLE
/* 757*/ 148, // AllReduce PreMulSum u64 RING LL
/* 758*/ 149, // AllReduce PreMulSum u64 RING LL128
/* 759*/ 150, // AllReduce PreMulSum u64 RING SIMPLE
/* 760*/ -1,
/* 761*/ -1,
/* 762*/ 147, // AllReduce PreMulSum u64 COLLNET_DIRECT SIMPLE
/* 763*/ -1,
/* 764*/ -1,
/* 765*/ 146, // AllReduce PreMulSum u64 COLLNET_CHAIN SIMPLE
/* 766*/ -1,
/* 767*/ -1,
/* 768*/ -1,
/* 769*/ -1,
/* 770*/ -1,
/* 771*/ -1,
/* 772*/ 103, // AllReduce PreMulSum f16 TREE LL
/* 773*/ 104, // AllReduce PreMulSum f16 TREE LL128
/* 774*/ 105, // AllReduce PreMulSum f16 TREE SIMPLE
/* 775*/ 100, // AllReduce PreMulSum f16 RING LL
/* 776*/ 101, // AllReduce PreMulSum f16 RING LL128
/* 777*/ 102, // AllReduce PreMulSum f16 RING SIMPLE
/* 778*/ -1,
/* 779*/ -1,
/* 780*/ 99, // AllReduce PreMulSum f16 COLLNET_DIRECT SIMPLE
/* 781*/ -1,
/* 782*/ -1,
/* 783*/ 98, // AllReduce PreMulSum f16 COLLNET_CHAIN SIMPLE
/* 784*/ -1,
/* 785*/ -1,
/* 786*/ -1,
/* 787*/ -1,
/* 788*/ -1,
/* 789*/ -1,
/* 790*/ 111, // AllReduce PreMulSum f32 TREE LL
/* 791*/ 112, // AllReduce PreMulSum f32 TREE LL128
/* 792*/ 113, // AllReduce PreMulSum f32 TREE SIMPLE
/* 793*/ 108, // AllReduce PreMulSum f32 RING LL
/* 794*/ 109, // AllReduce PreMulSum f32 RING LL128
/* 795*/ 110, // AllReduce PreMulSum f32 RING SIMPLE
/* 796*/ -1,
/* 797*/ -1,
/* 798*/ 107, // AllReduce PreMulSum f32 COLLNET_DIRECT SIMPLE
/* 799*/ -1,
/* 800*/ -1,
/* 801*/ 106, // AllReduce PreMulSum f32 COLLNET_CHAIN SIMPLE
/* 802*/ -1,
/* 803*/ -1,
/* 804*/ -1,
/* 805*/ -1,
/* 806*/ -1,
/* 807*/ -1,
/* 808*/ 119, // AllReduce PreMulSum f64 TREE LL
/* 809*/ 120, // AllReduce PreMulSum f64 TREE LL128
/* 810*/ 121, // AllReduce PreMulSum f64 TREE SIMPLE
/* 811*/ 116, // AllReduce PreMulSum f64 RING LL
/* 812*/ 117, // AllReduce PreMulSum f64 RING LL128
/* 813*/ 118, // AllReduce PreMulSum f64 RING SIMPLE
/* 814*/ -1,
/* 815*/ -1,
/* 816*/ 115, // AllReduce PreMulSum f64 COLLNET_DIRECT SIMPLE
/* 817*/ -1,
/* 818*/ -1,
/* 819*/ 114, // AllReduce PreMulSum f64 COLLNET_CHAIN SIMPLE
/* 820*/ -1,
/* 821*/ -1,
/* 822*/ -1,
/* 823*/ -1,
/* 824*/ -1,
/* 825*/ -1,
/* 826*/ 95, // AllReduce PreMulSum bf16 TREE LL
/* 827*/ 96, // AllReduce PreMulSum bf16 TREE LL128
/* 828*/ 97, // AllReduce PreMulSum bf16 TREE SIMPLE
/* 829*/ 92, // AllReduce PreMulSum bf16 RING LL
/* 830*/ 93, // AllReduce PreMulSum bf16 RING LL128
/* 831*/ 94, // AllReduce PreMulSum bf16 RING SIMPLE
/* 832*/ -1,
/* 833*/ -1,
/* 834*/ 91, // AllReduce PreMulSum bf16 COLLNET_DIRECT SIMPLE
/* 835*/ -1,
/* 836*/ -1,
/* 837*/ 90, // AllReduce PreMulSum bf16 COLLNET_CHAIN SIMPLE
/* 838*/ -1,
/* 839*/ -1,
/* 840*/ -1,
/* 841*/ -1,
/* 842*/ -1,
/* 843*/ -1,
/* 844*/ 127, // AllReduce PreMulSum f8e4m3 TREE LL
/* 845*/ 128, // AllReduce PreMulSum f8e4m3 TREE LL128
/* 846*/ 129, // AllReduce PreMulSum f8e4m3 TREE SIMPLE
/* 847*/ 124, // AllReduce PreMulSum f8e4m3 RING LL
/* 848*/ 125, // AllReduce PreMulSum f8e4m3 RING LL128
/* 849*/ 126, // AllReduce PreMulSum f8e4m3 RING SIMPLE
/* 850*/ -1,
/* 851*/ -1,
/* 852*/ 123, // AllReduce PreMulSum f8e4m3 COLLNET_DIRECT SIMPLE
/* 853*/ -1,
/* 854*/ -1,
/* 855*/ 122, // AllReduce PreMulSum f8e4m3 COLLNET_CHAIN SIMPLE
/* 856*/ -1,
/* 857*/ -1,
/* 858*/ -1,
/* 859*/ -1,
/* 860*/ -1,
/* 861*/ -1,
/* 862*/ 135, // AllReduce PreMulSum f8e5m2 TREE LL
/* 863*/ 136, // AllReduce PreMulSum f8e5m2 TREE LL128
/* 864*/ 137, // AllReduce PreMulSum f8e5m2 TREE SIMPLE
/* 865*/ 132, // AllReduce PreMulSum f8e5m2 RING LL
/* 866*/ 133, // AllReduce PreMulSum f8e5m2 RING LL128
/* 867*/ 134, // AllReduce PreMulSum f8e5m2 RING SIMPLE
/* 868*/ -1,
/* 869*/ -1,
/* 870*/ 131, // AllReduce PreMulSum f8e5m2 COLLNET_DIRECT SIMPLE
/* 871*/ -1,
/* 872*/ -1,
/* 873*/ 130, // AllReduce PreMulSum f8e5m2 COLLNET_CHAIN SIMPLE
/* 874*/ -1,
/* 875*/ -1,
/* 876*/ -1,
/* 877*/ -1,
/* 878*/ -1,
/* 879*/ -1,
/* 880*/ 339, // AllReduce SumPostDiv i8 TREE LL
/* 881*/ 340, // AllReduce SumPostDiv i8 TREE LL128
/* 882*/ 341, // AllReduce SumPostDiv i8 TREE SIMPLE
/* 883*/ 336, // AllReduce SumPostDiv i8 RING LL
/* 884*/ 337, // AllReduce SumPostDiv i8 RING LL128
/* 885*/ 338, // AllReduce SumPostDiv i8 RING SIMPLE
/* 886*/ -1,
/* 887*/ -1,
/* 888*/ 335, // AllReduce SumPostDiv i8 COLLNET_DIRECT SIMPLE
/* 889*/ -1,
/* 890*/ -1,
/* 891*/ 334, // AllReduce SumPostDiv i8 COLLNET_CHAIN SIMPLE
/* 892*/ -1,
/* 893*/ -1,
/* 894*/ -1,
/* 895*/ -1,
/* 896*/ -1,
/* 897*/ -1,
/* 898*/ 339, // AllReduce SumPostDiv u8 TREE LL
/* 899*/ 340, // AllReduce SumPostDiv u8 TREE LL128
/* 900*/ 341, // AllReduce SumPostDiv u8 TREE SIMPLE
/* 901*/ 336, // AllReduce SumPostDiv u8 RING LL
/* 902*/ 337, // AllReduce SumPostDiv u8 RING LL128
/* 903*/ 338, // AllReduce SumPostDiv u8 RING SIMPLE
/* 904*/ -1,
/* 905*/ -1,
/* 906*/ 335, // AllReduce SumPostDiv u8 COLLNET_DIRECT SIMPLE
/* 907*/ -1,
/* 908*/ -1,
/* 909*/ 334, // AllReduce SumPostDiv u8 COLLNET_CHAIN SIMPLE
/* 910*/ -1,
/* 911*/ -1,
/* 912*/ -1,
/* 913*/ -1,
/* 914*/ -1,
/* 915*/ -1,
/* 916*/ 323, // AllReduce SumPostDiv i32 TREE LL
/* 917*/ 324, // AllReduce SumPostDiv i32 TREE LL128
/* 918*/ 325, // AllReduce SumPostDiv i32 TREE SIMPLE
/* 919*/ 320, // AllReduce SumPostDiv i32 RING LL
/* 920*/ 321, // AllReduce SumPostDiv i32 RING LL128
/* 921*/ 322, // AllReduce SumPostDiv i32 RING SIMPLE
/* 922*/ -1,
/* 923*/ -1,
/* 924*/ 319, // AllReduce SumPostDiv i32 COLLNET_DIRECT SIMPLE
/* 925*/ -1,
/* 926*/ -1,
/* 927*/ 318, // AllReduce SumPostDiv i32 COLLNET_CHAIN SIMPLE
/* 928*/ -1,
/* 929*/ -1,
/* 930*/ -1,
/* 931*/ -1,
/* 932*/ -1,
/* 933*/ -1,
/* 934*/ 323, // AllReduce SumPostDiv u32 TREE LL
/* 935*/ 324, // AllReduce SumPostDiv u32 TREE LL128
/* 936*/ 325, // AllReduce SumPostDiv u32 TREE SIMPLE
/* 937*/ 320, // AllReduce SumPostDiv u32 RING LL
/* 938*/ 321, // AllReduce SumPostDiv u32 RING LL128
/* 939*/ 322, // AllReduce SumPostDiv u32 RING SIMPLE
/* 940*/ -1,
/* 941*/ -1,
/* 942*/ 319, // AllReduce SumPostDiv u32 COLLNET_DIRECT SIMPLE
/* 943*/ -1,
/* 944*/ -1,
/* 945*/ 318, // AllReduce SumPostDiv u32 COLLNET_CHAIN SIMPLE
/* 946*/ -1,
/* 947*/ -1,
/* 948*/ -1,
/* 949*/ -1,
/* 950*/ -1,
/* 951*/ -1,
/* 952*/ 331, // AllReduce SumPostDiv i64 TREE LL
/* 953*/ 332, // AllReduce SumPostDiv i64 TREE LL128
/* 954*/ 333, // AllReduce SumPostDiv i64 TREE SIMPLE
/* 955*/ 328, // AllReduce SumPostDiv i64 RING LL
/* 956*/ 329, // AllReduce SumPostDiv i64 RING LL128
/* 957*/ 330, // AllReduce SumPostDiv i64 RING SIMPLE
/* 958*/ -1,
/* 959*/ -1,
/* 960*/ 327, // AllReduce SumPostDiv i64 COLLNET_DIRECT SIMPLE
/* 961*/ -1,
/* 962*/ -1,
/* 963*/ 326, // AllReduce SumPostDiv i64 COLLNET_CHAIN SIMPLE
/* 964*/ -1,
/* 965*/ -1,
/* 966*/ -1,
/* 967*/ -1,
/* 968*/ -1,
/* 969*/ -1,
/* 970*/ 331, // AllReduce SumPostDiv u64 TREE LL
/* 971*/ 332, // AllReduce SumPostDiv u64 TREE LL128
/* 972*/ 333, // AllReduce SumPostDiv u64 TREE SIMPLE
/* 973*/ 328, // AllReduce SumPostDiv u64 RING LL
/* 974*/ 329, // AllReduce SumPostDiv u64 RING LL128
/* 975*/ 330, // AllReduce SumPostDiv u64 RING SIMPLE
/* 976*/ -1,
/* 977*/ -1,
/* 978*/ 327, // AllReduce SumPostDiv u64 COLLNET_DIRECT SIMPLE
/* 979*/ -1,
/* 980*/ -1,
/* 981*/ 326, // AllReduce SumPostDiv u64 COLLNET_CHAIN SIMPLE
/* 982*/ -1,
/* 983*/ -1,
/* 984*/ -1,
/* 985*/ -1,
/* 986*/ -1,
/* 987*/ -1,
/* 988*/ -1,
/* 989*/ -1,
/* 990*/ -1,
/* 991*/ -1,
/* 992*/ -1,
/* 993*/ -1,
/* 994*/ -1,
/* 995*/ -1,
/* 996*/ -1,
/* 997*/ -1,
/* 998*/ -1,
/* 999*/ -1,
/*1000*/ -1,
/*1001*/ -1,
/*1002*/ -1,
/*1003*/ -1,
/*1004*/ -1,
/*1005*/ -1,
/*1006*/ -1,
/*1007*/ -1,
/*1008*/ -1,
/*1009*/ -1,
/*1010*/ -1,
/*1011*/ -1,
/*1012*/ -1,
/*1013*/ -1,
/*1014*/ -1,
/*1015*/ -1,
/*1016*/ -1,
/*1017*/ -1,
/*1018*/ -1,
/*1019*/ -1,
/*1020*/ -1,
/*1021*/ -1,
/*1022*/ -1,
/*1023*/ -1,
/*1024*/ -1,
/*1025*/ -1,
/*1026*/ -1,
/*1027*/ -1,
/*1028*/ -1,
/*1029*/ -1,
/*1030*/ -1,
/*1031*/ -1,
/*1032*/ -1,
/*1033*/ -1,
/*1034*/ -1,
/*1035*/ -1,
/*1036*/ -1,
/*1037*/ -1,
/*1038*/ -1,
/*1039*/ -1,
/*1040*/ -1,
/*1041*/ -1,
/*1042*/ -1,
/*1043*/ -1,
/*1044*/ -1,
/*1045*/ -1,
/*1046*/ -1,
/*1047*/ -1,
/*1048*/ -1,
/*1049*/ -1,
/*1050*/ -1,
/*1051*/ -1,
/*1052*/ -1,
/*1053*/ -1,
/*1054*/ -1,
/*1055*/ -1,
/*1056*/ -1,
/*1057*/ -1,
/*1058*/ -1,
/*1059*/ -1,
/*1060*/ -1,
/*1061*/ -1,
/*1062*/ -1,
/*1063*/ -1,
/*1064*/ -1,
/*1065*/ -1,
/*1066*/ -1,
/*1067*/ -1,
/*1068*/ -1,
/*1069*/ -1,
/*1070*/ -1,
/*1071*/ -1,
/*1072*/ -1,
/*1073*/ -1,
/*1074*/ -1,
/*1075*/ -1,
/*1076*/ -1,
/*1077*/ -1,
/*1078*/ -1,
/*1079*/ -1,
/*1080*/ -1,
/*1081*/ -1,
/*1082*/ -1,
/*1083*/ -1,
/*1084*/ -1,
/*1085*/ -1,
/*1086*/ -1,
/*1087*/ -1,
/*1088*/ -1,
/*1089*/ -1,
/*1090*/ -1,
/*1091*/ -1,
/*1092*/ -1,
/*1093*/ -1,
/*1094*/ -1,
/*1095*/ -1,
/*1096*/ 450, // Reduce Sum i8 RING LL
/*1097*/ 451, // Reduce Sum i8 RING LL128
/*1098*/ 452, // Reduce Sum i8 RING SIMPLE
/*1099*/ 450, // Reduce Sum u8 RING LL
/*1100*/ 451, // Reduce Sum u8 RING LL128
/*1101*/ 452, // Reduce Sum u8 RING SIMPLE
/*1102*/ 444, // Reduce Sum i32 RING LL
/*1103*/ 445, // Reduce Sum i32 RING LL128
/*1104*/ 446, // Reduce Sum i32 RING SIMPLE
/*1105*/ 444, // Reduce Sum u32 RING LL
/*1106*/ 445, // Reduce Sum u32 RING LL128
/*1107*/ 446, // Reduce Sum u32 RING SIMPLE
/*1108*/ 447, // Reduce Sum i64 RING LL
/*1109*/ 448, // Reduce Sum i64 RING LL128
/*1110*/ 449, // Reduce Sum i64 RING SIMPLE
/*1111*/ 447, // Reduce Sum u64 RING LL
/*1112*/ 448, // Reduce Sum u64 RING LL128
/*1113*/ 449, // Reduce Sum u64 RING SIMPLE
/*1114*/ 429, // Reduce Sum f16 RING LL
/*1115*/ 430, // Reduce Sum f16 RING LL128
/*1116*/ 431, // Reduce Sum f16 RING SIMPLE
/*1117*/ 432, // Reduce Sum f32 RING LL
/*1118*/ 433, // Reduce Sum f32 RING LL128
/*1119*/ 434, // Reduce Sum f32 RING SIMPLE
/*1120*/ 435, // Reduce Sum f64 RING LL
/*1121*/ 436, // Reduce Sum f64 RING LL128
/*1122*/ 437, // Reduce Sum f64 RING SIMPLE
/*1123*/ 426, // Reduce Sum bf16 RING LL
/*1124*/ 427, // Reduce Sum bf16 RING LL128
/*1125*/ 428, // Reduce Sum bf16 RING SIMPLE
/*1126*/ 438, // Reduce Sum f8e4m3 RING LL
/*1127*/ 439, // Reduce Sum f8e4m3 RING LL128
/*1128*/ 440, // Reduce Sum f8e4m3 RING SIMPLE
/*1129*/ 441, // Reduce Sum f8e5m2 RING LL
/*1130*/ 442, // Reduce Sum f8e5m2 RING LL128
/*1131*/ 443, // Reduce Sum f8e5m2 RING SIMPLE
/*1132*/ 423, // Reduce Prod i8 RING LL
/*1133*/ 424, // Reduce Prod i8 RING LL128
/*1134*/ 425, // Reduce Prod i8 RING SIMPLE
/*1135*/ 423, // Reduce Prod u8 RING LL
/*1136*/ 424, // Reduce Prod u8 RING LL128
/*1137*/ 425, // Reduce Prod u8 RING SIMPLE
/*1138*/ 417, // Reduce Prod i32 RING LL
/*1139*/ 418, // Reduce Prod i32 RING LL128
/*1140*/ 419, // Reduce Prod i32 RING SIMPLE
/*1141*/ 417, // Reduce Prod u32 RING LL
/*1142*/ 418, // Reduce Prod u32 RING LL128
/*1143*/ 419, // Reduce Prod u32 RING SIMPLE
/*1144*/ 420, // Reduce Prod i64 RING LL
/*1145*/ 421, // Reduce Prod i64 RING LL128
/*1146*/ 422, // Reduce Prod i64 RING SIMPLE
/*1147*/ 420, // Reduce Prod u64 RING LL
/*1148*/ 421, // Reduce Prod u64 RING LL128
/*1149*/ 422, // Reduce Prod u64 RING SIMPLE
/*1150*/ 402, // Reduce Prod f16 RING LL
/*1151*/ 403, // Reduce Prod f16 RING LL128
/*1152*/ 404, // Reduce Prod f16 RING SIMPLE
/*1153*/ 405, // Reduce Prod f32 RING LL
/*1154*/ 406, // Reduce Prod f32 RING LL128
/*1155*/ 407, // Reduce Prod f32 RING SIMPLE
/*1156*/ 408, // Reduce Prod f64 RING LL
/*1157*/ 409, // Reduce Prod f64 RING LL128
/*1158*/ 410, // Reduce Prod f64 RING SIMPLE
/*1159*/ 399, // Reduce Prod bf16 RING LL
/*1160*/ 400, // Reduce Prod bf16 RING LL128
/*1161*/ 401, // Reduce Prod bf16 RING SIMPLE
/*1162*/ 411, // Reduce Prod f8e4m3 RING LL
/*1163*/ 412, // Reduce Prod f8e4m3 RING LL128
/*1164*/ 413, // Reduce Prod f8e4m3 RING SIMPLE
/*1165*/ 414, // Reduce Prod f8e5m2 RING LL
/*1166*/ 415, // Reduce Prod f8e5m2 RING LL128
/*1167*/ 416, // Reduce Prod f8e5m2 RING SIMPLE
/*1168*/ 369, // Reduce MinMax i8 RING LL
/*1169*/ 370, // Reduce MinMax i8 RING LL128
/*1170*/ 371, // Reduce MinMax i8 RING SIMPLE
/*1171*/ 369, // Reduce MinMax u8 RING LL
/*1172*/ 370, // Reduce MinMax u8 RING LL128
/*1173*/ 371, // Reduce MinMax u8 RING SIMPLE
/*1174*/ 363, // Reduce MinMax i32 RING LL
/*1175*/ 364, // Reduce MinMax i32 RING LL128
/*1176*/ 365, // Reduce MinMax i32 RING SIMPLE
/*1177*/ 363, // Reduce MinMax u32 RING LL
/*1178*/ 364, // Reduce MinMax u32 RING LL128
/*1179*/ 365, // Reduce MinMax u32 RING SIMPLE
/*1180*/ 366, // Reduce MinMax i64 RING LL
/*1181*/ 367, // Reduce MinMax i64 RING LL128
/*1182*/ 368, // Reduce MinMax i64 RING SIMPLE
/*1183*/ 366, // Reduce MinMax u64 RING LL
/*1184*/ 367, // Reduce MinMax u64 RING LL128
/*1185*/ 368, // Reduce MinMax u64 RING SIMPLE
/*1186*/ 348, // Reduce MinMax f16 RING LL
/*1187*/ 349, // Reduce MinMax f16 RING LL128
/*1188*/ 350, // Reduce MinMax f16 RING SIMPLE
/*1189*/ 351, // Reduce MinMax f32 RING LL
/*1190*/ 352, // Reduce MinMax f32 RING LL128
/*1191*/ 353, // Reduce MinMax f32 RING SIMPLE
/*1192*/ 354, // Reduce MinMax f64 RING LL
/*1193*/ 355, // Reduce MinMax f64 RING LL128
/*1194*/ 356, // Reduce MinMax f64 RING SIMPLE
/*1195*/ 345, // Reduce MinMax bf16 RING LL
/*1196*/ 346, // Reduce MinMax bf16 RING LL128
/*1197*/ 347, // Reduce MinMax bf16 RING SIMPLE
/*1198*/ 357, // Reduce MinMax f8e4m3 RING LL
/*1199*/ 358, // Reduce MinMax f8e4m3 RING LL128
/*1200*/ 359, // Reduce MinMax f8e4m3 RING SIMPLE
/*1201*/ 360, // Reduce MinMax f8e5m2 RING LL
/*1202*/ 361, // Reduce MinMax f8e5m2 RING LL128
/*1203*/ 362, // Reduce MinMax f8e5m2 RING SIMPLE
/*1204*/ 396, // Reduce PreMulSum i8 RING LL
/*1205*/ 397, // Reduce PreMulSum i8 RING LL128
/*1206*/ 398, // Reduce PreMulSum i8 RING SIMPLE
/*1207*/ 396, // Reduce PreMulSum u8 RING LL
/*1208*/ 397, // Reduce PreMulSum u8 RING LL128
/*1209*/ 398, // Reduce PreMulSum u8 RING SIMPLE
/*1210*/ 390, // Reduce PreMulSum i32 RING LL
/*1211*/ 391, // Reduce PreMulSum i32 RING LL128
/*1212*/ 392, // Reduce PreMulSum i32 RING SIMPLE
/*1213*/ 390, // Reduce PreMulSum u32 RING LL
/*1214*/ 391, // Reduce PreMulSum u32 RING LL128
/*1215*/ 392, // Reduce PreMulSum u32 RING SIMPLE
/*1216*/ 393, // Reduce PreMulSum i64 RING LL
/*1217*/ 394, // Reduce PreMulSum i64 RING LL128
/*1218*/ 395, // Reduce PreMulSum i64 RING SIMPLE
/*1219*/ 393, // Reduce PreMulSum u64 RING LL
/*1220*/ 394, // Reduce PreMulSum u64 RING LL128
/*1221*/ 395, // Reduce PreMulSum u64 RING SIMPLE
/*1222*/ 375, // Reduce PreMulSum f16 RING LL
/*1223*/ 376, // Reduce PreMulSum f16 RING LL128
/*1224*/ 377, // Reduce PreMulSum f16 RING SIMPLE
/*1225*/ 378, // Reduce PreMulSum f32 RING LL
/*1226*/ 379, // Reduce PreMulSum f32 RING LL128
/*1227*/ 380, // Reduce PreMulSum f32 RING SIMPLE
/*1228*/ 381, // Reduce PreMulSum f64 RING LL
/*1229*/ 382, // Reduce PreMulSum f64 RING LL128
/*1230*/ 383, // Reduce PreMulSum f64 RING SIMPLE
/*1231*/ 372, // Reduce PreMulSum bf16 RING LL
/*1232*/ 373, // Reduce PreMulSum bf16 RING LL128
/*1233*/ 374, // Reduce PreMulSum bf16 RING SIMPLE
/*1234*/ 384, // Reduce PreMulSum f8e4m3 RING LL
/*1235*/ 385, // Reduce PreMulSum f8e4m3 RING LL128
/*1236*/ 386, // Reduce PreMulSum f8e4m3 RING SIMPLE
/*1237*/ 387, // Reduce PreMulSum f8e5m2 RING LL
/*1238*/ 388, // Reduce PreMulSum f8e5m2 RING LL128
/*1239*/ 389, // Reduce PreMulSum f8e5m2 RING SIMPLE
/*1240*/ 459, // Reduce SumPostDiv i8 RING LL
/*1241*/ 460, // Reduce SumPostDiv i8 RING LL128
/*1242*/ 461, // Reduce SumPostDiv i8 RING SIMPLE
/*1243*/ 459, // Reduce SumPostDiv u8 RING LL
/*1244*/ 460, // Reduce SumPostDiv u8 RING LL128
/*1245*/ 461, // Reduce SumPostDiv u8 RING SIMPLE
/*1246*/ 453, // Reduce SumPostDiv i32 RING LL
/*1247*/ 454, // Reduce SumPostDiv i32 RING LL128
/*1248*/ 455, // Reduce SumPostDiv i32 RING SIMPLE
/*1249*/ 453, // Reduce SumPostDiv u32 RING LL
/*1250*/ 454, // Reduce SumPostDiv u32 RING LL128
/*1251*/ 455, // Reduce SumPostDiv u32 RING SIMPLE
/*1252*/ 456, // Reduce SumPostDiv i64 RING LL
/*1253*/ 457, // Reduce SumPostDiv i64 RING LL128
/*1254*/ 458, // Reduce SumPostDiv i64 RING SIMPLE
/*1255*/ 456, // Reduce SumPostDiv u64 RING LL
/*1256*/ 457, // Reduce SumPostDiv u64 RING LL128
/*1257*/ 458, // Reduce SumPostDiv u64 RING SIMPLE
/*1258*/ -1,
/*1259*/ -1,
/*1260*/ -1,
/*1261*/ -1,
/*1262*/ -1,
/*1263*/ -1,
/*1264*/ -1,
/*1265*/ -1,
/*1266*/ -1,
/*1267*/ -1,
/*1268*/ -1,
/*1269*/ -1,
/*1270*/ -1,
/*1271*/ -1,
/*1272*/ -1,
/*1273*/ -1,
/*1274*/ -1,
/*1275*/ -1,
/*1276*/ 651, // ReduceScatter Sum i8 RING LL
/*1277*/ 652, // ReduceScatter Sum i8 RING LL128
/*1278*/ 653, // ReduceScatter Sum i8 RING SIMPLE
/*1279*/ -1,
/*1280*/ -1,
/*1281*/ 649, // ReduceScatter Sum i8 COLLNET_DIRECT SIMPLE
/*1282*/ -1,
/*1283*/ -1,
/*1284*/ -1,
/*1285*/ -1,
/*1286*/ -1,
/*1287*/ 650, // ReduceScatter Sum i8 PAT SIMPLE
/*1288*/ 651, // ReduceScatter Sum u8 RING LL
/*1289*/ 652, // ReduceScatter Sum u8 RING LL128
/*1290*/ 653, // ReduceScatter Sum u8 RING SIMPLE
/*1291*/ -1,
/*1292*/ -1,
/*1293*/ 649, // ReduceScatter Sum u8 COLLNET_DIRECT SIMPLE
/*1294*/ -1,
/*1295*/ -1,
/*1296*/ -1,
/*1297*/ -1,
/*1298*/ -1,
/*1299*/ 650, // ReduceScatter Sum u8 PAT SIMPLE
/*1300*/ 640, // ReduceScatter Sum i32 RING LL
/*1301*/ 641, // ReduceScatter Sum i32 RING LL128
/*1302*/ 642, // ReduceScatter Sum i32 RING SIMPLE
/*1303*/ -1,
/*1304*/ -1,
/*1305*/ 637, // ReduceScatter Sum i32 COLLNET_DIRECT SIMPLE
/*1306*/ -1,
/*1307*/ -1,
/*1308*/ 638, // ReduceScatter Sum i32 NVLS SIMPLE
/*1309*/ -1,
/*1310*/ -1,
/*1311*/ 639, // ReduceScatter Sum i32 PAT SIMPLE
/*1312*/ 640, // ReduceScatter Sum u32 RING LL
/*1313*/ 641, // ReduceScatter Sum u32 RING LL128
/*1314*/ 642, // ReduceScatter Sum u32 RING SIMPLE
/*1315*/ -1,
/*1316*/ -1,
/*1317*/ 637, // ReduceScatter Sum u32 COLLNET_DIRECT SIMPLE
/*1318*/ -1,
/*1319*/ -1,
/*1320*/ 638, // ReduceScatter Sum u32 NVLS SIMPLE
/*1321*/ -1,
/*1322*/ -1,
/*1323*/ 639, // ReduceScatter Sum u32 PAT SIMPLE
/*1324*/ 646, // ReduceScatter Sum i64 RING LL
/*1325*/ 647, // ReduceScatter Sum i64 RING LL128
/*1326*/ 648, // ReduceScatter Sum i64 RING SIMPLE
/*1327*/ -1,
/*1328*/ -1,
/*1329*/ 643, // ReduceScatter Sum i64 COLLNET_DIRECT SIMPLE
/*1330*/ -1,
/*1331*/ -1,
/*1332*/ 644, // ReduceScatter Sum i64 NVLS SIMPLE
/*1333*/ -1,
/*1334*/ -1,
/*1335*/ 645, // ReduceScatter Sum i64 PAT SIMPLE
/*1336*/ 646, // ReduceScatter Sum u64 RING LL
/*1337*/ 647, // ReduceScatter Sum u64 RING LL128
/*1338*/ 648, // ReduceScatter Sum u64 RING SIMPLE
/*1339*/ -1,
/*1340*/ -1,
/*1341*/ 643, // ReduceScatter Sum u64 COLLNET_DIRECT SIMPLE
/*1342*/ -1,
/*1343*/ -1,
/*1344*/ 644, // ReduceScatter Sum u64 NVLS SIMPLE
/*1345*/ -1,
/*1346*/ -1,
/*1347*/ 645, // ReduceScatter Sum u64 PAT SIMPLE
/*1348*/ 612, // ReduceScatter Sum f16 RING LL
/*1349*/ 613, // ReduceScatter Sum f16 RING LL128
/*1350*/ 614, // ReduceScatter Sum f16 RING SIMPLE
/*1351*/ -1,
/*1352*/ -1,
/*1353*/ 609, // ReduceScatter Sum f16 COLLNET_DIRECT SIMPLE
/*1354*/ -1,
/*1355*/ -1,
/*1356*/ 610, // ReduceScatter Sum f16 NVLS SIMPLE
/*1357*/ -1,
/*1358*/ -1,
/*1359*/ 611, // ReduceScatter Sum f16 PAT SIMPLE
/*1360*/ 618, // ReduceScatter Sum f32 RING LL
/*1361*/ 619, // ReduceScatter Sum f32 RING LL128
/*1362*/ 620, // ReduceScatter Sum f32 RING SIMPLE
/*1363*/ -1,
/*1364*/ -1,
/*1365*/ 615, // ReduceScatter Sum f32 COLLNET_DIRECT SIMPLE
/*1366*/ -1,
/*1367*/ -1,
/*1368*/ 616, // ReduceScatter Sum f32 NVLS SIMPLE
/*1369*/ -1,
/*1370*/ -1,
/*1371*/ 617, // ReduceScatter Sum f32 PAT SIMPLE
/*1372*/ 624, // ReduceScatter Sum f64 RING LL
/*1373*/ 625, // ReduceScatter Sum f64 RING LL128
/*1374*/ 626, // ReduceScatter Sum f64 RING SIMPLE
/*1375*/ -1,
/*1376*/ -1,
/*1377*/ 621, // ReduceScatter Sum f64 COLLNET_DIRECT SIMPLE
/*1378*/ -1,
/*1379*/ -1,
/*1380*/ 622, // ReduceScatter Sum f64 NVLS SIMPLE
/*1381*/ -1,
/*1382*/ -1,
/*1383*/ 623, // ReduceScatter Sum f64 PAT SIMPLE
/*1384*/ 606, // ReduceScatter Sum bf16 RING LL
/*1385*/ 607, // ReduceScatter Sum bf16 RING LL128
/*1386*/ 608, // ReduceScatter Sum bf16 RING SIMPLE
/*1387*/ -1,
/*1388*/ -1,
/*1389*/ 603, // ReduceScatter Sum bf16 COLLNET_DIRECT SIMPLE
/*1390*/ -1,
/*1391*/ -1,
/*1392*/ 604, // ReduceScatter Sum bf16 NVLS SIMPLE
/*1393*/ -1,
/*1394*/ -1,
/*1395*/ 605, // ReduceScatter Sum bf16 PAT SIMPLE
/*1396*/ 629, // ReduceScatter Sum f8e4m3 RING LL
/*1397*/ 630, // ReduceScatter Sum f8e4m3 RING LL128
/*1398*/ 631, // ReduceScatter Sum f8e4m3 RING SIMPLE
/*1399*/ -1,
/*1400*/ -1,
/*1401*/ 627, // ReduceScatter Sum f8e4m3 COLLNET_DIRECT SIMPLE
/*1402*/ -1,
/*1403*/ -1,
/*1404*/ -1,
/*1405*/ -1,
/*1406*/ -1,
/*1407*/ 628, // ReduceScatter Sum f8e4m3 PAT SIMPLE
/*1408*/ 634, // ReduceScatter Sum f8e5m2 RING LL
/*1409*/ 635, // ReduceScatter Sum f8e5m2 RING LL128
/*1410*/ 636, // ReduceScatter Sum f8e5m2 RING SIMPLE
/*1411*/ -1,
/*1412*/ -1,
/*1413*/ 632, // ReduceScatter Sum f8e5m2 COLLNET_DIRECT SIMPLE
/*1414*/ -1,
/*1415*/ -1,
/*1416*/ -1,
/*1417*/ -1,
/*1418*/ -1,
/*1419*/ 633, // ReduceScatter Sum f8e5m2 PAT SIMPLE
/*1420*/ 600, // ReduceScatter Prod i8 RING LL
/*1421*/ 601, // ReduceScatter Prod i8 RING LL128
/*1422*/ 602, // ReduceScatter Prod i8 RING SIMPLE
/*1423*/ -1,
/*1424*/ -1,
/*1425*/ 598, // ReduceScatter Prod i8 COLLNET_DIRECT SIMPLE
/*1426*/ -1,
/*1427*/ -1,
/*1428*/ -1,
/*1429*/ -1,
/*1430*/ -1,
/*1431*/ 599, // ReduceScatter Prod i8 PAT SIMPLE
/*1432*/ 600, // ReduceScatter Prod u8 RING LL
/*1433*/ 601, // ReduceScatter Prod u8 RING LL128
/*1434*/ 602, // ReduceScatter Prod u8 RING SIMPLE
/*1435*/ -1,
/*1436*/ -1,
/*1437*/ 598, // ReduceScatter Prod u8 COLLNET_DIRECT SIMPLE
/*1438*/ -1,
/*1439*/ -1,
/*1440*/ -1,
/*1441*/ -1,
/*1442*/ -1,
/*1443*/ 599, // ReduceScatter Prod u8 PAT SIMPLE
/*1444*/ 590, // ReduceScatter Prod i32 RING LL
/*1445*/ 591, // ReduceScatter Prod i32 RING LL128
/*1446*/ 592, // ReduceScatter Prod i32 RING SIMPLE
/*1447*/ -1,
/*1448*/ -1,
/*1449*/ 588, // ReduceScatter Prod i32 COLLNET_DIRECT SIMPLE
/*1450*/ -1,
/*1451*/ -1,
/*1452*/ -1,
/*1453*/ -1,
/*1454*/ -1,
/*1455*/ 589, // ReduceScatter Prod i32 PAT SIMPLE
/*1456*/ 590, // ReduceScatter Prod u32 RING LL
/*1457*/ 591, // ReduceScatter Prod u32 RING LL128
/*1458*/ 592, // ReduceScatter Prod u32 RING SIMPLE
/*1459*/ -1,
/*1460*/ -1,
/*1461*/ 588, // ReduceScatter Prod u32 COLLNET_DIRECT SIMPLE
/*1462*/ -1,
/*1463*/ -1,
/*1464*/ -1,
/*1465*/ -1,
/*1466*/ -1,
/*1467*/ 589, // ReduceScatter Prod u32 PAT SIMPLE
/*1468*/ 595, // ReduceScatter Prod i64 RING LL
/*1469*/ 596, // ReduceScatter Prod i64 RING LL128
/*1470*/ 597, // ReduceScatter Prod i64 RING SIMPLE
/*1471*/ -1,
/*1472*/ -1,
/*1473*/ 593, // ReduceScatter Prod i64 COLLNET_DIRECT SIMPLE
/*1474*/ -1,
/*1475*/ -1,
/*1476*/ -1,
/*1477*/ -1,
/*1478*/ -1,
/*1479*/ 594, // ReduceScatter Prod i64 PAT SIMPLE
/*1480*/ 595, // ReduceScatter Prod u64 RING LL
/*1481*/ 596, // ReduceScatter Prod u64 RING LL128
/*1482*/ 597, // ReduceScatter Prod u64 RING SIMPLE
/*1483*/ -1,
/*1484*/ -1,
/*1485*/ 593, // ReduceScatter Prod u64 COLLNET_DIRECT SIMPLE
/*1486*/ -1,
/*1487*/ -1,
/*1488*/ -1,
/*1489*/ -1,
/*1490*/ -1,
/*1491*/ 594, // ReduceScatter Prod u64 PAT SIMPLE
/*1492*/ 565, // ReduceScatter Prod f16 RING LL
/*1493*/ 566, // ReduceScatter Prod f16 RING LL128
/*1494*/ 567, // ReduceScatter Prod f16 RING SIMPLE
/*1495*/ -1,
/*1496*/ -1,
/*1497*/ 563, // ReduceScatter Prod f16 COLLNET_DIRECT SIMPLE
/*1498*/ -1,
/*1499*/ -1,
/*1500*/ -1,
/*1501*/ -1,
/*1502*/ -1,
/*1503*/ 564, // ReduceScatter Prod f16 PAT SIMPLE
/*1504*/ 570, // ReduceScatter Prod f32 RING LL
/*1505*/ 571, // ReduceScatter Prod f32 RING LL128
/*1506*/ 572, // ReduceScatter Prod f32 RING SIMPLE
/*1507*/ -1,
/*1508*/ -1,
/*1509*/ 568, // ReduceScatter Prod f32 COLLNET_DIRECT SIMPLE
/*1510*/ -1,
/*1511*/ -1,
/*1512*/ -1,
/*1513*/ -1,
/*1514*/ -1,
/*1515*/ 569, // ReduceScatter Prod f32 PAT SIMPLE
/*1516*/ 575, // ReduceScatter Prod f64 RING LL
/*1517*/ 576, // ReduceScatter Prod f64 RING LL128
/*1518*/ 577, // ReduceScatter Prod f64 RING SIMPLE
/*1519*/ -1,
/*1520*/ -1,
/*1521*/ 573, // ReduceScatter Prod f64 COLLNET_DIRECT SIMPLE
/*1522*/ -1,
/*1523*/ -1,
/*1524*/ -1,
/*1525*/ -1,
/*1526*/ -1,
/*1527*/ 574, // ReduceScatter Prod f64 PAT SIMPLE
/*1528*/ 560, // ReduceScatter Prod bf16 RING LL
/*1529*/ 561, // ReduceScatter Prod bf16 RING LL128
/*1530*/ 562, // ReduceScatter Prod bf16 RING SIMPLE
/*1531*/ -1,
/*1532*/ -1,
/*1533*/ 558, // ReduceScatter Prod bf16 COLLNET_DIRECT SIMPLE
/*1534*/ -1,
/*1535*/ -1,
/*1536*/ -1,
/*1537*/ -1,
/*1538*/ -1,
/*1539*/ 559, // ReduceScatter Prod bf16 PAT SIMPLE
/*1540*/ 580, // ReduceScatter Prod f8e4m3 RING LL
/*1541*/ 581, // ReduceScatter Prod f8e4m3 RING LL128
/*1542*/ 582, // ReduceScatter Prod f8e4m3 RING SIMPLE
/*1543*/ -1,
/*1544*/ -1,
/*1545*/ 578, // ReduceScatter Prod f8e4m3 COLLNET_DIRECT SIMPLE
/*1546*/ -1,
/*1547*/ -1,
/*1548*/ -1,
/*1549*/ -1,
/*1550*/ -1,
/*1551*/ 579, // ReduceScatter Prod f8e4m3 PAT SIMPLE
/*1552*/ 585, // ReduceScatter Prod f8e5m2 RING LL
/*1553*/ 586, // ReduceScatter Prod f8e5m2 RING LL128
/*1554*/ 587, // ReduceScatter Prod f8e5m2 RING SIMPLE
/*1555*/ -1,
/*1556*/ -1,
/*1557*/ 583, // ReduceScatter Prod f8e5m2 COLLNET_DIRECT SIMPLE
/*1558*/ -1,
/*1559*/ -1,
/*1560*/ -1,
/*1561*/ -1,
/*1562*/ -1,
/*1563*/ 584, // ReduceScatter Prod f8e5m2 PAT SIMPLE
/*1564*/ 510, // ReduceScatter MinMax i8 RING LL
/*1565*/ 511, // ReduceScatter MinMax i8 RING LL128
/*1566*/ 512, // ReduceScatter MinMax i8 RING SIMPLE
/*1567*/ -1,
/*1568*/ -1,
/*1569*/ 508, // ReduceScatter MinMax i8 COLLNET_DIRECT SIMPLE
/*1570*/ -1,
/*1571*/ -1,
/*1572*/ -1,
/*1573*/ -1,
/*1574*/ -1,
/*1575*/ 509, // ReduceScatter MinMax i8 PAT SIMPLE
/*1576*/ 510, // ReduceScatter MinMax u8 RING LL
/*1577*/ 511, // ReduceScatter MinMax u8 RING LL128
/*1578*/ 512, // ReduceScatter MinMax u8 RING SIMPLE
/*1579*/ -1,
/*1580*/ -1,
/*1581*/ 508, // ReduceScatter MinMax u8 COLLNET_DIRECT SIMPLE
/*1582*/ -1,
/*1583*/ -1,
/*1584*/ -1,
/*1585*/ -1,
/*1586*/ -1,
/*1587*/ 509, // ReduceScatter MinMax u8 PAT SIMPLE
/*1588*/ 499, // ReduceScatter MinMax i32 RING LL
/*1589*/ 500, // ReduceScatter MinMax i32 RING LL128
/*1590*/ 501, // ReduceScatter MinMax i32 RING SIMPLE
/*1591*/ -1,
/*1592*/ -1,
/*1593*/ 496, // ReduceScatter MinMax i32 COLLNET_DIRECT SIMPLE
/*1594*/ -1,
/*1595*/ -1,
/*1596*/ 494, // ReduceScatter MinMax i32 NVLS SIMPLE
/*1597*/ -1,
/*1598*/ -1,
/*1599*/ 498, // ReduceScatter MinMax i32 PAT SIMPLE
/*1600*/ 499, // ReduceScatter MinMax u32 RING LL
/*1601*/ 500, // ReduceScatter MinMax u32 RING LL128
/*1602*/ 501, // ReduceScatter MinMax u32 RING SIMPLE
/*1603*/ -1,
/*1604*/ -1,
/*1605*/ 496, // ReduceScatter MinMax u32 COLLNET_DIRECT SIMPLE
/*1606*/ -1,
/*1607*/ -1,
/*1608*/ 497, // ReduceScatter MinMax u32 NVLS SIMPLE
/*1609*/ -1,
/*1610*/ -1,
/*1611*/ 498, // ReduceScatter MinMax u32 PAT SIMPLE
/*1612*/ 505, // ReduceScatter MinMax i64 RING LL
/*1613*/ 506, // ReduceScatter MinMax i64 RING LL128
/*1614*/ 507, // ReduceScatter MinMax i64 RING SIMPLE
/*1615*/ -1,
/*1616*/ -1,
/*1617*/ 502, // ReduceScatter MinMax i64 COLLNET_DIRECT SIMPLE
/*1618*/ -1,
/*1619*/ -1,
/*1620*/ 495, // ReduceScatter MinMax i64 NVLS SIMPLE
/*1621*/ -1,
/*1622*/ -1,
/*1623*/ 504, // ReduceScatter MinMax i64 PAT SIMPLE
/*1624*/ 505, // ReduceScatter MinMax u64 RING LL
/*1625*/ 506, // ReduceScatter MinMax u64 RING LL128
/*1626*/ 507, // ReduceScatter MinMax u64 RING SIMPLE
/*1627*/ -1,
/*1628*/ -1,
/*1629*/ 502, // ReduceScatter MinMax u64 COLLNET_DIRECT SIMPLE
/*1630*/ -1,
/*1631*/ -1,
/*1632*/ 503, // ReduceScatter MinMax u64 NVLS SIMPLE
/*1633*/ -1,
/*1634*/ -1,
/*1635*/ 504, // ReduceScatter MinMax u64 PAT SIMPLE
/*1636*/ 471, // ReduceScatter MinMax f16 RING LL
/*1637*/ 472, // ReduceScatter MinMax f16 RING LL128
/*1638*/ 473, // ReduceScatter MinMax f16 RING SIMPLE
/*1639*/ -1,
/*1640*/ -1,
/*1641*/ 468, // ReduceScatter MinMax f16 COLLNET_DIRECT SIMPLE
/*1642*/ -1,
/*1643*/ -1,
/*1644*/ 469, // ReduceScatter MinMax f16 NVLS SIMPLE
/*1645*/ -1,
/*1646*/ -1,
/*1647*/ 470, // ReduceScatter MinMax f16 PAT SIMPLE
/*1648*/ 476, // ReduceScatter MinMax f32 RING LL
/*1649*/ 477, // ReduceScatter MinMax f32 RING LL128
/*1650*/ 478, // ReduceScatter MinMax f32 RING SIMPLE
/*1651*/ -1,
/*1652*/ -1,
/*1653*/ 474, // ReduceScatter MinMax f32 COLLNET_DIRECT SIMPLE
/*1654*/ -1,
/*1655*/ -1,
/*1656*/ -1,
/*1657*/ -1,
/*1658*/ -1,
/*1659*/ 475, // ReduceScatter MinMax f32 PAT SIMPLE
/*1660*/ 481, // ReduceScatter MinMax f64 RING LL
/*1661*/ 482, // ReduceScatter MinMax f64 RING LL128
/*1662*/ 483, // ReduceScatter MinMax f64 RING SIMPLE
/*1663*/ -1,
/*1664*/ -1,
/*1665*/ 479, // ReduceScatter MinMax f64 COLLNET_DIRECT SIMPLE
/*1666*/ -1,
/*1667*/ -1,
/*1668*/ -1,
/*1669*/ -1,
/*1670*/ -1,
/*1671*/ 480, // ReduceScatter MinMax f64 PAT SIMPLE
/*1672*/ 465, // ReduceScatter MinMax bf16 RING LL
/*1673*/ 466, // ReduceScatter MinMax bf16 RING LL128
/*1674*/ 467, // ReduceScatter MinMax bf16 RING SIMPLE
/*1675*/ -1,
/*1676*/ -1,
/*1677*/ 462, // ReduceScatter MinMax bf16 COLLNET_DIRECT SIMPLE
/*1678*/ -1,
/*1679*/ -1,
/*1680*/ 463, // ReduceScatter MinMax bf16 NVLS SIMPLE
/*1681*/ -1,
/*1682*/ -1,
/*1683*/ 464, // ReduceScatter MinMax bf16 PAT SIMPLE
/*1684*/ 486, // ReduceScatter MinMax f8e4m3 RING LL
/*1685*/ 487, // ReduceScatter MinMax f8e4m3 RING LL128
/*1686*/ 488, // ReduceScatter MinMax f8e4m3 RING SIMPLE
/*1687*/ -1,
/*1688*/ -1,
/*1689*/ 484, // ReduceScatter MinMax f8e4m3 COLLNET_DIRECT SIMPLE
/*1690*/ -1,
/*1691*/ -1,
/*1692*/ -1,
/*1693*/ -1,
/*1694*/ -1,
/*1695*/ 485, // ReduceScatter MinMax f8e4m3 PAT SIMPLE
/*1696*/ 491, // ReduceScatter MinMax f8e5m2 RING LL
/*1697*/ 492, // ReduceScatter MinMax f8e5m2 RING LL128
/*1698*/ 493, // ReduceScatter MinMax f8e5m2 RING SIMPLE
/*1699*/ -1,
/*1700*/ -1,
/*1701*/ 489, // ReduceScatter MinMax f8e5m2 COLLNET_DIRECT SIMPLE
/*1702*/ -1,
/*1703*/ -1,
/*1704*/ -1,
/*1705*/ -1,
/*1706*/ -1,
/*1707*/ 490, // ReduceScatter MinMax f8e5m2 PAT SIMPLE
/*1708*/ 555, // ReduceScatter PreMulSum i8 RING LL
/*1709*/ 556, // ReduceScatter PreMulSum i8 RING LL128
/*1710*/ 557, // ReduceScatter PreMulSum i8 RING SIMPLE
/*1711*/ -1,
/*1712*/ -1,
/*1713*/ 553, // ReduceScatter PreMulSum i8 COLLNET_DIRECT SIMPLE
/*1714*/ -1,
/*1715*/ -1,
/*1716*/ -1,
/*1717*/ -1,
/*1718*/ -1,
/*1719*/ 554, // ReduceScatter PreMulSum i8 PAT SIMPLE
/*1720*/ 555, // ReduceScatter PreMulSum u8 RING LL
/*1721*/ 556, // ReduceScatter PreMulSum u8 RING LL128
/*1722*/ 557, // ReduceScatter PreMulSum u8 RING SIMPLE
/*1723*/ -1,
/*1724*/ -1,
/*1725*/ 553, // ReduceScatter PreMulSum u8 COLLNET_DIRECT SIMPLE
/*1726*/ -1,
/*1727*/ -1,
/*1728*/ -1,
/*1729*/ -1,
/*1730*/ -1,
/*1731*/ 554, // ReduceScatter PreMulSum u8 PAT SIMPLE
/*1732*/ 545, // ReduceScatter PreMulSum i32 RING LL
/*1733*/ 546, // ReduceScatter PreMulSum i32 RING LL128
/*1734*/ 547, // ReduceScatter PreMulSum i32 RING SIMPLE
/*1735*/ -1,
/*1736*/ -1,
/*1737*/ 543, // ReduceScatter PreMulSum i32 COLLNET_DIRECT SIMPLE
/*1738*/ -1,
/*1739*/ -1,
/*1740*/ -1,
/*1741*/ -1,
/*1742*/ -1,
/*1743*/ 544, // ReduceScatter PreMulSum i32 PAT SIMPLE
/*1744*/ 545, // ReduceScatter PreMulSum u32 RING LL
/*1745*/ 546, // ReduceScatter PreMulSum u32 RING LL128
/*1746*/ 547, // ReduceScatter PreMulSum u32 RING SIMPLE
/*1747*/ -1,
/*1748*/ -1,
/*1749*/ 543, // ReduceScatter PreMulSum u32 COLLNET_DIRECT SIMPLE
/*1750*/ -1,
/*1751*/ -1,
/*1752*/ -1,
/*1753*/ -1,
/*1754*/ -1,
/*1755*/ 544, // ReduceScatter PreMulSum u32 PAT SIMPLE
/*1756*/ 550, // ReduceScatter PreMulSum i64 RING LL
/*1757*/ 551, // ReduceScatter PreMulSum i64 RING LL128
/*1758*/ 552, // ReduceScatter PreMulSum i64 RING SIMPLE
/*1759*/ -1,
/*1760*/ -1,
/*1761*/ 548, // ReduceScatter PreMulSum i64 COLLNET_DIRECT SIMPLE
/*1762*/ -1,
/*1763*/ -1,
/*1764*/ -1,
/*1765*/ -1,
/*1766*/ -1,
/*1767*/ 549, // ReduceScatter PreMulSum i64 PAT SIMPLE
/*1768*/ 550, // ReduceScatter PreMulSum u64 RING LL
/*1769*/ 551, // ReduceScatter PreMulSum u64 RING LL128
/*1770*/ 552, // ReduceScatter PreMulSum u64 RING SIMPLE
/*1771*/ -1,
/*1772*/ -1,
/*1773*/ 548, // ReduceScatter PreMulSum u64 COLLNET_DIRECT SIMPLE
/*1774*/ -1,
/*1775*/ -1,
/*1776*/ -1,
/*1777*/ -1,
/*1778*/ -1,
/*1779*/ 549, // ReduceScatter PreMulSum u64 PAT SIMPLE
/*1780*/ 520, // ReduceScatter PreMulSum f16 RING LL
/*1781*/ 521, // ReduceScatter PreMulSum f16 RING LL128
/*1782*/ 522, // ReduceScatter PreMulSum f16 RING SIMPLE
/*1783*/ -1,
/*1784*/ -1,
/*1785*/ 518, // ReduceScatter PreMulSum f16 COLLNET_DIRECT SIMPLE
/*1786*/ -1,
/*1787*/ -1,
/*1788*/ -1,
/*1789*/ -1,
/*1790*/ -1,
/*1791*/ 519, // ReduceScatter PreMulSum f16 PAT SIMPLE
/*1792*/ 525, // ReduceScatter PreMulSum f32 RING LL
/*1793*/ 526, // ReduceScatter PreMulSum f32 RING LL128
/*1794*/ 527, // ReduceScatter PreMulSum f32 RING SIMPLE
/*1795*/ -1,
/*1796*/ -1,
/*1797*/ 523, // ReduceScatter PreMulSum f32 COLLNET_DIRECT SIMPLE
/*1798*/ -1,
/*1799*/ -1,
/*1800*/ -1,
/*1801*/ -1,
/*1802*/ -1,
/*1803*/ 524, // ReduceScatter PreMulSum f32 PAT SIMPLE
/*1804*/ 530, // ReduceScatter PreMulSum f64 RING LL
/*1805*/ 531, // ReduceScatter PreMulSum f64 RING LL128
/*1806*/ 532, // ReduceScatter PreMulSum f64 RING SIMPLE
/*1807*/ -1,
/*1808*/ -1,
/*1809*/ 528, // ReduceScatter PreMulSum f64 COLLNET_DIRECT SIMPLE
/*1810*/ -1,
/*1811*/ -1,
/*1812*/ -1,
/*1813*/ -1,
/*1814*/ -1,
/*1815*/ 529, // ReduceScatter PreMulSum f64 PAT SIMPLE
/*1816*/ 515, // ReduceScatter PreMulSum bf16 RING LL
/*1817*/ 516, // ReduceScatter PreMulSum bf16 RING LL128
/*1818*/ 517, // ReduceScatter PreMulSum bf16 RING SIMPLE
/*1819*/ -1,
/*1820*/ -1,
/*1821*/ 513, // ReduceScatter PreMulSum bf16 COLLNET_DIRECT SIMPLE
/*1822*/ -1,
/*1823*/ -1,
/*1824*/ -1,
/*1825*/ -1,
/*1826*/ -1,
/*1827*/ 514, // ReduceScatter PreMulSum bf16 PAT SIMPLE
/*1828*/ 535, // ReduceScatter PreMulSum f8e4m3 RING LL
/*1829*/ 536, // ReduceScatter PreMulSum f8e4m3 RING LL128
/*1830*/ 537, // ReduceScatter PreMulSum f8e4m3 RING SIMPLE
/*1831*/ -1,
/*1832*/ -1,
/*1833*/ 533, // ReduceScatter PreMulSum f8e4m3 COLLNET_DIRECT SIMPLE
/*1834*/ -1,
/*1835*/ -1,
/*1836*/ -1,
/*1837*/ -1,
/*1838*/ -1,
/*1839*/ 534, // ReduceScatter PreMulSum f8e4m3 PAT SIMPLE
/*1840*/ 540, // ReduceScatter PreMulSum f8e5m2 RING LL
/*1841*/ 541, // ReduceScatter PreMulSum f8e5m2 RING LL128
/*1842*/ 542, // ReduceScatter PreMulSum f8e5m2 RING SIMPLE
/*1843*/ -1,
/*1844*/ -1,
/*1845*/ 538, // ReduceScatter PreMulSum f8e5m2 COLLNET_DIRECT SIMPLE
/*1846*/ -1,
/*1847*/ -1,
/*1848*/ -1,
/*1849*/ -1,
/*1850*/ -1,
/*1851*/ 539, // ReduceScatter PreMulSum f8e5m2 PAT SIMPLE
/*1852*/ 666, // ReduceScatter SumPostDiv i8 RING LL
/*1853*/ 667, // ReduceScatter SumPostDiv i8 RING LL128
/*1854*/ 668, // ReduceScatter SumPostDiv i8 RING SIMPLE
/*1855*/ -1,
/*1856*/ -1,
/*1857*/ 664, // ReduceScatter SumPostDiv i8 COLLNET_DIRECT SIMPLE
/*1858*/ -1,
/*1859*/ -1,
/*1860*/ -1,
/*1861*/ -1,
/*1862*/ -1,
/*1863*/ 665, // ReduceScatter SumPostDiv i8 PAT SIMPLE
/*1864*/ 666, // ReduceScatter SumPostDiv u8 RING LL
/*1865*/ 667, // ReduceScatter SumPostDiv u8 RING LL128
/*1866*/ 668, // ReduceScatter SumPostDiv u8 RING SIMPLE
/*1867*/ -1,
/*1868*/ -1,
/*1869*/ 664, // ReduceScatter SumPostDiv u8 COLLNET_DIRECT SIMPLE
/*1870*/ -1,
/*1871*/ -1,
/*1872*/ -1,
/*1873*/ -1,
/*1874*/ -1,
/*1875*/ 665, // ReduceScatter SumPostDiv u8 PAT SIMPLE
/*1876*/ 656, // ReduceScatter SumPostDiv i32 RING LL
/*1877*/ 657, // ReduceScatter SumPostDiv i32 RING LL128
/*1878*/ 658, // ReduceScatter SumPostDiv i32 RING SIMPLE
/*1879*/ -1,
/*1880*/ -1,
/*1881*/ 654, // ReduceScatter SumPostDiv i32 COLLNET_DIRECT SIMPLE
/*1882*/ -1,
/*1883*/ -1,
/*1884*/ -1,
/*1885*/ -1,
/*1886*/ -1,
/*1887*/ 655, // ReduceScatter SumPostDiv i32 PAT SIMPLE
/*1888*/ 656, // ReduceScatter SumPostDiv u32 RING LL
/*1889*/ 657, // ReduceScatter SumPostDiv u32 RING LL128
/*1890*/ 658, // ReduceScatter SumPostDiv u32 RING SIMPLE
/*1891*/ -1,
/*1892*/ -1,
/*1893*/ 654, // ReduceScatter SumPostDiv u32 COLLNET_DIRECT SIMPLE
/*1894*/ -1,
/*1895*/ -1,
/*1896*/ -1,
/*1897*/ -1,
/*1898*/ -1,
/*1899*/ 655, // ReduceScatter SumPostDiv u32 PAT SIMPLE
/*1900*/ 661, // ReduceScatter SumPostDiv i64 RING LL
/*1901*/ 662, // ReduceScatter SumPostDiv i64 RING LL128
/*1902*/ 663, // ReduceScatter SumPostDiv i64 RING SIMPLE
/*1903*/ -1,
/*1904*/ -1,
/*1905*/ 659, // ReduceScatter SumPostDiv i64 COLLNET_DIRECT SIMPLE
/*1906*/ -1,
/*1907*/ -1,
/*1908*/ -1,
/*1909*/ -1,
/*1910*/ -1,
/*1911*/ 660, // ReduceScatter SumPostDiv i64 PAT SIMPLE
/*1912*/ 661, // ReduceScatter SumPostDiv u64 RING LL
/*1913*/ 662, // ReduceScatter SumPostDiv u64 RING LL128
/*1914*/ 663, // ReduceScatter SumPostDiv u64 RING SIMPLE
/*1915*/ -1,
/*1916*/ -1,
/*1917*/ 659, // ReduceScatter SumPostDiv u64 COLLNET_DIRECT SIMPLE
/*1918*/ -1,
/*1919*/ -1,
/*1920*/ -1,
/*1921*/ -1,
/*1922*/ -1,
/*1923*/ 660, // ReduceScatter SumPostDiv u64 PAT SIMPLE
/*1924*/ -1,
/*1925*/ -1,
/*1926*/ -1,
/*1927*/ -1,
/*1928*/ -1,
/*1929*/ -1,
/*1930*/ -1,
/*1931*/ -1,
/*1932*/ -1,
/*1933*/ -1,
/*1934*/ -1,
/*1935*/ -1,
/*1936*/ -1,
/*1937*/ -1,
/*1938*/ -1,
/*1939*/ -1,
/*1940*/ -1,
/*1941*/ -1,
/*1942*/ -1,
/*1943*/ -1,
/*1944*/ -1,
/*1945*/ -1,
/*1946*/ -1,
/*1947*/ -1,
/*1948*/ -1,
/*1949*/ -1,
/*1950*/ -1,
/*1951*/ -1,
/*1952*/ -1,
/*1953*/ -1,
/*1954*/ -1,
/*1955*/ -1,
/*1956*/ -1,
/*1957*/ -1,
/*1958*/ -1,
/*1959*/ -1,
/*1960*/ -1,
/*1961*/ -1,
/*1962*/ -1,
/*1963*/ -1,
/*1964*/ -1,
/*1965*/ -1,
/*1966*/ -1,
/*1967*/ -1,
/*1968*/ -1,
/*1969*/ -1,
/*1970*/ -1,
/*1971*/ -1,
/*1972*/ -1,
/*1973*/ -1,
/*1974*/ -1,
/*1975*/ -1,
/*1976*/ -1,
/*1977*/ -1,
/*1978*/ -1,
/*1979*/ -1,
/*1980*/ -1,
/*1981*/ -1,
/*1982*/ -1,
/*1983*/ -1,
/*1984*/ -1,
/*1985*/ -1,
/*1986*/ -1,
/*1987*/ -1,
/*1988*/ -1,
/*1989*/ -1,
/*1990*/ -1,
/*1991*/ -1,
/*1992*/ -1,
/*1993*/ -1,
/*1994*/ -1,
/*1995*/ -1,
-1};

// coverity[declaration]
__global__ void ncclDevKernel_AllGather_RING_LL(ncclDevKernelArgs4K const);
#if CUDART_VERSION >= 11000
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_bf16_RING_LL(ncclDevKernelArgs4K const);
#endif
#if CUDART_VERSION >= 11000
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_bf16_TREE_LL(ncclDevKernelArgs4K const);
#endif
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f16_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f16_TREE_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f32_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f32_TREE_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f64_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f64_TREE_LL(ncclDevKernelArgs4K const);
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL(ncclDevKernelArgs4K const);
#endif
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL(ncclDevKernelArgs4K const);
#endif
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL(ncclDevKernelArgs4K const);
#endif
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL(ncclDevKernelArgs4K const);
#endif
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_u32_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_u32_TREE_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_u64_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_u64_TREE_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_u8_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_AllReduce_Sum_u8_TREE_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_Broadcast_RING_LL(ncclDevKernelArgs4K const);
#if CUDART_VERSION >= 11000
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_bf16_RING_LL(ncclDevKernelArgs4K const);
#endif
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_f16_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_f32_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_f64_RING_LL(ncclDevKernelArgs4K const);
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL(ncclDevKernelArgs4K const);
#endif
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL(ncclDevKernelArgs4K const);
#endif
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_u32_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_u64_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_Reduce_Sum_u8_RING_LL(ncclDevKernelArgs4K const);
#if CUDART_VERSION >= 11000
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL(ncclDevKernelArgs4K const);
#endif
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_f16_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_f32_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_f64_RING_LL(ncclDevKernelArgs4K const);
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL(ncclDevKernelArgs4K const);
#endif
#if CUDART_VERSION >= 11080
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL(ncclDevKernelArgs4K const);
#endif
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_u32_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_u64_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_ReduceScatter_Sum_u8_RING_LL(ncclDevKernelArgs4K const);
// coverity[declaration]
__global__ void ncclDevKernel_SendRecv(ncclDevKernelArgs4K const);

extern int const ncclDevKernelCount = 39;
extern void* const ncclDevKernelList[] = {
/*   0*/ (void*)ncclDevKernel_AllGather_RING_LL,
#if CUDART_VERSION >= 11000
/*   1*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*   1*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*   2*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/*   2*/ nullptr,
#endif
/*   3*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*   4*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/*   5*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/*   6*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/*   7*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/*   8*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
#if CUDART_VERSION >= 11080
/*   9*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/*   9*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  10*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/*  10*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  11*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/*  11*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  12*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/*  12*/ nullptr,
#endif
/*  13*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  14*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/*  15*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  16*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/*  17*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/*  18*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/*  19*/ (void*)ncclDevKernel_Broadcast_RING_LL,
#if CUDART_VERSION >= 11000
/*  20*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/*  20*/ nullptr,
#endif
/*  21*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/*  22*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/*  23*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/*  24*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/*  24*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  25*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/*  25*/ nullptr,
#endif
/*  26*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/*  27*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/*  28*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/*  29*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/*  29*/ nullptr,
#endif
/*  30*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/*  31*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/*  32*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/*  33*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/*  33*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  34*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/*  34*/ nullptr,
#endif
/*  35*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/*  36*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/*  37*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/*  38*/ (void*)ncclDevKernel_SendRecv,
nullptr};

extern void* const ncclDevKernelForFunc[] = {
/*   0*/ (void*)ncclDevKernel_AllGather_RING_LL,
/*   1*/ (void*)ncclDevKernel_AllGather_RING_LL,
/*   2*/ (void*)ncclDevKernel_AllGather_RING_LL,
/*   3*/ (void*)ncclDevKernel_AllGather_RING_LL,
/*   4*/ (void*)ncclDevKernel_AllGather_RING_LL,
/*   5*/ (void*)ncclDevKernel_AllGather_RING_LL,
#if CUDART_VERSION >= 11000
/*   6*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*   6*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*   7*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*   7*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*   8*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*   8*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*   9*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*   9*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  10*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  10*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  11*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  11*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  12*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  12*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  13*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/*  13*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  14*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/*  14*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  15*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/*  15*/ nullptr,
#endif
/*  16*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  17*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  18*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  19*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  20*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  21*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  22*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  23*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/*  24*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/*  25*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/*  26*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/*  27*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/*  28*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/*  29*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/*  30*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/*  31*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/*  32*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/*  33*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/*  34*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/*  35*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/*  36*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/*  37*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/*  38*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/*  39*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/*  40*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/*  41*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
#if CUDART_VERSION >= 11080
/*  42*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/*  42*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  43*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/*  43*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  44*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/*  44*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  45*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/*  45*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  46*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/*  46*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  47*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/*  47*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  48*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/*  48*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  49*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/*  49*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  50*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/*  50*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  51*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/*  51*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  52*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/*  52*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  53*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/*  53*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  54*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/*  54*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  55*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/*  55*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  56*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/*  56*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/*  57*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/*  57*/ nullptr,
#endif
/*  58*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  59*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  60*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  61*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  62*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  63*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  64*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  65*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  66*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  67*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  68*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/*  69*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/*  70*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/*  71*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/*  72*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  73*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  74*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  75*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  76*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  77*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  78*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/*  79*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/*  80*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/*  81*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/*  82*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/*  83*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/*  84*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/*  85*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/*  86*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/*  87*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/*  88*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/*  89*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
#if CUDART_VERSION >= 11000
/*  90*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  90*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  91*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  91*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  92*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  92*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  93*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  93*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  94*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/*  94*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  95*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/*  95*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  96*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/*  96*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/*  97*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/*  97*/ nullptr,
#endif
/*  98*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/*  99*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 100*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 101*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 102*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 103*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 104*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 105*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 106*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 107*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 108*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 109*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 110*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 111*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 112*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 113*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 114*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 115*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 116*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 117*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 118*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 119*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/* 120*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/* 121*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
#if CUDART_VERSION >= 11080
/* 122*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 122*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 123*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 123*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 124*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 124*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 125*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 125*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 126*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 126*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 127*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 127*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 128*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 128*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 129*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 129*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 130*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 130*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 131*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 131*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 132*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 132*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 133*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 133*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 134*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 134*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 135*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 135*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 136*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 136*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 137*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 137*/ nullptr,
#endif
/* 138*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 139*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 140*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 141*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 142*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 143*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 144*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 145*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 146*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 147*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 148*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 149*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 150*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 151*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 152*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 153*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 154*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 155*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 156*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 157*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 158*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 159*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 160*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 161*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
#if CUDART_VERSION >= 11000
/* 162*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 162*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 163*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 163*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 164*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 164*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 165*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 165*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 166*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 166*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 167*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/* 167*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 168*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/* 168*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 169*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/* 169*/ nullptr,
#endif
/* 170*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 171*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 172*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 173*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 174*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 175*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 176*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 177*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 178*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 179*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 180*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 181*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 182*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 183*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 184*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 185*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 186*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 187*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 188*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 189*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 190*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 191*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/* 192*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/* 193*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
#if CUDART_VERSION >= 11080
/* 194*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 194*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 195*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 195*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 196*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 196*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 197*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 197*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 198*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 198*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 199*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 199*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 200*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 200*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 201*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 201*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 202*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 202*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 203*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 203*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 204*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 204*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 205*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 205*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 206*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 206*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 207*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 207*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 208*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 208*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 209*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 209*/ nullptr,
#endif
/* 210*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 211*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 212*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 213*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 214*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 215*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 216*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 217*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 218*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 219*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 220*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 221*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 222*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 223*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 224*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 225*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 226*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 227*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 228*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 229*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 230*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 231*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 232*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 233*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
#if CUDART_VERSION >= 11000
/* 234*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 234*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 235*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 235*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 236*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 236*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 237*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 237*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 238*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 238*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 239*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 239*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 240*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_RING_LL,
#else
/* 240*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 241*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/* 241*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 242*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/* 242*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 243*/ (void*)ncclDevKernel_AllReduce_Sum_bf16_TREE_LL,
#else
/* 243*/ nullptr,
#endif
/* 244*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 245*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 246*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 247*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 248*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 249*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 250*/ (void*)ncclDevKernel_AllReduce_Sum_f16_RING_LL,
/* 251*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 252*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 253*/ (void*)ncclDevKernel_AllReduce_Sum_f16_TREE_LL,
/* 254*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 255*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 256*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 257*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 258*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 259*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 260*/ (void*)ncclDevKernel_AllReduce_Sum_f32_RING_LL,
/* 261*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 262*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 263*/ (void*)ncclDevKernel_AllReduce_Sum_f32_TREE_LL,
/* 264*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 265*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 266*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 267*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 268*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 269*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 270*/ (void*)ncclDevKernel_AllReduce_Sum_f64_RING_LL,
/* 271*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/* 272*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
/* 273*/ (void*)ncclDevKernel_AllReduce_Sum_f64_TREE_LL,
#if CUDART_VERSION >= 11080
/* 274*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 274*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 275*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 275*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 276*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 276*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 277*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 277*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 278*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 278*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 279*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 279*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 280*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 280*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 281*/ (void*)ncclDevKernel_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 281*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 282*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 282*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 283*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 283*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 284*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 284*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 285*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 285*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 286*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 286*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 287*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 287*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 288*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 288*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 289*/ (void*)ncclDevKernel_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 289*/ nullptr,
#endif
/* 290*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 291*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 292*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 293*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 294*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 295*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 296*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 297*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 298*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 299*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 300*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 301*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 302*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 303*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 304*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 305*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 306*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 307*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 308*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 309*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 310*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 311*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 312*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 313*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 314*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 315*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 316*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 317*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 318*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 319*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 320*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 321*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 322*/ (void*)ncclDevKernel_AllReduce_Sum_u32_RING_LL,
/* 323*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 324*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 325*/ (void*)ncclDevKernel_AllReduce_Sum_u32_TREE_LL,
/* 326*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 327*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 328*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 329*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 330*/ (void*)ncclDevKernel_AllReduce_Sum_u64_RING_LL,
/* 331*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 332*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 333*/ (void*)ncclDevKernel_AllReduce_Sum_u64_TREE_LL,
/* 334*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 335*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 336*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 337*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 338*/ (void*)ncclDevKernel_AllReduce_Sum_u8_RING_LL,
/* 339*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 340*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 341*/ (void*)ncclDevKernel_AllReduce_Sum_u8_TREE_LL,
/* 342*/ (void*)ncclDevKernel_Broadcast_RING_LL,
/* 343*/ (void*)ncclDevKernel_Broadcast_RING_LL,
/* 344*/ (void*)ncclDevKernel_Broadcast_RING_LL,
#if CUDART_VERSION >= 11000
/* 345*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 345*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 346*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 346*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 347*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 347*/ nullptr,
#endif
/* 348*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 349*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 350*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 351*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 352*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 353*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 354*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 355*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 356*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 357*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 357*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 358*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 358*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 359*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 359*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 360*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 360*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 361*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 361*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 362*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 362*/ nullptr,
#endif
/* 363*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 364*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 365*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 366*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 367*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 368*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 369*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 370*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 371*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/* 372*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 372*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 373*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 373*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 374*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 374*/ nullptr,
#endif
/* 375*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 376*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 377*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 378*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 379*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 380*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 381*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 382*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 383*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 384*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 384*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 385*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 385*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 386*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 386*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 387*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 387*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 388*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 388*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 389*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 389*/ nullptr,
#endif
/* 390*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 391*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 392*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 393*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 394*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 395*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 396*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 397*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 398*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/* 399*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 399*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 400*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 400*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 401*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 401*/ nullptr,
#endif
/* 402*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 403*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 404*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 405*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 406*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 407*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 408*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 409*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 410*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 411*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 411*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 412*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 412*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 413*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 413*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 414*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 414*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 415*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 415*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 416*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 416*/ nullptr,
#endif
/* 417*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 418*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 419*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 420*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 421*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 422*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 423*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 424*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 425*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/* 426*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 426*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 427*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 427*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 428*/ (void*)ncclDevKernel_Reduce_Sum_bf16_RING_LL,
#else
/* 428*/ nullptr,
#endif
/* 429*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 430*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 431*/ (void*)ncclDevKernel_Reduce_Sum_f16_RING_LL,
/* 432*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 433*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 434*/ (void*)ncclDevKernel_Reduce_Sum_f32_RING_LL,
/* 435*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 436*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
/* 437*/ (void*)ncclDevKernel_Reduce_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 438*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 438*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 439*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 439*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 440*/ (void*)ncclDevKernel_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 440*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 441*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 441*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 442*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 442*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 443*/ (void*)ncclDevKernel_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 443*/ nullptr,
#endif
/* 444*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 445*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 446*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 447*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 448*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 449*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 450*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 451*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 452*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 453*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 454*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 455*/ (void*)ncclDevKernel_Reduce_Sum_u32_RING_LL,
/* 456*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 457*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 458*/ (void*)ncclDevKernel_Reduce_Sum_u64_RING_LL,
/* 459*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 460*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
/* 461*/ (void*)ncclDevKernel_Reduce_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/* 462*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 462*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 463*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 463*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 464*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 464*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 465*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 465*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 466*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 466*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 467*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 467*/ nullptr,
#endif
/* 468*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 469*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 470*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 471*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 472*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 473*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 474*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 475*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 476*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 477*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 478*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 479*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 480*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 481*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 482*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 483*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 484*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 484*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 485*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 485*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 486*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 486*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 487*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 487*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 488*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 488*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 489*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 489*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 490*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 490*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 491*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 491*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 492*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 492*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 493*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 493*/ nullptr,
#endif
/* 494*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 495*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 496*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 497*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 498*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 499*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 500*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 501*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 502*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 503*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 504*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 505*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 506*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 507*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 508*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 509*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 510*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 511*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 512*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/* 513*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 513*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 514*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 514*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 515*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 515*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 516*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 516*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 517*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 517*/ nullptr,
#endif
/* 518*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 519*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 520*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 521*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 522*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 523*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 524*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 525*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 526*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 527*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 528*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 529*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 530*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 531*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 532*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 533*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 533*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 534*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 534*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 535*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 535*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 536*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 536*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 537*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 537*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 538*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 538*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 539*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 539*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 540*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 540*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 541*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 541*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 542*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 542*/ nullptr,
#endif
/* 543*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 544*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 545*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 546*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 547*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 548*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 549*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 550*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 551*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 552*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 553*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 554*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 555*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 556*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 557*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/* 558*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 558*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 559*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 559*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 560*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 560*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 561*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 561*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 562*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 562*/ nullptr,
#endif
/* 563*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 564*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 565*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 566*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 567*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 568*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 569*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 570*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 571*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 572*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 573*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 574*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 575*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 576*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 577*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 578*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 578*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 579*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 579*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 580*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 580*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 581*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 581*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 582*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 582*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 583*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 583*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 584*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 584*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 585*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 585*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 586*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 586*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 587*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 587*/ nullptr,
#endif
/* 588*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 589*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 590*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 591*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 592*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 593*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 594*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 595*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 596*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 597*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 598*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 599*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 600*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 601*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 602*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
#if CUDART_VERSION >= 11000
/* 603*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 603*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 604*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 604*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 605*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 605*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 606*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 606*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 607*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 607*/ nullptr,
#endif
#if CUDART_VERSION >= 11000
/* 608*/ (void*)ncclDevKernel_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 608*/ nullptr,
#endif
/* 609*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 610*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 611*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 612*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 613*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 614*/ (void*)ncclDevKernel_ReduceScatter_Sum_f16_RING_LL,
/* 615*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 616*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 617*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 618*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 619*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 620*/ (void*)ncclDevKernel_ReduceScatter_Sum_f32_RING_LL,
/* 621*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 622*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 623*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 624*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 625*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
/* 626*/ (void*)ncclDevKernel_ReduceScatter_Sum_f64_RING_LL,
#if CUDART_VERSION >= 11080
/* 627*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 627*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 628*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 628*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 629*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 629*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 630*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 630*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 631*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 631*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 632*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 632*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 633*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 633*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 634*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 634*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 635*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 635*/ nullptr,
#endif
#if CUDART_VERSION >= 11080
/* 636*/ (void*)ncclDevKernel_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 636*/ nullptr,
#endif
/* 637*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 638*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 639*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 640*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 641*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 642*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 643*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 644*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 645*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 646*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 647*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 648*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 649*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 650*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 651*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 652*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 653*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 654*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 655*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 656*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 657*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 658*/ (void*)ncclDevKernel_ReduceScatter_Sum_u32_RING_LL,
/* 659*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 660*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 661*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 662*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 663*/ (void*)ncclDevKernel_ReduceScatter_Sum_u64_RING_LL,
/* 664*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 665*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 666*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 667*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 668*/ (void*)ncclDevKernel_ReduceScatter_Sum_u8_RING_LL,
/* 669*/ (void*)ncclDevKernel_SendRecv,
nullptr};

extern bool const ncclDevKernelForFuncIsSpecialized[] = {
/*   0*/ 0,
/*   1*/ 0,
/*   2*/ 0,
/*   3*/ 1,
/*   4*/ 0,
/*   5*/ 0,
/*   6*/ 0,
/*   7*/ 0,
/*   8*/ 0,
/*   9*/ 0,
/*  10*/ 0,
/*  11*/ 0,
/*  12*/ 0,
/*  13*/ 0,
/*  14*/ 0,
/*  15*/ 0,
/*  16*/ 0,
/*  17*/ 0,
/*  18*/ 0,
/*  19*/ 0,
/*  20*/ 0,
/*  21*/ 0,
/*  22*/ 0,
/*  23*/ 0,
/*  24*/ 0,
/*  25*/ 0,
/*  26*/ 0,
/*  27*/ 0,
/*  28*/ 0,
/*  29*/ 0,
/*  30*/ 0,
/*  31*/ 0,
/*  32*/ 0,
/*  33*/ 0,
/*  34*/ 0,
/*  35*/ 0,
/*  36*/ 0,
/*  37*/ 0,
/*  38*/ 0,
/*  39*/ 0,
/*  40*/ 0,
/*  41*/ 0,
/*  42*/ 0,
/*  43*/ 0,
/*  44*/ 0,
/*  45*/ 0,
/*  46*/ 0,
/*  47*/ 0,
/*  48*/ 0,
/*  49*/ 0,
/*  50*/ 0,
/*  51*/ 0,
/*  52*/ 0,
/*  53*/ 0,
/*  54*/ 0,
/*  55*/ 0,
/*  56*/ 0,
/*  57*/ 0,
/*  58*/ 0,
/*  59*/ 0,
/*  60*/ 0,
/*  61*/ 0,
/*  62*/ 0,
/*  63*/ 0,
/*  64*/ 0,
/*  65*/ 0,
/*  66*/ 0,
/*  67*/ 0,
/*  68*/ 0,
/*  69*/ 0,
/*  70*/ 0,
/*  71*/ 0,
/*  72*/ 0,
/*  73*/ 0,
/*  74*/ 0,
/*  75*/ 0,
/*  76*/ 0,
/*  77*/ 0,
/*  78*/ 0,
/*  79*/ 0,
/*  80*/ 0,
/*  81*/ 0,
/*  82*/ 0,
/*  83*/ 0,
/*  84*/ 0,
/*  85*/ 0,
/*  86*/ 0,
/*  87*/ 0,
/*  88*/ 0,
/*  89*/ 0,
/*  90*/ 0,
/*  91*/ 0,
/*  92*/ 0,
/*  93*/ 0,
/*  94*/ 0,
/*  95*/ 0,
/*  96*/ 0,
/*  97*/ 0,
/*  98*/ 0,
/*  99*/ 0,
/* 100*/ 0,
/* 101*/ 0,
/* 102*/ 0,
/* 103*/ 0,
/* 104*/ 0,
/* 105*/ 0,
/* 106*/ 0,
/* 107*/ 0,
/* 108*/ 0,
/* 109*/ 0,
/* 110*/ 0,
/* 111*/ 0,
/* 112*/ 0,
/* 113*/ 0,
/* 114*/ 0,
/* 115*/ 0,
/* 116*/ 0,
/* 117*/ 0,
/* 118*/ 0,
/* 119*/ 0,
/* 120*/ 0,
/* 121*/ 0,
/* 122*/ 0,
/* 123*/ 0,
/* 124*/ 0,
/* 125*/ 0,
/* 126*/ 0,
/* 127*/ 0,
/* 128*/ 0,
/* 129*/ 0,
/* 130*/ 0,
/* 131*/ 0,
/* 132*/ 0,
/* 133*/ 0,
/* 134*/ 0,
/* 135*/ 0,
/* 136*/ 0,
/* 137*/ 0,
/* 138*/ 0,
/* 139*/ 0,
/* 140*/ 0,
/* 141*/ 0,
/* 142*/ 0,
/* 143*/ 0,
/* 144*/ 0,
/* 145*/ 0,
/* 146*/ 0,
/* 147*/ 0,
/* 148*/ 0,
/* 149*/ 0,
/* 150*/ 0,
/* 151*/ 0,
/* 152*/ 0,
/* 153*/ 0,
/* 154*/ 0,
/* 155*/ 0,
/* 156*/ 0,
/* 157*/ 0,
/* 158*/ 0,
/* 159*/ 0,
/* 160*/ 0,
/* 161*/ 0,
/* 162*/ 0,
/* 163*/ 0,
/* 164*/ 0,
/* 165*/ 0,
/* 166*/ 0,
/* 167*/ 0,
/* 168*/ 0,
/* 169*/ 0,
/* 170*/ 0,
/* 171*/ 0,
/* 172*/ 0,
/* 173*/ 0,
/* 174*/ 0,
/* 175*/ 0,
/* 176*/ 0,
/* 177*/ 0,
/* 178*/ 0,
/* 179*/ 0,
/* 180*/ 0,
/* 181*/ 0,
/* 182*/ 0,
/* 183*/ 0,
/* 184*/ 0,
/* 185*/ 0,
/* 186*/ 0,
/* 187*/ 0,
/* 188*/ 0,
/* 189*/ 0,
/* 190*/ 0,
/* 191*/ 0,
/* 192*/ 0,
/* 193*/ 0,
/* 194*/ 0,
/* 195*/ 0,
/* 196*/ 0,
/* 197*/ 0,
/* 198*/ 0,
/* 199*/ 0,
/* 200*/ 0,
/* 201*/ 0,
/* 202*/ 0,
/* 203*/ 0,
/* 204*/ 0,
/* 205*/ 0,
/* 206*/ 0,
/* 207*/ 0,
/* 208*/ 0,
/* 209*/ 0,
/* 210*/ 0,
/* 211*/ 0,
/* 212*/ 0,
/* 213*/ 0,
/* 214*/ 0,
/* 215*/ 0,
/* 216*/ 0,
/* 217*/ 0,
/* 218*/ 0,
/* 219*/ 0,
/* 220*/ 0,
/* 221*/ 0,
/* 222*/ 0,
/* 223*/ 0,
/* 224*/ 0,
/* 225*/ 0,
/* 226*/ 0,
/* 227*/ 0,
/* 228*/ 0,
/* 229*/ 0,
/* 230*/ 0,
/* 231*/ 0,
/* 232*/ 0,
/* 233*/ 0,
/* 234*/ 0,
/* 235*/ 0,
/* 236*/ 0,
/* 237*/ 0,
/* 238*/ 1,
/* 239*/ 0,
/* 240*/ 0,
/* 241*/ 1,
/* 242*/ 0,
/* 243*/ 0,
/* 244*/ 0,
/* 245*/ 0,
/* 246*/ 0,
/* 247*/ 0,
/* 248*/ 1,
/* 249*/ 0,
/* 250*/ 0,
/* 251*/ 1,
/* 252*/ 0,
/* 253*/ 0,
/* 254*/ 0,
/* 255*/ 0,
/* 256*/ 0,
/* 257*/ 0,
/* 258*/ 1,
/* 259*/ 0,
/* 260*/ 0,
/* 261*/ 1,
/* 262*/ 0,
/* 263*/ 0,
/* 264*/ 0,
/* 265*/ 0,
/* 266*/ 0,
/* 267*/ 0,
/* 268*/ 1,
/* 269*/ 0,
/* 270*/ 0,
/* 271*/ 1,
/* 272*/ 0,
/* 273*/ 0,
/* 274*/ 0,
/* 275*/ 0,
/* 276*/ 1,
/* 277*/ 0,
/* 278*/ 0,
/* 279*/ 1,
/* 280*/ 0,
/* 281*/ 0,
/* 282*/ 0,
/* 283*/ 0,
/* 284*/ 1,
/* 285*/ 0,
/* 286*/ 0,
/* 287*/ 1,
/* 288*/ 0,
/* 289*/ 0,
/* 290*/ 0,
/* 291*/ 0,
/* 292*/ 0,
/* 293*/ 0,
/* 294*/ 1,
/* 295*/ 0,
/* 296*/ 0,
/* 297*/ 1,
/* 298*/ 0,
/* 299*/ 0,
/* 300*/ 0,
/* 301*/ 0,
/* 302*/ 0,
/* 303*/ 0,
/* 304*/ 1,
/* 305*/ 0,
/* 306*/ 0,
/* 307*/ 1,
/* 308*/ 0,
/* 309*/ 0,
/* 310*/ 0,
/* 311*/ 0,
/* 312*/ 1,
/* 313*/ 0,
/* 314*/ 0,
/* 315*/ 1,
/* 316*/ 0,
/* 317*/ 0,
/* 318*/ 0,
/* 319*/ 0,
/* 320*/ 0,
/* 321*/ 0,
/* 322*/ 0,
/* 323*/ 0,
/* 324*/ 0,
/* 325*/ 0,
/* 326*/ 0,
/* 327*/ 0,
/* 328*/ 0,
/* 329*/ 0,
/* 330*/ 0,
/* 331*/ 0,
/* 332*/ 0,
/* 333*/ 0,
/* 334*/ 0,
/* 335*/ 0,
/* 336*/ 0,
/* 337*/ 0,
/* 338*/ 0,
/* 339*/ 0,
/* 340*/ 0,
/* 341*/ 0,
/* 342*/ 1,
/* 343*/ 0,
/* 344*/ 0,
/* 345*/ 0,
/* 346*/ 0,
/* 347*/ 0,
/* 348*/ 0,
/* 349*/ 0,
/* 350*/ 0,
/* 351*/ 0,
/* 352*/ 0,
/* 353*/ 0,
/* 354*/ 0,
/* 355*/ 0,
/* 356*/ 0,
/* 357*/ 0,
/* 358*/ 0,
/* 359*/ 0,
/* 360*/ 0,
/* 361*/ 0,
/* 362*/ 0,
/* 363*/ 0,
/* 364*/ 0,
/* 365*/ 0,
/* 366*/ 0,
/* 367*/ 0,
/* 368*/ 0,
/* 369*/ 0,
/* 370*/ 0,
/* 371*/ 0,
/* 372*/ 0,
/* 373*/ 0,
/* 374*/ 0,
/* 375*/ 0,
/* 376*/ 0,
/* 377*/ 0,
/* 378*/ 0,
/* 379*/ 0,
/* 380*/ 0,
/* 381*/ 0,
/* 382*/ 0,
/* 383*/ 0,
/* 384*/ 0,
/* 385*/ 0,
/* 386*/ 0,
/* 387*/ 0,
/* 388*/ 0,
/* 389*/ 0,
/* 390*/ 0,
/* 391*/ 0,
/* 392*/ 0,
/* 393*/ 0,
/* 394*/ 0,
/* 395*/ 0,
/* 396*/ 0,
/* 397*/ 0,
/* 398*/ 0,
/* 399*/ 0,
/* 400*/ 0,
/* 401*/ 0,
/* 402*/ 0,
/* 403*/ 0,
/* 404*/ 0,
/* 405*/ 0,
/* 406*/ 0,
/* 407*/ 0,
/* 408*/ 0,
/* 409*/ 0,
/* 410*/ 0,
/* 411*/ 0,
/* 412*/ 0,
/* 413*/ 0,
/* 414*/ 0,
/* 415*/ 0,
/* 416*/ 0,
/* 417*/ 0,
/* 418*/ 0,
/* 419*/ 0,
/* 420*/ 0,
/* 421*/ 0,
/* 422*/ 0,
/* 423*/ 0,
/* 424*/ 0,
/* 425*/ 0,
/* 426*/ 1,
/* 427*/ 0,
/* 428*/ 0,
/* 429*/ 1,
/* 430*/ 0,
/* 431*/ 0,
/* 432*/ 1,
/* 433*/ 0,
/* 434*/ 0,
/* 435*/ 1,
/* 436*/ 0,
/* 437*/ 0,
/* 438*/ 1,
/* 439*/ 0,
/* 440*/ 0,
/* 441*/ 1,
/* 442*/ 0,
/* 443*/ 0,
/* 444*/ 1,
/* 445*/ 0,
/* 446*/ 0,
/* 447*/ 1,
/* 448*/ 0,
/* 449*/ 0,
/* 450*/ 1,
/* 451*/ 0,
/* 452*/ 0,
/* 453*/ 0,
/* 454*/ 0,
/* 455*/ 0,
/* 456*/ 0,
/* 457*/ 0,
/* 458*/ 0,
/* 459*/ 0,
/* 460*/ 0,
/* 461*/ 0,
/* 462*/ 0,
/* 463*/ 0,
/* 464*/ 0,
/* 465*/ 0,
/* 466*/ 0,
/* 467*/ 0,
/* 468*/ 0,
/* 469*/ 0,
/* 470*/ 0,
/* 471*/ 0,
/* 472*/ 0,
/* 473*/ 0,
/* 474*/ 0,
/* 475*/ 0,
/* 476*/ 0,
/* 477*/ 0,
/* 478*/ 0,
/* 479*/ 0,
/* 480*/ 0,
/* 481*/ 0,
/* 482*/ 0,
/* 483*/ 0,
/* 484*/ 0,
/* 485*/ 0,
/* 486*/ 0,
/* 487*/ 0,
/* 488*/ 0,
/* 489*/ 0,
/* 490*/ 0,
/* 491*/ 0,
/* 492*/ 0,
/* 493*/ 0,
/* 494*/ 0,
/* 495*/ 0,
/* 496*/ 0,
/* 497*/ 0,
/* 498*/ 0,
/* 499*/ 0,
/* 500*/ 0,
/* 501*/ 0,
/* 502*/ 0,
/* 503*/ 0,
/* 504*/ 0,
/* 505*/ 0,
/* 506*/ 0,
/* 507*/ 0,
/* 508*/ 0,
/* 509*/ 0,
/* 510*/ 0,
/* 511*/ 0,
/* 512*/ 0,
/* 513*/ 0,
/* 514*/ 0,
/* 515*/ 0,
/* 516*/ 0,
/* 517*/ 0,
/* 518*/ 0,
/* 519*/ 0,
/* 520*/ 0,
/* 521*/ 0,
/* 522*/ 0,
/* 523*/ 0,
/* 524*/ 0,
/* 525*/ 0,
/* 526*/ 0,
/* 527*/ 0,
/* 528*/ 0,
/* 529*/ 0,
/* 530*/ 0,
/* 531*/ 0,
/* 532*/ 0,
/* 533*/ 0,
/* 534*/ 0,
/* 535*/ 0,
/* 536*/ 0,
/* 537*/ 0,
/* 538*/ 0,
/* 539*/ 0,
/* 540*/ 0,
/* 541*/ 0,
/* 542*/ 0,
/* 543*/ 0,
/* 544*/ 0,
/* 545*/ 0,
/* 546*/ 0,
/* 547*/ 0,
/* 548*/ 0,
/* 549*/ 0,
/* 550*/ 0,
/* 551*/ 0,
/* 552*/ 0,
/* 553*/ 0,
/* 554*/ 0,
/* 555*/ 0,
/* 556*/ 0,
/* 557*/ 0,
/* 558*/ 0,
/* 559*/ 0,
/* 560*/ 0,
/* 561*/ 0,
/* 562*/ 0,
/* 563*/ 0,
/* 564*/ 0,
/* 565*/ 0,
/* 566*/ 0,
/* 567*/ 0,
/* 568*/ 0,
/* 569*/ 0,
/* 570*/ 0,
/* 571*/ 0,
/* 572*/ 0,
/* 573*/ 0,
/* 574*/ 0,
/* 575*/ 0,
/* 576*/ 0,
/* 577*/ 0,
/* 578*/ 0,
/* 579*/ 0,
/* 580*/ 0,
/* 581*/ 0,
/* 582*/ 0,
/* 583*/ 0,
/* 584*/ 0,
/* 585*/ 0,
/* 586*/ 0,
/* 587*/ 0,
/* 588*/ 0,
/* 589*/ 0,
/* 590*/ 0,
/* 591*/ 0,
/* 592*/ 0,
/* 593*/ 0,
/* 594*/ 0,
/* 595*/ 0,
/* 596*/ 0,
/* 597*/ 0,
/* 598*/ 0,
/* 599*/ 0,
/* 600*/ 0,
/* 601*/ 0,
/* 602*/ 0,
/* 603*/ 0,
/* 604*/ 0,
/* 605*/ 0,
/* 606*/ 1,
/* 607*/ 0,
/* 608*/ 0,
/* 609*/ 0,
/* 610*/ 0,
/* 611*/ 0,
/* 612*/ 1,
/* 613*/ 0,
/* 614*/ 0,
/* 615*/ 0,
/* 616*/ 0,
/* 617*/ 0,
/* 618*/ 1,
/* 619*/ 0,
/* 620*/ 0,
/* 621*/ 0,
/* 622*/ 0,
/* 623*/ 0,
/* 624*/ 1,
/* 625*/ 0,
/* 626*/ 0,
/* 627*/ 0,
/* 628*/ 0,
/* 629*/ 1,
/* 630*/ 0,
/* 631*/ 0,
/* 632*/ 0,
/* 633*/ 0,
/* 634*/ 1,
/* 635*/ 0,
/* 636*/ 0,
/* 637*/ 0,
/* 638*/ 0,
/* 639*/ 0,
/* 640*/ 1,
/* 641*/ 0,
/* 642*/ 0,
/* 643*/ 0,
/* 644*/ 0,
/* 645*/ 0,
/* 646*/ 1,
/* 647*/ 0,
/* 648*/ 0,
/* 649*/ 0,
/* 650*/ 0,
/* 651*/ 1,
/* 652*/ 0,
/* 653*/ 0,
/* 654*/ 0,
/* 655*/ 0,
/* 656*/ 0,
/* 657*/ 0,
/* 658*/ 0,
/* 659*/ 0,
/* 660*/ 0,
/* 661*/ 0,
/* 662*/ 0,
/* 663*/ 0,
/* 664*/ 0,
/* 665*/ 0,
/* 666*/ 0,
/* 667*/ 0,
/* 668*/ 0,
/* 669*/ 1,
0};
