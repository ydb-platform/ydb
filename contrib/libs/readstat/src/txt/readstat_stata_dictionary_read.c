#line 1 "src/txt/readstat_stata_dictionary_read.rl"
#include <stdlib.h>

#include "../readstat.h"
#include "readstat_schema.h"

#include "readstat_copy.h"


#line 11 "src/txt/readstat_stata_dictionary_read.c"
static const signed char _stata_dictionary_actions[] = {
	0, 1, 1, 1, 4, 1, 6, 1,
	7, 1, 8, 1, 9, 1, 11, 1,
	13, 1, 14, 1, 15, 1, 16, 1,
	17, 1, 18, 1, 19, 1, 20, 1,
	27, 2, 0, 1, 2, 2, 11, 2,
	7, 8, 2, 10, 4, 2, 12, 5,
	2, 13, 3, 2, 13, 9, 3, 13,
	2, 11, 3, 14, 2, 11, 3, 15,
	2, 11, 3, 16, 2, 11, 3, 17,
	2, 11, 3, 18, 2, 11, 3, 19,
	2, 11, 3, 20, 2, 11, 3, 21,
	12, 5, 3, 22, 12, 5, 3, 23,
	12, 5, 3, 24, 12, 5, 3, 25,
	12, 5, 3, 26, 12, 5, 3, 28,
	0, 1, 4, 13, 3, 2, 11, 0
};

static const short _stata_dictionary_key_offsets[] = {
	0, 0, 4, 6, 10, 11, 12, 14,
	15, 16, 17, 18, 19, 20, 21, 22,
	23, 27, 33, 39, 40, 41, 42, 43,
	44, 48, 61, 74, 75, 76, 77, 81,
	86, 91, 92, 110, 128, 129, 131, 132,
	133, 135, 147, 153, 171, 175, 176, 177,
	178, 179, 180, 181, 185, 190, 193, 211,
	224, 225, 238, 251, 263, 273, 274, 275,
	279, 283, 285, 293, 295, 299, 303, 308,
	310, 323, 336, 349, 362, 375, 387, 400,
	413, 426, 439, 451, 464, 477, 489, 502,
	515, 528, 540, 553, 566, 578, 590, 591,
	592, 593, 594, 595, 596, 597, 598, 599,
	600, 601, 602, 603, 604, 605, 609, 614,
	617, 635, 637, 638, 639, 641, 645, 650,
	653, 671, 672, 676, 681, 684, 702, 703,
	704, 705, 706, 710, 715, 718, 736, 737,
	738, 739, 740, 741, 742, 761, 765, 770,
	773, 791, 803, 804, 805, 806, 807, 808,
	812, 817, 822, 823, 824, 0
};

static const char _stata_dictionary_trans_keys[] = {
	42, 47, 100, 105, 10, 13, 42, 47,
	100, 105, 42, 42, 42, 47, 105, 99,
	116, 105, 111, 110, 97, 114, 121, 9,
	10, 13, 32, 9, 10, 13, 32, 117,
	123, 9, 10, 13, 32, 117, 123, 10,
	115, 105, 110, 103, 9, 10, 13, 32,
	9, 10, 13, 32, 34, 92, 95, 45,
	57, 65, 90, 97, 122, 9, 10, 13,
	32, 34, 92, 95, 45, 57, 65, 90,
	97, 122, 10, 34, 34, 9, 10, 13,
	32, 9, 10, 13, 32, 123, 9, 10,
	13, 32, 123, 10, 9, 10, 13, 32,
	42, 47, 95, 98, 100, 102, 105, 108,
	115, 125, 65, 90, 97, 122, 9, 10,
	13, 32, 42, 47, 95, 98, 100, 102,
	105, 108, 115, 125, 65, 90, 97, 122,
	10, 10, 13, 42, 42, 42, 47, 9,
	10, 13, 32, 46, 95, 48, 57, 65,
	90, 97, 122, 9, 10, 13, 32, 34,
	37, 9, 10, 13, 32, 42, 47, 95,
	98, 100, 102, 105, 108, 115, 125, 65,
	90, 97, 122, 99, 102, 108, 110, 111,
	108, 117, 109, 110, 40, 9, 32, 48,
	57, 9, 32, 41, 48, 57, 9, 32,
	41, 9, 10, 13, 32, 42, 47, 95,
	98, 100, 102, 105, 108, 115, 125, 65,
	90, 97, 122, 9, 10, 13, 32, 46,
	95, 121, 48, 57, 65, 90, 97, 122,
	10, 9, 10, 13, 32, 46, 95, 116,
	48, 57, 65, 90, 97, 122, 9, 10,
	13, 32, 46, 95, 101, 48, 57, 65,
	90, 97, 122, 9, 10, 13, 32, 46,
	95, 48, 57, 65, 90, 97, 122, 9,
	10, 13, 32, 34, 37, 65, 90, 97,
	122, 34, 34, 9, 10, 13, 32, 9,
	10, 13, 32, 48, 57, 44, 46, 83,
	115, 48, 57, 101, 103, 48, 57, 48,
	57, 101, 103, 9, 10, 13, 32, 9,
	10, 13, 32, 34, 48, 57, 9, 10,
	13, 32, 46, 95, 111, 48, 57, 65,
	90, 97, 122, 9, 10, 13, 32, 46,
	95, 117, 48, 57, 65, 90, 97, 122,
	9, 10, 13, 32, 46, 95, 98, 48,
	57, 65, 90, 97, 122, 9, 10, 13,
	32, 46, 95, 108, 48, 57, 65, 90,
	97, 122, 9, 10, 13, 32, 46, 95,
	101, 48, 57, 65, 90, 97, 122, 9,
	10, 13, 32, 46, 95, 48, 57, 65,
	90, 97, 122, 9, 10, 13, 32, 46,
	95, 108, 48, 57, 65, 90, 97, 122,
	9, 10, 13, 32, 46, 95, 111, 48,
	57, 65, 90, 97, 122, 9, 10, 13,
	32, 46, 95, 97, 48, 57, 65, 90,
	98, 122, 9, 10, 13, 32, 46, 95,
	116, 48, 57, 65, 90, 97, 122, 9,
	10, 13, 32, 46, 95, 48, 57, 65,
	90, 97, 122, 9, 10, 13, 32, 46,
	95, 110, 48, 57, 65, 90, 97, 122,
	9, 10, 13, 32, 46, 95, 116, 48,
	57, 65, 90, 97, 122, 9, 10, 13,
	32, 46, 95, 48, 57, 65, 90, 97,
	122, 9, 10, 13, 32, 46, 95, 111,
	48, 57, 65, 90, 97, 122, 9, 10,
	13, 32, 46, 95, 110, 48, 57, 65,
	90, 97, 122, 9, 10, 13, 32, 46,
	95, 103, 48, 57, 65, 90, 97, 122,
	9, 10, 13, 32, 46, 95, 48, 57,
	65, 90, 97, 122, 9, 10, 13, 32,
	46, 95, 116, 48, 57, 65, 90, 97,
	122, 9, 10, 13, 32, 46, 95, 114,
	48, 57, 65, 90, 97, 122, 9, 10,
	13, 32, 46, 95, 48, 57, 65, 90,
	97, 122, 9, 10, 13, 32, 46, 95,
	48, 57, 65, 90, 97, 122, 105, 114,
	115, 116, 108, 105, 110, 101, 111, 102,
	102, 105, 108, 101, 40, 9, 32, 48,
	57, 9, 32, 41, 48, 57, 9, 32,
	41, 9, 10, 13, 32, 42, 47, 95,
	98, 100, 102, 105, 108, 115, 125, 65,
	90, 97, 122, 105, 114, 110, 101, 40,
	115, 9, 32, 48, 57, 9, 32, 41,
	48, 57, 9, 32, 41, 9, 10, 13,
	32, 42, 47, 95, 98, 100, 102, 105,
	108, 115, 125, 65, 90, 97, 122, 40,
	9, 32, 48, 57, 9, 32, 41, 48,
	57, 9, 32, 41, 9, 10, 13, 32,
	42, 47, 95, 98, 100, 102, 105, 108,
	115, 125, 65, 90, 97, 122, 101, 99,
	108, 40, 9, 32, 48, 57, 9, 32,
	41, 48, 57, 9, 32, 41, 9, 10,
	13, 32, 42, 47, 95, 98, 100, 102,
	105, 108, 115, 125, 65, 90, 97, 122,
	101, 119, 108, 105, 110, 101, 9, 10,
	13, 32, 40, 42, 47, 95, 98, 100,
	102, 105, 108, 115, 125, 65, 90, 97,
	122, 9, 32, 48, 57, 9, 32, 41,
	48, 57, 9, 32, 41, 9, 10, 13,
	32, 42, 47, 95, 98, 100, 102, 105,
	108, 115, 125, 65, 90, 97, 122, 9,
	10, 13, 32, 92, 95, 45, 57, 65,
	90, 97, 122, 110, 102, 105, 108, 101,
	9, 10, 13, 32, 9, 10, 13, 32,
	100, 9, 10, 13, 32, 100, 10, 10,
	0
};

static const signed char _stata_dictionary_single_lengths[] = {
	0, 4, 2, 4, 1, 1, 2, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	4, 6, 6, 1, 1, 1, 1, 1,
	4, 7, 7, 1, 1, 1, 4, 5,
	5, 1, 14, 14, 1, 2, 1, 1,
	2, 6, 6, 14, 4, 1, 1, 1,
	1, 1, 1, 2, 3, 3, 14, 7,
	1, 7, 7, 6, 6, 1, 1, 4,
	4, 0, 4, 0, 0, 4, 5, 0,
	7, 7, 7, 7, 7, 6, 7, 7,
	7, 7, 6, 7, 7, 6, 7, 7,
	7, 6, 7, 7, 6, 6, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 2, 3, 3,
	14, 2, 1, 1, 2, 2, 3, 3,
	14, 1, 2, 3, 3, 14, 1, 1,
	1, 1, 2, 3, 3, 14, 1, 1,
	1, 1, 1, 1, 15, 2, 3, 3,
	14, 6, 1, 1, 1, 1, 1, 4,
	5, 5, 1, 1, 0, 0
};

static const signed char _stata_dictionary_range_lengths[] = {
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 3, 3, 0, 0, 0, 0, 0,
	0, 0, 2, 2, 0, 0, 0, 0,
	0, 3, 0, 2, 0, 0, 0, 0,
	0, 0, 0, 1, 1, 0, 2, 3,
	0, 3, 3, 3, 2, 0, 0, 0,
	0, 1, 2, 1, 2, 0, 0, 1,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 1, 1, 0,
	2, 0, 0, 0, 0, 1, 1, 0,
	2, 0, 1, 1, 0, 2, 0, 0,
	0, 0, 1, 1, 0, 2, 0, 0,
	0, 0, 0, 0, 2, 1, 1, 0,
	2, 3, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0
};

static const short _stata_dictionary_index_offsets[] = {
	0, 0, 5, 8, 13, 15, 17, 20,
	22, 24, 26, 28, 30, 32, 34, 36,
	38, 43, 50, 57, 59, 61, 63, 65,
	67, 72, 83, 94, 96, 98, 100, 105,
	111, 117, 119, 136, 153, 155, 158, 160,
	162, 165, 175, 182, 199, 204, 206, 208,
	210, 212, 214, 216, 220, 225, 229, 246,
	257, 259, 270, 281, 291, 300, 302, 304,
	309, 314, 316, 323, 325, 328, 333, 339,
	341, 352, 363, 374, 385, 396, 406, 417,
	428, 439, 450, 460, 471, 482, 492, 503,
	514, 525, 535, 546, 557, 567, 577, 579,
	581, 583, 585, 587, 589, 591, 593, 595,
	597, 599, 601, 603, 605, 607, 611, 616,
	620, 637, 640, 642, 644, 647, 651, 656,
	660, 677, 679, 683, 688, 692, 709, 711,
	713, 715, 717, 721, 726, 730, 747, 749,
	751, 753, 755, 757, 759, 777, 781, 786,
	790, 807, 817, 819, 821, 823, 825, 827,
	832, 838, 844, 846, 848, 0
};

static const short _stata_dictionary_cond_targs[] = {
	2, 4, 7, 146, 0, 3, 155, 2,
	2, 4, 7, 146, 0, 5, 0, 6,
	5, 6, 1, 5, 8, 0, 9, 0,
	10, 0, 11, 0, 12, 0, 13, 0,
	14, 0, 15, 0, 16, 0, 17, 18,
	19, 17, 0, 17, 18, 19, 17, 20,
	34, 0, 17, 18, 19, 17, 20, 34,
	0, 18, 0, 21, 0, 22, 0, 23,
	0, 24, 0, 25, 26, 27, 25, 0,
	25, 26, 27, 25, 28, 145, 145, 145,
	145, 145, 0, 25, 26, 27, 25, 28,
	145, 145, 145, 145, 145, 0, 26, 0,
	30, 29, 30, 29, 31, 32, 33, 31,
	0, 31, 32, 33, 31, 34, 0, 31,
	32, 33, 31, 34, 0, 32, 0, 34,
	35, 36, 34, 37, 38, 44, 55, 72,
	78, 83, 86, 90, 156, 41, 41, 0,
	34, 35, 36, 34, 37, 38, 44, 55,
	72, 78, 83, 86, 90, 156, 41, 41,
	0, 35, 0, 35, 36, 37, 39, 0,
	40, 39, 40, 34, 39, 42, 43, 56,
	42, 41, 41, 41, 41, 41, 0, 42,
	43, 56, 42, 61, 65, 0, 34, 35,
	36, 34, 37, 38, 44, 55, 72, 78,
	83, 86, 90, 156, 41, 41, 0, 45,
	94, 113, 134, 0, 46, 0, 47, 0,
	48, 0, 49, 0, 50, 0, 51, 0,
	51, 51, 52, 0, 53, 53, 54, 52,
	0, 53, 53, 54, 0, 34, 35, 36,
	34, 37, 38, 44, 55, 72, 78, 83,
	86, 90, 156, 41, 41, 0, 42, 43,
	56, 42, 41, 41, 57, 41, 41, 41,
	0, 43, 0, 42, 43, 56, 42, 41,
	41, 58, 41, 41, 41, 0, 42, 43,
	56, 42, 41, 41, 59, 41, 41, 41,
	0, 60, 43, 56, 60, 41, 41, 41,
	41, 41, 0, 60, 43, 56, 60, 61,
	65, 41, 41, 0, 63, 62, 63, 62,
	64, 43, 56, 64, 0, 64, 43, 56,
	64, 0, 66, 0, 67, 71, 69, 69,
	66, 69, 0, 68, 0, 68, 69, 0,
	70, 43, 56, 70, 0, 70, 43, 56,
	70, 61, 0, 68, 0, 42, 43, 56,
	42, 41, 41, 73, 41, 41, 41, 0,
	42, 43, 56, 42, 41, 41, 74, 41,
	41, 41, 0, 42, 43, 56, 42, 41,
	41, 75, 41, 41, 41, 0, 42, 43,
	56, 42, 41, 41, 76, 41, 41, 41,
	0, 42, 43, 56, 42, 41, 41, 77,
	41, 41, 41, 0, 60, 43, 56, 60,
	41, 41, 41, 41, 41, 0, 42, 43,
	56, 42, 41, 41, 79, 41, 41, 41,
	0, 42, 43, 56, 42, 41, 41, 80,
	41, 41, 41, 0, 42, 43, 56, 42,
	41, 41, 81, 41, 41, 41, 0, 42,
	43, 56, 42, 41, 41, 82, 41, 41,
	41, 0, 60, 43, 56, 60, 41, 41,
	41, 41, 41, 0, 42, 43, 56, 42,
	41, 41, 84, 41, 41, 41, 0, 42,
	43, 56, 42, 41, 41, 85, 41, 41,
	41, 0, 60, 43, 56, 60, 41, 41,
	41, 41, 41, 0, 42, 43, 56, 42,
	41, 41, 87, 41, 41, 41, 0, 42,
	43, 56, 42, 41, 41, 88, 41, 41,
	41, 0, 42, 43, 56, 42, 41, 41,
	89, 41, 41, 41, 0, 60, 43, 56,
	60, 41, 41, 41, 41, 41, 0, 42,
	43, 56, 42, 41, 41, 91, 41, 41,
	41, 0, 42, 43, 56, 42, 41, 41,
	92, 41, 41, 41, 0, 42, 43, 56,
	42, 41, 41, 93, 41, 41, 0, 60,
	43, 56, 60, 41, 41, 93, 41, 41,
	0, 95, 0, 96, 0, 97, 0, 98,
	0, 99, 0, 100, 0, 101, 0, 102,
	0, 103, 0, 104, 0, 105, 0, 106,
	0, 107, 0, 108, 0, 109, 0, 109,
	109, 110, 0, 111, 111, 112, 110, 0,
	111, 111, 112, 0, 34, 35, 36, 34,
	37, 38, 44, 55, 72, 78, 83, 86,
	90, 156, 41, 41, 0, 114, 126, 0,
	115, 0, 116, 0, 117, 121, 0, 117,
	117, 118, 0, 119, 119, 120, 118, 0,
	119, 119, 120, 0, 34, 35, 36, 34,
	37, 38, 44, 55, 72, 78, 83, 86,
	90, 156, 41, 41, 0, 122, 0, 122,
	122, 123, 0, 124, 124, 125, 123, 0,
	124, 124, 125, 0, 34, 35, 36, 34,
	37, 38, 44, 55, 72, 78, 83, 86,
	90, 156, 41, 41, 0, 127, 0, 128,
	0, 129, 0, 130, 0, 130, 130, 131,
	0, 132, 132, 133, 131, 0, 132, 132,
	133, 0, 34, 35, 36, 34, 37, 38,
	44, 55, 72, 78, 83, 86, 90, 156,
	41, 41, 0, 135, 0, 136, 0, 137,
	0, 138, 0, 139, 0, 140, 0, 34,
	35, 36, 34, 141, 37, 38, 44, 55,
	72, 78, 83, 86, 90, 156, 41, 41,
	0, 141, 141, 142, 0, 143, 143, 144,
	142, 0, 143, 143, 144, 0, 34, 35,
	36, 34, 37, 38, 44, 55, 72, 78,
	83, 86, 90, 156, 41, 41, 0, 31,
	32, 33, 31, 145, 145, 145, 145, 145,
	0, 147, 0, 148, 0, 149, 0, 150,
	0, 151, 0, 152, 153, 154, 152, 0,
	152, 153, 154, 152, 7, 0, 152, 153,
	154, 152, 7, 0, 153, 0, 3, 0,
	156, 0, 1, 2, 3, 4, 5, 6,
	7, 8, 9, 10, 11, 12, 13, 14,
	15, 16, 17, 18, 19, 20, 21, 22,
	23, 24, 25, 26, 27, 28, 29, 30,
	31, 32, 33, 34, 35, 36, 37, 38,
	39, 40, 41, 42, 43, 44, 45, 46,
	47, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62,
	63, 64, 65, 66, 67, 68, 69, 70,
	71, 72, 73, 74, 75, 76, 77, 78,
	79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94,
	95, 96, 97, 98, 99, 100, 101, 102,
	103, 104, 105, 106, 107, 108, 109, 110,
	111, 112, 113, 114, 115, 116, 117, 118,
	119, 120, 121, 122, 123, 124, 125, 126,
	127, 128, 129, 130, 131, 132, 133, 134,
	135, 136, 137, 138, 139, 140, 141, 142,
	143, 144, 145, 146, 147, 148, 149, 150,
	151, 152, 153, 154, 155, 156, 0
};

static const signed char _stata_dictionary_cond_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0,
	15, 15, 15, 15, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 15, 15, 15, 15, 15, 15,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 11, 11, 11,
	11, 11, 0, 15, 15, 15, 15, 15,
	51, 51, 51, 51, 51, 0, 0, 0,
	39, 7, 9, 0, 3, 3, 3, 3,
	0, 0, 0, 0, 0, 0, 0, 15,
	15, 15, 15, 15, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 36, 36,
	36, 36, 36, 36, 0, 36, 36, 0,
	15, 15, 15, 15, 15, 15, 15, 54,
	54, 54, 54, 54, 54, 15, 54, 54,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 45, 45, 45,
	45, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 48, 48,
	48, 48, 48, 48, 48, 114, 114, 114,
	114, 114, 114, 48, 114, 114, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 33, 0, 0, 0, 0, 1,
	0, 0, 0, 0, 0, 21, 21, 21,
	21, 21, 21, 21, 66, 66, 66, 66,
	66, 66, 21, 66, 66, 0, 45, 45,
	45, 45, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 45, 45, 45, 45, 0,
	0, 0, 0, 0, 0, 0, 45, 45,
	45, 45, 0, 0, 0, 0, 0, 0,
	0, 86, 45, 45, 86, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 13, 13, 0, 39, 7, 9, 0,
	5, 5, 5, 5, 0, 0, 0, 0,
	0, 0, 33, 0, 31, 31, 31, 31,
	1, 31, 0, 110, 0, 1, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 33, 0, 45, 45, 45,
	45, 0, 0, 0, 0, 0, 0, 0,
	45, 45, 45, 45, 0, 0, 0, 0,
	0, 0, 0, 45, 45, 45, 45, 0,
	0, 0, 0, 0, 0, 0, 45, 45,
	45, 45, 0, 0, 0, 0, 0, 0,
	0, 45, 45, 45, 45, 0, 0, 0,
	0, 0, 0, 0, 102, 45, 45, 102,
	0, 0, 0, 0, 0, 0, 45, 45,
	45, 45, 0, 0, 0, 0, 0, 0,
	0, 45, 45, 45, 45, 0, 0, 0,
	0, 0, 0, 0, 45, 45, 45, 45,
	0, 0, 0, 0, 0, 0, 0, 45,
	45, 45, 45, 0, 0, 0, 0, 0,
	0, 0, 98, 45, 45, 98, 0, 0,
	0, 0, 0, 0, 45, 45, 45, 45,
	0, 0, 0, 0, 0, 0, 0, 45,
	45, 45, 45, 0, 0, 0, 0, 0,
	0, 0, 90, 45, 45, 90, 0, 0,
	0, 0, 0, 0, 45, 45, 45, 45,
	0, 0, 0, 0, 0, 0, 0, 45,
	45, 45, 45, 0, 0, 0, 0, 0,
	0, 0, 45, 45, 45, 45, 0, 0,
	0, 0, 0, 0, 0, 94, 45, 45,
	94, 0, 0, 0, 0, 0, 0, 45,
	45, 45, 45, 0, 0, 0, 0, 0,
	0, 0, 45, 45, 45, 45, 0, 0,
	0, 0, 0, 0, 0, 45, 45, 45,
	45, 0, 0, 33, 0, 0, 0, 106,
	45, 45, 106, 0, 0, 1, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 33, 0, 0, 0, 0, 1, 0,
	0, 0, 0, 0, 29, 29, 29, 29,
	29, 29, 29, 82, 82, 82, 82, 82,
	82, 29, 82, 82, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 33, 0, 0, 0, 0, 1, 0,
	0, 0, 0, 0, 19, 19, 19, 19,
	19, 19, 19, 62, 62, 62, 62, 62,
	62, 19, 62, 62, 0, 0, 0, 0,
	0, 33, 0, 0, 0, 0, 1, 0,
	0, 0, 0, 0, 17, 17, 17, 17,
	17, 17, 17, 58, 58, 58, 58, 58,
	58, 17, 58, 58, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 33,
	0, 0, 0, 0, 1, 0, 0, 0,
	0, 0, 27, 27, 27, 27, 27, 27,
	27, 78, 78, 78, 78, 78, 78, 27,
	78, 78, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 23,
	23, 23, 23, 23, 23, 23, 23, 70,
	70, 70, 70, 70, 70, 23, 70, 70,
	0, 0, 0, 33, 0, 0, 0, 0,
	1, 0, 0, 0, 0, 0, 25, 25,
	25, 25, 25, 25, 25, 74, 74, 74,
	74, 74, 74, 25, 74, 74, 0, 42,
	42, 42, 42, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 15, 15,
	15, 15, 15, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0
};

static const int stata_dictionary_start = 1;

static const int stata_dictionary_en_main = 1;


#line 11 "src/txt/readstat_stata_dictionary_read.rl"


readstat_schema_t *readstat_parse_stata_dictionary(readstat_parser_t *parser,
const char *filepath, void *user_ctx, readstat_error_t *outError) {
	if (parser->io->open(filepath, parser->io->io_ctx) == -1) {
		if (outError)
			*outError = READSTAT_ERROR_OPEN;
		return NULL;
	}
	readstat_schema_t *schema = NULL;
	unsigned char *bytes = NULL;
	int cb_return_value = READSTAT_HANDLER_OK;
	int total_entry_count = 0;
	int partial_entry_count = 0;
	readstat_error_t error = READSTAT_OK;
	ssize_t len = parser->io->seek(0, READSTAT_SEEK_END, parser->io->io_ctx);
	if (len == -1) {
		error = READSTAT_ERROR_SEEK;
		goto cleanup;
	}
	parser->io->seek(0, READSTAT_SEEK_SET, parser->io->io_ctx);
	
	bytes = malloc(len);
	
	parser->io->read(bytes, len, parser->io->io_ctx);
	
	unsigned char *p = bytes;
	unsigned char *pe = bytes + len;
	
	unsigned char *str_start = NULL;
	
	size_t str_len = 0;
	
	int cs;
	//    u_char *eof = pe;
	
	int integer = 0;
	int current_row = 0;
	int current_col = 0;
	int line_no = 0;
	unsigned char *line_start = p;
	
	readstat_schema_entry_t current_entry;
	
	if ((schema = calloc(1, sizeof(readstat_schema_t))) == NULL) {
		error = READSTAT_ERROR_MALLOC;
		goto cleanup;
	}
	
	schema->rows_per_observation = 1;
	
	
#line 545 "src/txt/readstat_stata_dictionary_read.c"
	{
		cs = (int)stata_dictionary_start;
	}
	
#line 550 "src/txt/readstat_stata_dictionary_read.c"
	{
		int _klen;
		unsigned int _trans = 0;
		const char * _keys;
		const signed char * _acts;
		unsigned int _nacts;
		_resume: {}
		if ( p == pe )
			goto _out;
		_keys = ( _stata_dictionary_trans_keys + (_stata_dictionary_key_offsets[cs]));
		_trans = (unsigned int)_stata_dictionary_index_offsets[cs];
		
		_klen = (int)_stata_dictionary_single_lengths[cs];
		if ( _klen > 0 ) {
			const char *_lower = _keys;
			const char *_upper = _keys + _klen - 1;
			const char *_mid;
			while ( 1 ) {
				if ( _upper < _lower ) {
					_keys += _klen;
					_trans += (unsigned int)_klen;
					break;
				}
				
				_mid = _lower + ((_upper-_lower) >> 1);
				if ( ( (*( p))) < (*( _mid)) )
					_upper = _mid - 1;
				else if ( ( (*( p))) > (*( _mid)) )
					_lower = _mid + 1;
				else {
					_trans += (unsigned int)(_mid - _keys);
					goto _match;
				}
			}
		}
		
		_klen = (int)_stata_dictionary_range_lengths[cs];
		if ( _klen > 0 ) {
			const char *_lower = _keys;
			const char *_upper = _keys + (_klen<<1) - 2;
			const char *_mid;
			while ( 1 ) {
				if ( _upper < _lower ) {
					_trans += (unsigned int)_klen;
					break;
				}
				
				_mid = _lower + (((_upper-_lower) >> 1) & ~1);
				if ( ( (*( p))) < (*( _mid)) )
					_upper = _mid - 2;
				else if ( ( (*( p))) > (*( _mid + 1)) )
					_lower = _mid + 2;
				else {
					_trans += (unsigned int)((_mid - _keys)>>1);
					break;
				}
			}
		}
		
		_match: {}
		cs = (int)_stata_dictionary_cond_targs[_trans];
		
		if ( _stata_dictionary_cond_actions[_trans] != 0 ) {
			
			_acts = ( _stata_dictionary_actions + (_stata_dictionary_cond_actions[_trans]));
			_nacts = (unsigned int)(*( _acts));
			_acts += 1;
			while ( _nacts > 0 ) {
				switch ( (*( _acts)) )
				{
					case 0:  {
						{
#line 63 "src/txt/readstat_stata_dictionary_read.rl"
							
							integer = 0;
						}
						
#line 628 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 1:  {
						{
#line 67 "src/txt/readstat_stata_dictionary_read.rl"
							
							integer = 10 * integer + ((( (*( p)))) - '0');
						}
						
#line 639 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 2:  {
						{
#line 71 "src/txt/readstat_stata_dictionary_read.rl"
							
							memset(&current_entry, 0, sizeof(readstat_schema_entry_t));
							current_entry.decimal_separator = '.';
							current_entry.variable.type = READSTAT_TYPE_DOUBLE;
							current_entry.variable.index = total_entry_count;
						}
						
#line 653 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 3:  {
						{
#line 78 "src/txt/readstat_stata_dictionary_read.rl"
							
							current_entry.row = current_row;
							current_entry.col = current_col;
							current_col += current_entry.len;
							cb_return_value = READSTAT_HANDLER_OK;
							if (parser->handlers.variable) {
								current_entry.variable.index_after_skipping = partial_entry_count;
								cb_return_value = parser->handlers.variable(total_entry_count, &current_entry.variable, NULL, user_ctx);
								if (cb_return_value == READSTAT_HANDLER_ABORT) {
									error = READSTAT_ERROR_USER_ABORT;
									goto cleanup;
								}
							}
							if (cb_return_value == READSTAT_HANDLER_SKIP_VARIABLE) {
								current_entry.skip = 1;
							} else {
								partial_entry_count++;
							} 
							schema->entries = realloc(schema->entries, sizeof(readstat_schema_entry_t) * (schema->entry_count+1));
							memcpy(&schema->entries[schema->entry_count++], &current_entry, sizeof(readstat_schema_entry_t));
							total_entry_count++;
						}
						
#line 683 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 4:  {
						{
#line 101 "src/txt/readstat_stata_dictionary_read.rl"
							
							readstat_copy(schema->filename, sizeof(schema->filename), (char *)str_start, str_len);
						}
						
#line 694 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 5:  {
						{
#line 105 "src/txt/readstat_stata_dictionary_read.rl"
							
							readstat_copy(current_entry.variable.name, sizeof(current_entry.variable.name),
							(char *)str_start, str_len);
						}
						
#line 706 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 6:  {
						{
#line 110 "src/txt/readstat_stata_dictionary_read.rl"
							
							readstat_copy(current_entry.variable.label, sizeof(current_entry.variable.label),
							(char *)str_start, str_len);
						}
						
#line 718 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 7:  {
						{
#line 115 "src/txt/readstat_stata_dictionary_read.rl"
							str_start = p; }
						
#line 727 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 8:  {
						{
#line 115 "src/txt/readstat_stata_dictionary_read.rl"
							str_len = p - str_start; }
						
#line 736 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 9:  {
						{
#line 117 "src/txt/readstat_stata_dictionary_read.rl"
							str_start = p; }
						
#line 745 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 10:  {
						{
#line 117 "src/txt/readstat_stata_dictionary_read.rl"
							str_len = p - str_start; }
						
#line 754 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 11:  {
						{
#line 119 "src/txt/readstat_stata_dictionary_read.rl"
							str_start = p; }
						
#line 763 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 12:  {
						{
#line 119 "src/txt/readstat_stata_dictionary_read.rl"
							str_len = p - str_start; }
						
#line 772 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 13:  {
						{
#line 121 "src/txt/readstat_stata_dictionary_read.rl"
							line_no++; line_start = p; }
						
#line 781 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 14:  {
						{
#line 131 "src/txt/readstat_stata_dictionary_read.rl"
							schema->rows_per_observation = integer; }
						
#line 790 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 15:  {
						{
#line 133 "src/txt/readstat_stata_dictionary_read.rl"
							current_row = integer - 1; }
						
#line 799 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 16:  {
						{
#line 135 "src/txt/readstat_stata_dictionary_read.rl"
							current_col = integer - 1; }
						
#line 808 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 17:  {
						{
#line 137 "src/txt/readstat_stata_dictionary_read.rl"
							current_row++; }
						
#line 817 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 18:  {
						{
#line 137 "src/txt/readstat_stata_dictionary_read.rl"
							current_row += (integer - 1); }
						
#line 826 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 19:  {
						{
#line 141 "src/txt/readstat_stata_dictionary_read.rl"
							schema->cols_per_observation = integer; }
						
#line 835 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 20:  {
						{
#line 143 "src/txt/readstat_stata_dictionary_read.rl"
							schema->first_line = integer - 1; }
						
#line 844 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 21:  {
						{
#line 147 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.variable.type = READSTAT_TYPE_INT8; }
						
#line 853 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 22:  {
						{
#line 148 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.variable.type = READSTAT_TYPE_INT16; }
						
#line 862 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 23:  {
						{
#line 149 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.variable.type = READSTAT_TYPE_INT32; }
						
#line 871 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 24:  {
						{
#line 150 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.variable.type = READSTAT_TYPE_FLOAT; }
						
#line 880 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 25:  {
						{
#line 151 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.variable.type = READSTAT_TYPE_DOUBLE; }
						
#line 889 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 26:  {
						{
#line 152 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.variable.type = READSTAT_TYPE_STRING;
							current_entry.variable.storage_width = integer; }
						
#line 899 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 27:  {
						{
#line 159 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.len = integer; }
						
#line 908 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
					case 28:  {
						{
#line 160 "src/txt/readstat_stata_dictionary_read.rl"
							current_entry.decimal_separator = ','; }
						
#line 917 "src/txt/readstat_stata_dictionary_read.c"
						
						break; 
					}
				}
				_nacts -= 1;
				_acts += 1;
			}
			
		}
		
		if ( cs != 0 ) {
			p += 1;
			goto _resume;
		}
		_out: {}
	}
	
#line 174 "src/txt/readstat_stata_dictionary_read.rl"
	
	
	/* suppress warnings */
	(void)stata_dictionary_en_main;
	
	if (cs < 
#line 942 "src/txt/readstat_stata_dictionary_read.c"
	156
#line 179 "src/txt/readstat_stata_dictionary_read.rl"
	) {
		char error_buf[1024];
		if (p == pe) {
			snprintf(error_buf, sizeof(error_buf), "Error parsing .dct file (end-of-file unexpectedly reached)");
		} else {
			snprintf(error_buf, sizeof(error_buf), "Error parsing .dct file around line #%d, col #%ld (%c)",
			line_no + 1, (long)(p - line_start + 1), *p);
		}
		if (parser->handlers.error) {
			parser->handlers.error(error_buf, user_ctx);
		}
		error = READSTAT_ERROR_PARSE;
		goto cleanup;
	}
	
	cleanup:
	parser->io->close(parser->io->io_ctx);
	free(bytes);
	if (error != READSTAT_OK) {
		if (outError)
			*outError = error;
		readstat_schema_free(schema);
		schema = NULL;
	}
	
	return schema;
}
