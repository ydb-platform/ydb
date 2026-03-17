/* Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 * 	Redistributions of source code must retain the above
 * 	copyright notice, this list of conditions and the following
 * 	disclaimer.
 * 	Redistributions in binary form must reproduce the above
 * 	copyright notice, this list of conditions and the following
 * 	disclaimer in the documentation and/or other materials
 * 	provided with the distribution.
 * 	The name of Contributor may not be used to endorse or
 * 	promote products derived from this software without
 * 	specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 * CopyrightÂ© 2009, Liu Liu All rights reserved.
 *
 * OpenCV functions for MSER extraction
 *
 * 1. there are two different implementation of MSER, one for grey image, one for color image
 * 2. the grey image algorithm is taken from: Linear Time Maximally Stable Extremal Regions;
 *    the paper claims to be faster than union-find method;
 *    it actually get 1.5~2m/s on my centrino L7200 1.2GHz laptop.
 * 3. the color image algorithm is taken from: Maximally Stable Colour Regions for Recognition and Match;
 *    it should be much slower than grey image method ( 3~4 times );
 *    the chi_table.h file is taken directly from paper's source code which is distributed under GPL.
 * 4. though the name is *contours*, the result actually is a list of point set.
 */




// IMPORTANT: This is modified version of mser extraction from opencv 2.2 - used for OCR purposes (cause version from opencv 3 give much worse results)
// Somewhen we'll modify new version of MSER to give same results, but till then this version is needed.

#include "precomp.hpp"
#include <contrib/libs/opencv/include/opencv2/imgproc/types_c.h>
#include <contrib/libs/opencv/include/opencv2/imgproc/imgproc_c.h>

#define TABLE_SIZE 400
static double chitab3[]={0,  0.0150057,  0.0239478,  0.0315227,
                  0.0383427,  0.0446605,  0.0506115,  0.0562786,
                  0.0617174,  0.0669672,  0.0720573,  0.0770099,
                  0.081843,  0.0865705,  0.0912043,  0.0957541,
                  0.100228,  0.104633,  0.108976,  0.113261,
                  0.117493,  0.121676,  0.125814,  0.12991,
                  0.133967,  0.137987,  0.141974,  0.145929,
                  0.149853,  0.15375,  0.15762,  0.161466,
                  0.165287,  0.169087,  0.172866,  0.176625,
                  0.180365,  0.184088,  0.187794,  0.191483,
                  0.195158,  0.198819,  0.202466,  0.2061,
                  0.209722,  0.213332,  0.216932,  0.220521,
                  0.2241,  0.22767,  0.231231,  0.234783,
                  0.238328,  0.241865,  0.245395,  0.248918,
                  0.252435,  0.255947,  0.259452,  0.262952,
                  0.266448,  0.269939,  0.273425,  0.276908,
                  0.280386,  0.283862,  0.287334,  0.290803,
                  0.29427,  0.297734,  0.301197,  0.304657,
                  0.308115,  0.311573,  0.315028,  0.318483,
                  0.321937,  0.32539,  0.328843,  0.332296,
                  0.335749,  0.339201,  0.342654,  0.346108,
                  0.349562,  0.353017,  0.356473,  0.35993,
                  0.363389,  0.366849,  0.37031,  0.373774,
                  0.377239,  0.380706,  0.384176,  0.387648,
                  0.391123,  0.3946,  0.39808,  0.401563,
                  0.405049,  0.408539,  0.412032,  0.415528,
                  0.419028,  0.422531,  0.426039,  0.429551,
                  0.433066,  0.436586,  0.440111,  0.44364,
                  0.447173,  0.450712,  0.454255,  0.457803,
                  0.461356,  0.464915,  0.468479,  0.472049,
                  0.475624,  0.479205,  0.482792,  0.486384,
                  0.489983,  0.493588,  0.4972,  0.500818,
                  0.504442,  0.508073,  0.511711,  0.515356,
                  0.519008,  0.522667,  0.526334,  0.530008,
                  0.533689,  0.537378,  0.541075,  0.54478,
                  0.548492,  0.552213,  0.555942,  0.55968,
                  0.563425,  0.56718,  0.570943,  0.574715,
                  0.578497,  0.582287,  0.586086,  0.589895,
                  0.593713,  0.597541,  0.601379,  0.605227,
                  0.609084,  0.612952,  0.61683,  0.620718,
                  0.624617,  0.628526,  0.632447,  0.636378,
                  0.64032,  0.644274,  0.648239,  0.652215,
                  0.656203,  0.660203,  0.664215,  0.668238,
                  0.672274,  0.676323,  0.680384,  0.684457,
                  0.688543,  0.692643,  0.696755,  0.700881,
                  0.70502,  0.709172,  0.713339,  0.717519,
                  0.721714,  0.725922,  0.730145,  0.734383,
                  0.738636,  0.742903,  0.747185,  0.751483,
                  0.755796,  0.760125,  0.76447,  0.768831,
                  0.773208,  0.777601,  0.782011,  0.786438,
                  0.790882,  0.795343,  0.799821,  0.804318,
                  0.808831,  0.813363,  0.817913,  0.822482,
                  0.827069,  0.831676,  0.836301,  0.840946,
                  0.84561,  0.850295,  0.854999,  0.859724,
                  0.864469,  0.869235,  0.874022,  0.878831,
                  0.883661,  0.888513,  0.893387,  0.898284,
                  0.903204,  0.908146,  0.913112,  0.918101,
                  0.923114,  0.928152,  0.933214,  0.938301,
                  0.943413,  0.94855,  0.953713,  0.958903,
                  0.964119,  0.969361,  0.974631,  0.979929,
                  0.985254,  0.990608,  0.99599,  1.0014,
                  1.00684,  1.01231,  1.01781,  1.02335,
                  1.02891,  1.0345,  1.04013,  1.04579,
                  1.05148,  1.05721,  1.06296,  1.06876,
                  1.07459,  1.08045,  1.08635,  1.09228,
                  1.09826,  1.10427,  1.11032,  1.1164,
                  1.12253,  1.1287,  1.1349,  1.14115,
                  1.14744,  1.15377,  1.16015,  1.16656,
                  1.17303,  1.17954,  1.18609,  1.19269,
                  1.19934,  1.20603,  1.21278,  1.21958,
                  1.22642,  1.23332,  1.24027,  1.24727,
                  1.25433,  1.26144,  1.26861,  1.27584,
                  1.28312,  1.29047,  1.29787,  1.30534,
                  1.31287,  1.32046,  1.32812,  1.33585,
                  1.34364,  1.3515,  1.35943,  1.36744,
                  1.37551,  1.38367,  1.39189,  1.4002,
                  1.40859,  1.41705,  1.42561,  1.43424,
                  1.44296,  1.45177,  1.46068,  1.46967,
                  1.47876,  1.48795,  1.49723,  1.50662,
                  1.51611,  1.52571,  1.53541,  1.54523,
                  1.55517,  1.56522,  1.57539,  1.58568,
                  1.59611,  1.60666,  1.61735,  1.62817,
                  1.63914,  1.65025,  1.66152,  1.67293,
                  1.68451,  1.69625,  1.70815,  1.72023,
                  1.73249,  1.74494,  1.75757,  1.77041,
                  1.78344,  1.79669,  1.81016,  1.82385,
                  1.83777,  1.85194,  1.86635,  1.88103,
                  1.89598,  1.91121,  1.92674,  1.94257,
                  1.95871,  1.97519,  1.99201,  2.0092,
                  2.02676,  2.04471,  2.06309,  2.08189,
                  2.10115,  2.12089,  2.14114,  2.16192,
                  2.18326,  2.2052,  2.22777,  2.25101,
                  2.27496,  2.29966,  2.32518,  2.35156,
                  2.37886,  2.40717,  2.43655,  2.46709,
                  2.49889,  2.53206,  2.56673,  2.60305,
                  2.64117,  2.6813,  2.72367,  2.76854,
                  2.81623,  2.86714,  2.92173,  2.98059,
                  3.04446,  3.1143,  3.19135,  3.27731,
                  3.37455,  3.48653,  3.61862,  3.77982,
                  3.98692,  4.2776,  4.77167,  133.333 };
struct CvPoint16U
{
    CvPoint16U() = default;
    CvPoint16U( unsigned short int xVal, unsigned short int yVal)
        : x(xVal)
        , y(yVal)
    {
    }
    unsigned short int x;
    unsigned short int y;
};
const unsigned int NULL_IDX = 0xffffffff;
typedef struct CvLinkedPoint
{
    unsigned int nextIdx;
	CvPoint16U pt;
}
CvLinkedPoint;
// the history of region grown
typedef struct CvMSERGrowHistory
{
    unsigned int shortcutIdx;
    unsigned int childIdx;
	unsigned short int stable; // when it ever stabled before, record the size
	uchar val;
	unsigned short int size;
}
CvMSERGrowHistory;
typedef struct CvMSERConnectedComp
{
    unsigned int headIdx;
    unsigned int tailIdx;
    unsigned int historyIdx;
    unsigned long grey_level;
	int size;
	int dvar; // the derivative of last var
	float var; // the current variation (most time is the variation of one-step back)
}
CvMSERConnectedComp;

// Linear Time MSER claims by using bsf can get performance gain, here is the implementation
// however it seems that will not do any good in real world test
inline void _bitset(unsigned long * a, unsigned long b)
{
	*a |= 1<<b;
}
inline void _bitreset(unsigned long * a, unsigned long b)
{
	*a &= ~(1<<b);
}

cv::CvMSERParams::CvMSERParams(int delta, int min_area,
        int max_area, float max_variation,
        float min_diversity, int max_evolution,
        double area_threshold,
        double min_margin,
        int edge_blur_size)
    : delta(delta)
    , minArea(min_area)
    , maxArea(max_area)
    , maxVariation(max_variation)
    , minDiversity(min_diversity)
    , maxEvolution(max_evolution)
    , areaThreshold(area_threshold)
    , minMargin(min_margin)
    , edgeBlurSize(edge_blur_size)
{}

// clear the connected component in stack
CV_INLINE void
icvInitMSERComp( CvMSERConnectedComp* comp )
{
	comp->size = 0;
	comp->var = 0;
	comp->dvar = 1;
	comp->historyIdx = NULL_IDX;
}
// add history of size to a connected component
CV_INLINE void
icvMSERNewHistory( CvMSERConnectedComp* comp,
		   CvMSERGrowHistory* history,
           CvMSERGrowHistory* historyStart )
{
    unsigned int historyIdx = history - historyStart;
	history->childIdx = historyIdx;
	if ( NULL_IDX == comp->historyIdx )
	{
		history->shortcutIdx = historyIdx;
		history->stable = 0;
	} else {
	    CvMSERGrowHistory* tmp = historyStart + comp->historyIdx;
	    tmp->childIdx = historyIdx;
		history->shortcutIdx = tmp->shortcutIdx;
		history->stable = tmp->stable;
	}
	history->val = comp->grey_level;
	history->size = comp->size;
	comp->historyIdx = historyIdx;
}
// merging two connected component
CV_INLINE void
icvMSERMergeComp( CvMSERConnectedComp* comp1,
		  CvMSERConnectedComp* comp2,
		  CvMSERConnectedComp* comp,
		  CvMSERGrowHistory* history,
          CvLinkedPoint* ptsptrStart,
          CvMSERGrowHistory* historyStart
		  )
{
    unsigned int headIdx;
    unsigned int tailIdx;
	comp->grey_level = comp2->grey_level;
    unsigned int historyIdx = history - historyStart;
	history->childIdx = historyIdx;
	// select the winner by size
	if ( comp1->size >= comp2->size )
	{
            if ( NULL_IDX == comp1->historyIdx )
		{
			history->shortcutIdx = historyIdx;
			history->stable = 0;
		} else {
		    CvMSERGrowHistory* tmp = historyStart + comp1->historyIdx;
		    tmp->childIdx = historyIdx;
			history->shortcutIdx = tmp->shortcutIdx;
			history->stable = tmp->stable;
		}
        CvMSERGrowHistory* tmp = historyStart + comp2->historyIdx;
		if ( NULL_IDX != comp2->historyIdx && tmp->stable > history->stable )
			history->stable = tmp->stable;
		history->val = comp1->grey_level;
		history->size = comp1->size;
		// put comp1 to history
		comp->var = comp1->var;
		comp->dvar = comp1->dvar;
		if ( comp1->size > 0 && comp2->size > 0 )
		{
		    ptsptrStart[comp1->tailIdx].nextIdx = comp2->headIdx;
		}
		headIdx = ( comp1->size > 0 ) ? comp1->headIdx : comp2->headIdx;
		tailIdx = ( comp2->size > 0 ) ? comp2->tailIdx : comp1->tailIdx;
		// always made the newly added in the last of the pixel list (comp1 ... comp2)
	} else {
		if ( NULL_IDX == comp2->historyIdx )
		{
			history->shortcutIdx = historyIdx;
			history->stable = 0;
		} else {
            CvMSERGrowHistory* tmp = historyStart + comp2->historyIdx;
            tmp->childIdx = historyIdx;
			history->shortcutIdx = tmp->shortcutIdx;
			history->stable = tmp->stable;
		}
        CvMSERGrowHistory* tmp = historyStart + comp1->historyIdx;
		if ( NULL_IDX != comp1->historyIdx && tmp->stable > history->stable )
			history->stable = tmp->stable;
		history->val = comp2->grey_level;
		history->size = comp2->size;
		// put comp2 to history
		comp->var = comp2->var;
		comp->dvar = comp2->dvar;
		if ( comp1->size > 0 && comp2->size > 0 )
		{
		    ptsptrStart[comp2->tailIdx].nextIdx = comp1->headIdx;
		}
		headIdx = ( comp2->size > 0 ) ? comp2->headIdx : comp1->headIdx;
		tailIdx = ( comp1->size > 0 ) ? comp1->tailIdx : comp2->tailIdx;
		// always made the newly added in the last of the pixel list (comp2 ... comp1)
	}
	comp->headIdx = headIdx;
	comp->tailIdx = tailIdx;
	comp->historyIdx = historyIdx;
	comp->size = comp1->size + comp2->size;
}
CV_INLINE float
icvMSERVariationCalc( CvMSERConnectedComp* comp,
		      int delta,
	          CvMSERGrowHistory* historyStart )
{
	if ( NULL_IDX != comp->historyIdx )
	{
	    CvMSERGrowHistory* history = historyStart + comp->historyIdx;
	    int val = comp->grey_level;
		CvMSERGrowHistory* shortcut = historyStart + history->shortcutIdx;
		while ( shortcut - historyStart != shortcut->shortcutIdx && shortcut->val + delta > val )
			shortcut = historyStart + shortcut->shortcutIdx;
		CvMSERGrowHistory* child = historyStart + shortcut->childIdx;
		while ( child - historyStart != child->childIdx && child->val + delta <= val )
		{
			shortcut = child;
			child = historyStart + child->childIdx;
		}
		// get the position of history where the shortcut->val <= delta+val and shortcut->child->val >= delta+val
		history->shortcutIdx = shortcut - historyStart;
		return shortcut->size == 0 ? 0.0f : (float)(comp->size - shortcut->size) / (float)shortcut->size;
		// here is a small modification of MSER where cal ||R_{i}-R_{i-delta}||/||R_{i-delta}||
		// in standard MSER, cal ||R_{i+delta}-R_{i-delta}||/||R_{i}||
		// my calculation is simpler and much easier to implement
	}
	return 1.;
}
CV_INLINE bool
icvMSERStableCheckConst( CvMSERConnectedComp* comp,
                    const cv::CvMSERParams& params, float& var, int& dvar, bool modified_mser, CvMSERGrowHistory* historyStart )
{
    CvMSERGrowHistory* tmp = historyStart + comp->historyIdx;
    // tricky part: it actually check the stablity of one-step back
    if ( comp->historyIdx == NULL_IDX || tmp->size <= params.minArea || tmp->size >= params.maxArea )
            return 0;
    float div = (float)(tmp->size-tmp->stable)/(float)tmp->size;
    var = icvMSERVariationCalc( comp, params.delta, historyStart);
    dvar = ( comp->var < var || (unsigned long)(tmp->val + 1) < comp->grey_level );
    // Removed local maxim stability criteria (it seems so)
    int stable;
    if (modified_mser) {
        stable = ( comp->var < params.maxVariation && div > params.minDiversity );
    } else { // Old code
        stable = ( dvar && !comp->dvar && comp->var < params.maxVariation && div > params.minDiversity );
    }
    return stable != 0;
}
CV_INLINE bool
icvMSERStableCheck( CvMSERConnectedComp* comp,
		    const cv::CvMSERParams& params, bool modified_mser, CvMSERGrowHistory* historyStart )
{
    CvMSERGrowHistory* tmp = historyStart + comp->historyIdx;
    // tricky part: it actually check the stablity of one-step back
    if ( comp->historyIdx == NULL_IDX || tmp->size <= params.minArea || tmp->size >= params.maxArea ) {
            return 0;
    } else {
        float div = (float)(tmp->size-tmp->stable)/(float)tmp->size;
        float var = icvMSERVariationCalc( comp, params.delta, historyStart);
        int dvar = ( comp->var < var || (unsigned long)(tmp->val + 1) < comp->grey_level );
        // Removed local maxim stability criteria (it seems so)
        int stable;
        if (modified_mser) {
            stable = ( comp->var < params.maxVariation && div > params.minDiversity );
        } else { // Old code
            stable = ( dvar && !comp->dvar && comp->var < params.maxVariation && div > params.minDiversity );
        }
        // Removed local maxim stability criteria (it seems so)
        comp->var = var;
        comp->dvar = dvar;
        if ( stable )
            tmp->stable = tmp->size;
        return stable;
    }
}
CV_INLINE bool
icvMSERStableCheckCurrent( CvMSERConnectedComp* comp,
		    const cv::CvMSERParams& params )
{
	return comp->historyIdx == NULL_IDX && comp->size > params.minArea && comp->size < params.maxArea;
}
// add a pixel to the pixel list
CV_INLINE void
icvAccumulateMSERComp( CvMSERConnectedComp* comp,
		       CvLinkedPoint* point,
		       CvLinkedPoint* ptsptrStart)
{
    unsigned int pointIdx = point - ptsptrStart;
	if ( comp->size > 0 )
	{
	    ptsptrStart[comp->tailIdx].nextIdx = pointIdx;
		point->nextIdx = NULL_IDX;
	} else {
		point->nextIdx = NULL_IDX;
		comp->headIdx = pointIdx;
	}
	comp->tailIdx = pointIdx;
	comp->size++;
}
// convert the point set to CvSeq
CV_INLINE CvContour*
icvMSERToContour( CvMSERConnectedComp* comp,
                 CvMemStorage* storage,
                 const CvLinkedPoint* ptsptrStart, CvMSERGrowHistory* historyStart,
                 int size = -1 )
{
    size = (size < 0) ? historyStart[comp->historyIdx].size : size;
	CvSeq* _contour = cvCreateSeq( CV_SEQ_KIND_GENERIC|CV_32SC2, sizeof(CvContour), sizeof(CvPoint), storage );
	CvContour* contour = (CvContour*)_contour;
	cvSeqPushMulti( _contour, 0, size );
	const CvLinkedPoint* lpt = ptsptrStart + comp->headIdx;
	for ( int i = 0; i < size; i++ )
	{
		CvPoint* pt = CV_GET_SEQ_ELEM( CvPoint, _contour, i );
		pt->x = lpt->pt.x;
		pt->y = lpt->pt.y;
		lpt = ptsptrStart + lpt->nextIdx;
	}
	return contour;
}
// to preprocess src image to following format
// 32-bit image
// > 0 is available, < 0 is visited
// 17~19 bits is the direction
// 8~11 bits is the bucket it falls to (for BitScanForward)
// 0~8 bits is the color
static int*
icvPreprocessMSER_8UC1( CvMat* img,
			int*** heap_cur,
			CvMat* src,
			CvMat* mask )
{
	int srccpt = src->step-src->cols;
	int cpt_1 = img->cols-src->cols-1;
	int* imgptr = img->data.i;
	int* startptr;
	int level_size[256];
	for ( int i = 0; i < 256; i++ )
		level_size[i] = 0;
	for ( int i = 0; i < src->cols+2; i++ )
	{
		*imgptr = -1;
		imgptr++;
	}
	imgptr += cpt_1-1;
	uchar* srcptr = src->data.ptr;
	if ( mask )
	{
		startptr = 0;
		uchar* maskptr = mask->data.ptr;
		for ( int i = 0; i < src->rows; i++ )
		{
			*imgptr = -1;
			imgptr++;
			for ( int j = 0; j < src->cols; j++ )
			{
				if ( *maskptr )
				{
					if ( !startptr )
						startptr = imgptr;
					*srcptr = 0xff-*srcptr;
					level_size[*srcptr]++;
					*imgptr = ((*srcptr>>5)<<8)|(*srcptr);
				} else {
					*imgptr = -1;
				}
				imgptr++;
				srcptr++;
				maskptr++;
			}
			*imgptr = -1;
			imgptr += cpt_1;
			srcptr += srccpt;
			maskptr += srccpt;
		}
	} else {
		startptr = imgptr+img->cols+1;
		for ( int i = 0; i < src->rows; i++ )
		{
			*imgptr = -1;
			imgptr++;
			for ( int j = 0; j < src->cols; j++ )
			{
				*srcptr = 0xff-*srcptr;
				level_size[*srcptr]++;
				*imgptr = ((*srcptr>>5)<<8)|(*srcptr);
				imgptr++;
				srcptr++;
			}
			*imgptr = -1;
			imgptr += cpt_1;
			srcptr += srccpt;
		}
	}
	for ( int i = 0; i < src->cols+2; i++ )
	{
		*imgptr = -1;
		imgptr++;
	}
	heap_cur[0][0] = 0;
	for ( int i = 1; i < 256; i++ )
	{
		heap_cur[i] = heap_cur[i-1]+level_size[i-1]+1;
		heap_cur[i][0] = 0;
	}
	return startptr;
}
static void
icvExtractMSER_8UC1_Pass( int* ioptr,
			  int* imgptr,
			  int*** heap_cur,
			  CvLinkedPoint* ptsptr,
			  CvMSERGrowHistory* histptr,
			  CvMSERConnectedComp* comptr,
			  int step,
			  int stepmask,
			  int stepgap,
			  const cv::CvMSERParams& params,
			  int color,
			  CvSeq* contours,
			  CvMemStorage* storage,
              bool modified_mser )
{
    CvLinkedPoint* ptsptrStart = ptsptr;
    CvMSERGrowHistory* historyStart = histptr;
    bool startGroup = modified_mser;
    icvInitMSERComp(comptr);
    comptr->grey_level = 256;
    comptr++;
    comptr->grey_level = (*imgptr)&0xff;
    icvInitMSERComp( comptr );
    *imgptr |= 0x80000000;
    heap_cur += (*imgptr)&0xff;
    int dir[] = { 1, step, -1, -step };
    //int dir[] = { 1, step + 1, step, step - 1, -1, -step - 1, -step, -step + 1 };
#ifdef __INTRIN_ENABLED__
    unsigned long heapbit[] = { 0, 0, 0, 0, 0, 0, 0, 0 };
    unsigned long* bit_cur = heapbit+(((*imgptr)&0x700)>>8);
#endif
    for ( ; ; )
    {
        // take tour of all the 4 directions
        while ( ((*imgptr)&0x70000) < 0x40000 )
            //while ( ((*imgptr)&0xf0000) < 0x80000 )
        {
            // get the neighbor
            int* imgptr_nbr = imgptr+dir[((*imgptr)&0x70000)>>16];
            //int* imgptr_nbr = imgptr+dir[((*imgptr)&0xf0000)>>16];
            if ( *imgptr_nbr >= 0 ) // if the neighbor is not visited yet
            {
                *imgptr_nbr |= 0x80000000; // mark it as visited
                if ( ((*imgptr_nbr)&0xff) < ((*imgptr)&0xff) )
                {
                    // when the value of neighbor smaller than current
                    // push current to boundary heap and make the neighbor to be the current one
                    // create an empty comp
                    (*heap_cur)++;
                    **heap_cur = imgptr;
                    *imgptr += 0x10000;
                    heap_cur += ((*imgptr_nbr)&0xff)-((*imgptr)&0xff);
#ifdef __INTRIN_ENABLED__
                    _bitset( bit_cur, (*imgptr)&0x1f );
                    bit_cur += (((*imgptr_nbr)&0x700)-((*imgptr)&0x700))>>8;
#endif
                    imgptr = imgptr_nbr;
                    comptr++;
                    icvInitMSERComp( comptr );
                    comptr->grey_level = (*imgptr)&0xff;
                    continue;
                } else {
                    // otherwise, push the neighbor to boundary heap
                    heap_cur[((*imgptr_nbr)&0xff)-((*imgptr)&0xff)]++;
                    *heap_cur[((*imgptr_nbr)&0xff)-((*imgptr)&0xff)] = imgptr_nbr;
#ifdef __INTRIN_ENABLED__
                    _bitset( bit_cur+((((*imgptr_nbr)&0x700)-((*imgptr)&0x700))>>8), (*imgptr_nbr)&0x1f );
#endif
                }
            }
            *imgptr += 0x10000;
        }
        int i = (int)(imgptr-ioptr);
        ptsptr->pt = CvPoint16U( i&stepmask, i>>stepgap );
        // get the current location
        icvAccumulateMSERComp( comptr, ptsptr, ptsptrStart);
        ptsptr++;
        // get the next pixel from boundary heap
        if ( **heap_cur )
        {
            imgptr = **heap_cur;
            (*heap_cur)--;
#ifdef __INTRIN_ENABLED__
            if ( !**heap_cur )
                _bitreset( bit_cur, (*imgptr)&0x1f );
#endif
        } else {
#ifdef __INTRIN_ENABLED__
            bool found_pixel = 0;
            unsigned long pixel_val;
            for ( int i = ((*imgptr)&0x700)>>8; i < 8; i++ )
            {
                if ( _BitScanForward( &pixel_val, *bit_cur ) )
                {
                    found_pixel = 1;
                    pixel_val += i<<5;
                    heap_cur += pixel_val-((*imgptr)&0xff);
                    break;
                }
                bit_cur++;
            }
            if ( found_pixel )
#else
            heap_cur++;
            unsigned long pixel_val = 0;
            for ( unsigned long i = ((*imgptr)&0xff)+1; i < 256; i++ )
            {
                if ( **heap_cur )
                {
                    pixel_val = i;
                    break;
                }
                heap_cur++;
            }
            if ( pixel_val )
#endif
            {
                imgptr = **heap_cur;
                (*heap_cur)--;
#ifdef __INTRIN_ENABLED__
                if ( !**heap_cur )
                    _bitreset( bit_cur, pixel_val&0x1f );
#endif
                if ( pixel_val < comptr[-1].grey_level )
                {
                    // check the stablity and push a new history, increase the grey level
                    if ( icvMSERStableCheck( comptr, params, modified_mser, historyStart ) )
                    {
                        CvContour* contour = icvMSERToContour( comptr, storage, ptsptrStart, historyStart);
                        contour->color = color;
                        cvSeqPush( contours, &contour );
                    }
                    icvMSERNewHistory( comptr, histptr, historyStart );
                    comptr[0].grey_level = pixel_val;
                    histptr++;
                } else {
                    startGroup = false;
                    float var;
                    int dvar;
                    // keep merging top two comp in stack until the grey level >= pixel_val
                    for ( ; ; )
                    {
                        CvMSERConnectedComp tmp;
                        icvInitMSERComp( &tmp );
                        comptr--;
                        icvMSERMergeComp( comptr + 1, comptr, &tmp, histptr, ptsptrStart, historyStart);
                        histptr++;
                        if (modified_mser && icvMSERStableCheckConst( &tmp, params, var, dvar, modified_mser, historyStart ))
                        {
                            CvContour* contour = NULL;
                            if(comptr[0].grey_level > comptr[1].grey_level) {
                                if (icvMSERStableCheckCurrent(comptr + 1, params))
                                    contour = icvMSERToContour( comptr + 1, storage, ptsptrStart, historyStart, comptr[1].size );
                            }
                            if (contour != NULL) {
                                contour->color = color;
                                cvSeqPush( contours, &contour);
                            }
                        }
                        *comptr = tmp;
                        if ( pixel_val <= comptr[0].grey_level )
                            break;
                        if ( pixel_val < comptr[-1].grey_level )
                        {
                            // check the stablity here otherwise it wouldn't be an ER
                            if ( icvMSERStableCheck( comptr, params, modified_mser, historyStart ) )
                            {
                                CvContour* contour = icvMSERToContour( comptr, storage, ptsptrStart, historyStart );
                                contour->color = color;
                                cvSeqPush( contours, &contour );
                            }
                            icvMSERNewHistory( comptr, histptr, historyStart );
                            comptr[0].grey_level = pixel_val;
                            histptr++;
                            break;
                        }
                    }
                }
            } else {
                if ( startGroup && icvMSERStableCheck( comptr, params, modified_mser, historyStart))
                {
                    CvContour* contour = icvMSERToContour( comptr, storage, ptsptrStart, historyStart );
                    contour->color = color;
                    cvSeqPush( contours, &contour );
                }
                break;
            }
        }
    }
}
static void
icvExtractMSER_8UC1( CvMat* src,
		     CvMat* mask,
		     CvSeq* contours,
		     CvMemStorage* storage,
		     const cv::CvMSERParams& params,
             bool modified_mser )
{
	int step = 8;
	int stepgap = 3;
	while ( step < src->step+2 )
	{
		step <<= 1;
		stepgap++;
	}
	int stepmask = step-1;
	// to speedup the process, make the width to be 2^N
	CvMat* img = cvCreateMat( src->rows+2, step, CV_32SC1 );
	int* ioptr = img->data.i+step+1;
	int* imgptr;
	// pre-allocate boundary heap
	int** heap = (int**)cvAlloc( (src->rows*src->cols+256)*sizeof(heap[0]) );
	int** heap_start[256];
	heap_start[0] = heap;
    CV_Assert(uint(src->height * src->width) < NULL_IDX && src->height <= 0xffff && src->width <= 0xffff);
    CV_Assert(params.maxArea < 0xffff);
	// pre-allocate linked point and grow history
	CvLinkedPoint* pts = (CvLinkedPoint*)cvAlloc( src->rows*src->cols*sizeof(pts[0]) );
	CvMSERGrowHistory* history = (CvMSERGrowHistory*)cvAlloc( src->rows*src->cols*sizeof(history[0]) );
	CvMSERConnectedComp comp[257];
	// darker to brighter (MSER-)
	imgptr = icvPreprocessMSER_8UC1( img, heap_start, src, mask );
	icvExtractMSER_8UC1_Pass( ioptr, imgptr, heap_start, pts, history, comp, step, stepmask, stepgap, params, -1, contours, storage, modified_mser );
        // brighter to darker (MSER+)
	imgptr = icvPreprocessMSER_8UC1( img, heap_start, src, mask );
	icvExtractMSER_8UC1_Pass( ioptr, imgptr, heap_start, pts, history, comp, step, stepmask, stepgap, params, 1, contours, storage , modified_mser);
	// clean up
	cvFree( &history );
	cvFree( &heap );
	cvFree( &pts );
	cvReleaseMat( &img );
}
struct CvMSCRNode;
typedef struct CvTempMSCR
{
	CvMSCRNode* head;
	CvMSCRNode* tail;
	double m; // the margin used to prune area later
	int size;
} CvTempMSCR;
typedef struct CvMSCRNode
{
	CvMSCRNode* shortcut;
	// to make the finding of root less painful
	CvMSCRNode* prev;
	CvMSCRNode* next;
	// a point double-linked list
	CvTempMSCR* tmsr;
	// the temporary msr (set to NULL at every re-initialise)
	CvTempMSCR* gmsr;
	// the global msr (once set, never to NULL)
	int index;
	// the index of the node, at this point, it should be x at the first 16-bits, and y at the last 16-bits.
	int rank;
	int reinit;
	int size, sizei;
	double dt, di;
	double s;
} CvMSCRNode;
typedef struct CvMSCREdge
{
	double chi;
	CvMSCRNode* left;
	CvMSCRNode* right;
} CvMSCREdge;
CV_INLINE double
icvChisquaredDistance( uchar* x, uchar* y )
{
	return (double)((x[0]-y[0])*(x[0]-y[0]))/(double)(x[0]+y[0]+1e-10)+
	       (double)((x[1]-y[1])*(x[1]-y[1]))/(double)(x[1]+y[1]+1e-10)+
	       (double)((x[2]-y[2])*(x[2]-y[2]))/(double)(x[2]+y[2]+1e-10);
}
CV_INLINE void
icvInitMSCRNode( CvMSCRNode* node )
{
	node->gmsr = node->tmsr = NULL;
	node->reinit = 0xffff;
	node->rank = 0;
	node->sizei = node->size = 1;
	node->prev = node->next = node->shortcut = node;
}
// the preprocess to get the edge list with proper gaussian blur
static int
icvPreprocessMSER_8UC3( CvMSCRNode* node,
			CvMSCREdge* edge,
			double* total,
			CvMat* src,
			CvMat* mask,
			CvMat* dx,
			CvMat* dy,
			int Ne,
			int edgeBlurSize )
{
	int srccpt = src->step-src->cols*3;
	uchar* srcptr = src->data.ptr;
	uchar* lastptr = src->data.ptr+3;
	double* dxptr = dx->data.db;
	for ( int i = 0; i < src->rows; i++ )
	{
		for ( int j = 0; j < src->cols-1; j++ )
		{
			*dxptr = icvChisquaredDistance( srcptr, lastptr );
			dxptr++;
			srcptr += 3;
			lastptr += 3;
		}
		srcptr += srccpt+3;
		lastptr += srccpt+3;
	}
	srcptr = src->data.ptr;
	lastptr = src->data.ptr+src->step;
	double* dyptr = dy->data.db;
	for ( int i = 0; i < src->rows-1; i++ )
	{
		for ( int j = 0; j < src->cols; j++ )
		{
			*dyptr = icvChisquaredDistance( srcptr, lastptr );
			dyptr++;
			srcptr += 3;
			lastptr += 3;
		}
		srcptr += srccpt;
		lastptr += srccpt;
	}
	// get dx and dy and blur it
	if ( edgeBlurSize >= 1 )
	{
		cvSmooth( dx, dx, CV_GAUSSIAN, edgeBlurSize, edgeBlurSize );
		cvSmooth( dy, dy, CV_GAUSSIAN, edgeBlurSize, edgeBlurSize );
	}
	dxptr = dx->data.db;
	dyptr = dy->data.db;
	// assian dx, dy to proper edge list and initialize mscr node
	// the nasty code here intended to avoid extra loops
	if ( mask )
	{
		Ne = 0;
		int maskcpt = mask->step-mask->cols+1;
		uchar* maskptr = mask->data.ptr;
		CvMSCRNode* nodeptr = node;
		icvInitMSCRNode( nodeptr );
		nodeptr->index = 0;
		*total += edge->chi = *dxptr;
		if ( maskptr[0] && maskptr[1] )
		{
			edge->left = nodeptr;
			edge->right = nodeptr+1;
			edge++;
			Ne++;
		}
		dxptr++;
		nodeptr++;
		maskptr++;
		for ( int i = 1; i < src->cols-1; i++ )
		{
			icvInitMSCRNode( nodeptr );
			nodeptr->index = i;
			if ( maskptr[0] && maskptr[1] )
			{
				*total += edge->chi = *dxptr;
				edge->left = nodeptr;
				edge->right = nodeptr+1;
				edge++;
				Ne++;
			}
			dxptr++;
			nodeptr++;
			maskptr++;
		}
		icvInitMSCRNode( nodeptr );
		nodeptr->index = src->cols-1;
		nodeptr++;
		maskptr += maskcpt;
		for ( int i = 1; i < src->rows-1; i++ )
		{
			icvInitMSCRNode( nodeptr );
			nodeptr->index = i<<16;
			if ( maskptr[0] )
			{
				if ( maskptr[-mask->step] )
				{
					*total += edge->chi = *dyptr;
					edge->left = nodeptr-src->cols;
					edge->right = nodeptr;
					edge++;
					Ne++;
				}
				if ( maskptr[1] )
				{
					*total += edge->chi = *dxptr;
					edge->left = nodeptr;
					edge->right = nodeptr+1;
					edge++;
					Ne++;
				}
			}
			dyptr++;
			dxptr++;
			nodeptr++;
			maskptr++;
			for ( int j = 1; j < src->cols-1; j++ )
			{
				icvInitMSCRNode( nodeptr );
				nodeptr->index = (i<<16)|j;
				if ( maskptr[0] )
				{
					if ( maskptr[-mask->step] )
					{
						*total += edge->chi = *dyptr;
						edge->left = nodeptr-src->cols;
						edge->right = nodeptr;
						edge++;
						Ne++;
					}
					if ( maskptr[1] )
					{
						*total += edge->chi = *dxptr;
						edge->left = nodeptr;
						edge->right = nodeptr+1;
						edge++;
						Ne++;
					}
				}
				dyptr++;
				dxptr++;
				nodeptr++;
				maskptr++;
			}
			icvInitMSCRNode( nodeptr );
			nodeptr->index = (i<<16)|(src->cols-1);
			if ( maskptr[0] && maskptr[-mask->step] )
			{
				*total += edge->chi = *dyptr;
				edge->left = nodeptr-src->cols;
				edge->right = nodeptr;
				edge++;
				Ne++;
			}
			dyptr++;
			nodeptr++;
			maskptr += maskcpt;
		}
		icvInitMSCRNode( nodeptr );
		nodeptr->index = (src->rows-1)<<16;
		if ( maskptr[0] )
		{
			if ( maskptr[1] )
			{
				*total += edge->chi = *dxptr;
				edge->left = nodeptr;
				edge->right = nodeptr+1;
				edge++;
				Ne++;
			}
			if ( maskptr[-mask->step] )
			{
				*total += edge->chi = *dyptr;
				edge->left = nodeptr-src->cols;
				edge->right = nodeptr;
				edge++;
				Ne++;
			}
		}
		dxptr++;
		dyptr++;
		nodeptr++;
		maskptr++;
		for ( int i = 1; i < src->cols-1; i++ )
		{
			icvInitMSCRNode( nodeptr );
			nodeptr->index = ((src->rows-1)<<16)|i;
			if ( maskptr[0] )
			{
				if ( maskptr[1] )
				{
					*total += edge->chi = *dxptr;
					edge->left = nodeptr;
					edge->right = nodeptr+1;
					edge++;
					Ne++;
				}
				if ( maskptr[-mask->step] )
				{
					*total += edge->chi = *dyptr;
					edge->left = nodeptr-src->cols;
					edge->right = nodeptr;
					edge++;
					Ne++;
				}
			}
			dxptr++;
			dyptr++;
			nodeptr++;
			maskptr++;
		}
		icvInitMSCRNode( nodeptr );
		nodeptr->index = ((src->rows-1)<<16)|(src->cols-1);
		if ( maskptr[0] && maskptr[-mask->step] )
		{
			*total += edge->chi = *dyptr;
			edge->left = nodeptr-src->cols;
			edge->right = nodeptr;
			Ne++;
		}
	} else {
		CvMSCRNode* nodeptr = node;
		icvInitMSCRNode( nodeptr );
		nodeptr->index = 0;
		*total += edge->chi = *dxptr;
		dxptr++;
		edge->left = nodeptr;
		edge->right = nodeptr+1;
		edge++;
		nodeptr++;
		for ( int i = 1; i < src->cols-1; i++ )
		{
			icvInitMSCRNode( nodeptr );
			nodeptr->index = i;
			*total += edge->chi = *dxptr;
			dxptr++;
			edge->left = nodeptr;
			edge->right = nodeptr+1;
			edge++;
			nodeptr++;
		}
		icvInitMSCRNode( nodeptr );
		nodeptr->index = src->cols-1;
		nodeptr++;
		for ( int i = 1; i < src->rows-1; i++ )
		{
			icvInitMSCRNode( nodeptr );
			nodeptr->index = i<<16;
			*total += edge->chi = *dyptr;
			dyptr++;
			edge->left = nodeptr-src->cols;
			edge->right = nodeptr;
			edge++;
			*total += edge->chi = *dxptr;
			dxptr++;
			edge->left = nodeptr;
			edge->right = nodeptr+1;
			edge++;
			nodeptr++;
			for ( int j = 1; j < src->cols-1; j++ )
			{
				icvInitMSCRNode( nodeptr );
				nodeptr->index = (i<<16)|j;
				*total += edge->chi = *dyptr;
				dyptr++;
				edge->left = nodeptr-src->cols;
				edge->right = nodeptr;
				edge++;
				*total += edge->chi = *dxptr;
				dxptr++;
				edge->left = nodeptr;
				edge->right = nodeptr+1;
				edge++;
				nodeptr++;
			}
			icvInitMSCRNode( nodeptr );
			nodeptr->index = (i<<16)|(src->cols-1);
			*total += edge->chi = *dyptr;
			dyptr++;
			edge->left = nodeptr-src->cols;
			edge->right = nodeptr;
			edge++;
			nodeptr++;
		}
		icvInitMSCRNode( nodeptr );
		nodeptr->index = (src->rows-1)<<16;
		*total += edge->chi = *dxptr;
		dxptr++;
		edge->left = nodeptr;
		edge->right = nodeptr+1;
		edge++;
		*total += edge->chi = *dyptr;
		dyptr++;
		edge->left = nodeptr-src->cols;
		edge->right = nodeptr;
		edge++;
		nodeptr++;
		for ( int i = 1; i < src->cols-1; i++ )
		{
			icvInitMSCRNode( nodeptr );
			nodeptr->index = ((src->rows-1)<<16)|i;
			*total += edge->chi = *dxptr;
			dxptr++;
			edge->left = nodeptr;
			edge->right = nodeptr+1;
			edge++;
			*total += edge->chi = *dyptr;
			dyptr++;
			edge->left = nodeptr-src->cols;
			edge->right = nodeptr;
			edge++;
			nodeptr++;
		}
		icvInitMSCRNode( nodeptr );
		nodeptr->index = ((src->rows-1)<<16)|(src->cols-1);
		*total += edge->chi = *dyptr;
		edge->left = nodeptr-src->cols;
		edge->right = nodeptr;
	}
	return Ne;
}

#define CV_IMPLEMENT_QSORT_EX( func_name, T, LT, user_data_type )                   \
void func_name( T *array, size_t total, user_data_type aux )                        \
{                                                                                   \
    int isort_thresh = 7;                                                           \
    T t;                                                                            \
    int sp = 0;                                                                     \
                                                                                    \
    struct                                                                          \
    {                                                                               \
        T *lb;                                                                      \
        T *ub;                                                                      \
    }                                                                               \
    stack[48];                                                                      \
                                                                                    \
    aux = aux;                                                                      \
                                                                                    \
    if( total <= 1 )                                                                \
        return;                                                                     \
                                                                                    \
    stack[0].lb = array;                                                            \
    stack[0].ub = array + (total - 1);                                              \
                                                                                    \
    while( sp >= 0 )                                                                \
    {                                                                               \
        T* left = stack[sp].lb;                                                     \
        T* right = stack[sp--].ub;                                                  \
                                                                                    \
        for(;;)                                                                     \
        {                                                                           \
            int i, n = (int)(right - left) + 1, m;                                  \
            T* ptr;                                                                 \
            T* ptr2;                                                                \
                                                                                    \
            if( n <= isort_thresh )                                                 \
            {                                                                       \
            insert_sort:                                                            \
                for( ptr = left + 1; ptr <= right; ptr++ )                          \
                {                                                                   \
                    for( ptr2 = ptr; ptr2 > left && LT(ptr2[0],ptr2[-1]); ptr2--)   \
                        CV_SWAP( ptr2[0], ptr2[-1], t );                            \
                }                                                                   \
                break;                                                              \
            }                                                                       \
            else                                                                    \
            {                                                                       \
                T* left0;                                                           \
                T* left1;                                                           \
                T* right0;                                                          \
                T* right1;                                                          \
                T* pivot;                                                           \
                T* a;                                                               \
                T* b;                                                               \
                T* c;                                                               \
                int swap_cnt = 0;                                                   \
                                                                                    \
                left0 = left;                                                       \
                right0 = right;                                                     \
                pivot = left + (n/2);                                               \
                                                                                    \
                if( n > 40 )                                                        \
                {                                                                   \
                    int d = n / 8;                                                  \
                    a = left, b = left + d, c = left + 2*d;                         \
                    left = LT(*a, *b) ? (LT(*b, *c) ? b : (LT(*a, *c) ? c : a))     \
                                      : (LT(*c, *b) ? b : (LT(*a, *c) ? a : c));    \
                                                                                    \
                    a = pivot - d, b = pivot, c = pivot + d;                        \
                    pivot = LT(*a, *b) ? (LT(*b, *c) ? b : (LT(*a, *c) ? c : a))    \
                                      : (LT(*c, *b) ? b : (LT(*a, *c) ? a : c));    \
                                                                                    \
                    a = right - 2*d, b = right - d, c = right;                      \
                    right = LT(*a, *b) ? (LT(*b, *c) ? b : (LT(*a, *c) ? c : a))    \
                                      : (LT(*c, *b) ? b : (LT(*a, *c) ? a : c));    \
                }                                                                   \
                                                                                    \
                a = left, b = pivot, c = right;                                     \
                pivot = LT(*a, *b) ? (LT(*b, *c) ? b : (LT(*a, *c) ? c : a))        \
                                   : (LT(*c, *b) ? b : (LT(*a, *c) ? a : c));       \
                if( pivot != left0 )                                                \
                {                                                                   \
                    CV_SWAP( *pivot, *left0, t );                                   \
                    pivot = left0;                                                  \
                }                                                                   \
                left = left1 = left0 + 1;                                           \
                right = right1 = right0;                                            \
                                                                                    \
                for(;;)                                                             \
                {                                                                   \
                    while( left <= right && !LT(*pivot, *left) )                    \
                    {                                                               \
                        if( !LT(*left, *pivot) )                                    \
                        {                                                           \
                            if( left > left1 )                                      \
                                CV_SWAP( *left1, *left, t );                        \
                            swap_cnt = 1;                                           \
                            left1++;                                                \
                        }                                                           \
                        left++;                                                     \
                    }                                                               \
                                                                                    \
                    while( left <= right && !LT(*right, *pivot) )                   \
                    {                                                               \
                        if( !LT(*pivot, *right) )                                   \
                        {                                                           \
                            if( right < right1 )                                    \
                                CV_SWAP( *right1, *right, t );                      \
                            swap_cnt = 1;                                           \
                            right1--;                                               \
                        }                                                           \
                        right--;                                                    \
                    }                                                               \
                                                                                    \
                    if( left > right )                                              \
                        break;                                                      \
                    CV_SWAP( *left, *right, t );                                    \
                    swap_cnt = 1;                                                   \
                    left++;                                                         \
                    right--;                                                        \
                }                                                                   \
                                                                                    \
                if( swap_cnt == 0 )                                                 \
                {                                                                   \
                    left = left0, right = right0;                                   \
                    goto insert_sort;                                               \
                }                                                                   \
                                                                                    \
                n = MIN( (int)(left1 - left0), (int)(left - left1) );               \
                for( i = 0; i < n; i++ )                                            \
                    CV_SWAP( left0[i], left[i-n], t );                              \
                                                                                    \
                n = MIN( (int)(right0 - right1), (int)(right1 - right) );           \
                for( i = 0; i < n; i++ )                                            \
                    CV_SWAP( left[i], right0[i-n+1], t );                           \
                n = (int)(left - left1);                                            \
                m = (int)(right1 - right);                                          \
                if( n > 1 )                                                         \
                {                                                                   \
                    if( m > 1 )                                                     \
                    {                                                               \
                        if( n > m )                                                 \
                        {                                                           \
                            stack[++sp].lb = left0;                                 \
                            stack[sp].ub = left0 + n - 1;                           \
                            left = right0 - m + 1, right = right0;                  \
                        }                                                           \
                        else                                                        \
                        {                                                           \
                            stack[++sp].lb = right0 - m + 1;                        \
                            stack[sp].ub = right0;                                  \
                            left = left0, right = left0 + n - 1;                    \
                        }                                                           \
                    }                                                               \
                    else                                                            \
                        left = left0, right = left0 + n - 1;                        \
                }                                                                   \
                else if( m > 1 )                                                    \
                    left = right0 - m + 1, right = right0;                          \
                else                                                                \
                    break;                                                          \
            }                                                                       \
        }                                                                           \
    }                                                                               \
}

#define CV_IMPLEMENT_QSORT( func_name, T, cmp )  \
    CV_IMPLEMENT_QSORT_EX( func_name, T, cmp, int )

#define cmp_mscr_edge(edge1, edge2) \
    ((edge1).chi < (edge2).chi)

static CV_IMPLEMENT_QSORT( icvQuickSortMSCREdge, CvMSCREdge, cmp_mscr_edge )

// to find the root of one region
CV_INLINE CvMSCRNode*
icvFindMSCR( CvMSCRNode* x )
{
	CvMSCRNode* prev = x;
	CvMSCRNode* next;
	for ( ; ; )
	{
		next = x->shortcut;
		x->shortcut = prev;
		if ( next == x ) break;
		prev= x;
		x = next;
	}
	CvMSCRNode* root = x;
	for ( ; ; )
	{
		prev = x->shortcut;
		x->shortcut = root;
		if ( prev == x ) break;
		x = prev;
	}
	return root;
}
// the stable mscr should be:
// bigger than minArea and smaller than maxArea
// differ from its ancestor more than minDiversity
CV_INLINE bool
icvMSCRStableCheck( CvMSCRNode* x,
		    const cv::CvMSERParams& params )
{
	if ( x->size <= params.minArea || x->size >= params.maxArea )
		return 0;
	if ( x->gmsr == NULL )
		return 1;
	double div = (double)(x->size-x->gmsr->size)/(double)x->size;
	return div > params.minDiversity;
}
static void
icvExtractMSER_8UC3( CvMat* src,
		     CvMat* mask,
		     CvSeq* contours,
		     CvMemStorage* storage,
		     const cv::CvMSERParams& params )
{
	CvMSCRNode* map = (CvMSCRNode*)cvAlloc( src->cols*src->rows*sizeof(map[0]) );
	int Ne = src->cols*src->rows*2-src->cols-src->rows;
	CvMSCREdge* edge = (CvMSCREdge*)cvAlloc( Ne*sizeof(edge[0]) );
	CvTempMSCR* mscr = (CvTempMSCR*)cvAlloc( src->cols*src->rows*sizeof(mscr[0]) );
	double emean = 0;
	CvMat* dx = cvCreateMat( src->rows, src->cols-1, CV_64FC1 );
	CvMat* dy = cvCreateMat( src->rows-1, src->cols, CV_64FC1 );
	Ne = icvPreprocessMSER_8UC3( map, edge, &emean, src, mask, dx, dy, Ne, params.edgeBlurSize );
	emean = emean / (double)Ne;
	icvQuickSortMSCREdge( edge, Ne, 0 );
	CvMSCREdge* edge_ub = edge+Ne;
	CvMSCREdge* edgeptr = edge;
	CvTempMSCR* mscrptr = mscr;
	// the evolution process
	for ( int i = 0; i < params.maxEvolution; i++ )
	{
		double k = (double)i/(double)params.maxEvolution*(TABLE_SIZE-1);
		int ti = cvFloor(k);
		double reminder = k-ti;
		double thres = emean*(chitab3[ti]*(1-reminder)+chitab3[ti+1]*reminder);
		// to process all the edges in the list that chi < thres
		while ( edgeptr < edge_ub && edgeptr->chi < thres )
		{
			CvMSCRNode* lr = icvFindMSCR( edgeptr->left );
			CvMSCRNode* rr = icvFindMSCR( edgeptr->right );
			// get the region root (who is responsible)
			if ( lr != rr )
			{
				// rank idea take from: N-tree Disjoint-Set Forests for Maximally Stable Extremal Regions
				if ( rr->rank > lr->rank )
				{
					CvMSCRNode* tmp;
					CV_SWAP( lr, rr, tmp );
				} else if ( lr->rank == rr->rank ) {
					// at the same rank, we will compare the size
					if ( lr->size > rr->size )
					{
						CvMSCRNode* tmp;
						CV_SWAP( lr, rr, tmp );
					}
					lr->rank++;
				}
				rr->shortcut = lr;
				lr->size += rr->size;
				// join rr to the end of list lr (lr is a endless double-linked list)
				lr->prev->next = rr;
				lr->prev = rr->prev;
				rr->prev->next = lr;
				rr->prev = lr;
				// area threshold force to reinitialize
				if ( lr->size > (lr->size-rr->size)*params.areaThreshold )
				{
					lr->sizei = lr->size;
					lr->reinit = i;
					if ( lr->tmsr != NULL )
					{
						lr->tmsr->m = lr->dt-lr->di;
						lr->tmsr = NULL;
					}
					lr->di = edgeptr->chi;
					lr->s = 1e10;
				}
				lr->dt = edgeptr->chi;
				if ( i > lr->reinit )
				{
					double s = (double)(lr->size-lr->sizei)/(lr->dt-lr->di);
					if ( s < lr->s )
					{
						// skip the first one and check stablity
						if ( i > lr->reinit+1 && icvMSCRStableCheck( lr, params ) )
						{
							if ( lr->tmsr == NULL )
							{
								lr->gmsr = lr->tmsr = mscrptr;
								mscrptr++;
							}
							lr->tmsr->size = lr->size;
							lr->tmsr->head = lr;
							lr->tmsr->tail = lr->prev;
							lr->tmsr->m = 0;
						}
						lr->s = s;
					}
				}
			}
			edgeptr++;
		}
		if ( edgeptr >= edge_ub )
			break;
	}
	for ( CvTempMSCR* ptr = mscr; ptr < mscrptr; ptr++ )
		// to prune area with margin less than minMargin
		if ( ptr->m > params.minMargin )
		{
			CvSeq* _contour = cvCreateSeq( CV_SEQ_KIND_GENERIC|CV_32SC2, sizeof(CvContour), sizeof(CvPoint), storage );
			cvSeqPushMulti( _contour, 0, ptr->size );
			CvMSCRNode* lpt = ptr->head;
			for ( int i = 0; i < ptr->size; i++ )
			{
				CvPoint* pt = CV_GET_SEQ_ELEM( CvPoint, _contour, i );
				pt->x = (lpt->index)&0xffff;
				pt->y = (lpt->index)>>16;
				lpt = lpt->next;
			}
			CvContour* contour = (CvContour*)_contour;
			cvBoundingRect( contour );
			contour->color = 0;
			cvSeqPush( contours, &contour );
		}
	cvReleaseMat( &dx );
	cvReleaseMat( &dy );
	cvFree( &mscr );
	cvFree( &edge );
	cvFree( &map );
}
void
cv::ExtractOldMSER( CvArr* _img,
	       CvArr* _mask,
	       CvSeq** _contours,
	       CvMemStorage* storage,
	       const cv::CvMSERParams& params,
           bool modified_mser )
{
	CvMat srchdr, *src = cvGetMat( _img, &srchdr );
	CvMat maskhdr, *mask = _mask ? cvGetMat( _mask, &maskhdr ) : 0;
	CvSeq* contours = 0;
	CV_Assert(src != 0);
	CV_Assert(CV_MAT_TYPE(src->type) == CV_8UC1 || CV_MAT_TYPE(src->type) == CV_8UC3);
	CV_Assert(mask == 0 || (CV_ARE_SIZES_EQ(src, mask) && CV_MAT_TYPE(mask->type) == CV_8UC1));
	CV_Assert(storage != 0);
	contours = *_contours = cvCreateSeq( 0, sizeof(CvSeq), sizeof(CvSeq*), storage );
	// choose different method for different image type
	// for grey image, it is: Linear Time Maximally Stable Extremal Regions
	// for color image, it is: Maximally Stable Colour Regions for Recognition and Matching
	switch ( CV_MAT_TYPE(src->type) )
	{
		case CV_8UC1:
			icvExtractMSER_8UC1( src, mask, contours, storage, params, modified_mser );
			break;
		case CV_8UC3:
			icvExtractMSER_8UC3( src, mask, contours, storage, params );
			break;
	}
}

