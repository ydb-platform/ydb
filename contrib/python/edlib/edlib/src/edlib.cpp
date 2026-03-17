#include "edlib.h"

#include <stdint.h>
#include <cstdlib>
#include <algorithm>
#include <vector>
#include <cstring>
#include <string>

using namespace std;

typedef uint64_t Word;
static const int WORD_SIZE = sizeof(Word) * 8; // Size of Word in bits
static const Word WORD_1 = static_cast<Word>(1);
static const Word HIGH_BIT_MASK = WORD_1 << (WORD_SIZE - 1);  // 100..00
static const int MAX_UCHAR = 255;

// Data needed to find alignment.
struct AlignmentData {
    Word* Ps;
    Word* Ms;
    int* scores;
    int* firstBlocks;
    int* lastBlocks;

    AlignmentData(int maxNumBlocks, int targetLength) {
        // We build a complete table and mark first and last block for each column
        // (because algorithm is banded so only part of each columns is used).
        // TODO: do not build a whole table, but just enough blocks for each column.
         Ps     = new Word[maxNumBlocks * targetLength];
         Ms     = new Word[maxNumBlocks * targetLength];
         scores = new  int[maxNumBlocks * targetLength];
         firstBlocks = new int[targetLength];
         lastBlocks  = new int[targetLength];
    }

    ~AlignmentData() {
        delete[] Ps;
        delete[] Ms;
        delete[] scores;
        delete[] firstBlocks;
        delete[] lastBlocks;
    }
};

struct Block {
    Word P;  // Pvin
    Word M;  // Mvin
    int score; // score of last cell in block;

    Block() {}
    Block(Word p, Word m, int s) :P(p), M(m), score(s) {}
};


/**
 * Defines equality relation on alphabet characters.
 * By default each character is always equal only to itself, but you can also provide additional equalities.
 */
class EqualityDefinition {
private:
    bool matrix[MAX_UCHAR + 1][MAX_UCHAR + 1];
public:
    EqualityDefinition(const string& alphabet,
                       const EdlibEqualityPair* additionalEqualities = NULL,
                       const int additionalEqualitiesLength = 0) {
        for (int i = 0; i < static_cast<int>(alphabet.size()); i++) {
            for (int j = 0; j < static_cast<int>(alphabet.size()); j++) {
                matrix[i][j] = (i == j);
            }
        }
        if (additionalEqualities != NULL) {
            for (int i = 0; i < additionalEqualitiesLength; i++) {
                size_t firstTransformed = alphabet.find(additionalEqualities[i].first);
                size_t secondTransformed = alphabet.find(additionalEqualities[i].second);
                if (firstTransformed != string::npos && secondTransformed != string::npos) {
                    matrix[firstTransformed][secondTransformed] = matrix[secondTransformed][firstTransformed] = true;
                }
            }
        }
    }

    /**
     * @param a  Element from transformed sequence.
     * @param b  Element from transformed sequence.
     * @return True if a and b are defined as equal, false otherwise.
     */
    bool areEqual(unsigned char a, unsigned char b) const {
        return matrix[a][b];
    }
};

static int myersCalcEditDistanceSemiGlobal(const Word* Peq, int W, int maxNumBlocks,
                                           int queryLength,
                                           const unsigned char* target, int targetLength,
                                           int k, EdlibAlignMode mode,
                                           int* bestScore_, int** positions_, int* numPositions_);

static int myersCalcEditDistanceNW(const Word* Peq, int W, int maxNumBlocks,
                                   int queryLength,
                                   const unsigned char* target, int targetLength,
                                   int k, int* bestScore_,
                                   int* position_, bool findAlignment,
                                   AlignmentData** alignData, int targetStopPosition);


static int obtainAlignment(
        const unsigned char* query, const unsigned char* rQuery, int queryLength,
        const unsigned char* target, const unsigned char* rTarget, int targetLength,
        const EqualityDefinition& equalityDefinition, int alphabetLength, int bestScore,
        unsigned char** alignment, int* alignmentLength);

static int obtainAlignmentHirschberg(
        const unsigned char* query, const unsigned char* rQuery, int queryLength,
        const unsigned char* target, const unsigned char* rTarget, int targetLength,
        const EqualityDefinition& equalityDefinition, int alphabetLength, int bestScore,
        unsigned char** alignment, int* alignmentLength);

static int obtainAlignmentTraceback(int queryLength, int targetLength,
                                    int bestScore, const AlignmentData* alignData,
                                    unsigned char** alignment, int* alignmentLength);

static string transformSequences(const char* queryOriginal, int queryLength,
                                 const char* targetOriginal, int targetLength,
                                 unsigned char** queryTransformed,
                                 unsigned char** targetTransformed);

static inline int ceilDiv(int x, int y);

static inline unsigned char* createReverseCopy(const unsigned char* seq, int length);

static inline Word* buildPeq(const int alphabetLength,
                             const unsigned char* query,
                             const int queryLength,
                             const EqualityDefinition& equalityDefinition);


/**
 * Main edlib method.
 */
extern "C" EdlibAlignResult edlibAlign(const char* const queryOriginal, const int queryLength,
                                       const char* const targetOriginal, const int targetLength,
                                       const EdlibAlignConfig config) {
    EdlibAlignResult result;
    result.status = EDLIB_STATUS_OK;
    result.editDistance = -1;
    result.endLocations = result.startLocations = NULL;
    result.numLocations = 0;
    result.alignment = NULL;
    result.alignmentLength = 0;
    result.alphabetLength = 0;

    /*------------ TRANSFORM SEQUENCES AND RECOGNIZE ALPHABET -----------*/
    unsigned char* query, * target;
    string alphabet = transformSequences(queryOriginal, queryLength, targetOriginal, targetLength,
                                         &query, &target);
    result.alphabetLength = static_cast<int>(alphabet.size());
    /*-------------------------------------------------------*/

    // Handle special situation when at least one of the sequences has length 0.
    if (queryLength == 0 || targetLength == 0) {
        if (config.mode == EDLIB_MODE_NW) {
            result.editDistance = std::max(queryLength, targetLength);
            result.endLocations = static_cast<int *>(malloc(sizeof(int) * 1));
            result.endLocations[0] = targetLength - 1;
            result.numLocations = 1;
        } else if (config.mode == EDLIB_MODE_SHW || config.mode == EDLIB_MODE_HW) {
            result.editDistance = queryLength;
            result.endLocations = static_cast<int *>(malloc(sizeof(int) * 1));
            result.endLocations[0] = -1;
            result.numLocations = 1;
        } else {
            result.status = EDLIB_STATUS_ERROR;
        }

        free(query);
        free(target);
        return result;
    }

    /*--------------------- INITIALIZATION ------------------*/
    int maxNumBlocks = ceilDiv(queryLength, WORD_SIZE); // bmax in Myers
    int W = maxNumBlocks * WORD_SIZE - queryLength; // number of redundant cells in last level blocks
    EqualityDefinition equalityDefinition(alphabet, config.additionalEqualities, config.additionalEqualitiesLength);
    Word* Peq = buildPeq(static_cast<int>(alphabet.size()), query, queryLength, equalityDefinition);
    /*-------------------------------------------------------*/

    /*------------------ MAIN CALCULATION -------------------*/
    // TODO: Store alignment data only after k is determined? That could make things faster.
    int positionNW; // Used only when mode is NW.
    AlignmentData* alignData = NULL;
    bool dynamicK = false;
    int k = config.k;
    if (k < 0) { // If valid k is not given, auto-adjust k until solution is found.
        dynamicK = true;
        k = WORD_SIZE; // Gives better results than smaller k.
    }

    do {
        if (config.mode == EDLIB_MODE_HW || config.mode == EDLIB_MODE_SHW) {
            myersCalcEditDistanceSemiGlobal(Peq, W, maxNumBlocks,
                                            queryLength, target, targetLength,
                                            k, config.mode, &(result.editDistance),
                                            &(result.endLocations), &(result.numLocations));
        } else {  // mode == EDLIB_MODE_NW
            myersCalcEditDistanceNW(Peq, W, maxNumBlocks,
                                    queryLength, target, targetLength,
                                    k, &(result.editDistance), &positionNW,
                                    false, &alignData, -1);
        }
        k *= 2;
    } while(dynamicK && result.editDistance == -1);

    if (result.editDistance >= 0) {  // If there is solution.
        // If NW mode, set end location explicitly.
        if (config.mode == EDLIB_MODE_NW) {
            result.endLocations = static_cast<int *>(malloc(sizeof(int) * 1));
            result.endLocations[0] = targetLength - 1;
            result.numLocations = 1;
        }

        // Find starting locations.
        if (config.task == EDLIB_TASK_LOC || config.task == EDLIB_TASK_PATH) {
            result.startLocations = static_cast<int *>(malloc(result.numLocations * sizeof(int)));
            if (config.mode == EDLIB_MODE_HW) {  // If HW, I need to calculate start locations.
                const unsigned char* rTarget = createReverseCopy(target, targetLength);
                const unsigned char* rQuery  = createReverseCopy(query, queryLength);
                // Peq for reversed query.
                Word* rPeq = buildPeq(static_cast<int>(alphabet.size()), rQuery, queryLength, equalityDefinition);
                for (int i = 0; i < result.numLocations; i++) {
                    int endLocation = result.endLocations[i];
                    if (endLocation == -1) {
                        // NOTE: Sometimes one of optimal solutions is that query starts before target, like this:
                        //                       AAGG <- target
                        //                   CCTT     <- query
                        //   It will never be only optimal solution and it does not happen often, however it is
                        //   possible and in that case end location will be -1. What should we do with that?
                        //   Should we just skip reporting such end location, although it is a solution?
                        //   If we do report it, what is the start location? -4? -1? Nothing?
                        // TODO: Figure this out. This has to do in general with how we think about start
                        //   and end locations.
                        //   Also, we have alignment later relying on this locations to limit the space of it's
                        //   search -> how can it do it right if these locations are negative or incorrect?
                        result.startLocations[i] = 0;  // I put 0 for now, but it does not make much sense.
                    } else {
                        int bestScoreSHW, numPositionsSHW;
                        int* positionsSHW;
                        myersCalcEditDistanceSemiGlobal(
                                rPeq, W, maxNumBlocks,
                                queryLength, rTarget + targetLength - endLocation - 1, endLocation + 1,
                                result.editDistance, EDLIB_MODE_SHW,
                                &bestScoreSHW, &positionsSHW, &numPositionsSHW);
                        // Taking last location as start ensures that alignment will not start with insertions
                        // if it can start with mismatches instead.
                        result.startLocations[i] = endLocation - positionsSHW[numPositionsSHW - 1];
                        free(positionsSHW);
                    }
                }
                delete[] rTarget;
                delete[] rQuery;
                delete[] rPeq;
            } else {  // If mode is SHW or NW
                for (int i = 0; i < result.numLocations; i++) {
                    result.startLocations[i] = 0;
                }
            }
        }

        // Find alignment -> all comes down to finding alignment for NW.
        // Currently we return alignment only for first pair of locations.
        if (config.task == EDLIB_TASK_PATH) {
            int alnStartLocation = result.startLocations[0];
            int alnEndLocation = result.endLocations[0];
            const unsigned char* alnTarget = target + alnStartLocation;
            const int alnTargetLength = alnEndLocation - alnStartLocation + 1;
            const unsigned char* rAlnTarget = createReverseCopy(alnTarget, alnTargetLength);
            const unsigned char* rQuery  = createReverseCopy(query, queryLength);
            obtainAlignment(query, rQuery, queryLength,
                            alnTarget, rAlnTarget, alnTargetLength,
                            equalityDefinition, static_cast<int>(alphabet.size()), result.editDistance,
                            &(result.alignment), &(result.alignmentLength));
            delete[] rAlnTarget;
            delete[] rQuery;
        }
    }
    /*-------------------------------------------------------*/

    //--- Free memory ---//
    delete[] Peq;
    free(query);
    free(target);
    if (alignData) delete alignData;
    //-------------------//

    return result;
}

extern "C" char* edlibAlignmentToCigar(const unsigned char* const alignment, const int alignmentLength,
                                       const EdlibCigarFormat cigarFormat) {
    if (cigarFormat != EDLIB_CIGAR_EXTENDED && cigarFormat != EDLIB_CIGAR_STANDARD) {
        return 0;
    }

    // Maps move code from alignment to char in cigar.
    //                        0    1    2    3
    char moveCodeToChar[] = {'=', 'I', 'D', 'X'};
    if (cigarFormat == EDLIB_CIGAR_STANDARD) {
        moveCodeToChar[0] = moveCodeToChar[3] = 'M';
    }

    vector<char>* cigar = new vector<char>();
    char lastMove = 0;  // Char of last move. 0 if there was no previous move.
    int numOfSameMoves = 0;
    for (int i = 0; i <= alignmentLength; i++) {
        // if new sequence of same moves started
        if (i == alignmentLength || (moveCodeToChar[alignment[i]] != lastMove && lastMove != 0)) {
            // Write number of moves to cigar string.
            int numDigits = 0;
            for (; numOfSameMoves; numOfSameMoves /= 10) {
                cigar->push_back('0' + numOfSameMoves % 10);
                numDigits++;
            }
            reverse(cigar->end() - numDigits, cigar->end());
            // Write code of move to cigar string.
            cigar->push_back(lastMove);
            // If not at the end, start new sequence of moves.
            if (i < alignmentLength) {
                // Check if alignment has valid values.
                if (alignment[i] > 3) {
                    delete cigar;
                    return 0;
                }
                numOfSameMoves = 0;
            }
        }
        if (i < alignmentLength) {
            lastMove = moveCodeToChar[alignment[i]];
            numOfSameMoves++;
        }
    }
    cigar->push_back(0);  // Null character termination.
    char* cigar_ = static_cast<char *>(malloc(cigar->size() * sizeof(char)));
    memcpy(cigar_, &(*cigar)[0], cigar->size() * sizeof(char));
    delete cigar;

    return cigar_;
}

/**
 * Build Peq table for given query and alphabet.
 * Peq is table of dimensions alphabetLength+1 x maxNumBlocks.
 * Bit i of Peq[s * maxNumBlocks + b] is 1 if i-th symbol from block b of query equals symbol s, otherwise it is 0.
 * NOTICE: free returned array with delete[]!
 */
static inline Word* buildPeq(const int alphabetLength,
                             const unsigned char* const query,
                             const int queryLength,
                             const EqualityDefinition& equalityDefinition) {
    int maxNumBlocks = ceilDiv(queryLength, WORD_SIZE);
    // table of dimensions alphabetLength+1 x maxNumBlocks. Last symbol is wildcard.
    Word* Peq = new Word[(alphabetLength + 1) * maxNumBlocks];

    // Build Peq (1 is match, 0 is mismatch). NOTE: last column is wildcard(symbol that matches anything) with just 1s
    for (int symbol = 0; symbol <= alphabetLength; symbol++) {
        for (int b = 0; b < maxNumBlocks; b++) {
            if (symbol < alphabetLength) {
                Peq[symbol * maxNumBlocks + b] = 0;
                for (int r = (b+1) * WORD_SIZE - 1; r >= b * WORD_SIZE; r--) {
                    Peq[symbol * maxNumBlocks + b] <<= 1;
                    // NOTE: We pretend like query is padded at the end with W wildcard symbols
                    if (r >= queryLength || equalityDefinition.areEqual(query[r], symbol))
                        Peq[symbol * maxNumBlocks + b] += 1;
                }
            } else { // Last symbol is wildcard, so it is all 1s
                Peq[symbol * maxNumBlocks + b] = static_cast<Word>(-1);
            }
        }
    }

    return Peq;
}


/**
 * Returns new sequence that is reverse of given sequence.
 * Free returned array with delete[].
 */
static inline unsigned char* createReverseCopy(const unsigned char* const seq, const int length) {
    unsigned char* rSeq = new unsigned char[length];
    for (int i = 0; i < length; i++) {
        rSeq[i] = seq[length - i - 1];
    }
    return rSeq;
}

/**
 * Corresponds to Advance_Block function from Myers.
 * Calculates one word(block), which is part of a column.
 * Highest bit of word (one most to the left) is most bottom cell of block from column.
 * Pv[i] and Mv[i] define vin of cell[i]: vin = cell[i] - cell[i-1].
 * @param [in] Pv  Bitset, Pv[i] == 1 if vin is +1, otherwise Pv[i] == 0.
 * @param [in] Mv  Bitset, Mv[i] == 1 if vin is -1, otherwise Mv[i] == 0.
 * @param [in] Eq  Bitset, Eq[i] == 1 if match, 0 if mismatch.
 * @param [in] hin  Will be +1, 0 or -1.
 * @param [out] PvOut  Bitset, PvOut[i] == 1 if vout is +1, otherwise PvOut[i] == 0.
 * @param [out] MvOut  Bitset, MvOut[i] == 1 if vout is -1, otherwise MvOut[i] == 0.
 * @param [out] hout  Will be +1, 0 or -1.
 */
static inline int calculateBlock(Word Pv, Word Mv, Word Eq, const int hin,
                                 Word &PvOut, Word &MvOut) {
    // hin can be 1, -1 or 0.
    // 1  -> 00...01
    // 0  -> 00...00
    // -1 -> 11...11 (2-complement)

    Word hinIsNeg = static_cast<Word>(hin >> 2) & WORD_1; // 00...001 if hin is -1, 00...000 if 0 or 1

    Word Xv = Eq | Mv;
    // This is instruction below written using 'if': if (hin < 0) Eq |= (Word)1;
    Eq |= hinIsNeg;
    Word Xh = (((Eq & Pv) + Pv) ^ Pv) | Eq;

    Word Ph = Mv | ~(Xh | Pv);
    Word Mh = Pv & Xh;

    int hout = 0;
    // This is instruction below written using 'if': if (Ph & HIGH_BIT_MASK) hout = 1;
    hout = (Ph & HIGH_BIT_MASK) >> (WORD_SIZE - 1);
    // This is instruction below written using 'if': if (Mh & HIGH_BIT_MASK) hout = -1;
    hout -= (Mh & HIGH_BIT_MASK) >> (WORD_SIZE - 1);

    Ph <<= 1;
    Mh <<= 1;

    // This is instruction below written using 'if': if (hin < 0) Mh |= (Word)1;
    Mh |= hinIsNeg;
    // This is instruction below written using 'if': if (hin > 0) Ph |= (Word)1;
    Ph |= static_cast<Word>((hin + 1) >> 1);

    PvOut = Mh | ~(Xv | Ph);
    MvOut = Ph & Xv;

    return hout;
}

/**
 * Does ceiling division x / y.
 * Note: x and y must be non-negative and x + y must not overflow.
 */
static inline int ceilDiv(const int x, const int y) {
    return x % y ? x / y + 1 : x / y;
}

static inline int min(const int x, const int y) {
    return x < y ? x : y;
}

static inline int max(const int x, const int y) {
    return x > y ? x : y;
}


/**
 * @param [in] block
 * @return Values of cells in block, starting with bottom cell in block.
 */
static inline vector<int> getBlockCellValues(const Block block) {
    vector<int> scores(WORD_SIZE);
    int score = block.score;
    Word mask = HIGH_BIT_MASK;
    for (int i = 0; i < WORD_SIZE - 1; i++) {
        scores[i] = score;
        if (block.P & mask) score--;
        if (block.M & mask) score++;
        mask >>= 1;
    }
    scores[WORD_SIZE - 1] = score;
    return scores;
}

/**
 * Writes values of cells in block into given array, starting with first/top cell.
 * @param [in] block
 * @param [out] dest  Array into which cell values are written. Must have size of at least WORD_SIZE.
 */
static inline void readBlock(const Block block, int* const dest) {
    int score = block.score;
    Word mask = HIGH_BIT_MASK;
    for (int i = 0; i < WORD_SIZE - 1; i++) {
        dest[WORD_SIZE - 1 - i] = score;
        if (block.P & mask) score--;
        if (block.M & mask) score++;
        mask >>= 1;
    }
    dest[0] = score;
}

/**
 * Writes values of cells in block into given array, starting with last/bottom cell.
 * @param [in] block
 * @param [out] dest  Array into which cell values are written. Must have size of at least WORD_SIZE.
 */
static inline void readBlockReverse(const Block block, int* const dest) {
    int score = block.score;
    Word mask = HIGH_BIT_MASK;
    for (int i = 0; i < WORD_SIZE - 1; i++) {
        dest[i] = score;
        if (block.P & mask) score--;
        if (block.M & mask) score++;
        mask >>= 1;
    }
    dest[WORD_SIZE - 1] = score;
}

/**
 * @param [in] block
 * @param [in] k
 * @return True if all cells in block have value larger than k, otherwise false.
 */
static inline bool allBlockCellsLarger(const Block block, const int k) {
    vector<int> scores = getBlockCellValues(block);
    for (int i = 0; i < WORD_SIZE; i++) {
        if (scores[i] <= k) return false;
    }
    return true;
}


/**
 * Uses Myers' bit-vector algorithm to find edit distance for one of semi-global alignment methods.
 * @param [in] Peq  Query profile.
 * @param [in] W  Size of padding in last block.
 *                TODO: Calculate this directly from query, instead of passing it.
 * @param [in] maxNumBlocks  Number of blocks needed to cover the whole query.
 *                           TODO: Calculate this directly from query, instead of passing it.
 * @param [in] queryLength
 * @param [in] target
 * @param [in] targetLength
 * @param [in] k
 * @param [in] mode  EDLIB_MODE_HW or EDLIB_MODE_SHW
 * @param [out] bestScore_  Edit distance.
 * @param [out] positions_  Array of 0-indexed positions in target at which best score was found.
                            Make sure to free this array with free().
 * @param [out] numPositions_  Number of positions in the positions_ array.
 * @return Status.
 */
static int myersCalcEditDistanceSemiGlobal(
        const Word* const Peq, const int W, const int maxNumBlocks,
        const int queryLength,
        const unsigned char* const target, const int targetLength,
        int k, const EdlibAlignMode mode,
        int* const bestScore_, int** const positions_, int* const numPositions_) {
    *positions_ = NULL;
    *numPositions_ = 0;

    // firstBlock is 0-based index of first block in Ukkonen band.
    // lastBlock is 0-based index of last block in Ukkonen band.
    int firstBlock = 0;
    int lastBlock = min(ceilDiv(k + 1, WORD_SIZE), maxNumBlocks) - 1; // y in Myers
    Block *bl; // Current block

    Block* blocks = new Block[maxNumBlocks];

    // For HW, solution will never be larger then queryLength.
    if (mode == EDLIB_MODE_HW) {
        k = min(queryLength, k);
    }

    // Each STRONG_REDUCE_NUM column is reduced in more expensive way.
    // This gives speed up of about 2 times for small k.
    const int STRONG_REDUCE_NUM = 2048;

    // Initialize P, M and score
    bl = blocks;
    for (int b = 0; b <= lastBlock; b++) {
        bl->score = (b + 1) * WORD_SIZE;
        bl->P = static_cast<Word>(-1); // All 1s
        bl->M = static_cast<Word>(0);
        bl++;
    }

    int bestScore = -1;
    vector<int> positions; // TODO: Maybe put this on heap?
    const int startHout = mode == EDLIB_MODE_HW ? 0 : 1; // If 0 then gap before query is not penalized;
    const unsigned char* targetChar = target;
    for (int c = 0; c < targetLength; c++) { // for each column
        const Word* Peq_c = Peq + (*targetChar) * maxNumBlocks;

        //----------------------- Calculate column -------------------------//
        int hout = startHout;
        bl = blocks + firstBlock;
        Peq_c += firstBlock;
        for (int b = firstBlock; b <= lastBlock; b++) {
            hout = calculateBlock(bl->P, bl->M, *Peq_c, hout, bl->P, bl->M);
            bl->score += hout;
            bl++; Peq_c++;
        }
        bl--; Peq_c--;
        //------------------------------------------------------------------//

        //---------- Adjust number of blocks according to Ukkonen ----------//
        if ((lastBlock < maxNumBlocks - 1) && (bl->score - hout <= k) // bl is pointing to last block
            && ((*(Peq_c + 1) & WORD_1) || hout < 0)) { // Peq_c is pointing to last block
            // If score of left block is not too big, calculate one more block
            lastBlock++; bl++; Peq_c++;
            bl->P = static_cast<Word>(-1); // All 1s
            bl->M = static_cast<Word>(0);
            bl->score = (bl - 1)->score - hout + WORD_SIZE + calculateBlock(bl->P, bl->M, *Peq_c, hout, bl->P, bl->M);
        } else {
            while (lastBlock >= firstBlock && bl->score >= k + WORD_SIZE) {
                lastBlock--; bl--; Peq_c--;
            }
        }

        // Every some columns, do some expensive but also more efficient block reducing.
        // This is important!
        //
        // Reduce the band by decreasing last block if possible.
        if (c % STRONG_REDUCE_NUM == 0) {
            while (lastBlock >= 0 && lastBlock >= firstBlock && allBlockCellsLarger(*bl, k)) {
                lastBlock--; bl--; Peq_c--;
            }
        }
        // For HW, even if all cells are > k, there still may be solution in next
        // column because starting conditions at upper boundary are 0.
        // That means that first block is always candidate for solution,
        // and we can never end calculation before last column.
        if (mode == EDLIB_MODE_HW && lastBlock == -1) {
            lastBlock++; bl++; Peq_c++;
        }

        // Reduce band by increasing first block if possible. Not applicable to HW.
        if (mode != EDLIB_MODE_HW) {
            while (firstBlock <= lastBlock && blocks[firstBlock].score >= k + WORD_SIZE) {
                firstBlock++;
            }
            if (c % STRONG_REDUCE_NUM == 0) { // Do strong reduction every some blocks
                while (firstBlock <= lastBlock && allBlockCellsLarger(blocks[firstBlock], k)) {
                    firstBlock++;
                }
            }
        }

        // If band stops to exist finish
        if (lastBlock < firstBlock) {
            *bestScore_ = bestScore;
            if (bestScore != -1) {
                *positions_ = static_cast<int *>(malloc(sizeof(int) * static_cast<int>(positions.size())));
                *numPositions_ = static_cast<int>(positions.size());
                copy(positions.begin(), positions.end(), *positions_);
            }
            delete[] blocks;
            return EDLIB_STATUS_OK;
        }
        //------------------------------------------------------------------//

        //------------------------- Update best score ----------------------//
        if (lastBlock == maxNumBlocks - 1) {
            int colScore = bl->score;
            if (colScore <= k) { // Scores > k dont have correct values (so we cannot use them), but are certainly > k.
                // NOTE: Score that I find in column c is actually score from column c-W
                if (bestScore == -1 || colScore <= bestScore) {
                    if (colScore != bestScore) {
                        positions.clear();
                        bestScore = colScore;
                        // Change k so we will look only for equal or better
                        // scores then the best found so far.
                        k = bestScore;
                    }
                    positions.push_back(c - W);
                }
            }
        }
        //------------------------------------------------------------------//

        targetChar++;
    }


    // Obtain results for last W columns from last column.
    if (lastBlock == maxNumBlocks - 1) {
        vector<int> blockScores = getBlockCellValues(*bl);
        for (int i = 0; i < W; i++) {
            int colScore = blockScores[i + 1];
            if (colScore <= k && (bestScore == -1 || colScore <= bestScore)) {
                if (colScore != bestScore) {
                    positions.clear();
                    k = bestScore = colScore;
                }
                positions.push_back(targetLength - W + i);
            }
        }
    }

    *bestScore_ = bestScore;
    if (bestScore != -1) {
        *positions_ = static_cast<int *>(malloc(sizeof(int) * static_cast<int>(positions.size())));
        *numPositions_ = static_cast<int>(positions.size());
        copy(positions.begin(), positions.end(), *positions_);
    }

    delete[] blocks;
    return EDLIB_STATUS_OK;
}


/**
 * Uses Myers' bit-vector algorithm to find edit distance for global(NW) alignment method.
 * @param [in] Peq  Query profile.
 * @param [in] W  Size of padding in last block.
 *                TODO: Calculate this directly from query, instead of passing it.
 * @param [in] maxNumBlocks  Number of blocks needed to cover the whole query.
 *                           TODO: Calculate this directly from query, instead of passing it.
 * @param [in] queryLength
 * @param [in] target
 * @param [in] targetLength
 * @param [in] k
 * @param [out] bestScore_  Edit distance.
 * @param [out] position_  0-indexed position in target at which best score was found.
 * @param [in] findAlignment  If true, whole matrix is remembered and alignment data is returned.
 *                            Quadratic amount of memory is consumed.
 * @param [out] alignData  Data needed for alignment traceback (for reconstruction of alignment).
 *                         Set only if findAlignment is set to true, otherwise it is NULL.
 *                         Make sure to free this array using delete[].
 * @param [out] targetStopPosition  If set to -1, whole calculation is performed normally, as expected.
 *         If set to p, calculation is performed up to position p in target (inclusive)
 *         and column p is returned as the only column in alignData.
 * @return Status.
 */
static int myersCalcEditDistanceNW(const Word* const Peq, const int W, const int maxNumBlocks,
                                   const int queryLength,
                                   const unsigned char* const target, const int targetLength,
                                   int k, int* const bestScore_,
                                   int* const position_, const bool findAlignment,
                                   AlignmentData** const alignData, const int targetStopPosition) {
    if (targetStopPosition > -1 && findAlignment) {
        // They can not be both set at the same time!
        return EDLIB_STATUS_ERROR;
    }

    // Each STRONG_REDUCE_NUM column is reduced in more expensive way.
    const int STRONG_REDUCE_NUM = 2048; // TODO: Choose this number dinamically (based on query and target lengths?), so it does not affect speed of computation

    if (k < abs(targetLength - queryLength)) {
        *bestScore_ = *position_ = -1;
        return EDLIB_STATUS_OK;
    }

    k = min(k, max(queryLength, targetLength));  // Upper bound for k

    // firstBlock is 0-based index of first block in Ukkonen band.
    // lastBlock is 0-based index of last block in Ukkonen band.
    int firstBlock = 0;
    // This is optimal now, by my formula.
    int lastBlock = min(maxNumBlocks, ceilDiv(min(k, (k + queryLength - targetLength) / 2) + 1, WORD_SIZE)) - 1;
    Block* bl; // Current block

    Block* blocks = new Block[maxNumBlocks];

    // Initialize P, M and score
    bl = blocks;
    for (int b = 0; b <= lastBlock; b++) {
        bl->score = (b + 1) * WORD_SIZE;
        bl->P = static_cast<Word>(-1); // All 1s
        bl->M = static_cast<Word>(0);
        bl++;
    }

    // If we want to find alignment, we have to store needed data.
    if (findAlignment)
        *alignData = new AlignmentData(maxNumBlocks, targetLength);
    else if (targetStopPosition > -1)
        *alignData = new AlignmentData(maxNumBlocks, 1);
    else
        *alignData = NULL;

    const unsigned char* targetChar = target;
    for (int c = 0; c < targetLength; c++) { // for each column
        const Word* Peq_c = Peq + *targetChar * maxNumBlocks;

        //----------------------- Calculate column -------------------------//
        int hout = 1;
        bl = blocks + firstBlock;
        for (int b = firstBlock; b <= lastBlock; b++) {
            hout = calculateBlock(bl->P, bl->M, Peq_c[b], hout, bl->P, bl->M);
            bl->score += hout;
            bl++;
        }
        bl--;
        //------------------------------------------------------------------//
        // bl now points to last block

        // Update k. I do it only on end of column because it would slow calculation too much otherwise.
        // NOTICE: I add W when in last block because it is actually result from W cells to the left and W cells up.
        k = min(k, bl->score
                + max(targetLength - c - 1, queryLength - ((1 + lastBlock) * WORD_SIZE - 1) - 1)
                + (lastBlock == maxNumBlocks - 1 ? W : 0));

        //---------- Adjust number of blocks according to Ukkonen ----------//
        //--- Adjust last block ---//
        // If block is not beneath band, calculate next block. Only next because others are certainly beneath band.
        if (lastBlock + 1 < maxNumBlocks
            && !(//score[lastBlock] >= k + WORD_SIZE ||  // NOTICE: this condition could be satisfied if above block also!
                 ((lastBlock + 1) * WORD_SIZE - 1
                  > k - bl->score + 2 * WORD_SIZE - 2 - targetLength + c + queryLength))) {
            lastBlock++; bl++;
            bl->P = static_cast<Word>(-1); // All 1s
            bl->M = static_cast<Word>(0);
            int newHout = calculateBlock(bl->P, bl->M, Peq_c[lastBlock], hout, bl->P, bl->M);
            bl->score = (bl - 1)->score - hout + WORD_SIZE + newHout;
            hout = newHout;
        }

        // While block is out of band, move one block up.
        // NOTE: Condition used here is more loose than the one from the article, since I simplified the max() part of it.
        // I could consider adding that max part, for optimal performance.
        while (lastBlock >= firstBlock
               && (bl->score >= k + WORD_SIZE
                   || ((lastBlock + 1) * WORD_SIZE - 1 >
                       // TODO: Does not work if do not put +1! Why???
                       k - bl->score + 2 * WORD_SIZE - 2 - targetLength + c + queryLength + 1))) {
            lastBlock--; bl--;
        }
        //-------------------------//

        //--- Adjust first block ---//
        // While outside of band, advance block
        while (firstBlock <= lastBlock
               && (blocks[firstBlock].score >= k + WORD_SIZE
                   || ((firstBlock + 1) * WORD_SIZE - 1 <
                       blocks[firstBlock].score - k - targetLength + queryLength + c))) {
            firstBlock++;
        }
        //--------------------------/


        // TODO: consider if this part is useful, it does not seem to help much
        if (c % STRONG_REDUCE_NUM == 0) { // Every some columns do more expensive but more efficient reduction
            while (lastBlock >= firstBlock) {
                // If all cells outside of band, remove block
                vector<int> scores = getBlockCellValues(*bl);
                int numCells = lastBlock == maxNumBlocks - 1 ? WORD_SIZE - W : WORD_SIZE;
                int r = lastBlock * WORD_SIZE + numCells - 1;
                bool reduce = true;
                for (int i = WORD_SIZE - numCells; i < WORD_SIZE; i++) {
                    // TODO: Does not work if do not put +1! Why???
                    if (scores[i] <= k && r <= k - scores[i] - targetLength + c + queryLength + 1) {
                        reduce = false;
                        break;
                    }
                    r--;
                }
                if (!reduce) break;
                lastBlock--; bl--;
            }

            while (firstBlock <= lastBlock) {
                // If all cells outside of band, remove block
                vector<int> scores = getBlockCellValues(blocks[firstBlock]);
                int numCells = firstBlock == maxNumBlocks - 1 ? WORD_SIZE - W : WORD_SIZE;
                int r = firstBlock * WORD_SIZE + numCells - 1;
                bool reduce = true;
                for (int i = WORD_SIZE - numCells; i < WORD_SIZE; i++) {
                    if (scores[i] <= k && r >= scores[i] - k - targetLength + c + queryLength) {
                        reduce = false;
                        break;
                    }
                    r--;
                }
                if (!reduce) break;
                firstBlock++;
            }
        }


        // If band stops to exist finish
        if (lastBlock < firstBlock) {
            *bestScore_ = *position_ = -1;
            delete[] blocks;
            return EDLIB_STATUS_OK;
        }
        //------------------------------------------------------------------//


        //---- Save column so it can be used for reconstruction ----//
        if (findAlignment && c < targetLength) {
            bl = blocks + firstBlock;
            for (int b = firstBlock; b <= lastBlock; b++) {
                (*alignData)->Ps[maxNumBlocks * c + b] = bl->P;
                (*alignData)->Ms[maxNumBlocks * c + b] = bl->M;
                (*alignData)->scores[maxNumBlocks * c + b] = bl->score;
                bl++;
            }
            (*alignData)->firstBlocks[c] = firstBlock;
            (*alignData)->lastBlocks[c] = lastBlock;
        }
        //----------------------------------------------------------//
        //---- If this is stop column, save it and finish ----//
        if (c == targetStopPosition) {
            for (int b = firstBlock; b <= lastBlock; b++) {
                (*alignData)->Ps[b] = (blocks + b)->P;
                (*alignData)->Ms[b] = (blocks + b)->M;
                (*alignData)->scores[b] = (blocks + b)->score;
            }
            (*alignData)->firstBlocks[0] = firstBlock;
            (*alignData)->lastBlocks[0] = lastBlock;
            *bestScore_ = -1;
            *position_ = targetStopPosition;
            delete[] blocks;
            return EDLIB_STATUS_OK;
        }
        //----------------------------------------------------//

        targetChar++;
    }

    if (lastBlock == maxNumBlocks - 1) { // If last block of last column was calculated
        // Obtain best score from block -> it is complicated because query is padded with W cells
        int bestScore = getBlockCellValues(blocks[lastBlock])[W];
        if (bestScore <= k) {
            *bestScore_ = bestScore;
            *position_ = targetLength - 1;
            delete[] blocks;
            return EDLIB_STATUS_OK;
        }
    }

    *bestScore_ = *position_ = -1;
    delete[] blocks;
    return EDLIB_STATUS_OK;
}


/**
 * Finds one possible alignment that gives optimal score by moving back through the dynamic programming matrix,
 * that is stored in alignData. Consumes large amount of memory: O(queryLength * targetLength).
 * @param [in] queryLength  Normal length, without W.
 * @param [in] targetLength  Normal length, without W.
 * @param [in] bestScore  Best score.
 * @param [in] alignData  Data obtained during finding best score that is useful for finding alignment.
 * @param [out] alignment  Alignment.
 * @param [out] alignmentLength  Length of alignment.
 * @return Status code.
 */
static int obtainAlignmentTraceback(const int queryLength, const int targetLength,
                                    const int bestScore, const AlignmentData* const alignData,
                                    unsigned char** const alignment, int* const alignmentLength) {
    const int maxNumBlocks = ceilDiv(queryLength, WORD_SIZE);
    const int W = maxNumBlocks * WORD_SIZE - queryLength;

    *alignment = static_cast<unsigned char*>(malloc((queryLength + targetLength - 1) * sizeof(unsigned char)));
    *alignmentLength = 0;
    int c = targetLength - 1; // index of column
    int b = maxNumBlocks - 1; // index of block in column
    int currScore = bestScore; // Score of current cell
    int lScore  = -1; // Score of left cell
    int uScore  = -1; // Score of upper cell
    int ulScore = -1; // Score of upper left cell
    Word currP = alignData->Ps[c * maxNumBlocks + b]; // P of current block
    Word currM = alignData->Ms[c * maxNumBlocks + b]; // M of current block
    // True if block to left exists and is in band
    bool thereIsLeftBlock = c > 0 && b >= alignData->firstBlocks[c-1] && b <= alignData->lastBlocks[c-1];
    // We set initial values of lP and lM to 0 only to avoid compiler warnings, they should not affect the
    // calculation as both lP and lM should be initialized at some moment later (but compiler can not
    // detect it since this initialization is guaranteed by "business" logic).
    Word lP = 0, lM = 0;
    if (thereIsLeftBlock) {
        lP = alignData->Ps[(c - 1) * maxNumBlocks + b]; // P of block to the left
        lM = alignData->Ms[(c - 1) * maxNumBlocks + b]; // M of block to the left
    }
    currP <<= W;
    currM <<= W;
    int blockPos = WORD_SIZE - W - 1; // 0 based index of current cell in blockPos

    // TODO(martin): refactor this whole piece of code. There are too many if-else statements,
    // it is too easy for a bug to hide and to hard to effectively cover all the edge-cases.
    // We need better separation of logic and responsibilities.
    while (true) {
        if (c == 0) {
            thereIsLeftBlock = true;
            lScore = b * WORD_SIZE + blockPos + 1;
            ulScore = lScore - 1;
        }

        // TODO: improvement: calculate only those cells that are needed,
        //       for example if I calculate upper cell and can move up,
        //       there is no need to calculate left and upper left cell
        //---------- Calculate scores ---------//
        if (lScore == -1 && thereIsLeftBlock) {
            lScore = alignData->scores[(c - 1) * maxNumBlocks + b]; // score of block to the left
            for (int i = 0; i < WORD_SIZE - blockPos - 1; i++) {
                if (lP & HIGH_BIT_MASK) lScore--;
                if (lM & HIGH_BIT_MASK) lScore++;
                lP <<= 1;
                lM <<= 1;
            }
        }
        if (ulScore == -1) {
            if (lScore != -1) {
                ulScore = lScore;
                if (lP & HIGH_BIT_MASK) ulScore--;
                if (lM & HIGH_BIT_MASK) ulScore++;
            }
            else if (c > 0 && b-1 >= alignData->firstBlocks[c-1] && b-1 <= alignData->lastBlocks[c-1]) {
                // This is the case when upper left cell is last cell in block,
                // and block to left is not in band so lScore is -1.
                ulScore = alignData->scores[(c - 1) * maxNumBlocks + b - 1];
            }
        }
        if (uScore == -1) {
            uScore = currScore;
            if (currP & HIGH_BIT_MASK) uScore--;
            if (currM & HIGH_BIT_MASK) uScore++;
            currP <<= 1;
            currM <<= 1;
        }
        //-------------------------------------//

        // TODO: should I check if there is upper block?

        //-------------- Move --------------//
        // Move up - insertion to target - deletion from query
        if (uScore != -1 && uScore + 1 == currScore) {
            currScore = uScore;
            lScore = ulScore;
            uScore = ulScore = -1;
            if (blockPos == 0) { // If entering new (upper) block
                if (b == 0) { // If there are no cells above (only boundary cells)
                    (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_INSERT; // Move up
                    for (int i = 0; i < c + 1; i++) // Move left until end
                        (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_DELETE;
                    break;
                } else {
                    blockPos = WORD_SIZE - 1;
                    b--;
                    currP = alignData->Ps[c * maxNumBlocks + b];
                    currM = alignData->Ms[c * maxNumBlocks + b];
                    if (c > 0 && b >= alignData->firstBlocks[c-1] && b <= alignData->lastBlocks[c-1]) {
                        thereIsLeftBlock = true;
                        lP = alignData->Ps[(c - 1) * maxNumBlocks + b]; // TODO: improve this, too many operations
                        lM = alignData->Ms[(c - 1) * maxNumBlocks + b];
                    } else {
                        thereIsLeftBlock = false;
                        // TODO(martin): There may not be left block, but there can be left boundary - do we
                        // handle this correctly then? Are l and ul score set correctly? I should check that / refactor this.
                    }
                }
            } else {
                blockPos--;
                lP <<= 1;
                lM <<= 1;
            }
            // Mark move
            (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_INSERT;
        }
        // Move left - deletion from target - insertion to query
        else if (lScore != -1 && lScore + 1 == currScore) {
            currScore = lScore;
            uScore = ulScore;
            lScore = ulScore = -1;
            c--;
            if (c == -1) { // If there are no cells to the left (only boundary cells)
                (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_DELETE; // Move left
                int numUp = b * WORD_SIZE + blockPos + 1;
                for (int i = 0; i < numUp; i++) // Move up until end
                    (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_INSERT;
                break;
            }
            currP = lP;
            currM = lM;
            if (c > 0 && b >= alignData->firstBlocks[c-1] && b <= alignData->lastBlocks[c-1]) {
                thereIsLeftBlock = true;
                lP = alignData->Ps[(c - 1) * maxNumBlocks + b];
                lM = alignData->Ms[(c - 1) * maxNumBlocks + b];
            } else {
                if (c == 0) { // If there are no cells to the left (only boundary cells)
                    thereIsLeftBlock = true;
                    lScore = b * WORD_SIZE + blockPos + 1;
                    ulScore = lScore - 1;
                } else {
                    thereIsLeftBlock = false;
                }
            }
            // Mark move
            (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_DELETE;
        }
        // Move up left - (mis)match
        else if (ulScore != -1) {
            unsigned char moveCode = ulScore == currScore ? EDLIB_EDOP_MATCH : EDLIB_EDOP_MISMATCH;
            currScore = ulScore;
            uScore = lScore = ulScore = -1;
            c--;
            if (c == -1) { // If there are no cells to the left (only boundary cells)
                (*alignment)[(*alignmentLength)++] = moveCode; // Move left
                int numUp = b * WORD_SIZE + blockPos;
                for (int i = 0; i < numUp; i++) // Move up until end
                    (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_INSERT;
                break;
            }
            if (blockPos == 0) { // If entering upper left block
                if (b == 0) { // If there are no more cells above (only boundary cells)
                    (*alignment)[(*alignmentLength)++] = moveCode; // Move up left
                    for (int i = 0; i < c + 1; i++) // Move left until end
                        (*alignment)[(*alignmentLength)++] = EDLIB_EDOP_DELETE;
                    break;
                }
                blockPos = WORD_SIZE - 1;
                b--;
                currP = alignData->Ps[c * maxNumBlocks + b];
                currM = alignData->Ms[c * maxNumBlocks + b];
            } else { // If entering left block
                blockPos--;
                currP = lP;
                currM = lM;
                currP <<= 1;
                currM <<= 1;
            }
            // Set new left block
            if (c > 0 && b >= alignData->firstBlocks[c-1] && b <= alignData->lastBlocks[c-1]) {
                thereIsLeftBlock = true;
                lP = alignData->Ps[(c - 1) * maxNumBlocks + b];
                lM = alignData->Ms[(c - 1) * maxNumBlocks + b];
            } else {
                if (c == 0) { // If there are no cells to the left (only boundary cells)
                    thereIsLeftBlock = true;
                    lScore = b * WORD_SIZE + blockPos + 1;
                    ulScore = lScore - 1;
                } else {
                    thereIsLeftBlock = false;
                }
            }
            // Mark move
            (*alignment)[(*alignmentLength)++] = moveCode;
        } else {
            // Reached end - finished!
            break;
        }
        //----------------------------------//
    }

    *alignment = static_cast<unsigned char*>(realloc(*alignment, (*alignmentLength) * sizeof(unsigned char)));
    reverse(*alignment, *alignment + (*alignmentLength));
    return EDLIB_STATUS_OK;
}


/**
 * Finds one possible alignment that gives optimal score (bestScore).
 * It will split problem into smaller problems using Hirschberg's algorithm and when they are small enough,
 * it will solve them using traceback algorithm.
 * @param [in] query
 * @param [in] rQuery  Reversed query.
 * @param [in] queryLength
 * @param [in] target
 * @param [in] rTarget  Reversed target.
 * @param [in] targetLength
 * @param [in] equalityDefinition
 * @param [in] alphabetLength
 * @param [in] bestScore  Best(optimal) score.
 * @param [out] alignment  Sequence of edit operations that make target equal to query.
 * @param [out] alignmentLength  Length of alignment.
 * @return Status code.
 */
static int obtainAlignment(
        const unsigned char* const query, const unsigned char* const rQuery, const int queryLength,
        const unsigned char* const target, const unsigned char* const rTarget, const int targetLength,
        const EqualityDefinition& equalityDefinition, const int alphabetLength, const int bestScore,
        unsigned char** const alignment, int* const alignmentLength) {

    // Handle special case when one of sequences has length of 0.
    if (queryLength == 0 || targetLength == 0) {
        *alignmentLength = targetLength + queryLength;
        *alignment = static_cast<unsigned char*>(malloc((*alignmentLength) * sizeof(unsigned char)));
        for (int i = 0; i < *alignmentLength; i++) {
            (*alignment)[i] = queryLength == 0 ? EDLIB_EDOP_DELETE : EDLIB_EDOP_INSERT;
        }
        return EDLIB_STATUS_OK;
    }

    const int maxNumBlocks = ceilDiv(queryLength, WORD_SIZE);
    const int W = maxNumBlocks * WORD_SIZE - queryLength;
    int statusCode;

    // TODO: think about reducing number of memory allocations in alignment functions, probably
    // by sharing some memory that is allocated only once. That refers to: Peq, columns in Hirschberg,
    // and it could also be done for alignments - we could have one big array for alignment that would be
    // sparsely populated by each of steps in recursion, and at the end we would just consolidate those results.

    // If estimated memory consumption for traceback algorithm is smaller than 1MB use it,
    // otherwise use Hirschberg's algorithm. By running few tests I choose boundary of 1MB as optimal.
    long long alignmentDataSize = (2ll * sizeof(Word) + sizeof(int)) * maxNumBlocks * targetLength
        + 2ll * sizeof(int) * targetLength;
    if (alignmentDataSize < 1024 * 1024) {
        int score_, endLocation_;  // Used only to call function.
        AlignmentData* alignData = NULL;
        Word* Peq = buildPeq(alphabetLength, query, queryLength, equalityDefinition);
        myersCalcEditDistanceNW(Peq, W, maxNumBlocks,
                                queryLength,
                                target, targetLength,
                                bestScore,
                                &score_, &endLocation_, true, &alignData, -1);
        //assert(score_ == bestScore);
        //assert(endLocation_ == targetLength - 1);

        statusCode = obtainAlignmentTraceback(queryLength, targetLength,
                                              bestScore, alignData, alignment, alignmentLength);
        delete alignData;
        delete[] Peq;
    } else {
        statusCode = obtainAlignmentHirschberg(query, rQuery, queryLength,
                                               target, rTarget, targetLength,
                                               equalityDefinition, alphabetLength, bestScore,
                                               alignment, alignmentLength);
    }
    return statusCode;
}


/**
 * Finds one possible alignment that gives optimal score (bestScore).
 * Uses Hirschberg's algorithm to split problem into two sub-problems, solve them and combine them together.
 * @param [in] query
 * @param [in] rQuery  Reversed query.
 * @param [in] queryLength
 * @param [in] target
 * @param [in] rTarget  Reversed target.
 * @param [in] targetLength
 * @param [in] alphabetLength
 * @param [in] bestScore  Best(optimal) score.
 * @param [out] alignment  Sequence of edit operations that make target equal to query.
 * @param [out] alignmentLength  Length of alignment.
 * @return Status code.
 */
static int obtainAlignmentHirschberg(
        const unsigned char* const query, const unsigned char* const rQuery, const int queryLength,
        const unsigned char* const target, const unsigned char* const rTarget, const int targetLength,
        const EqualityDefinition& equalityDefinition, const int alphabetLength, const int bestScore,
        unsigned char** const alignment, int* const alignmentLength) {

    const int maxNumBlocks = ceilDiv(queryLength, WORD_SIZE);
    const int W = maxNumBlocks * WORD_SIZE - queryLength;

    Word* Peq = buildPeq(alphabetLength, query, queryLength, equalityDefinition);
    Word* rPeq = buildPeq(alphabetLength, rQuery, queryLength, equalityDefinition);

    // Used only to call functions.
    int score_, endLocation_;

    // Divide dynamic matrix into two halfs, left and right.
    const int leftHalfWidth = targetLength / 2;
    const int rightHalfWidth = targetLength - leftHalfWidth;

    // Calculate left half.
    AlignmentData* alignDataLeftHalf = NULL;
    int leftHalfCalcStatus = myersCalcEditDistanceNW(
            Peq, W, maxNumBlocks, queryLength, target, targetLength, bestScore,
            &score_, &endLocation_, false, &alignDataLeftHalf, leftHalfWidth - 1);

    // Calculate right half.
    AlignmentData* alignDataRightHalf = NULL;
    int rightHalfCalcStatus = myersCalcEditDistanceNW(
            rPeq, W, maxNumBlocks, queryLength, rTarget, targetLength, bestScore,
            &score_, &endLocation_, false, &alignDataRightHalf, rightHalfWidth - 1);

    delete[] Peq;
    delete[] rPeq;

    if (leftHalfCalcStatus == EDLIB_STATUS_ERROR || rightHalfCalcStatus == EDLIB_STATUS_ERROR) {
        if (alignDataLeftHalf) delete alignDataLeftHalf;
        if (alignDataRightHalf) delete alignDataRightHalf;
        return EDLIB_STATUS_ERROR;
    }

    // Unwrap the left half.
    int firstBlockIdxLeft = alignDataLeftHalf->firstBlocks[0];
    int lastBlockIdxLeft = alignDataLeftHalf->lastBlocks[0];
    // TODO: avoid this allocation by using some shared array?
    // scoresLeft contains scores from left column, starting with scoresLeftStartIdx row (query index)
    // and ending with scoresLeftEndIdx row (0-indexed).
    int scoresLeftLength = (lastBlockIdxLeft - firstBlockIdxLeft + 1) * WORD_SIZE;
    int* scoresLeft = new int[scoresLeftLength];
    for (int blockIdx = firstBlockIdxLeft; blockIdx <= lastBlockIdxLeft; blockIdx++) {
        Block block(alignDataLeftHalf->Ps[blockIdx], alignDataLeftHalf->Ms[blockIdx],
                    alignDataLeftHalf->scores[blockIdx]);
        readBlock(block, scoresLeft + (blockIdx - firstBlockIdxLeft) * WORD_SIZE);
    }
    int scoresLeftStartIdx = firstBlockIdxLeft * WORD_SIZE;
    // If last block contains padding, shorten the length of scores for the length of padding.
    if (lastBlockIdxLeft == maxNumBlocks - 1) {
        scoresLeftLength -= W;
    }

    // Unwrap the right half (I also reverse it while unwraping).
    int firstBlockIdxRight = alignDataRightHalf->firstBlocks[0];
    int lastBlockIdxRight = alignDataRightHalf->lastBlocks[0];
    int scoresRightLength = (lastBlockIdxRight - firstBlockIdxRight + 1) * WORD_SIZE;
    int* scoresRight = new int[scoresRightLength];
    int* scoresRightOriginalStart = scoresRight;
    for (int blockIdx = firstBlockIdxRight; blockIdx <= lastBlockIdxRight; blockIdx++) {
        Block block(alignDataRightHalf->Ps[blockIdx], alignDataRightHalf->Ms[blockIdx],
                    alignDataRightHalf->scores[blockIdx]);
        readBlockReverse(block, scoresRight + (lastBlockIdxRight - blockIdx) * WORD_SIZE);
    }
    int scoresRightStartIdx = queryLength - (lastBlockIdxRight + 1) * WORD_SIZE;
    // If there is padding at the beginning of scoresRight (that can happen because of reversing that we do),
    // move pointer forward to remove the padding (that is why we remember originalStart).
    if (scoresRightStartIdx < 0) {
        //assert(scoresRightStartIdx == -1 * W);
        scoresRight += W;
        scoresRightStartIdx += W;
        scoresRightLength -= W;
    }

    delete alignDataLeftHalf;
    delete alignDataRightHalf;

    //--------------------- Find the best move ----------------//
    // Find the query/row index of cell in left column which together with its lower right neighbour
    // from right column gives the best score (when summed). We also have to consider boundary cells
    // (those cells at -1 indexes).
    //  x|
    //  -+-
    //   |x
    int queryIdxLeftStart = max(scoresLeftStartIdx, scoresRightStartIdx - 1);
    int queryIdxLeftEnd = min(scoresLeftStartIdx + scoresLeftLength - 1,
                          scoresRightStartIdx + scoresRightLength - 2);
    int leftScore = -1, rightScore = -1;
    int queryIdxLeftAlignment = -1;  // Query/row index of cell in left column where alignment is passing through.
    bool queryIdxLeftAlignmentFound = false;
    for (int queryIdx = queryIdxLeftStart; queryIdx <= queryIdxLeftEnd; queryIdx++) {
        leftScore = scoresLeft[queryIdx - scoresLeftStartIdx];
        rightScore = scoresRight[queryIdx + 1 - scoresRightStartIdx];
        if (leftScore + rightScore == bestScore) {
            queryIdxLeftAlignment = queryIdx;
            queryIdxLeftAlignmentFound = true;
            break;
        }
    }
    // Check boundary cells.
    if (!queryIdxLeftAlignmentFound && scoresLeftStartIdx == 0 && scoresRightStartIdx == 0) {
        leftScore = leftHalfWidth;
        rightScore = scoresRight[0];
        if (leftScore + rightScore == bestScore) {
            queryIdxLeftAlignment = -1;
            queryIdxLeftAlignmentFound = true;
        }
    }
    if (!queryIdxLeftAlignmentFound && scoresLeftStartIdx + scoresLeftLength == queryLength
        && scoresRightStartIdx + scoresRightLength == queryLength) {
        leftScore = scoresLeft[scoresLeftLength - 1];
        rightScore = rightHalfWidth;
        if (leftScore + rightScore == bestScore) {
            queryIdxLeftAlignment = queryLength - 1;
            queryIdxLeftAlignmentFound = true;
        }
    }

    delete[] scoresLeft;
    delete[] scoresRightOriginalStart;

    if (queryIdxLeftAlignmentFound == false) {
        // If there was no move that is part of optimal alignment, then there is no such alignment
        // or given bestScore is not correct!
        return EDLIB_STATUS_ERROR;
    }
    //----------------------------------------------------------//

    // Calculate alignments for upper half of left half (upper left - ul)
    // and lower half of right half (lower right - lr).
    const int ulHeight = queryIdxLeftAlignment + 1;
    const int lrHeight = queryLength - ulHeight;
    const int ulWidth = leftHalfWidth;
    const int lrWidth = rightHalfWidth;
    unsigned char* ulAlignment = NULL; int ulAlignmentLength;
    int ulStatusCode = obtainAlignment(query, rQuery + lrHeight, ulHeight,
                                       target, rTarget + lrWidth, ulWidth,
                                       equalityDefinition, alphabetLength, leftScore,
                                       &ulAlignment, &ulAlignmentLength);
    unsigned char* lrAlignment = NULL; int lrAlignmentLength;
    int lrStatusCode = obtainAlignment(query + ulHeight, rQuery, lrHeight,
                                       target + ulWidth, rTarget, lrWidth,
                                       equalityDefinition, alphabetLength, rightScore,
                                       &lrAlignment, &lrAlignmentLength);
    if (ulStatusCode == EDLIB_STATUS_ERROR || lrStatusCode == EDLIB_STATUS_ERROR) {
        if (ulAlignment) free(ulAlignment);
        if (lrAlignment) free(lrAlignment);
        return EDLIB_STATUS_ERROR;
    }

    // Build alignment by concatenating upper left alignment with lower right alignment.
    *alignmentLength = ulAlignmentLength + lrAlignmentLength;
    *alignment = static_cast<unsigned char*>(malloc((*alignmentLength) * sizeof(unsigned char)));
    memcpy(*alignment, ulAlignment, ulAlignmentLength);
    memcpy(*alignment + ulAlignmentLength, lrAlignment, lrAlignmentLength);

    free(ulAlignment);
    free(lrAlignment);
    return EDLIB_STATUS_OK;
}


/**
 * Takes char query and char target, recognizes alphabet and transforms them into unsigned char sequences
 * where elements in sequences are not any more letters of alphabet, but their index in alphabet.
 * Most of internal edlib functions expect such transformed sequences.
 * This function will allocate queryTransformed and targetTransformed, so make sure to free them when done.
 * Example:
 *   Original sequences: "ACT" and "CGT".
 *   Alphabet would be recognized as "ACTG". Alphabet length = 4.
 *   Transformed sequences: [0, 1, 2] and [1, 3, 2].
 * @param [in] queryOriginal
 * @param [in] queryLength
 * @param [in] targetOriginal
 * @param [in] targetLength
 * @param [out] queryTransformed  It will contain values in range [0, alphabet length - 1].
 * @param [out] targetTransformed  It will contain values in range [0, alphabet length - 1].
 * @return  Alphabet as a string of unique characters, where index of each character is its value in transformed
 *          sequences.
 */
static string transformSequences(const char* const queryOriginal, const int queryLength,
                                 const char* const targetOriginal, const int targetLength,
                                 unsigned char** const queryTransformed,
                                 unsigned char** const targetTransformed) {
    // Alphabet is constructed from letters that are present in sequences.
    // Each letter is assigned an ordinal number, starting from 0 up to alphabetLength - 1,
    // and new query and target are created in which letters are replaced with their ordinal numbers.
    // This query and target are used in all the calculations later.
    *queryTransformed = static_cast<unsigned char *>(malloc(sizeof(unsigned char) * queryLength));
    *targetTransformed = static_cast<unsigned char *>(malloc(sizeof(unsigned char) * targetLength));

    string alphabet = "";

    // Alphabet information, it is constructed on fly while transforming sequences.
    // letterIdx[c] is index of letter c in alphabet.
    unsigned char letterIdx[MAX_UCHAR + 1];
    bool inAlphabet[MAX_UCHAR + 1]; // inAlphabet[c] is true if c is in alphabet
    for (int i = 0; i < MAX_UCHAR + 1; i++) inAlphabet[i] = false;

    for (int i = 0; i < queryLength; i++) {
        unsigned char c = static_cast<unsigned char>(queryOriginal[i]);
        if (!inAlphabet[c]) {
            inAlphabet[c] = true;
            letterIdx[c] = static_cast<unsigned char>(alphabet.size());
            alphabet += queryOriginal[i];
        }
        (*queryTransformed)[i] = letterIdx[c];
    }
    for (int i = 0; i < targetLength; i++) {
        unsigned char c = static_cast<unsigned char>(targetOriginal[i]);
        if (!inAlphabet[c]) {
            inAlphabet[c] = true;
            letterIdx[c] = static_cast<unsigned char>(alphabet.size());
            alphabet += targetOriginal[i];
        }
        (*targetTransformed)[i] = letterIdx[c];
    }

    return alphabet;
}


extern "C" EdlibAlignConfig edlibNewAlignConfig(int k, EdlibAlignMode mode, EdlibAlignTask task,
                                                const EdlibEqualityPair* additionalEqualities,
                                                int additionalEqualitiesLength) {
    EdlibAlignConfig config;
    config.k = k;
    config.mode = mode;
    config.task = task;
    config.additionalEqualities = additionalEqualities;
    config.additionalEqualitiesLength = additionalEqualitiesLength;
    return config;
}

extern "C" EdlibAlignConfig edlibDefaultAlignConfig(void) {
    return edlibNewAlignConfig(-1, EDLIB_MODE_NW, EDLIB_TASK_DISTANCE, NULL, 0);
}

extern "C" void edlibFreeAlignResult(EdlibAlignResult result) {
    if (result.endLocations) free(result.endLocations);
    if (result.startLocations) free(result.startLocations);
    if (result.alignment) free(result.alignment);
}
