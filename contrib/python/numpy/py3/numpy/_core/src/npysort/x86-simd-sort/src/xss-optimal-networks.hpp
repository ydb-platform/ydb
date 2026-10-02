// All of these sources files are generated from the optimal networks described in
// https://bertdobbelaere.github.io/sorting_networks.html

template <typename vtype,
          typename comparator,
          typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_FINLINE void optimal_sort_4(reg_t *vecs)
{
    comparator::COEX(vecs[0], vecs[2]);
    comparator::COEX(vecs[1], vecs[3]);

    comparator::COEX(vecs[0], vecs[1]);
    comparator::COEX(vecs[2], vecs[3]);

    comparator::COEX(vecs[1], vecs[2]);
}

template <typename vtype,
          typename comparator,
          typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_FINLINE void optimal_sort_8(reg_t *vecs)
{
    comparator::COEX(vecs[0], vecs[2]);
    comparator::COEX(vecs[1], vecs[3]);
    comparator::COEX(vecs[4], vecs[6]);
    comparator::COEX(vecs[5], vecs[7]);

    comparator::COEX(vecs[0], vecs[4]);
    comparator::COEX(vecs[1], vecs[5]);
    comparator::COEX(vecs[2], vecs[6]);
    comparator::COEX(vecs[3], vecs[7]);

    comparator::COEX(vecs[0], vecs[1]);
    comparator::COEX(vecs[2], vecs[3]);
    comparator::COEX(vecs[4], vecs[5]);
    comparator::COEX(vecs[6], vecs[7]);

    comparator::COEX(vecs[2], vecs[4]);
    comparator::COEX(vecs[3], vecs[5]);

    comparator::COEX(vecs[1], vecs[4]);
    comparator::COEX(vecs[3], vecs[6]);

    comparator::COEX(vecs[1], vecs[2]);
    comparator::COEX(vecs[3], vecs[4]);
    comparator::COEX(vecs[5], vecs[6]);
}

template <typename vtype,
          typename comparator,
          typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_FINLINE void optimal_sort_16(reg_t *vecs)
{
    comparator::COEX(vecs[0], vecs[13]);
    comparator::COEX(vecs[1], vecs[12]);
    comparator::COEX(vecs[2], vecs[15]);
    comparator::COEX(vecs[3], vecs[14]);
    comparator::COEX(vecs[4], vecs[8]);
    comparator::COEX(vecs[5], vecs[6]);
    comparator::COEX(vecs[7], vecs[11]);
    comparator::COEX(vecs[9], vecs[10]);

    comparator::COEX(vecs[0], vecs[5]);
    comparator::COEX(vecs[1], vecs[7]);
    comparator::COEX(vecs[2], vecs[9]);
    comparator::COEX(vecs[3], vecs[4]);
    comparator::COEX(vecs[6], vecs[13]);
    comparator::COEX(vecs[8], vecs[14]);
    comparator::COEX(vecs[10], vecs[15]);
    comparator::COEX(vecs[11], vecs[12]);

    comparator::COEX(vecs[0], vecs[1]);
    comparator::COEX(vecs[2], vecs[3]);
    comparator::COEX(vecs[4], vecs[5]);
    comparator::COEX(vecs[6], vecs[8]);
    comparator::COEX(vecs[7], vecs[9]);
    comparator::COEX(vecs[10], vecs[11]);
    comparator::COEX(vecs[12], vecs[13]);
    comparator::COEX(vecs[14], vecs[15]);

    comparator::COEX(vecs[0], vecs[2]);
    comparator::COEX(vecs[1], vecs[3]);
    comparator::COEX(vecs[4], vecs[10]);
    comparator::COEX(vecs[5], vecs[11]);
    comparator::COEX(vecs[6], vecs[7]);
    comparator::COEX(vecs[8], vecs[9]);
    comparator::COEX(vecs[12], vecs[14]);
    comparator::COEX(vecs[13], vecs[15]);

    comparator::COEX(vecs[1], vecs[2]);
    comparator::COEX(vecs[3], vecs[12]);
    comparator::COEX(vecs[4], vecs[6]);
    comparator::COEX(vecs[5], vecs[7]);
    comparator::COEX(vecs[8], vecs[10]);
    comparator::COEX(vecs[9], vecs[11]);
    comparator::COEX(vecs[13], vecs[14]);

    comparator::COEX(vecs[1], vecs[4]);
    comparator::COEX(vecs[2], vecs[6]);
    comparator::COEX(vecs[5], vecs[8]);
    comparator::COEX(vecs[7], vecs[10]);
    comparator::COEX(vecs[9], vecs[13]);
    comparator::COEX(vecs[11], vecs[14]);

    comparator::COEX(vecs[2], vecs[4]);
    comparator::COEX(vecs[3], vecs[6]);
    comparator::COEX(vecs[9], vecs[12]);
    comparator::COEX(vecs[11], vecs[13]);

    comparator::COEX(vecs[3], vecs[5]);
    comparator::COEX(vecs[6], vecs[8]);
    comparator::COEX(vecs[7], vecs[9]);
    comparator::COEX(vecs[10], vecs[12]);

    comparator::COEX(vecs[3], vecs[4]);
    comparator::COEX(vecs[5], vecs[6]);
    comparator::COEX(vecs[7], vecs[8]);
    comparator::COEX(vecs[9], vecs[10]);
    comparator::COEX(vecs[11], vecs[12]);

    comparator::COEX(vecs[6], vecs[7]);
    comparator::COEX(vecs[8], vecs[9]);
}

template <typename vtype,
          typename comparator,
          typename reg_t = typename vtype::reg_t>
X86_SIMD_SORT_FINLINE void optimal_sort_32(reg_t *vecs)
{
    comparator::COEX(vecs[0], vecs[1]);
    comparator::COEX(vecs[2], vecs[3]);
    comparator::COEX(vecs[4], vecs[5]);
    comparator::COEX(vecs[6], vecs[7]);
    comparator::COEX(vecs[8], vecs[9]);
    comparator::COEX(vecs[10], vecs[11]);
    comparator::COEX(vecs[12], vecs[13]);
    comparator::COEX(vecs[14], vecs[15]);
    comparator::COEX(vecs[16], vecs[17]);
    comparator::COEX(vecs[18], vecs[19]);
    comparator::COEX(vecs[20], vecs[21]);
    comparator::COEX(vecs[22], vecs[23]);
    comparator::COEX(vecs[24], vecs[25]);
    comparator::COEX(vecs[26], vecs[27]);
    comparator::COEX(vecs[28], vecs[29]);
    comparator::COEX(vecs[30], vecs[31]);

    comparator::COEX(vecs[0], vecs[2]);
    comparator::COEX(vecs[1], vecs[3]);
    comparator::COEX(vecs[4], vecs[6]);
    comparator::COEX(vecs[5], vecs[7]);
    comparator::COEX(vecs[8], vecs[10]);
    comparator::COEX(vecs[9], vecs[11]);
    comparator::COEX(vecs[12], vecs[14]);
    comparator::COEX(vecs[13], vecs[15]);
    comparator::COEX(vecs[16], vecs[18]);
    comparator::COEX(vecs[17], vecs[19]);
    comparator::COEX(vecs[20], vecs[22]);
    comparator::COEX(vecs[21], vecs[23]);
    comparator::COEX(vecs[24], vecs[26]);
    comparator::COEX(vecs[25], vecs[27]);
    comparator::COEX(vecs[28], vecs[30]);
    comparator::COEX(vecs[29], vecs[31]);

    comparator::COEX(vecs[0], vecs[4]);
    comparator::COEX(vecs[1], vecs[5]);
    comparator::COEX(vecs[2], vecs[6]);
    comparator::COEX(vecs[3], vecs[7]);
    comparator::COEX(vecs[8], vecs[12]);
    comparator::COEX(vecs[9], vecs[13]);
    comparator::COEX(vecs[10], vecs[14]);
    comparator::COEX(vecs[11], vecs[15]);
    comparator::COEX(vecs[16], vecs[20]);
    comparator::COEX(vecs[17], vecs[21]);
    comparator::COEX(vecs[18], vecs[22]);
    comparator::COEX(vecs[19], vecs[23]);
    comparator::COEX(vecs[24], vecs[28]);
    comparator::COEX(vecs[25], vecs[29]);
    comparator::COEX(vecs[26], vecs[30]);
    comparator::COEX(vecs[27], vecs[31]);

    comparator::COEX(vecs[0], vecs[8]);
    comparator::COEX(vecs[1], vecs[9]);
    comparator::COEX(vecs[2], vecs[10]);
    comparator::COEX(vecs[3], vecs[11]);
    comparator::COEX(vecs[4], vecs[12]);
    comparator::COEX(vecs[5], vecs[13]);
    comparator::COEX(vecs[6], vecs[14]);
    comparator::COEX(vecs[7], vecs[15]);
    comparator::COEX(vecs[16], vecs[24]);
    comparator::COEX(vecs[17], vecs[25]);
    comparator::COEX(vecs[18], vecs[26]);
    comparator::COEX(vecs[19], vecs[27]);
    comparator::COEX(vecs[20], vecs[28]);
    comparator::COEX(vecs[21], vecs[29]);
    comparator::COEX(vecs[22], vecs[30]);
    comparator::COEX(vecs[23], vecs[31]);

    comparator::COEX(vecs[0], vecs[16]);
    comparator::COEX(vecs[1], vecs[8]);
    comparator::COEX(vecs[2], vecs[4]);
    comparator::COEX(vecs[3], vecs[12]);
    comparator::COEX(vecs[5], vecs[10]);
    comparator::COEX(vecs[6], vecs[9]);
    comparator::COEX(vecs[7], vecs[14]);
    comparator::COEX(vecs[11], vecs[13]);
    comparator::COEX(vecs[15], vecs[31]);
    comparator::COEX(vecs[17], vecs[24]);
    comparator::COEX(vecs[18], vecs[20]);
    comparator::COEX(vecs[19], vecs[28]);
    comparator::COEX(vecs[21], vecs[26]);
    comparator::COEX(vecs[22], vecs[25]);
    comparator::COEX(vecs[23], vecs[30]);
    comparator::COEX(vecs[27], vecs[29]);

    comparator::COEX(vecs[1], vecs[2]);
    comparator::COEX(vecs[3], vecs[5]);
    comparator::COEX(vecs[4], vecs[8]);
    comparator::COEX(vecs[6], vecs[22]);
    comparator::COEX(vecs[7], vecs[11]);
    comparator::COEX(vecs[9], vecs[25]);
    comparator::COEX(vecs[10], vecs[12]);
    comparator::COEX(vecs[13], vecs[14]);
    comparator::COEX(vecs[17], vecs[18]);
    comparator::COEX(vecs[19], vecs[21]);
    comparator::COEX(vecs[20], vecs[24]);
    comparator::COEX(vecs[23], vecs[27]);
    comparator::COEX(vecs[26], vecs[28]);
    comparator::COEX(vecs[29], vecs[30]);

    comparator::COEX(vecs[1], vecs[17]);
    comparator::COEX(vecs[2], vecs[18]);
    comparator::COEX(vecs[3], vecs[19]);
    comparator::COEX(vecs[4], vecs[20]);
    comparator::COEX(vecs[5], vecs[10]);
    comparator::COEX(vecs[7], vecs[23]);
    comparator::COEX(vecs[8], vecs[24]);
    comparator::COEX(vecs[11], vecs[27]);
    comparator::COEX(vecs[12], vecs[28]);
    comparator::COEX(vecs[13], vecs[29]);
    comparator::COEX(vecs[14], vecs[30]);
    comparator::COEX(vecs[21], vecs[26]);

    comparator::COEX(vecs[3], vecs[17]);
    comparator::COEX(vecs[4], vecs[16]);
    comparator::COEX(vecs[5], vecs[21]);
    comparator::COEX(vecs[6], vecs[18]);
    comparator::COEX(vecs[7], vecs[9]);
    comparator::COEX(vecs[8], vecs[20]);
    comparator::COEX(vecs[10], vecs[26]);
    comparator::COEX(vecs[11], vecs[23]);
    comparator::COEX(vecs[13], vecs[25]);
    comparator::COEX(vecs[14], vecs[28]);
    comparator::COEX(vecs[15], vecs[27]);
    comparator::COEX(vecs[22], vecs[24]);

    comparator::COEX(vecs[1], vecs[4]);
    comparator::COEX(vecs[3], vecs[8]);
    comparator::COEX(vecs[5], vecs[16]);
    comparator::COEX(vecs[7], vecs[17]);
    comparator::COEX(vecs[9], vecs[21]);
    comparator::COEX(vecs[10], vecs[22]);
    comparator::COEX(vecs[11], vecs[19]);
    comparator::COEX(vecs[12], vecs[20]);
    comparator::COEX(vecs[14], vecs[24]);
    comparator::COEX(vecs[15], vecs[26]);
    comparator::COEX(vecs[23], vecs[28]);
    comparator::COEX(vecs[27], vecs[30]);

    comparator::COEX(vecs[2], vecs[5]);
    comparator::COEX(vecs[7], vecs[8]);
    comparator::COEX(vecs[9], vecs[18]);
    comparator::COEX(vecs[11], vecs[17]);
    comparator::COEX(vecs[12], vecs[16]);
    comparator::COEX(vecs[13], vecs[22]);
    comparator::COEX(vecs[14], vecs[20]);
    comparator::COEX(vecs[15], vecs[19]);
    comparator::COEX(vecs[23], vecs[24]);
    comparator::COEX(vecs[26], vecs[29]);

    comparator::COEX(vecs[2], vecs[4]);
    comparator::COEX(vecs[6], vecs[12]);
    comparator::COEX(vecs[9], vecs[16]);
    comparator::COEX(vecs[10], vecs[11]);
    comparator::COEX(vecs[13], vecs[17]);
    comparator::COEX(vecs[14], vecs[18]);
    comparator::COEX(vecs[15], vecs[22]);
    comparator::COEX(vecs[19], vecs[25]);
    comparator::COEX(vecs[20], vecs[21]);
    comparator::COEX(vecs[27], vecs[29]);

    comparator::COEX(vecs[5], vecs[6]);
    comparator::COEX(vecs[8], vecs[12]);
    comparator::COEX(vecs[9], vecs[10]);
    comparator::COEX(vecs[11], vecs[13]);
    comparator::COEX(vecs[14], vecs[16]);
    comparator::COEX(vecs[15], vecs[17]);
    comparator::COEX(vecs[18], vecs[20]);
    comparator::COEX(vecs[19], vecs[23]);
    comparator::COEX(vecs[21], vecs[22]);
    comparator::COEX(vecs[25], vecs[26]);

    comparator::COEX(vecs[3], vecs[5]);
    comparator::COEX(vecs[6], vecs[7]);
    comparator::COEX(vecs[8], vecs[9]);
    comparator::COEX(vecs[10], vecs[12]);
    comparator::COEX(vecs[11], vecs[14]);
    comparator::COEX(vecs[13], vecs[16]);
    comparator::COEX(vecs[15], vecs[18]);
    comparator::COEX(vecs[17], vecs[20]);
    comparator::COEX(vecs[19], vecs[21]);
    comparator::COEX(vecs[22], vecs[23]);
    comparator::COEX(vecs[24], vecs[25]);
    comparator::COEX(vecs[26], vecs[28]);

    comparator::COEX(vecs[3], vecs[4]);
    comparator::COEX(vecs[5], vecs[6]);
    comparator::COEX(vecs[7], vecs[8]);
    comparator::COEX(vecs[9], vecs[10]);
    comparator::COEX(vecs[11], vecs[12]);
    comparator::COEX(vecs[13], vecs[14]);
    comparator::COEX(vecs[15], vecs[16]);
    comparator::COEX(vecs[17], vecs[18]);
    comparator::COEX(vecs[19], vecs[20]);
    comparator::COEX(vecs[21], vecs[22]);
    comparator::COEX(vecs[23], vecs[24]);
    comparator::COEX(vecs[25], vecs[26]);
    comparator::COEX(vecs[27], vecs[28]);
}
