LIBRARY()

# Non-Metric Space Library (nmslib), similarity_search core.
# Imported from https://github.com/nmslib/nmslib/tree/master/similarity_search
#
# The extra SQFD space (src/space/space_sqfd.cc) is guarded by WITH_EXTRAS in
# the upstream CMake build and requires Eigen3. It is excluded here, matching
# the default (WITH_EXTRAS=OFF) upstream configuration.

NO_COMPILER_WARNINGS()

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    src/distcomp_bregman.cc
    src/distcomp_diverg.cc
    src/distcomp_edist.cc
    src/distcomp_js.cc
    src/distcomp_l2sqr_sift.cc
    src/distcomp_lp.cc
    src/distcomp_overlap.cc
    src/distcomp_rankcorr.cc
    src/distcomp_scalar.cc
    src/distcomp_sparse_scalar_fast.cc
    src/experimentconf.cc
    src/global.cc
    src/init.cc
    src/knnquery.cc
    src/logging.cc
    src/memory.cc
    src/params.cc
    src/params_cmdline.cc
    src/query.cc
    src/rangequery.cc
    src/searchoracle.cc
    src/space.cc
    src/utils.cc
    src/space/space_ab_diverg.cc
    src/space/space_bregman.cc
    src/space/space_dummy.cc
    src/space/space_js.cc
    src/space/space_l2sqr_sift.cc
    src/space/space_lp.cc
    src/space/space_renyi_diverg.cc
    src/space/space_scalar.cc
    src/space/space_sparse_dense_fusion.cc
    src/space/space_sparse_jaccard.cc
    src/space/space_sparse_lp.cc
    src/space/space_sparse_scalar_bin_fast.cc
    src/space/space_sparse_scalar_fast.cc
    src/space/space_sparse_vector.cc
    src/space/space_sparse_vector_inter.cc
    src/space/space_string.cc
    src/space/space_vector.cc
    src/space/space_word_embed.cc
    src/method/dummy.cc
    src/method/hnsw.cc
    src/method/hnsw_distfunc_opt.cc
    src/method/pivot_neighb_invindx.cc
    src/method/seqsearch.cc
    src/method/simple_inverted_index.cc
    src/method/small_world_rand.cc
    src/method/vptree.cc
)

END()

RECURSE(
    apps
    test
)
