Release Notes       {#RELEASE_NOTES}
=============

\brief Release notes file for the netcdf-c package.

This file contains a high-level description of this package's evolution. Releases are in reverse chronological order (most recent first). Note that, as of netcdf 4.2, the `netcdf-c++` and `netcdf-fortran` libraries have been separated into their own libraries.

## 4.10.0 - February 25, 2026

* Regularize, cleanup, and refactor various AWS features, especially WRT regularizing AWS-related constants. See [Github 3229](https://github.com/Unidata/netcdf-c/issues/3229) for more information. 
* Add extra failure handling to the daos inferencing. See [Github 3238](https://github.com/Unidata/netcdf-c/issues/3238) for more information. 
* Regularize, cleanup, and refactor various AWS features, especially WRT regularizing AWS-related constants. See [Github 3229](https://github.com/Unidata/netcdf-c/issues/3229) for more information. 
* Add compatibility with HDF5 2.0.0.  See [Github #3247](https://github.com/Unidata/netcdf-c/pull/3247) for more information.
* Introduce consolidated metadata [Github #3225](https://github.com/Unidata/netcdf-c/pull/3225) via `mode=consolidated` or `NCZARR_CONSOLIDATED`
* Fix the H5FD_class_t problems. See [Github 3202](https://github.com/Unidata/netcdf-c/issues/3202) for more information. 
* Begin the consolidation of global state into two files: libdispatch/dglobal.c and include/ncglobal.h. See [Github 3197](https://github.com/Unidata/netcdf-c/issues/3197) for more information. 
* Modify the way xarray attribute sets are handled. See [Github 3218](https://github.com/Unidata/netcdf-c/issues/3218) for more information. 
* Fix Issue with Numcodecs encoding problems where integer filter parameters are being encoded as strings. See [Github 3201](https://github.com/Unidata/netcdf-c/issues/3201) for more information. 
* Clean up minor problems with DAP2/DAP4 code. See [Github #3215](https://github.com/Unidata/netcdf-c/pull/3215) for more information.
* Cleanup RELEASE_NOTES.md. See [Github 3190](https://github.com/Unidata/netcdf-c/issues/3190) for more information. 
* Rebuild the S3-related code and other changes necessary to build cleanly on github actions. See [Github #3159](https://github.com/Unidata/netcdf-c/pull/3159) for more information.
* Fix the problems around ncdap_test/test_manyurls.c. See [Github 3182](https://github.com/Unidata/netcdf-c/issues/3182) for more information. 
* Fix bug in ncdump when printing FQNs ([Issue 3184](https://github.com/Unidata/netcdf-c/issues/3184)). See [Github 3185](https://github.com/Unidata/netcdf-c/issues/3185) for more information. 
* Update `macOS` github runners from macos-13 to macos-14, due to deprecation. 
* Fix an error compiling netCDF with AWS-S3-SDK support using cmake. See [Github 3155](https://github.com/Unidata/netcdf-c/issues/3155) for more information. 
* Add new environmental logging variable for `netCDF4` related logging subsystem, `NC4LOGGING`.  If `libnetcdf` is compiled with logging enabled, logs can be enabled at runtime by setting this environmental variable to the desired log level.  
* Update ncjson.[ch] and ncproplist.[ch]. Also fix references to old API. Also fix include/netcdf_ncjson.h and include/netcdf_proplist.h builds. See [Github #3086](https://github.com/Unidata/netcdf-c/pull/3086) for more information.
* Refactor drc.c to move many of its purely utility functions into dutil.c. Also change the NC_mktmp signature. Change other files to match. See [Github #3094](https://github.com/Unidata/netcdf-c/pull/3094) for more information.
* Provide an auxiliary function, `ncaux_parse_provenance()`, that allows users to parse the _NCProperties attribute into a collection of character pointers. See [Github #3088](https://github.com/Unidata/netcdf-c/pull/3088) for more information.
* Fix a namespace problem in tinyxml2.cpp. Note that this is a visual studio problem hence use of _MSC_VER. Also turn off DAP4 tests against Hyrax server until DAP4 spec problems are fixed. See [Github #3089](https://github.com/Unidata/netcdf-c/pull/3089) for more information.

## 4.9.3 - February 7, 2025

## Known Issues

> Parallel operation using `mpich 4.2.0` (the default on `Ubuntu 24.04`) results in 'unexpected results' when running `nc_test4/run_par_test.sh`.  This can be fixed by removing `mpich` and associated libraries and development packages and installing `mpich 4.2.2` by hand, or by using `openmpi` provided via `apt`.

## Release Notes

* Extend the netcdf API to support programmatic changes to the plugin search path. See [Github #3034](https://github.com/Unidata/netcdf-c/pull/3034) for more information.

## What's Changed
* "Simplify" XGetopt usage by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2678
* Fix bug in szip handling. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2679
* Add documentation for logging by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2677
* v4.9.2 Wellspring branch by \@WardF in https://github.com/Unidata/netcdf-c/pull/2660
* Combine DAP4 test server fixes, resolve a couple conflicts.  by \@WardF in https://github.com/Unidata/netcdf-c/pull/2681
* Cleanup DAP4 testing by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2555
* Fix DAP4 remotetest server by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2558
* Fix issue #2674 by \@uweschulzweida in https://github.com/Unidata/netcdf-c/pull/2675
* Check at nc_open if file appears to be in NCZarr/Zarr format. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2658
* Fix a syntax issue in CMakeLists.txt by \@WardF in https://github.com/Unidata/netcdf-c/pull/2693
* hdf5open: check for the H5L info structure version directly by \@mathstuf in https://github.com/Unidata/netcdf-c/pull/2695
* Improve S3 documentation, testing and support by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2686
* Fix potential dead store by \@ZhipengXue97 in https://github.com/Unidata/netcdf-c/pull/2644
* CI: Test --without-plugin-dir on Cygwin by \@DWesl in https://github.com/Unidata/netcdf-c/pull/2659
* Fix handling of CURLOPT_CAINFO and CURLOPT_CAPATH by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2690
* Adding a workaround for older versions of cmake by \@WardF in https://github.com/Unidata/netcdf-c/pull/2703
* Remove obsolete code by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2680
* Improve performance of the nc_reclaim_data and nc_copy_data functions. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2699
* CMakeLists.txt use ON vs yes, indent by \@poelmanc in https://github.com/Unidata/netcdf-c/pull/2663
* Fix some dependency conditions between some ncdump tests. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2682
* awsincludes: remove executable permissions by \@mathstuf in https://github.com/Unidata/netcdf-c/pull/2689
* Fix some problems with Earthdata authorization. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2709
* Provide a single option to disable all network access and testing. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2708
* Fix building on macOS by \@skosukhin in https://github.com/Unidata/netcdf-c/pull/2710
* Update tinyxml and allow its use under OS/X. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2711
* Suppress filters on variables with non-fixed-size types. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2716
* Remove fortran bootstrap option by \@WardF in https://github.com/Unidata/netcdf-c/pull/2707
* Add support for HDF5 transient types by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2655
* Modify PR 2655 to ensure transient types have names. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2724
* Fix memory leak by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2725
* Enable/Disable some plugins at configure time by \@WardF in https://github.com/Unidata/netcdf-c/pull/2722
* Add capability to enable/disable compression libraries by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2712
* Release notes:  Minor.  Add historical tag, and spell fix. by \@Dave-Allured in https://github.com/Unidata/netcdf-c/pull/2684
* Fix potential null dereference by \@ZhipengXue97 in https://github.com/Unidata/netcdf-c/pull/2646
* Fix a crash when accessing a corrupted classic file. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2732
* Explicitly suppress variable length type compression by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2730
* Cleanup the handling of cache parameters. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2734
* Fix a number of minor bugs by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2726
* Fix major bug in the NCZarr cache management by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2737
* Fix --has-quantize in autotools-generated nc-config. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2749
* Fix an issue with unescaped paths in the build system by \@weiznich in https://github.com/Unidata/netcdf-c/pull/2756
* Mitigate S3 test interference + Unlimited Dimensions in NCZarr by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2755
* Fix cmake s3 support.wif by \@WardF in https://github.com/Unidata/netcdf-c/pull/2741
* CMake: Ensure all libraries link against MPI if needed by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2758
* CMake: Change header in check for HDF5 zlib/szip support by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2757
* Updated static software.html page with reference to met.3d by \@WardF in https://github.com/Unidata/netcdf-c/pull/2760
* Revert a change made in d3c2cf236 that is proving confounding in MSYS2 bash by \@WardF in https://github.com/Unidata/netcdf-c/pull/2769
* Address Windows and MacOS s3 issues by \@WardF in https://github.com/Unidata/netcdf-c/pull/2759
* Fix bug with displaying log messages by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2767
* Add ctest script to CI.  by \@WardF in https://github.com/Unidata/netcdf-c/pull/2778
* CI: Have nc-autotools use source distribution instead of repository by \@DWesl in https://github.com/Unidata/netcdf-c/pull/2601
* Added stanza to workflow actions so that pushed changes cancel tests by \@WardF in https://github.com/Unidata/netcdf-c/pull/2779
* netCDFConfig: find HDF5 if needed by \@mathstuf in https://github.com/Unidata/netcdf-c/pull/2751
* Cleanup a number of issues. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2763
* CMake: Don't add uninstall target and CPack config if not top-level by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2776
* Replace exec_program with execute_process by \@WardF in https://github.com/Unidata/netcdf-c/pull/2784
* Fix Proxy problem for DAP2 by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2764
* Remove stray character in cmake lfs tests for nczarr. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2786
* Renamed mmap variable, which conflicts with mmap() function on FreeBSD by \@seanm in https://github.com/Unidata/netcdf-c/pull/2790
* Make ncZarr-specific deps and options dependent on ncZarr being enabled. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2793
* Fix most float conversion warnings by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2782
* Fixed various UBSan warnings about invalid bit shifting by \@seanm in https://github.com/Unidata/netcdf-c/pull/2787
* disable test that depends on ncpathcvt in cmake build w/o utilities by \@tbussmann in https://github.com/Unidata/netcdf-c/pull/2795
* Update internal tinyxml2 code to the latest version by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2771
* Remove the execinfo capability by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2789
* Fixed various UBSan warnings about working with NULL pointers by \@seanm in https://github.com/Unidata/netcdf-c/pull/2803
* Improve fetch performance of DAP4 by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2765
* Fixed misaligned memory access flagged by UBSan by \@seanm in https://github.com/Unidata/netcdf-c/pull/2800
* Tweaking PR to work with Visual Studio by \@WardF in https://github.com/Unidata/netcdf-c/pull/2788
* CMake: Use helper libraries for nczarr tests by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2783
* Fixed various UBSan warnings about working with NULL pointers by \@seanm in https://github.com/Unidata/netcdf-c/pull/2802
* Fix some important bugs in various files by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2813
* Removed a use of sprintf that required changing a function signature by \@seanm in https://github.com/Unidata/netcdf-c/pull/2743
* sprintf -> snprintf by \@seanm in https://github.com/Unidata/netcdf-c/pull/2691
* chore: unset executable flag by \@e-kwsm in https://github.com/Unidata/netcdf-c/pull/2745
* Fix nc-config generated by cmake. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2825
* Do not compile test program unless required by \@magnusuMET in https://github.com/Unidata/netcdf-c/pull/2761
* CMake: Add improvements to MPI support by \@johnwparent in https://github.com/Unidata/netcdf-c/pull/2595
* Catching up on PRs by \@WardF in https://github.com/Unidata/netcdf-c/pull/2826
* Minor fix to doxygen documentation by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2450
* Enable compilation with C89 compiler by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2379
* count argument in H5Sselect_hyperslab by \@wkliao in https://github.com/Unidata/netcdf-c/pull/2296
* Changed link to netCDF-Fortran documentation. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2830
* Avoid segfault if opening file failed by \@rouault in https://github.com/Unidata/netcdf-c/pull/2427
* Add clarification for the meaning of NSB by \@rkouznetsov in https://github.com/Unidata/netcdf-c/pull/2388
* Add H5FD_http_finalize function and call on hdf5 finalize by \@lostbard in https://github.com/Unidata/netcdf-c/pull/2827
* Reduce warning by changing type of NC_OBJ.id. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2781
* Replaced ancient K&R function declarations to be C23 compatible by \@seanm in https://github.com/Unidata/netcdf-c/pull/2801
* add new compression to bm_file benchmark by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2821
* Fix doxygen warnings by \@WardF in https://github.com/Unidata/netcdf-c/pull/2834
* Fix szip linking by \@mwestphal in https://github.com/Unidata/netcdf-c/pull/2833
* Silence conversion warnings from `malloc` arguments by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2809
* Use explicit casts in `nc4_convert_type` to silence warnings by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2808
* Manage project version with cmake by \@K20shores in https://github.com/Unidata/netcdf-c/pull/2835
* Define USE_SZIP variable for nc-config.cmake.in by \@islas in https://github.com/Unidata/netcdf-c/pull/2836
* Place dependencies into separate file by \@K20shores in https://github.com/Unidata/netcdf-c/pull/2838
* Macros functions by \@K20shores in https://github.com/Unidata/netcdf-c/pull/2842
* CMake: Find HDF5 header we can safely include for other checks by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2762
* Rebase #2812 by \@WardF in https://github.com/Unidata/netcdf-c/pull/2844
* Silence sign conversion warnings from `NClist` functions by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2812
* CMake: Add support for UNITY_BUILD by \@jschueller in https://github.com/Unidata/netcdf-c/pull/2839
* Fix warnings in NCZarr tests by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2816
* Link against internally defined libraries by \@islas in https://github.com/Unidata/netcdf-c/pull/2837
* Fix some variable types.  Resolves #2849 by \@opoplawski in https://github.com/Unidata/netcdf-c/pull/2850
* Add citation.cff file by \@WardF in https://github.com/Unidata/netcdf-c/pull/2853
* Minor -- fix UNset to unset by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2856
* Update bundled utf8proc to 2.9.0 by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2857
* CMake: Export targets so the build directory can be used directly by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2774
* Modernize Doxygen CSS by \@WardF in https://github.com/Unidata/netcdf-c/pull/2860
* Fix Windows export by \@WardF in https://github.com/Unidata/netcdf-c/pull/2861
* Silence ncdump warnings by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2840
* Adopt more modern style for doxygen-generated documentation. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2864
* Add static build to one-off GitHub Actions testing. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2866
* Fixes finding HDF5 header by \@Julius-Plehn in https://github.com/Unidata/netcdf-c/pull/2867
* Properly handle missing regions in URLS by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2819
* fix cmake build with ENABLE_HDF4 and hdf requiring jpeg by \@aumuell in https://github.com/Unidata/netcdf-c/pull/2879
* Revert "fix cmake build with ENABLE_HDF4 and hdf requiring jpeg" by \@WardF in https://github.com/Unidata/netcdf-c/pull/2882
* Use cmake netCDF with target_* for many options by \@K20shores in https://github.com/Unidata/netcdf-c/pull/2847
* Pull out `FindPNETCDF` CMake module by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2841
* CI: Fix version of HDF5 used in one-off test by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2885
* Fix for H5Literate() callback versioning by \@derobins in https://github.com/Unidata/netcdf-c/pull/2888
* Silence conversion warnings in libsrc4 by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2892
* Remove now unused cmake by \@K20shores in https://github.com/Unidata/netcdf-c/pull/2890
* Prefix all options with NETCDF_ by \@K20shores in https://github.com/Unidata/netcdf-c/pull/2895
* Fix most warnings in `dumplib.c` by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2814
* Silence most warnings in libhdf5 by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2874
* Silence warnings in `oc2` by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2891
* CMake: Add option to always automatically regenerate `ncgen` source by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2822
* Silence most warnings in `libsrc` by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2883
* Fix warnings in tests and examples by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2884
* Fix warnings from backwards-loops by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2899
* Modernize CURL in netCDF cmake by \@WardF in https://github.com/Unidata/netcdf-c/pull/2904
* Misc clang-tidy fixes, and added a .clang-tidy config file by \@seanm in https://github.com/Unidata/netcdf-c/pull/2875
* Rename the vendored strlcat symbol by \@weiznich in https://github.com/Unidata/netcdf-c/pull/2906
* Remove superfluous check for libcurl  by \@WardF in https://github.com/Unidata/netcdf-c/pull/2907
* Fix warnings in `ncgen3` by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2900
* Add CI for a Windows Runner on Github Actions. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2910
* Fix conversion warnings in libdispatch by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2905
* Refactor _FillValue macro by \@WardF in https://github.com/Unidata/netcdf-c/pull/2911
* Fix warnings in `ncgen` by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2897
* Re-introduce targets into netCDFConfig.cmake.in by \@WardF in https://github.com/Unidata/netcdf-c/pull/2912
* changes associated with the removal of the Unidata ftp site. by \@oxelson in https://github.com/Unidata/netcdf-c/pull/2915
* CMake: Enable plugins on MinGW by \@MehdiChinoune in https://github.com/Unidata/netcdf-c/pull/2914
* Modify ncdump to print char-valued variables as utf8. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2921
* Honor CMAKE_INSTALL_MANDIR by \@WardF in https://github.com/Unidata/netcdf-c/pull/2922
* Convert the ENABLE_XXX options to NETCDF_ENABLE_XXX options for NCZarr by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2923
* ftp --> resources, part II by \@oxelson in https://github.com/Unidata/netcdf-c/pull/2924
* CI: Setup a CMake job for MSYS2/MinGW by \@MehdiChinoune in https://github.com/Unidata/netcdf-c/pull/2917
* Fix all warnings in `ncdap4` by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2898
* Cleanup various obsolete build issues by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2926
* Fix duplicate definition when using aws-sdk-cpp. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2928
* Fix a few issues related to detection of libhdf4. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2933
* Cleanup handling of NETCDF_ENABLE_SET_LOG_LEVEL and NETCDF_ENABLE_SET_LOG_LEVEL_FUNC by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2931
* Add compiler flag to fix infinities issue with intel compilers. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2935
* Fix some warnings in cmake by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2940
* Clean up some inconsistencies in filter documentation by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2943
* Different method for checking HDF5 version requirement by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2942
* Fix ordering in CMakeLists.txt by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2941
* Fix cmake-based libnetcdf.settings.in by \@WardF in https://github.com/Unidata/netcdf-c/pull/2944
* CI: Add Cygwin CMake run by \@DWesl in https://github.com/Unidata/netcdf-c/pull/2930
* Convert NCzarr meta-data to use only Zarr attributes by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2936
* Fix: CMAKE_MODULE_PATH contents is being overridden with -D contents, not merged with by \@gorloffslava in https://github.com/Unidata/netcdf-c/pull/2946
* Propagate change to metadata and use of anonymous dimensions to NCZarr test by \@WardF in https://github.com/Unidata/netcdf-c/pull/2949
* S3 Mode url reconstruction defaults to wrong server type by \@mannreis in https://github.com/Unidata/netcdf-c/pull/2947
* Fix most warnings in libdap2 by \@ZedThree in https://github.com/Unidata/netcdf-c/pull/2887
* Check if HDF5 "file" is a DAOS object by \@brtnfld in https://github.com/Unidata/netcdf-c/pull/2021
* Add stanza for Release Candidate 1 in Release Notes by \@WardF in https://github.com/Unidata/netcdf-c/pull/2934
* Provide Documentation for the .rc File Mechanism and API by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2956
* Now use H5Literate2() instead of H5Literate() when its available by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2958
* Some debugging output was left enabled by accident. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2970
* fix ncuriparse error,Correctly remove leading and trailing whitespace by \@ShawayL in https://github.com/Unidata/netcdf-c/pull/2971
* add autotools build instructions, add parallel I/O build and plugin info to CMake build document by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2964
* Expand logic around H5Literate2 data structure use. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2977
* fixed --with-plugin-dir option to match cmake behavior by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2973
* updated README with install documentation by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2975
* added NETCDF_MPIEXEC option to CMake by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2979
* Fix bug in run_newformat.sh by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2978
* Take into account that attach_dimscales can fail when dimensions and variables are named inconsistently by \@Alexander-Barth in https://github.com/Unidata/netcdf-c/pull/2968
* Add zstd test and fix plugin build for CMake/Cygwin by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2985
* fixed message commands in CMakeLists.txt, and ncdump dependency problems for tst_nccopy4 in CMake build by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2983
* added documentation for start/count/stride mandating same size arrays as data variable by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2981
* [S3] Parse AWS configuration with support for profile section by \@mannreis in https://github.com/Unidata/netcdf-c/pull/2969
* Clean up some `-` vs `_` in some comments by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2988
* added documentation about reading an unknown netCDF/HDF5 file in tutorial, and some other documentation fixes by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2986
* Add two missing includes by \@weiznich in https://github.com/Unidata/netcdf-c/pull/2991
* turned on some commented out test code by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2993
* now install m4 on macos in CI by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2998
* Bump incorrect minimum HDF5 version to 1.8.15. by \@WardF in https://github.com/Unidata/netcdf-c/pull/3009
* fixed some autoreconf warnings by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/3008
* adding more zstd testing by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2996
* Bump minimum required version of hdf5 in CMakeLists.txt by \@WardF in https://github.com/Unidata/netcdf-c/pull/3011
* H5FDunregister plus tests by \@WardF in https://github.com/Unidata/netcdf-c/pull/3014
* test to catch the HDF5 H5FDUnregister() problem... by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/3012
* Address ordering issue with HTTP VFD, H5FDunregister by \@WardF in https://github.com/Unidata/netcdf-c/pull/3013
* parallel zstd test which works for cmake and autotools by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/3005
* HDF5 testing for parallel I/O including zstd (when present) by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/3002
* Check for libcurl should only happen if DAP and/or ncZarr are enabled. by \@WardF in https://github.com/Unidata/netcdf-c/pull/3018
* Added information re: mpich version 4.2.0 and related 'error' messages. by \@WardF in https://github.com/Unidata/netcdf-c/pull/3023
* Add legacy macro option by \@WardF in https://github.com/Unidata/netcdf-c/pull/3030
* Fix in support of https://github.com/Unidata/netcdf-c/issues/3007 by \@WardF in https://github.com/Unidata/netcdf-c/pull/3035
* If libZstd isn't found, turn off netcdf_enable_filter_zstd by \@WardF in https://github.com/Unidata/netcdf-c/pull/3036
* Modify nc-config --libs and --static arguments by \@WardF in https://github.com/Unidata/netcdf-c/pull/3037
* Fix failing building with custom libzip by \@mannreis in https://github.com/Unidata/netcdf-c/pull/3040
* Cleanup the blosc testing in nc_test4 and nczarr_test. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/3046
* Replace PR https://github.com/Unidata/netcdf-c/pull/3046 by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/3047
* fixing some autoconf problems by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/3022
* autoconf cleanup by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/3019
* Extend the netcdf API to support programmatic changes to the plugin search path by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/3034
* Update nc-config in support of changes made in #3034 by \@WardF in https://github.com/Unidata/netcdf-c/pull/3049
* Various clang warning fixes by \@seanm in https://github.com/Unidata/netcdf-c/pull/3050
* Quick warning fix plugin/CMakeLists.txt by \@mannreis in https://github.com/Unidata/netcdf-c/pull/3053
* Simplify FORTRAN access to the new plugin path mechanism by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/3058
* CMAKE: Address #3055 - install plugins filters by \@mannreis in https://github.com/Unidata/netcdf-c/pull/3056
* v4.9.3-rc2 wellspring changes by \@WardF in https://github.com/Unidata/netcdf-c/pull/3052
* Update upload-artifact/download-artifact for github actions by \@WardF in https://github.com/Unidata/netcdf-c/pull/3065
* Fix out-of-tree builds generating netcdf_json.h and netcdf_proplist.h by \@opoplawski in https://github.com/Unidata/netcdf-c/pull/3060
* Update error code list. by \@WardF in https://github.com/Unidata/netcdf-c/pull/3073
* Capture ac-based log artifacts by \@WardF in https://github.com/Unidata/netcdf-c/pull/3074
* Clean up a couple small things while I'm looking at them. by \@WardF in https://github.com/Unidata/netcdf-c/pull/3076
* Update default_chunk_cache_size  by \@WardF in https://github.com/Unidata/netcdf-c/pull/3077
* Restore missing --has-nc4 by \@WardF in https://github.com/Unidata/netcdf-c/pull/3082

## New Contributors
* \@uweschulzweida made their first contribution in https://github.com/Unidata/netcdf-c/pull/2675
* \@ZhipengXue97 made their first contribution in https://github.com/Unidata/netcdf-c/pull/2644
* \@poelmanc made their first contribution in https://github.com/Unidata/netcdf-c/pull/2663
* \@weiznich made their first contribution in https://github.com/Unidata/netcdf-c/pull/2756
* \@tbussmann made their first contribution in https://github.com/Unidata/netcdf-c/pull/2795
* \@e-kwsm made their first contribution in https://github.com/Unidata/netcdf-c/pull/2745
* \@johnwparent made their first contribution in https://github.com/Unidata/netcdf-c/pull/2595
* \@lostbard made their first contribution in https://github.com/Unidata/netcdf-c/pull/2827
* \@K20shores made their first contribution in https://github.com/Unidata/netcdf-c/pull/2835
* \@islas made their first contribution in https://github.com/Unidata/netcdf-c/pull/2836
* \@Julius-Plehn made their first contribution in https://github.com/Unidata/netcdf-c/pull/2867
* \@aumuell made their first contribution in https://github.com/Unidata/netcdf-c/pull/2879
* \@derobins made their first contribution in https://github.com/Unidata/netcdf-c/pull/2888
* \@MehdiChinoune made their first contribution in https://github.com/Unidata/netcdf-c/pull/2914
* \@gorloffslava made their first contribution in https://github.com/Unidata/netcdf-c/pull/2946
* \@mannreis made their first contribution in https://github.com/Unidata/netcdf-c/pull/2947
* \@ShawayL made their first contribution in https://github.com/Unidata/netcdf-c/pull/2971

**Full Changelog**: https://github.com/Unidata/netcdf-c/compare/v4.9.2...tmp-tag

### Release Candidate 2 - December 6, 2024

> Note: To avoid a conflict between `_FillValue` and `libc++18`, we have introduced a new option, `--enable-legacy-macros` for autotools and `NETCDF_ENABLE_LEGACY_MACROS` for cmake.  These are turned on by default currently but will be turned off eventually.  Developers are encouraged to move away from the `FillValue` macro and replace it with the new `NC_FillValue` macro.  See [Github #2858](https://github.com/Unidata/netcdf-c/issues/2858) for more information. 


* Add new `nc-config` flag, `--plugin-searchpath` to report a plugin search path which may be different from the plugin install directory. See [Github #3046](https://github.com/Unidata/netcdf-c/pull/3034) for more information. 
* Cleanup the blosc testing in nc_test4 and nczarr_test. See [Github #3046](https://github.com/Unidata/netcdf-c/pull/3046) for more information.
* Provide better documentation for the .rc file mechanism and API. See [Github #2956](https://github.com/Unidata/netcdf-c/pull/2956) for more information.
* Convert NCZarr V2 to store all netcdf-4 specific info as attributes. This improves interoperability with other Zarr implementations by no longer using non-standard keys. The price to be paid is that lazy attribute reading cannot be supported. See [Github #2836](https://github.com/Unidata/netcdf-c/pull/2936) for more information.
* Cleanup the option code for NETCDF_ENABLE_SET_LOG_LEVEL\[_FUNC\] See [Github #2931](https://github.com/Unidata/netcdf-c/pull/2931) for more information.

### Release Candidate 1 - July 26, 2024

* Convert NCZarr V2 to store all netcdf-4 specific info as attributes. This improves interoperability with other Zarr implementations by no longer using non-standard keys. The price to be paid is that lazy attribute reading cannot be supported. See [Github #2836](https://github.com/Unidata/netcdf-c/issues/2936) for more information.
* Cleanup the option code for NETCDF_ENABLE_SET_LOG_LEVEL\[_FUNC\] See [Github #2931](https://github.com/Unidata/netcdf-c/issues/2931) for more information.
* Fix duplicate definition when using aws-sdk-cpp. See [Github #2928](https://github.com/Unidata/netcdf-c/issues/2928) for more information.
* Cleanup various obsolete options and do some code refactoring. See [Github #2926](https://github.com/Unidata/netcdf-c/pull/2926) for more information.
* Convert the Zarr-related ENABLE_XXX options to NETCDF_ENABLE_XXX options (part of the cmake overhaul). See [Github #2923](https://github.com/Unidata/netcdf-c/pull/2923) for more information.
* Refactor macro `_FillValue` to `NC_FillValue` to avoid conflict with libc++ headers. See [Github #2858](https://github.com/Unidata/netcdf-c/issues/2858) for more information.
* Changed `cmake` build options to be prefaced with `NETCDF`, to bring things in to line with best practices.  This will permit a number of overall quality of life improvements to netCDF, in terms of allowing it to be more easily integrated with upstream projects via `FetchContent()`, `subdirectory()`, etc. Currently, the naming convention in use thus far will still work, but will result in warning messages about deprecation, and instructions on how to update your workflow. See [Github #2895](https://github.com/Unidata/netcdf-c/pull/2895) for more information.
* Incorporate a more modern look and feel to user documentation generated by Doxygen.  See [Doxygen Awesome CSS](https://github.com/jothepro/doxygen-awesome-css) and [Github #2864](https://github.com/Unidata/netcdf-c/pull/2864) for more information.
* Added infrastructure to allow for `CMAKE_UNITY_BUILD`, (thanks \@jschueller).  See [Github #2839](https://github.com/Unidata/netcdf-c/pull/2839) for more information.
* [cmake] Move dependency management out of the root-level `CMakeLists.txt` into two different files in the `cmake/` folder, `dependencies.cmake` and `netcdf_functions_macros.cmake`. See [Github #2838](https://github.com/Unidata/netcdf-c/pull/2838/) for more information.
* Fix some problems in handling S3 urls with missing regions. See [Github #2819](https://github.com/Unidata/netcdf-c/pull/2819).
* Obviate a number of irrelevant warnings. See [Github #2781](https://github.com/Unidata/netcdf-c/pull/2781).
* Improve the speed and data quantity for DAP4 queries. See [Github #2765](https://github.com/Unidata/netcdf-c/pull/2765).
* Remove the use of execinfo to programmatically dump the stack; it never worked. See [Github #2789](https://github.com/Unidata/netcdf-c/pull/2789).
* Update the internal copy of tinyxml2 to latest code. See [Github #2771](https://github.com/Unidata/netcdf-c/pull/2771).
* Mitigate the problem of remote/nczarr-related test interference. See [Github #2755](https://github.com/Unidata/netcdf-c/pull/2755).
* Fix DAP2 proxy problems. See [Github #2764](https://github.com/Unidata/netcdf-c/pull/2764).
* Cleanup a number of misc issues. See [Github #2763](https://github.com/Unidata/netcdf-c/pull/2763).
* Mitigate the problem of test interference. See [Github #2755](https://github.com/Unidata/netcdf-c/pull/2755).
* Extend NCZarr to support unlimited dimensions. See [Github #2755](https://github.com/Unidata/netcdf-c/pull/2755).
* Fix significant bug in the NCZarr cache management. See [Github #2737](https://github.com/Unidata/netcdf-c/pull/2737).
* Fix default parameters for caching of NCZarr. See [Github #2734](https://github.com/Unidata/netcdf-c/pull/2734).
* Introducing configure-time options to disable various filters, even if the required libraries are available on the system, in support of [GitHub #2712](https://github.com/Unidata/netcdf-c/pull/2712). 
* Fix memory leak WRT unreclaimed HDF5 plist. See [Github #2752](https://github.com/Unidata/netcdf-c/pull/2752).
* Support HDF5 transient types when reading an HDF5 file.  See [Github #2724](https://github.com/Unidata/netcdf-c/pull/2724).
* Suppress filters on variables with non-fixed-size types. See [Github #2716](https://github.com/Unidata/netcdf-c/pull/2716).
* Provide a single option to disable all network access and testing. See [Github #2708](https://github.com/Unidata/netcdf-c/pull/2708).
* Fix some problems with earthdata authorization and data access. See [Github #2709](https://github.com/Unidata/netcdf-c/pull/2709).
* Fix a race condition in some ncdump tests. See [Github #2682](https://github.com/Unidata/netcdf-c/pull/2682).
* Fix a minor bug in reporting the use of szip. See [Github #2679](https://github.com/Unidata/netcdf-c/pull/2679).
* Simplify the handling of XGetopt. See [Github #2678](https://github.com/Unidata/netcdf-c/pull/2678).
* Improve performance and testing of the new nc_reclaim/copy functions. See [Github #2699](https://github.com/Unidata/netcdf-c/pull/2699).
* [Bug Fix] Provide a partial fix to the libcurl certificates problem. See [Github #2690](https://github.com/Unidata/netcdf-c/pull/2690).
* Improve S3 documentation, testing, and support See [Github #2686](https://github.com/Unidata/netcdf-c/pull/2686).
* Remove obsolete code. See [Github #2680](https://github.com/Unidata/netcdf-c/pull/2680).
* [Bug Fix] Add a crude test to see if an NCZarr path looks like a valid NCZarr/Zarr file. See [Github #2658](https://github.com/Unidata/netcdf-c/pull/2658).
* Fix 'make distcheck' error in run_interop.sh. See [Github #2631](https://github.com/Unidata/netcdf-c/pull/2631).
* Bump cmake minimum version to 3.20.0 to support finding HDF5 with cmake's find_package
* Refactor the cmake files to remove global include directories and compile definitions

## 4.9.2 - March 14, 2023

This is the maintenance release which adds support for HDF5 version 1.14.0, in addition to a handful of other changes and bugfixes.  

* Update `nc-config` to remove inclusion from automatically-detected `nf-config` and `ncxx-config` files, as the wrong files could be included in the output.  This is in support of [GitHub #2274](https://github.com/Unidata/netcdf-c/issues/2274).
* Update H5FDhttp.[ch] to work with HDF5 version 1.13.2 and later. See [Github #2635](https://github.com/Unidata/netcdf-c/pull/2635).
* [Bug Fix] Update DAP code to enable CURLOPT_ACCEPT_ENCODING by default. See [Github #2630](https://github.com/Unidata/netcdf-c/pull/2630).
* [Bug Fix] Fix byterange failures for certain URLs. See [Github #2649](https://github.com/Unidata/netcdf-c/pull/2649).
* [Bug Fix] Fix 'make distcheck' error in run_interop.sh. See [Github #2631](https://github.com/Unidata/netcdf-c/pull/2631).
* [Enhancement] Update `nc-config` to remove inclusion from automatically-detected `nf-config` and `ncxx-config` files, as the wrong files could be included in the output.  This is in support of [GitHub #2274](https://github.com/Unidata/netcdf-c/issues/2274).
* [Enhancement] Update H5FDhttp.[ch] to work with HDF5 version 1.14.0. See [Github #2615](https://github.com/Unidata/netcdf-c/pull/2615).

## 4.9.1 - February 2, 2023

## Known Issues

* A test in the `main` branch of `netcdf-cxx4` is broken by this rc; this will bear further investigation, but not being treated as a roadblock for the release candidate.
* The new document, `netcdf-c/docs/filter_quickstart.md` is in rough-draft form.
* Race conditions exist in some of the tests when run concurrently with large numbers of processors

## What's Changed from v4.9.0 (automatically generated)

* Fix nc_def_var_fletcher32 operation by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2403
* Merge relevant info updates back into `main` by \@WardF in https://github.com/Unidata/netcdf-c/pull/2387
* Add manual GitHub actions triggers for the tests. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2404
* Use env variable USERPROFILE instead of HOME for windows and mingw. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2405
* Make public a limited API for programmatic access to internal .rc tables by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2408
* Fix typo in CMakeLists.txt by \@georgthegreat in https://github.com/Unidata/netcdf-c/pull/2412
* Fix choice of HOME dir by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2416
* Check for libxml2 development files by \@WardF in https://github.com/Unidata/netcdf-c/pull/2417
* Updating Doxyfile.in with doxygen-1.8.17, turned on WARN_AS_ERROR, added doxygen build to CI run by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2377
* updated release notes by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2392
* increase read block size from 1 KB to 4 MB by \@wkliao in https://github.com/Unidata/netcdf-c/pull/2319
* fixed RELEASE_NOTES.md by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2423
* Fix pnetcdf tests in cmake by \@WardF in https://github.com/Unidata/netcdf-c/pull/2437
* Updated CMakeLists to avoid corner case cmake error by \@WardF in https://github.com/Unidata/netcdf-c/pull/2438
* Add `--disable-quantize` to configure by \@WardF in https://github.com/Unidata/netcdf-c/pull/2439
* Fix the way CMake handles -DPLUGIN_INSTALL_DIR by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2430
* fix and test quantize mode for NC_CLASSIC_MODEL by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2445
* Guard _declspec(dllexport) in support of #2446 by \@WardF in https://github.com/Unidata/netcdf-c/pull/2460
* Ensure that netcdf_json.h does not interfere with ncjson. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2448
* Prevent cmake writing to source dir by \@magnusuMET in https://github.com/Unidata/netcdf-c/pull/2463
* more quantize testing and adding pre-processor constant NC_MAX_FILENAME to nc_tests.h by \@edwardhartnett in https://github.com/Unidata/netcdf-c/pull/2457
* Provide a default enum const when fill value does not match any enum constant by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2462
* Fix support for reading arrays of HDF5 fixed size strings by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2466
* fix musl build by \@magnusuMET in https://github.com/Unidata/netcdf-c/pull/1701
* Fix AWS SDK linking errors by \@dzenanz in https://github.com/Unidata/netcdf-c/pull/2470
* Address jump-misses-init issue. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2488
* Remove stray merge conflict markers by \@WardF in https://github.com/Unidata/netcdf-c/pull/2493
* Add support for Zarr string type to NCZarr by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2492
* Fix some problems with PR 2492 by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2497
* Fix some bugs in the blosc filter wrapper by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2461
* Add option to control accessing external servers by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2491
* Changed attribute case in documentation by \@WardF in https://github.com/Unidata/netcdf-c/pull/2482
* Adding all-error-codes.md back in to distribution documentation. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2501
* Update hdf5 version in github actions. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2504
* Minor update to doxygen function documentation by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2451
* Fix some additional errors in NCZarr by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2503
* Cleanup szip handling some more by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2421
* Check for zstd development headers in autotools by \@WardF in https://github.com/Unidata/netcdf-c/pull/2507
* Add new options to nc-config by \@WardF in https://github.com/Unidata/netcdf-c/pull/2509
* Cleanup built test sources in nczarr_test by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2508
* Fix inconsistency in netcdf_meta.h by \@WardF in https://github.com/Unidata/netcdf-c/pull/2512
* Small fix in nc-config.in by \@WardF in https://github.com/Unidata/netcdf-c/pull/2513
* For loop initial declarations are only allowed in C99 mode by \@gsjaardema in https://github.com/Unidata/netcdf-c/pull/2517
* Fix some dependencies in tst_nccopy3 by \@WardF in https://github.com/Unidata/netcdf-c/pull/2518
* Update plugins/Makefile.am  by \@WardF in https://github.com/Unidata/netcdf-c/pull/2519
* Fix prereqs in ncdump/tst_nccopy4 in order to avoid race conditions. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2520
* Move construction of VERSION file to end of the build by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2527
* Add draft filter quickstart guide by \@WardF in https://github.com/Unidata/netcdf-c/pull/2531
* Turn off extraneous debug output by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2537
* typo fix by \@wkliao in https://github.com/Unidata/netcdf-c/pull/2538
* replace 4194304 with READ_BLOCK_SIZE by \@wkliao in https://github.com/Unidata/netcdf-c/pull/2539
* Rename variable to avoid function name conflict by \@ibaned in https://github.com/Unidata/netcdf-c/pull/2550
* Add Cygwin CI and stop installing unwanted plugins by \@DWesl in https://github.com/Unidata/netcdf-c/pull/2529
* Merge subset of v4.9.1 files back into main development branch by \@WardF in https://github.com/Unidata/netcdf-c/pull/2530
* Add a Filter quickstart guide document by \@WardF in https://github.com/Unidata/netcdf-c/pull/2524
* Fix race condition in ncdump (and other) tests. by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2552
* Make dap4 reference dap instead of hard-wired to be disabled. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2553
* Suppress nczarr_test/tst_unknown filter test by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2557
* Add fenceposting for HAVE_DECL_ISINF and HAVE_DECL_ISNAN by \@WardF in https://github.com/Unidata/netcdf-c/pull/2559
* Add an old static file. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2575
* Fix infinite loop in file inferencing by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2574
* Merge Wellspring back into development branch by \@WardF in https://github.com/Unidata/netcdf-c/pull/2560
* Allow ncdump -t to handle variable length string attributes by \@srherbener in https://github.com/Unidata/netcdf-c/pull/2584
* Fix an issue I introduced with make distcheck by \@WardF in https://github.com/Unidata/netcdf-c/pull/2590
* make UDF0 not require NC_NETCDF4 by \@jedwards4b in https://github.com/Unidata/netcdf-c/pull/2586
* Expose user-facing documentation related to byterange DAP functionality.  by \@WardF in https://github.com/Unidata/netcdf-c/pull/2596
* Fix Memory Leak by \@DennisHeimbigner in https://github.com/Unidata/netcdf-c/pull/2598
* CI: Change autotools CI build to out-of-tree build. by \@DWesl in https://github.com/Unidata/netcdf-c/pull/2577
* Update github action configuration scripts. by \@WardF in https://github.com/Unidata/netcdf-c/pull/2600
* Update the filter quickstart guide.  by \@WardF in https://github.com/Unidata/netcdf-c/pull/2602
* Fix symbol export on Windows by \@WardF in https://github.com/Unidata/netcdf-c/pull/2604

## New Contributors
* \@georgthegreat made their first contribution in https://github.com/Unidata/netcdf-c/pull/2412
* \@dzenanz made their first contribution in https://github.com/Unidata/netcdf-c/pull/2470
* \@DWesl made their first contribution in https://github.com/Unidata/netcdf-c/pull/2529
* \@srherbener made their first contribution in https://github.com/Unidata/netcdf-c/pull/2584
* \@jedwards4b made their first contribution in https://github.com/Unidata/netcdf-c/pull/2586

**Full Changelog**: https://github.com/Unidata/netcdf-c/compare/v4.9.0...v4.9.1

### 4.9.1 - Release Candidate 2 - November 21, 2022

#### Known Issues

* A test in the `main` branch of `netcdf-cxx4` is broken by this rc; this will bear further investigation, but not being treated as a roadblock for the release candidate.
* The new document, `netcdf-c/docs/filter_quickstart.md` is in rough-draft form.

#### Changes


* [Bug Fix] Get remotetest server working for DAP4 and re-enable its use in the netcdf-c library for testing. See [Github #2558](https://github.com/Unidata/netcdf-c/pull/2558).
* [Bug Fix] Fix a race condition when testing missing filters. See [Github #2557](https://github.com/Unidata/netcdf-c/pull/2557). 
* [Bug Fix] Make major changes to libdap4 and dap4_test to update the non-remote DAP4 tests. See [Github #2555](https://github.com/Unidata/netcdf-c/pull/2555).
* [Bug Fix] Fix some race conditions due to use of a common file in multiple shell scripts . See [Github #2552](https://github.com/Unidata/netcdf-c/pull/2552).


### 4.9.1 - Release Candidate 1 - October 24, 2022

* [Enhancement][Documentation] Add Plugins Quick Start Guide.  See [GitHub #2524](https://github.com/Unidata/netcdf-c/pull/2524) for more information.
* [Enhancement] Add new entries in `netcdf_meta.h`, `NC_HAS_BLOSC` and `NC_HAS_BZ2`. See [Github #2511](https://github.com/Unidata/netcdf-c/issues/2511) and [Github #2512](https://github.com/Unidata/netcdf-c/issues/2512) for more information.
* [Enhancement] Add new options to `nc-config`: `--has-multifilters`, `--has-stdfilters`, `--has-quantize`, `--plugindir`.  See [Github #2509](https://github.com/Unidata/netcdf-c/pull/2509) for more information.
* [Bug Fix] Fix some errors detected in PR 2497. [PR #2497](https://github.com/Unidata/netcdf-c/pull/2497) . See [Github #2503](https://github.com/Unidata/netcdf-c/pull/2503).
* [Bug Fix] Split the remote tests into two parts: one for the remotetest server and one for all other external servers. Also add a configure option to enable the latter set. See [Github #2491](https://github.com/Unidata/netcdf-c/pull/2491).
* [Bug Fix] Fix blosc plugin errors. See [Github #2461](https://github.com/Unidata/netcdf-c/pull/2461).
* [Bug Fix] Fix support for reading arrays of HDF5 fixed size strings. See [Github #2466](https://github.com/Unidata/netcdf-c/pull/2466).
* [Bug Fix] Fix some errors detected in [PR #2492](https://github.com/Unidata/netcdf-c/pull/2492) . See [Github #2497](https://github.com/Unidata/netcdf-c/pull/2497).
* [Enhancement] Add support for Zarr (fixed length) string type in nczarr. See [Github #2492](https://github.com/Unidata/netcdf-c/pull/2492). 
* [Bug Fix] Split the remote tests into two parts: one for the remotetest server and one for all other external servers. Also add a configure option to enable the latter set. See [Github #2491](https://github.com/Unidata/netcdf-c/pull/2491).
* [Bug Fix] Fix support for reading arrays of HDF5 fixed size strings. See [Github #2462](https://github.com/Unidata/netcdf-c/pull/2466).
* [Bug Fix] Provide a default enum const when fill value does not match any enum constant for the value zero. See [Github #2462](https://github.com/Unidata/netcdf-c/pull/2462).
* [Bug Fix] Fix the json submodule symbol conflicts between libnetcdf and the plugin specific netcdf_json.h. See [Github #2448](https://github.com/Unidata/netcdf-c/pull/2448).
* [Bug Fix] Fix quantize with CLASSIC_MODEL files. See [Github #2405](https://github.com/Unidata/netcdf-c/pull/2445).
* [Enhancement] Add `--disable-quantize` option to `configure`.
* [Bug Fix] Fix CMakeLists.txt to handle all acceptable boolean values for -DPLUGIN_INSTALL_DIR. See [Github #2430](https://github.com/Unidata/netcdf-c/pull/2430).
* [Bug Fix] Fix tst_vars3.c to use the proper szip flag. See [Github #2421](https://github.com/Unidata/netcdf-c/pull/2421).
* [Enhancement] Provide a simple API to allow user access to the internal .rc file table: supports get/set/overwrite of entries of the form "key=value". See [Github #2408](https://github.com/Unidata/netcdf-c/pull/2408).
* [Bug Fix] Use env variable USERPROFILE instead of HOME for windows and mingw. See [Github #2405](https://github.com/Unidata/netcdf-c/pull/2405).
* [Bug Fix] Fix the nc_def_var_fletcher32 code in hdf5 to properly test value of the fletcher32 argument. See [Github #2403](https://github.com/Unidata/netcdf-c/pull/2403).

## 4.9.0 - June 10, 2022

* [Enhancement] Add quantize functions nc_def_var_quantize() and nc_inq_var_quantize() to enable lossy compression. See [Github #1548](https://github.com/Unidata/netcdf-c/pull/1548). 
* [Enhancement] Add zstandard compression functions nc_def_var_zstandard() and nc_inq_var_zstandard(). See [Github #2173](https://github.com/Unidata/netcdf-c/issues/2173).
* [Enhancement] Have netCDF-4 logging output one file per processor when used with parallel I/O. See [Github #1762](https://github.com/Unidata/netcdf-c/issues/1762).
* [Enhancement] Improve filter installation process to avoid use of an extra shell script. See [Github #2348](https://github.com/Unidata/netcdf-c/pull/2348).
* [Bug Fix] Get "make distcheck" to work See [Github #2343](https://github.com/Unidata/netcdf-c/pull/2343).
* [Enhancement] Allow the read/write of JSON-valued Zarr attributes to allow
for domain specific info such as used by GDAL/Zarr. See [Github #2278](https://github.com/Unidata/netcdf-c/pull/2278).
* [Enhancement] Turn on the XArray convention for NCZarr files by default. WARNING, this means that the mode should explicitly specify "nczarr" or "zarr" even if "xarray" or "noxarray" is specified. See [Github #2257](https://github.com/Unidata/netcdf-c/pull/2257).
* [Enhancement] Update the documentation to match the current filter capabilities  See [Github #2249](https://github.com/Unidata/netcdf-c/pull/2249).
* [Enhancement] Update the documentation to match the current filter capabilities. See [Github #2249](https://github.com/Unidata/netcdf-c/pull/2249).
* [Enhancement] Support installation of pre-built standard filters into user-specified location. See [Github #2318](https://github.com/Unidata/netcdf-c/pull/2318).
* [Enhancement] Improve filter support. More specifically (1) add nc_inq_filter_avail to check if a filter is available, (2) add the notion of standard filters, (3) cleanup szip support to fix interaction with NCZarr. See [Github #2245](https://github.com/Unidata/netcdf-c/pull/2245).
* [Enhancement] Switch to tinyxml2 as the default xml parser implementation. See [Github #2170](https://github.com/Unidata/netcdf-c/pull/2170).
* [Bug Fix] Require that the type of the variable in nc_def_var_filter is not variable length. See [Github #/2231](https://github.com/Unidata/netcdf-c/pull/2231).
* [File Change] Apply HDF5 v1.8 format compatibility when writing to previous files, as well as when creating new files.  The superblock version remains at 2 for newly created files.  Full backward read/write compatibility for netCDF-4 is maintained in all cases.  See [Github #2176](https://github.com/Unidata/netcdf-c/issues/2176).
* [Enhancement] Add ability to set dataset alignment for netcdf-4/HDF5 files. See [Github #2206](https://github.com/Unidata/netcdf-c/pull/2206).
* [Bug Fix] Improve UTF8 support on windows so that it can use utf8 natively. See [Github #2222](https://github.com/Unidata/netcdf-c/pull/2222).
* [Enhancement] Add complete bitgroom support to NCZarr. See [Github #2197](https://github.com/Unidata/netcdf-c/pull/2197).
* [Bug Fix] Clean up the handling of deeply nested VLEN types. Marks nc_free_vlen() and nc_free_string as deprecated in favor of ncaux_reclaim_data(). See [Github #2179](https://github.com/Unidata/netcdf-c/pull/2179).
* [Bug Fix] Make sure that netcdf.h accurately defines the flags in the open/create mode flags. See [Github #2183](https://github.com/Unidata/netcdf-c/pull/2183).
* [Enhancement] Improve support for msys2+mingw platform. See [Github #2171](https://github.com/Unidata/netcdf-c/pull/2171).
* [Bug Fix] Clean up the various inter-test dependencies in ncdump for CMake. See [Github #2168](https://github.com/Unidata/netcdf-c/pull/2168).
* [Bug Fix] Fix use of non-aws appliances. See [Github #2152](https://github.com/Unidata/netcdf-c/pull/2152).
* [Enhancement] Added options to suppress the new behavior from [Github #2135](https://github.com/Unidata/netcdf-c/pull/2135).  The options for `cmake` and `configure` are, respectively `-DENABLE_LIBXML2` and `--(enable/disable)-libxml2`. Both of these options default to 'on/enabled'.  When disabled, the bundled `ezxml` XML interpreter is used regardless of whether `libxml2` is present on the system.
* [Enhancement] Support optional use of libxml2, otherwise default to ezxml. See [Github #2135](https://github.com/Unidata/netcdf-c/pull/2135) -- H/T to [Egbert Eich](https://github.com/e4t).
* [Bug Fix] Fix several os related errors. See [Github #2138](https://github.com/Unidata/netcdf-c/pull/2138).
* [Enhancement] Support byte-range reading of netcdf-3 files stored in private buckets in S3. See [Github #2134](https://github.com/Unidata/netcdf-c/pull/2134)
* [Enhancement] Support Amazon S3 access for NCZarr. Also support use of the existing Amazon SDK credentials system. See [Github #2114](https://github.com/Unidata/netcdf-c/pull/2114)
* [Bug Fix] Fix string allocation error in H5FDhttp.c. See [Github #2127](https://github.com/Unidata/netcdf-c/pull/2127).
* [Bug Fix] Apply patches for ezxml and for selected  oss-fuzz detected errors. See [Github #2125](https://github.com/Unidata/netcdf-c/pull/2125).
* [Bug Fix] Ensure that internal Fortran APIs are always defined. See [Github #2098](https://github.com/Unidata/netcdf-c/pull/2098).
* [Enhancement] Support filters for NCZarr. See [Github #2101](https://github.com/Unidata/netcdf-c/pull/2101)
* [Bug Fix] Make PR 2075 long file name be idempotent. See [Github #2094](https://github.com/Unidata/netcdf-c/pull/2094).
## 4.8.1 - August 18, 2021

* [Bug Fix] Fix multiple bugs in libnczarr. See [Github #2066](https://github.com/Unidata/netcdf-c/pull/2066).
* [Enhancement] Support windows network paths (e.g. \\svc\...). See [Github #2065](https://github.com/Unidata/netcdf-c/pull/2065).
* [Enhancement] Convert to a new representation of the NCZarr meta-data extensions: version 2. Read-only backward compatibility is provided. See [Github #2032](https://github.com/Unidata/netcdf-c/pull/2032).
* [Bug Fix] Fix dimension_separator bug in libnczarr. See [Github #2035](https://github.com/Unidata/netcdf-c/pull/2035).
* [Bug Fix] Fix bugs in libdap4. See [Github #2005](https://github.com/Unidata/netcdf-c/pull/2005).
* [Bug Fix] Store NCZarr fillvalue as a singleton instead of a 1-element array. See [Github #2017](https://github.com/Unidata/netcdf-c/pull/2017).
* [Bug Fixes] The netcdf-c library was incorrectly determining the scope of dimension; similar to the type scope problem. See [Github #2012](https://github.com/Unidata/netcdf-c/pull/2012) for more information.
* [Bug Fix] Re-enable DAP2 authorization testing. See [Github #2011](https://github.com/Unidata/netcdf-c/issues/2011).
* [Bug Fix] Fix bug with windows version of mkstemp that causes failure to create more than 26 temp files. See [Github #1998](https://github.com/Unidata/netcdf-c/pull/1998).
* [Bug Fix] Fix ncdump bug when printing VLENs with basetype char. See [Github #1986](https://github.com/Unidata/netcdf-c/issues/1986).
* [Bug Fixes] The netcdf-c library was incorrectly determining the scope of types referred to by nc_inq_type_equal. See [Github #1959](https://github.com/Unidata/netcdf-c/pull/1959) for more information.
* [Bug Fix] Fix bug in use of XGetopt when building under Mingw. See [Github #2009](https://github.com/Unidata/netcdf-c/issues/2009).
* [Enhancement] Improve the error reporting when attempting to use a filter for which no implementation can be found in HDF5_PLUGIN_PATH. See [Github #2000](https://github.com/Unidata/netcdf-c/pull/2000) for more information.
* [Bug Fix] Fix `make distcheck` issue in `nczarr_test/` directory. See [Github #2007](https://github.com/Unidata/netcdf-c/issues/2007).
* [Bug Fix] Fix bug in NCclosedir in dpathmgr.c. See [Github #2003](https://github.com/Unidata/netcdf-c/issues/2003).
* [Bug Fix] Fix bug in ncdump that assumes that there is a relationship between the total number of dimensions and the max dimension id. See [Github #2004](https://github.com/Unidata/netcdf-c/issues/2004).
* [Bug Fix] Fix bug in JSON processing of strings with embedded quotes. See [Github #1993](https://github.com/Unidata/netcdf-c/issues/1993).
* [Enhancement] Add support for the new "dimension_separator" enhancement to Zarr v2. See [Github #1990](https://github.com/Unidata/netcdf-c/pull/1990) for more information.
* [Bug Fix] Fix hack for handling failure of shell programs to properly handle escape characters. See [Github #1989](https://github.com/Unidata/netcdf-c/issues/1989).
* [Bug Fix] Allow some primitive type names to be used as identifiers depending on the file format. See [Github #1984](https://github.com/Unidata/netcdf-c/issues/1984).
* [Enhancement] Add support for reading/writing pure Zarr storage format that supports the XArray _ARRAY_DIMENSIONS attribute. See [Github #1952](https://github.com/Unidata/netcdf-c/pull/1952) for more information.
* [Update] Updated version of bzip2 used in filter testing/functionality, in support of [Github #1969](https://github.com/Unidata/netcdf-c/issues/1969).
* [Bug Fix] Corrected HDF5 version detection logic as described in [Github #1962](https://github.com/Unidata/netcdf-c/issues/1962).

## 4.8.0 - March 30, 2021

* [Enhancement] Bump the NC_DISPATCH_VERSION from 2 to 3, and as a side effect, unify the definition of NC_DISPATCH_VERSION so it only needs to be defined in CMakeLists.txt and configure.ac. See [Github #1945](https://github.com/Unidata/netcdf-c/pull/1945) for more information.
* [Enhancement] Provide better cross platform path name management. This converts paths for various platforms (e.g. Windows, MSYS, etc.) so that they are in the proper format for the executing platform. See [Github #1958](https://github.com/Unidata/netcdf-c/pull/1958) for more information.
* [Bug Fixes] The nccopy program was treating -d0 as turning deflation on rather than interpreting it as "turn off deflation". See [Github #1944](https://github.com/Unidata/netcdf-c/pull/1944) for more information.
* [Enhancement] Add support for storing NCZarr data in zip files. See [Github #1942](https://github.com/Unidata/netcdf-c/pull/1942) for more information.
* [Bug Fixes] Make fillmismatch the default for DAP2 and DAP4; too many servers ignore this requirement.
* [Bug Fixes] Fix some memory leaks in NCZarr, fix a bug with long strides in NCZarr. See [Github #1913](https://github.com/Unidata/netcdf-c/pull/1913) for more information.
* [Enhancement] Add some optimizations to NCZarr, dosome cleanup of code cruft, add some NCZarr test cases, add a performance test to NCZarr. See [Github #1908](https://github.com/Unidata/netcdf-c/pull/1908) for more information.
* [Bug Fix] Implement a better chunk cache system for NCZarr. The cache now uses extendible hashing plus a linked list for provide a combination of expandibility, fast access, and LRU behavior. See [Github #1887](https://github.com/Unidata/netcdf-c/pull/1887) for more information.
* [Enhancement] Provide .rc fields for S3 authentication: HTTP.S3.ACCESSID and HTTP.S3.SECRETKEY.
* [Enhancement] Give the client control over what parts of a DAP2 URL are URL encoded (i.e. %xx). This is to support the different decoding rules that servers apply to incoming URLS. See [Github #1884](https://github.com/Unidata/netcdf-c/pull/1884) for more information.
* [Bug Fix] Fix incorrect time offsets from `ncdump -t`, in some cases when the time `units` attribute contains both a **non-zero** time-of-day, and a time zone suffix containing the letter "T", such as "UTC".  See [Github #1866](https://github.com/Unidata/netcdf-c/pull/1866) for more information.
* [Bug Fix] Cleanup the NCZarr S3 build options. See [Github #1869](https://github.com/Unidata/netcdf-c/pull/1869) for more information.
* [Bug Fix] Support aligned access for selected ARM processors.  See [Github #1871](https://github.com/Unidata/netcdf-c/pull/1871) for more information.
* [Documentation] Migrated the documents in the NUG/ directory to the dedicated NUG repository found at https://github.com/Unidata/netcdf
* [Bug Fix] Revert the internal filter code to simplify it. From the user's point of view, the only visible change should be that (1) the functions that convert text to filter specs have had their signature reverted and renamed and have been moved to netcdf_aux.h, and (2) Some filter API functions now return NC_ENOFILTER when inquiry is made about some filter. Internally, the dispatch table has been modified to get rid of the complex structures.
* [Bug Fix] If the HDF5 byte-range Virtual File Driver is available )HDf5 1.10.6 or later) then use it because it has better performance than the one currently built into the netcdf library.
* [Bug Fix] Fixed byte-range support with cURL > 7.69. See [https://github.com/Unidata/netcdf-c/pull/1798].
* [Enhancement] Added new test for using compression with parallel I/O: nc_test4/tst_h_par_compress.c. See [https://github.com/Unidata/netcdf-c/pull/1784].
* [Bug Fix] Don't return error for extra calls to nc_redef() for netCDF/HDF5 files, unless classic model is in use. See [https://github.com/Unidata/netcdf-c/issues/1779].
* [Enhancement] Added new parallel I/O benchmark program to mimic NOAA UFS data writes, built when --enable-benchmarks is in configure. See [https://github.com/Unidata/netcdf-c/pull/1777].
* [Bug Fix] Now allow szip to be used on variables with unlimited dimension [https://github.com/Unidata/netcdf-c/issues/1774].
* [Enhancement] Add support for cloud storage using a variant of the Zarr storage format. Warning: this feature is highly experimental and is subject to rapid evolution [https://www.unidata.ucar.edu/blogs/developer/en/entry/overview-of-zarr-support-in].
* [Bug Fix] Fix nccopy to properly set default chunking parameters when not otherwise specified. This can significantly improve performance in selected cases. Note that if seeing slow performance with nccopy, then, as a work-around, specifically set the chunking parameters. [https://github.com/Unidata/netcdf-c/issues/1763].
* [Bug Fix] Fix some protocol bugs/differences between the netcdf-c library and the OPeNDAP Hyrax server. Also cleanup checksum handling [https://github.com/Unidata/netcdf-c/issues/1712].* [Bug Fix] IMPORTANT: Ncgen was not properly handling large
data sections. The problem manifests as incorrect ordering of
data in the created file. Aside from examining the file with
ncdump, the error can be detected by running ncgen with the -lc
flag (to produce a C file). Examine the file to see if any
variable is written in pieces as opposed to a single call to
nc_put_vara. If multiple calls to nc_put_vara are used to write
a variable, then it is probable that the data order is
incorrect. Such multiple writes can occur for large variables
and especially when one of the dimensions is unlimited.
* [Bug Fix] Add necessary __declspec declarations to allow compilation
of netcdf library without causing errors or (_declspec related)
warnings [https://github.com/Unidata/netcdf-c/issues/1725].
* [Enhancement] When a filter is applied twice with different
parameters, then the second set is used for writing the dataset
[https://github.com/Unidata/netcdf-c/issues/1713].
* [Bug Fix] Now larger cache settings are used for sequential HDF5 file creates/opens on parallel I/O capable builds; see [Github #1716](https://github.com/Unidata/netcdf-c/issues/1716) for more information.
* [Bug Fix] Add functions to libdispatch/dnotnc4.c to support
dispatch table operations that should work for any dispatch
table, even if they do not do anything; functions such as
nc_inq_var_filter [https://github.com/Unidata/netcdf-c/issues/1693].
* [Bug Fix] Fixed a scalar annotation error when scalar == 0; see [Github #1707](https://github.com/Unidata/netcdf-c/issues/1707) for more information.
* [Bug Fix] Use proper CURLOPT values for VERIFYHOST and VERIFYPEER; the semantics for VERIFYHOST in particular changed. Documented in NUG/DAP2.md. See  [https://github.com/Unidata/netcdf-c/issues/1684].
* [Bug Fix][cmake] Correct an issue with parallel filter test logic in CMake-based builds.
* [Bug Fix] Now allow nc_inq_var_deflate()/nc_inq_var_szip() to be called for all formats, not just HDF5. Non-HDF5 files return NC_NOERR and report no compression in use. This reverts behavior that was changed in the 4.7.4 release. See [https://github.com/Unidata/netcdf-c/issues/1691].
* [Bug Fix] Compiling on a big-endian machine exposes some missing forward declarations in dfilter.c.
* [File Change] Change from HDF5 v1.6 format compatibility, back to v1.8 compatibility, for newly created files.  The superblock changes from version 0 back to version 2.  An exception is when using libhdf5 deprecated versions 1.10.0 and 1.10.1, which can only create v1.6 compatible format.  Full backward read/write compatibility for netCDF-4 is maintained in all cases.  See [Github #951](https://github.com/Unidata/netcdf-c/issues/951).

## 4.7.4 - March 27, 2020

* [Windows] Bumped packaged HDF5 to 1.10.6, HDF4 to 4.2.14, and libcurl to 7.60.0.
* [Enhancement] Support has been added for HDF5-1.12.0.  See [https://github.com/Unidata/netcdf-c/issues/1528].
* [Bug Fix] Correct behavior for the command line utilities when directly accessing a directory using utf8 characters. See [Github #1669](https://github.com/Unidata/netcdf-c/issues/1669), [Github #1668](https://github.com/Unidata/netcdf-c/issues/1668) and [Github #1666](https://github.com/Unidata/netcdf-c/issues/1666) for more information.
* [Bug Fix] Attempts to set filters or chunked storage on scalar vars will now return NC_EINVAL. Scalar vars cannot be chunked, and only chunked vars can have filters. Previously the library ignored these attempts, and always storing scalars as contiguous storage. See [https://github.com/Unidata/netcdf-c/issues/1644].
* [Enhancement] Support has been added for multiple filters per variable.  See [https://github.com/Unidata/netcdf-c/issues/1584].
* [Enhancement] Now nc_inq_var_szip returns 0 for parameter values if szip is not in use for var. See [https://github.com/Unidata/netcdf-c/issues/1618].
* [Enhancement] Now allow parallel I/O with filters, for HDF5-1.10.3 and later. See [https://github.com/Unidata/netcdf-c/issues/1473].
* [Enhancement] Increased default size of cache buffer to 16 MB, from 4 MB. Increased number of slots to 4133. See [https://github.com/Unidata/netcdf-c/issues/1541].
* [Enhancement] Allow zlib compression to be used with parallel I/O writes, if HDF5 version is 1.10.3 or greater. See [https://github.com/Unidata/netcdf-c/issues/1580].
* [Enhancement] Restore use of szip compression when writing data (including writing in parallel if HDF5 version is 1.10.3 or greater). See [https://github.com/Unidata/netcdf-c/issues/1546].
* [Enhancement] Enable use of compact storage option for small vars in netCDF/HDF5 files. See [https://github.com/Unidata/netcdf-c/issues/1570].
* [Enhancement] Updated benchmarking program bm_file.c to better handle very large files. See [https://github.com/Unidata/netcdf-c/issues/1555].
* [Enhancement] Added version number to dispatch table, and now check version with nc_def_user_format(). See [https://github.com/Unidata/netcdf-c/issues/1599].
* [Bug Fix] Fixed user setting of MPI launcher for parallel I/O HDF5 test in h5_test. See [https://github.com/Unidata/netcdf-c/issues/1626].
* [Bug Fix] Fixed problem of growing memory when netCDF-4 files were opened and closed. See [https://github.com/Unidata/netcdf-c/issues/1575 and https://github.com/Unidata/netcdf-c/issues/1571].
* [Enhancement] Increased size of maximum allowed name in HDF4 files to NC_MAX_NAME. See [https://github.com/Unidata/netcdf-c/issues/1631].

## 4.7.3 - November 20, 2019

* [Bug Fix]Fixed an issue where installs from tarballs will not properly compile in parallel environments.
* [Bug Fix] Library was modified so that rewriting the same attribute happens without deleting the attribute, to avoid a limit on how many times this may be done in HDF5. This fix was thought to be in 4.6.2 but was not. See [https://github.com/Unidata/netcdf-c/issues/350].
* [Enhancement] Add a dispatch version number to netcdf_meta.h and libnetcdf.settings, in case we decide to change dispatch table in future. See [https://github.com/Unidata/netcdf-c/issues/1469].
* [Bug Fix] Now testing that endianness can only be set on atomic ints and floats. See [https://github.com/Unidata/netcdf-c/issues/1479].
* [Bug Fix] Fix for subtle error involving var and unlimited dim of the same name, but unrelated, in netCDF-4. See [https://github.com/Unidata/netcdf-c/issues/1496].
* [Enhancement] Update for attribute documentation. See [https://github.com/Unidata/netcdf-c/issues/1512].
* [Bug Fix][Enhancement] Corrected assignment of anonymous (a.k.a. phony) dimensions in an HDF5 file. Now when a dataset uses multiple dimensions of the same size, netcdf assumes they are different dimensions. See [GitHub #1484](https://github.com/Unidata/netcdf-c/issues/1484) for more information.

## 4.7.2 - October 22, 2019

* [Bug Fix][Enhancement] Various bug fixes and enhancements.
* [Bug Fix][Enhancement] Corrected an issue where protected memory was being written to with some pointer slight-of-hand.  This has been in the code for a while, but appears to be caught by the compiler on OSX, under circumstances yet to be completely nailed down.  See [GitHub #1486](https://github.com/Unidata/netcdf-c/issues/1486) for more information.
* [Enhancement] [Parallel IO] Added support for parallel functions in MSVC. See [Github #1492](https://github.com/Unidata/netcdf-c/pull/1492) for more information.
* [Enhancement] Added a function for changing the ncid of an open file.  This function should only be used if you know what you are doing, and is meant to be used primarily with PIO integration. See [GitHub #1483](https://github.com/Unidata/netcdf-c/pull/1483) and [GitHub #1487](https://github.com/Unidata/netcdf-c/pull/1487) for more information.

## 4.7.1 - August 27, 2019

* [Enhancement] Added unit_test directory, which contains unit tests for the libdispatch and libsrc4 code (and any other directories that want to put unit tests there). Use --disable-unit-tests to run without unit tests (ex. for code coverage analysis). See [GitHub #1458](https://github.com/Unidata/netcdf-c/issues/1458).

* [Bug Fix] Remove obsolete _CRAYMPP and LOCKNUMREC macros from code. Also brought documentation up to date in man page. These macros were used in ancient times, before modern parallel I/O systems were developed. Programmers interested in parallel I/O should see nc_open_par() and nc_create_par(). See [GitHub #1459](https://github.com/Unidata/netcdf-c/issues/1459).

* [Enhancement] Remove obsolete and deprecated functions nc_set_base_pe() and nc_inq_base_pe() from the dispatch table. (Both functions are still supported in the library, this is an internal change only.) See [GitHub #1468](https://github.com/Unidata/netcdf-c/issues/1468).

* [Bug Fix] Reverted nccopy behavior so that if no -c parameters are given, then any default chunking is left to the netcdf-c library to decide. See [GitHub #1436](https://github.com/Unidata/netcdf-c/issues/1436).

## 4.7.0 - April 29, 2019

* [Enhancement] Updated behavior of `pkgconfig` and `nc-config` to allow the use of the `--static` flags, e.g. `nc-config --libs --static`, which will show information for linking against `libnetcdf` statically. See [Github #1360](https://github.com/Unidata/netcdf-c/issues/1360) and [Github #1257](https://github.com/Unidata/netcdf-c/issues/1257) for more information.

* [Enhancement] Provide byte-range reading of remote datasets. This allows
read-only access to, for example, Amazon S3 objects and also Thredds Server
datasets via the HTTPService access method.
See [GitHub #1251](https://github.com/Unidata/netcdf-c/issues/1251).

* Update the license from the home-brewed NetCDF license to the standard 3-Clause BSD License.  This change does not result in any new restrictions; it is merely the adoption of a standard, well-known and well-understood license in place of the historic NetCDF license written at Unidata.  This is part of a broader push by Unidata to adopt modern, standardized licensing.

## 4.6.3 - February 28, 2019

* [Bug Fix] Correctly generated `netcdf.pc` generated either by `configure` or `cmake`.  If linking against a static netcdf, you would need to pass the `--static` argument to `pkg-config` in order to list all of the downstream dependencies.  See [Github #1324](https://github.com/Unidata/netcdf-c/issues/1324) for more information.
* Now always write hidden coordinates attribute, which allows faster file opens when present. See [Github #1262](https://github.com/Unidata/netcdf-c/issues/1262) for more information.
* Some fixes for rename, including fix for renumbering of varids after a rename (#1307), renaming var to dim without coordinate var. See [Github #1297](https://github.com/Unidata/netcdf-c/issues/1297).
* Fix of NULL parameter causing segfaults in put_vars functions. See [Github #1265](https://github.com/Unidata/netcdf-c/issues/1265) for more information.
* Fix of --enable-benchmark benchmark tests [Github #1211](https://github.com/Unidata/netcdf-c/issues/1211)
* Update the license from the home-brewed NetCDF license to the standard 3-Clause BSD License.  This change does not result in any new restrictions; it is merely the adoption of a standard, well-known and well-understood license in place of the historic NetCDF license written at Unidata.  This is part of a broader push by Unidata to adopt modern, standardized licensing.
* [BugFix] Corrected DAP-related issues on big-endian machines. See [Github #1321](https://github.com/Unidata/netcdf-c/issues/1321), [Github #1302](https://github.com/Unidata/netcdf-c/issues/1302) for more information.
* [BugFix][Enhancement]  Various and sundry bugfixes and performance enhancements, thanks to \@edhartnett, \@gsjarrdema, \@t-b, \@wkliao, and all of our other contributors.
* [Enhancement] Extended `nccopy -F` syntax to support multiple variables with a single invocation. See [Github #1311](https://github.com/Unidata/netcdf-c/issues/1311) for more information.
* [BugFix] Corrected an issue where DAP2 was incorrectly converting signed bytes, resulting in an erroneous error message under some circumstances. See [GitHub #1317](https://github.com/Unidata/netcdf-c/issues/1317) for more information.  See [Github #1319](https://github.com/Unidata/netcdf-c/issues/1319) for related information.
* [BugFix][Enhancement] Modified `nccopy` so that `_NCProperties` is not copied over verbatim but is instead generated based on the version of `libnetcdf` used when copying the file.  Additionally, `_NCProperties` are displayed if/when associated with a netcdf3 file, now. See [GitHub #803](https://github.com/Unidata/netcdf-c/issues/803) for more information.

## 4.6.2 - November 16, 2018

* [Enhancement] Lazy att read - only read atts when user requests one of them.  See [GitHub #857](https://github.com/Unidata/netcdf-c/issues/857).
* [Enhancement] Fast global att read - when global atts are read, they are read much more quickly.  See [GitHub #857](https://github.com/Unidata/netcdf-c/issues/857).

## 4.6.2-rc2 November 1, 2018


* [Enhancement] Add nccopy command options for per-variable chunk sizing, and minimum chunk size.  See [GitHub #1087](https://github.com/Unidata/netcdf-c/pull/1087).
* [Bug Fix] Fix nccopy handling of user specified chunk sizes.  See [GitHub #725](https://github.com/Unidata/netcdf-c/issues/725),[#1087](https://github.com/Unidata/netcdf-c/issues/1087).
* [Bug Fix] Avoid limit on number of times a netCDF4 attribute can be updated.  Not a complete fix for the HDF5 "maximum creation order" problem, but should greatly reduce occurrences in many real-world cases.  See [GitHub #350](https://github.com/Unidata/netcdf-c/issues/350).
* [Bug Fix] The use of NC_DISKLESS has been modified to make it cleaner. This adds a new flag called NC_PERSIST that takes over the now obsolete NC_MPIPOSIX.
* [Obsolete] Obsolete the MPIPOSIX flag.
* [Bug Fix] When using filters with HDF5 1.10.x or later, it is necessary to utilize the HDF5 replacements for malloc, realloc, and free in the filter code.

## 4.6.2-rc1 - September 19, 2018

* [Enhancement] Create a new version of _NCProperties provenance attribute. This version (version 2) supports arbitrary key-value pairs. It is the default when new files are created. Version 1 continues to be accepted.
* [Enhancement] Allow user to set http read buffersize for DAP2 and  DAP4 using the tag HTTP.READ.BUFFERSIZE in the .daprc file.
* [Enhancement] Allow user to set http keepalive for DAP2 and  DAP4 using the tag HTTP.KEEPALIVE in the .daprc file (see the OPeNDAP documentation for details).
* [Enhancement] Support DAP4 remote tests using a new remote test server located on the Unidata JetStream project.
* [Enhancement] Improved the performance of the nc_get/put_vars operations by using the equivalent slab capabilities of hdf5. Result is a significant speedup of these operations.  See [GitHub #1001](https://github.com/Unidata/netcdf-c/pull/1001) for more information.
* [Enhancement] Expanded the capabilities of `NC_INMEMORY` to support writing and accessing the final modified memory.  See [GitHub #879](https://github.com/Unidata/netcdf-c/pull/879) for more information.
* [Enhancement] Made CDF5 support enabled by default.  See [Github #931](https://github.com/Unidata/netcdf-c/issues/931) for more information.
* [Bug Fix] Corrected a number of memory issues identified in `ncgen`.  See [GitHub #558 for more information](https://github.com/Unidata/netcdf-c/pull/558).

## 4.6.1 - March 19, 2018

* [Bug Fix] Corrected an issue which could result in a dap4 failure. See [Github #888](https://github.com/Unidata/netcdf-c/pull/888) for more information.
* [Bug Fix][Enhancement] Allow `nccopy` to control output filter suppression.  See [Github #894](https://github.com/Unidata/netcdf-c/pull/894) for more information.
* [Enhancement] Reverted some new behaviors that, while in line with the netCDF specification, broke existing workflows.  See [Github #843](https://github.com/Unidata/netcdf-c/issues/843) for more information.
* [Bug Fix] Improved support for CRT builds with Visual Studio, improves zlib detection in hdf5 library. See [Github #853](https://github.com/Unidata/netcdf-c/pull/853) for more information.
* [Enhancement][Internal] Moved HDF4 into a distinct dispatch layer. See [Github #849](https://github.com/Unidata/netcdf-c/pull/849) for more information.

## 4.6.0 - January 24, 2018
* [Enhancement] Full support for using HDF5 dynamic filters, both for reading and writing. See the file docs/filters.md.
* [Enhancement] Added an option to enable strict null-byte padding for headers; this padding was specified in the spec but was not enforced.  Enabling this option will allow you to check your files, as it will return an E_NULLPAD error.  It is possible for these files to have been written by older versions of libnetcdf.  There is no effective problem caused by this lack of null padding, so enabling these options is informational only.  The options for `configure` and `cmake` are `--enable-strict-null-byte-header-padding` and `-DENABLE_STRICT_NULL_BYTE_HEADER_PADDING`, respectively.  See [Github #657](https://github.com/Unidata/netcdf-c/issues/657) for more information.
* [Enhancement] Reverted behavior/handling of out-of-range attribute values to pre-4.5.0 default. See [Github #512](https://github.com/Unidata/netcdf-c/issues/512) for more information.
* [Bug] Fixed error in tst_parallel2.c. See [Github #545](https://github.com/Unidata/netcdf-c/issues/545) for more information.
* [Bug] Fixed handling of corrupt files + proper offset handling for hdf5 files. See [Github #552](https://github.com/Unidata/netcdf-c/issues/552) for more information.
* [Bug] Corrected a memory overflow in `tst_h_dimscales`, see [Github #511](https://github.com/Unidata/netcdf-c/issues/511), [Github #505](https://github.com/Unidata/netcdf-c/issues/505), [Github #363](https://github.com/Unidata/netcdf-c/issues/363) and [Github #244](https://github.com/Unidata/netcdf-c/issues/244) for more information.

## 4.5.0 - October 20, 2017

* Corrected an issue which could potential result in a hang while using parallel file I/O. See [Github #449](https://github.com/Unidata/netcdf-c/pull/449) for more information.
* Addressed an issue with `ncdump` not properly handling dates on a 366 day calendar. See [GitHub #359](https://github.com/Unidata/netcdf-c/issues/359) for more information.

### 4.5.0-rc3 - September 29, 2017

* [Update] Due to ongoing issues, native CDF5 support has been disabled by **default**.  You can use the options mentioned below (`--enable-cdf5` or `-DENABLE_CDF5=TRUE` for `configure` or `cmake`, respectively).  Just be aware that for the time being, Reading/Writing CDF5 files on 32-bit platforms may result in unexpected behavior when using extremely large variables.  For 32-bit platforms it is best to continue using `NC_FORMAT_64BIT_OFFSET`.
* [Bug] Corrected an issue where older versions of curl might fail. See [GitHub #487](https://github.com/Unidata/netcdf-c/issues/487) for more information.
* [Enhancement] Added options to enable/disable `CDF5` support at configure time for autotools and cmake-based builds.  The options are `--enable/disable-cdf5` and `ENABLE_CDF5`, respectively.  See [Github #484](https://github.com/Unidata/netcdf-c/issues/484) for more information.
* [Bug Fix] Corrected an issue when subsetting a netcdf3 file via `nccopy -v/-V`. See [Github #425](https://github.com/Unidata/netcdf-c/issues/425) and [Github #463](https://github.com/Unidata/netcdf-c/issues/463) for more information.
* [Bug Fix] Corrected `--has-dap` and `--has-dap4` output for cmake-based builds. See [GitHub #473](https://github.com/Unidata/netcdf-c/pull/473) for more information.
* [Bug Fix] Corrected an issue where `NC_64BIT_DATA` files were being read incorrectly by ncdump, despite the data having been written correctly.  See [GitHub #457](https://github.com/Unidata/netcdf-c/issues/457) for more information.
* [Bug Fix] Corrected a potential stack buffer overflow.  See [GitHub #450](https://github.com/Unidata/netcdf-c/pull/450) for more information.

### 4.5.0-rc2 - August 7, 2017

* [Bug Fix] Addressed an issue with how cmake was implementing large file support on 32-bit systems. See [GitHub #385](https://github.com/Unidata/netcdf-c/issues/385) for more information.
* [Bug Fix] Addressed an issue where ncgen would not respect keyword case. See [GitHub #310](https://github.com/Unidata/netcdf-c/issues/310) for more information.

### 4.5.0-rc1 - June 5, 2017

* [Enhancement] DAP4 is now included. Since dap2 is the default for urls, dap4 must be specified by
    1. using "dap4:" as the url protocol, or
    2. appending "\#protocol=dap4" to the end of the url, or
    3. appending "\#dap4" to the end of the url

    Note that dap4 is enabled by default but remote-testing is
    disabled until the testserver situation is resolved.

* [Enhancement] The remote testing server can now be specified with the `--with-testserver` option to ./configure.
* [Enhancement] Modified netCDF4 to use ASCII for NC_CHAR.  See [Github Pull request #316](https://github.com/Unidata/netcdf-c/pull/316) for more information.
* [Bug Fix] Corrected an error with how dimsizes might be read. See [Github #410](https://github.com/unidata/netcdf-c/issues/410) for more information.
* [Bug Fix] Corrected an issue where 'make check' would fail if 'make' or 'make all' had not run first.  See [Github #339](https://github.com/Unidata/netcdf-c/issues/339) for more information.
* [Bug Fix] Corrected an issue on Windows with Large file tests. See [Github #385](https://github.com/Unidata/netcdf-c/issues/385]) for more information.
* [Bug Fix] Corrected an issue with diskless file access, see [Pull Request #400](https://github.com/Unidata/netcdf-c/issues/400) and [Pull Request #403](https://github.com/Unidata/netcdf-c/issues/403) for more information.
* [Upgrade] The bash based test scripts have been upgraded to use a common test_common.sh include file that isolates build specific information.
* [Upgrade] The bash based test scripts have been upgraded to use a common test_common.sh include file that isolates build specific information.
* [Refactor] the oc2 library is no longer independent of the main netcdf-c library. For example, it now uses ncuri, nclist, and ncbytes instead of its homegrown equivalents.
* [Bug Fix] `NC_EGLOBAL` is now properly returned when attempting to set a global `_FillValue` attribute. See [GitHub #388](https://github.com/Unidata/netcdf-c/issues/388) and [GitHub #389](https://github.com/Unidata/netcdf-c/issues/389) for more information.
* [Bug Fix] Corrected an issue where data loss would occur when `_FillValue` was mistakenly allowed to be redefined.  See [Github #390](https://github.com/Unidata/netcdf-c/issues/390), [GitHub #387](https://github.com/Unidata/netcdf-c/pull/387) for more information.
* [Upgrade][Bug] Corrected an issue regarding how "orphaned" DAS attributes were handled. See [GitHub #376](https://github.com/Unidata/netcdf-c/pull/376) for more information.
* [Upgrade] Update utf8proc.[ch] to use the version now maintained by the Julia Language project (https://github.com/JuliaLang/utf8proc/blob/master/LICENSE.md).
* [Bug] Addressed conversion problem with Windows sscanf.  This primarily affected some OPeNDAP URLs on Windows.  See [GitHub #365](https://github.com/Unidata/netcdf-c/issues/365) and [GitHub #366](https://github.com/Unidata/netcdf-c/issues/366) for more information.
* [Enhancement] Added support for HDF5 collective metadata operations when available. Patch submitted by Greg Sjaardema, see [Pull request #335](https://github.com/Unidata/netcdf-c/pull/335) for more information.
* [Bug] Addressed a potential type punning issue. See [GitHub #351](https://github.com/Unidata/netcdf-c/issues/351) for more information.
* [Bug] Addressed an issue where netCDF wouldn't build on Windows systems using MSVC 2012. See [GitHub #304](https://github.com/Unidata/netcdf-c/issues/304) for more information.
* [Bug] Fixed an issue related to potential type punning, see [GitHub #344](https://github.com/Unidata/netcdf-c/issues/344) for more information.
* [Enhancement] Incorporated an enhancement provided by Greg Sjaardema, which may improve read/write times for some complex files.  Basically, linked lists were replaced in some locations where it was safe to use an array/table.  See [Pull request #328](https://github.com/Unidata/netcdf-c/pull/328) for more information.

## 4.4.1.1 - November 21, 2016

* [Bug] Fixed an issue where `ncgen` would potentially crash or write incorrect netCDF4 binary data under very specific circumstances.  This bug did *not* affect data written on 32-bit systems or by using the netCDF library; it was specific to `ncgen`.  This would only happen when writing a compound data type containing an 8-byte data type followed by a 4-byte data type *and* the 4-byte data type was not properly aligned; this would *possibly* result in incorrect padding. This did not affect 32-bit systems, or data written directly by the library.  See [GitHub #323](https://github.com/Unidata/netcdf-c/issues/323) for more information.
* [Documentation] Updated documentation related to netCDF variable names and DAP2 access to reflect the undefined behavior potentially observed when DAP2 reserved keywords are used as netCDF variable names. See [GitHub #308](https://github.com/Unidata/netcdf-c/issues/308) for more information.
* [Bug] Fixed an issue with `nc_inq_type()` not returning proper value in some circumstances.  See [GitHub #317](https://github.com/Unidata/netcdf-c/issues/317) for more information.
* [Bug] Corrected an issue related to test failures when `--disable-utilities` or `-DENABLE_UTILITIES=OFF` are specified when building with autotools or cmake, respectively.  See [GitHub #313](https://github.com/Unidata/netcdf-c/issues/313) for more information.
* [Bug][Enhancement] Corrected a behavioral issue with the `_NCProperties` attribute taking up too much space.  See [GitHub #300](https://github.com/Unidata/netcdf-c/issues/300) and [GitHub #301](https://github.com/Unidata/netcdf-c/pull/301) for more information.

* [Bug] Corrected behavior for `nc-config` so that, if `nf-config` is found in system, the proper fortran-related information will be conveyed.  See [GitHub #296](https://github.com/Unidata/netcdf-c/issues/296] for more information.

## 4.4.1 - June 28, 2016

* [File Change] Starting with release 4.4.1, netCDF-4 files are created with HDF5 v1.6 format compatibility, rather than v1.8.  The superblock changes from version 2, as was observed in previous netCDF versions, to version 0.  This is due to a workaround required to avoid backwards binary incompatibility when using libhdf5 1.10.x or greater.  Superblock versions 0 and 2 appear to be forward and backward compatible for netCDF purposes.  Other than a different superblock number, the data should remain consistent.  See [GitHub #250](https://github.com/Unidata/netcdf-c/issues/250).
* [Enhancement] Added better error reporting when ncdump/nccopy are given a bad constraint in a DAP url. See [GitHub #279](https://github.com/Unidata/netcdf-c/pull/279) for more information.

### 4.4.1-RC3 - June 17, 2016

* [Bug Fix] Misc. bug fixes and improvements.
* [Bug Fix] Corrected an issue where adding a \_FillValue attribute to a variable would result in other attributes being lost. See [GitHub #239](https://github.com/Unidata/netcdf-c/issues/239) for more details.
* [Bug Fix][Parallel I/O] Corrected an issue reported by Kent Yang at the HDF group related to Collective Parallel I/O and a potential hang.

### 4.4.1-RC2 - May 13, 2016

* [Enhancement] Added provenance information to files created.  This information consists of a persistent attribute named `_NCProperties` plus two computed attributes, `_IsNetcdf4` and `_SuperblockVersion`.  Associated documentation was added to the file `docs/attribute_conventions.md`.  See [GitHub pull request #260](https://github.com/Unidata/netcdf-c/pull/260) for more information.
* [Bug Fix] Cleaned up some dead links in the doxygen-generated documentation.
* [Bug Fix] Corrected several issues related to building under Visual Studio 2014.
* [Bug Fix] Corrected several test failures related to HDF5 `1.10.0`
* [Bug Fix] Reverted SOVersion *current* to 11 from 12; it was incorrectly incremented in netCDF-C release 4.4.1-RC1.
* [Enhancement][Windows] Bumped the included libhdf5 to 1.8.16 from 1.8.15 for pre-built Visual Studio installer files.


### 4.4.1-RC1 - April 15, 2016

* [Bug Fix][Enhancement][File Change] Fixed an issue with netCDF4 files generated using version `1.10.0` of the HDF5 library.  The 1.10 release potentially changed the underlying file format, introducing a backwards compatibility issue with the files generated.  HDF5 provided an API for retaining the 1.8.x file format, which is now on by default.  See [GitHub Issue #250](https://github.com/Unidata/netcdf-c/issues/250) for more information.
* [Bug Fix] Corrected an issue with autotools-based builds performed out-of-source-tree.  See [GitHub Issue #242](https://github.com/Unidata/netcdf-c/issues/242) for more information.
* [Enhancement] Modified `nc_inq_type()` so that it would work more broadly without requiring a valid ncid.  See [GitHub Issue #240](https://github.com/Unidata/netcdf-c/issues/240) for more information.
* [Enhancement] Accepted a patch code which added a hashmap lookup for rapid var and dim retrieval in nc3 files, contributed by Greg Sjaardema.  See [GitHub Pull Request #238](https://github.com/Unidata/netcdf-c/pull/238) for more information.
* [Bug Fix] Accepted a contributed pull request which corrected an issue with how the cmake-generated `nc-config` file determined the location of installed files. See [GitHub Pull Request #235](https://github.com/Unidata/netcdf-c/pull/235) for more information.
* [Enhancement] Added an advanced option for CMake-based builds, `ENABLE_SHARED_LIBRARY_VERSION`.  This option is `ON` by default, but if turned off, only `libnetcdf.dylib` will be generated, instead of files containing the SOVERSION in the file name.  This is a requested feature most people might not care about.  See [GitHub #228](https://github.com/Unidata/netcdf-c/issues/228) for more information.
* [Bug Fix] Corrected an issue with duplicated error codes defined in multiple header files.  See [GitHub #213](https://github.com/Unidata/netcdf-c/issues/213) for more information.
* [Bug Fix] Addressed an issue specific to Visual Studio 2015 on Windows.  On very large files, some calls to the `fstat` class of functions would fail for no apparent reason. This behavior was **not** observed under Visual Studio 2013. This has now been mitigated.  See [GitHub #188](https://github.com/Unidata/netcdf-c/issues/188) for more information.
* [Enhancement] Updated `nc-config` to report whether `logging` is enabled in netcdf.  Additionally, if `f03` is available in an installed netcdf-fortran library, it will now be reported as well.
* [Bug Fix] Addressed an issue where `netcdf_mem.h` was not being installed by cmake. See [GitHub #227](https://github.com/Unidata/netcdf-c/issues/227) for more information.
* [Bug Fix] Addressed an issue where `ncdump` would crash when trying to read a netcdf file containing an empty ragged `VLEN` variable in an unlimited dimension. See [GitHub #221](https://github.com/Unidata/netcdf-c/issues/221) for more information.

## 4.4.0 Released - January 13, 2016

* Bumped SO version to 11.0.0.

* Modified `CMakeLists.txt` to work with the re-organized cmake configuration used by the latest HDF5, `1.8.16`, on Windows. Before this fix, netCDF would fail to locate hdf5 1.8.16 when using cmake on Windows.  See [GitHub #186](https://github.com/Unidata/netcdf-c/issues/186) for more information.

* Addressed an issue with `ncdump` when annotations were used.  The indices for the last row suffered from an off-by-1 error.  See [GitHub issue #181](https://github.com/Unidata/netcdf-c/issues/181) for more information.

* Addressed an issue on platforms where `char` is `unsigned` by default (such as `ARM`), as well as an issue describing regarding undefined behavior, again on `ARM`.  See [GitHub issue #159](https://github.com/Unidata/netcdf-c/issues/159) for detailed information.

* Fixed an ambiguity in the grammar for cdl files.  See [GitHub #178](https://github.com/Unidata/netcdf-c/issues/178) for more information.

* Updated documentation for `nc_get_att_string()` to reflect the fact that it returns allocated memory which must be explicitly free'd using `nc_free_string()`. Reported by Constantine Khroulev, see [GitHub Issue 171](https://github.com/Unidata/netcdf-c/issues/171) for more information.

* Modified ncgen to properly handle the L and UL suffixes for integer constants
  to keep backward compatibility. Now it is the case the single L suffix
  (e.g. 111L) is treated as a 32 bit integer. This makes it consistent with
  the fact that NC_LONG (netcdf.h) is an alias for NC_INT. Existing .cdl
  files should be examined for occurrences of the L prefix to ensure that
  this change will not affect them.
  (see Github issue 156[https://github.com/Unidata/netcdf-c/issues/156]).

* Updated documentation to reference the new `NodeJS` interface to netcdf4, by Sven Willner.  It is available from [https://www.npmjs.com/package/netcdf4](https://www.npmjs.com/package/netcdf4) or from the GitHub repository at [https://github.com/swillner/netcdf4-js](https://github.com/swillner/netcdf4-js).

* Incorporated pull request https://github.com/Unidata/netcdf-c/pull/150 from Greg Sjaardema to remove the internal hard-wired use of `NC_MAX_DIMS`, instead using a dynamic memory allocation.

### 4.4.0-RC5 Released - November 11, 2015

* Added a fix for https://github.com/Unidata/netcdf-c/issues/149, which was reported several times in quick succession within an hour of the RC4 release.

### 4.4.0-RC4 Released - November 10, 2015

* Added CDM-5 support via new mode flag called NC_64BIT_DATA (alias NC_CDF5).

	Major kudos to Wei-Keng Liao for all the effort he put into getting this to work.

    This cascaded into a number of other changes.

    1. Renamed libsrcp5 -> libsrcp because PnetCDF can do parallel io for CDF-1, CDF-2 and CDF-5, not just CDF-5.
    2. Given #1, then the NC_PNETCDF mode flag becomes a subset of NC_MPIIO, so made NC_PNETCDF an alias for NC_MPII.
    3. NC_FORMAT_64BIT is now deprecated.  Use NC_FORMAT_64BIT_OFFSET.

Further information regarding the CDF-5 file format specification may be found here: http://cucis.ece.northwestern.edu/projects/PnetCDF/CDF-5.html

* Modified configure.ac to provide finer control over parallel
  support. Specifically, add flags for:

    1. HDF5_PARALLEL when hdf5 library has parallel enabled
    2. --disable-parallel4 to be used when we do not want
     netcdf-4 to use parallelism even if hdf5 has it enabled.


* Deprecating various extended format flags.

The various extended format flags of the format `NC_FORMAT_FOO` have been refactored into the form `NC_FORMATX_FOO`.  The old flags still exist but have been marked as deprecated and will be removed at some point.  This was done to avoid confusion between the extended format flags and the format flags `NC_FORMAT_CLASSIC`, `NC_FORMAT_64BIT_OFFSET`, etc.  The mapping of deprecated-to-new flags is as follows:

Deprecated | Replaced with
-----------|-------------
NC\_FORMAT\_NC3       | NC\_FORMATX\_NC3
NC\_FORMAT\_NC\_HDF5  | NC\_FORMATX\_NC\_HDF5
NC\_FORMAT\_NC4       | NC\_FORMATX\_NC4
NC\_FORMAT\_NC\_HDF4  | NC\_FORMATX\_NC\_HDF4
NC\_FORMAT\_PNETCDF   | NC\_FORMATX\_PNETCDF
NC\_FORMAT\_DAP2      | NC\_FORMATX\_DAP2
NC\_FORMAT\_DAP4      | NC\_FORMATX\_DAP4
NC\_FORMAT\_UNDEFINED | NC\_FORMATX\_UNDEFINED

* Reduced minimum cmake version to `2.8.11` from `2.8.12`. This will allow for cmake use on a broader set of popular linux platforms without having to do a custom cmake install.  See https://github.com/Unidata/netcdf-c/issues/135 for more information.

* The documentation section `The Default Chunking Scheme` has been updated with more information.  This lives in the `guide.dox` file in the `docs/` directory, or can be found online in the appropriate location (typically https://docs.unidata.ucar.edu/netcdf-c/), once this release has been published.

### 4.4.0-RC3 2015-10-08

* Addressed an inefficiency in how bytes would be swapped when converting between `LITTLE` and `BIG` ENDIANNESS.  See [NCF-338](https://bugtracking.unidata.ucar.edu/browse/NCF-338) for more information.

* Addressed an issue where an interrupted read on a `POSIX` system would return an error even if errno had been properly set to `EINTR`.  This issue was initially reported by David Knaak at Cray.  More information may be found at [NCF-337](https://bugtracking.unidata.ucar.edu/browse/NCF-337).

* Added a note to the install directions pointing out that parallel make
cannot be used for 'make check'.

### 4.4.0-RC2 Released 2015-07-09

* Minor bug fixes and cleanup of issues reported with first release candidate.

### 4.4.0-RC1 Released 2015-06-09

* The pre-built Windows binaries are now built using `Visual Studio 2012`, instead of `Visual Studio 2010`.  Source-code compilation remains function with `Visual Studio 2010`, this is just a change in the pre-built binaries.

* Added support for opening in-memory file content. See `include/netcdf_mem.h` for the procedure signature. Basically, it allows one to fill a chunk of memory with the equivalent of some netCDF file and then open it and read from it as if it were any other file. See [NCF-328](https://bugtracking.unidata.ucar.edu/browse/NCF-328) for more information.

* Addressed an issue when reading hdf4 files with explicit little-endian datatypes. This issue was [reported by Tim Burgess at GitHub](https://github.com/Unidata/netcdf-c/issues/113).  See [NCF-332](https://bugtracking.unidata.ucar.edu/browse/NCF-332) for more information.

* Addressed an issue with IBM's `XL C` compiler on AIX and how it handled some calls to malloc.  Also, as suggested by Wolfgang Hayek, developers using this compiler may need to pass `CPPFLAGS=-D_LINUX_SOURCE_COMPAT` to avoid some test failures.

* Addressed an issure in netcdf4 related to specifying an endianness explicitly.  When specifying an endianness for `NC_FLOAT`, the value would appear to not be written to file, if checked with `ncdump -s`.  The issue was more subtle; the value would be written but was not being read from file properly for non-`NC_INT`.  See [GitHub Issue](https://github.com/Unidata/netcdf-c/issues/112) or [NCF-331](https://bugtracking.unidata.ucar.edu/browse/NCF-331) for more information.

* Addressed an issue in netcdf4 on Windows w/DAP related to how byte values were copied with sscanf.  Issue originally reported by Ellen Johnson at Mathworks, see [NCF-330](https://bugtracking.unidata.ucar.edu/browse/NCF-330) for more information.

* Addressed in issue in netcdf4 files on Windows, built with Microsoft Visual Studio, which could result in a memory leak.  See [NCF-329](https://bugtracking.unidata.ucar.edu/browse/NCF-329) for more information.

* Addressed an issue in netcdf4 files where writing unlimited dimensions that were not declared at head of the dimensions list, as reported by Ellen Johnson at Mathworks.  See [NCF-326](https://bugtracking.unidata.ucar.edu/browse/NCF-326) for more information.

* Added an authorization reference document as oc2/ocauth.html.

* Fixed bug resulting in segmentation violation when trying to add a
  _FillValue attribute to a variable in an existing netCDF-4 file
  defined without it (thanks to Alexander Barth). See
  [NCF-187](https://bugtracking.unidata.ucar.edu/browse/NCF-187) for
  more information.

## 4.3.3.1 Released 2015-02-25

* Fixed a bug related to renaming the attributes of coordinate variables in a subgroup. See [NCF-325](https://bugtracking.unidata.ucar.edu/browse/NCF-325) for more information.

## 4.3.3 Released 2015-02-12

* Fixed bug resulting in error closing a valid netCDF-4 file with a dimension and a non-coordinate variable with the same name. [NCF-324](https://bugtracking.unidata.ucar.edu/browse/NCF-324)

* Enabled previously-disabled shell-script-based tests for Visual Studio when `bash` is detected.

### 4.3.3-rc3 Released 2015-01-14

* Added functionality to make it easier to build `netcdf-fortran` as part of the `netcdf-c` build for *NON-MSVC* builds.  This functionality is enabled at configure time by using the following **Highly Experimental** options:

 * CMake:  `-DENABLE_REMOTE_FORTRAN_BOOTSTRAP=ON`
 * Autotools: `--enable-remote-fortran-bootstrap`

Details are as follows:

----

Enabling these options creates two new make targets:

*  `build-netcdf-fortran`
* `install-netcdf-fortran`

Example Work Flow from netcdf-c source directory:

* $ `./configure --enable-remote-fortran-bootstrap --prefix=$HOME/local`
* $ `make check`
* $ `make install`
* $ `make build-netcdf-fortran`
* $ `make install-netcdf-fortran`

> These make targets are **only** valid after `make install` has been invoked.  This cannot be enforced rigidly in the makefile for reasons we will expand on in the documentation, but in short: `make install` may require sudo, but using sudo will discard environmental variables required when attempting to build netcdf-fortran in this manner.

> It is important to note that this is functionality is for *convenience only*. It will remain possible to build `netcdf-c` and `netcdf-fortran` manually.  These make targets should hopefully suffice for the majority of our users, but for corner cases it may still be required of the user to perform a manual build.  [NCF-323](https://bugtracking.unidata.ucar.edu/browse/NCF-323)

* Added a failure state if the `m4` utility is not found on non-Windows systems; previously, the build would fail when it reached the point of invoking m4.

* Added an explicit check in the build systems (autotools, cmake) for the CURL-related option `CURLOPT_CHUNK_BGN_FUNCTION`.  This option was introduced in libcurl version `7.21.0`.  On installations which require libcurl and have this version, `CURLOPT_CHUNK_BGN_FUNCTION` will be available. Otherwise, it will not.

* The PnetCDF support was not properly being used to provide mpi parallel io for netcdf-3 classic files. The wrong dispatch table was being used. [NCF-319](https://bugtracking.unidata.ucar.edu/browse/NCF-319)

* In nccopy utility, provided proper default for unlimited dimension in chunk-size specification instead of requiring explicit chunk size. Added associated test. [NCF-321](https://bugtracking.unidata.ucar.edu/browse/NCF-321)

* Fixed documentation typo in FILL_DOUBLE definition in classic format specification grammar. Fixed other typos and inconsistencies in Doxygen version of User Guide.

* For nccopy and ncgen, added numeric options (-3, -4, -6, -7) for output format, to provide less confusing format version specifications than the error-prone equivalent -k options (-k1, -k2, -k3, -k4). The new numeric options are compatible with NCO's mnemonic version options. The old -k numeric options will still be accepted but are deprecated, due to easy confusion between format numbers and format names. [NCF-314](https://bugtracking.unidata.ucar.edu/browse/NCF-314)

* Fixed bug in ncgen. When classic format was in force (k=1 or k=4), the "long" datatype should be treated as int32. Was returning an error. [NCF-318](https://bugtracking.unidata.ucar.edu/browse/NCF-318)

* Fixed bug where if the netCDF-C library is built with the HDF5 library but without the HDF4 library and one attempts to open an HDF4 file, an abort occurs rather than returning a proper error code (NC_ENOTNC). [NCF-317](https://bugtracking.unidata.ucar.edu/browse/NCF-317)

* Added a new option, `NC_EXTRA_DEPS`, for cmake-based builds.  This is analogous to `LIBS` in autotools-based builds.  Example usage:

    $ cmake .. -NC_EXTRA_DEPS="-lcustom_lib"

More details may be found at the Unidata JIRA Dashboard.  [NCF-316](https://bugtracking.unidata.ucar.edu/browse/NCF-316)


### 4.3.3-rc2 Released 2014-09-24

* Fixed the code for handling character constants
  in datalists in ncgen. Two of the problems were:
  1. It failed on large constants
  2. It did not handle e.g. var = 'a', 'b', ...
     in the same way that ncgen3 did.
  See [NCF-309](https://bugtracking.unidata.ucar.edu/browse/NCF-309).

* Added a new file, `netcdf_meta.h`.  This file is generated automatically at configure time and contains information related to the capabilities of the netcdf library.  This file may be used by projects dependent upon `netcdf` to make decisions during configuration, based on how the `netcdf` library was built.  The macro `NC_HAVE_META_H` is defined in `netcdf.h`.  Paired with judicious use of `ifdef`'s, this macro will indicate to developers whether or not the meta-header file is present. See [NCF-313](https://bugtracking.unidata.ucar.edu/browse/NCF-313).

    > Determining the presence of `netcdf_meta.h` can also be accomplished by methods common to autotools and cmake-based build systems.

* Changed `Doxygen`-generated documentation hosted by Unidata to use more robust server-based searching.
* Corrected embedded URLs in release notes.
* Corrected an issue where building with HDF4 support with Visual Studio would fail.

### 4.3.3-rc1 Released 2014-08-25

* Added `CMake`-based export files, contributed by Nico Schlömer. See https://github.com/Unidata/netcdf-c/pull/74.

* Documented that ncgen input can come from standard input.

* Regularized generation of libnetcdf.settings file to make parsing it easier.

* Fixed ncdump bug for char variables with multiple unlimited dimensions and added an associated test.  Now the output CDL properly disambiguates dimension groupings, so that ncgen can generate the original file from the CDL. [NCF-310](https://bugtracking.unidata.ucar.edu/browse/NCF-310)

* Converted the [Manually-maintained FAQ page](https://docs.unidata.ucar.edu/netcdf-c/current/faq.html) into markdown and added it to the `docs/` directory.  This way the html version will be generated when the rest of the documentation is built, the FAQ will be under version control, and it will be in a more visible location, hopefully making it easier to maintain.

* Bumped minimum required version of `cmake` to `2.8.12`.  This was necessitated by the adoption of the new `CMAKE_MACOSX_RPATH` property, for use on OSX.

* Jennifer Adams has requested a reversion in behavior so that all dap requests include a constraint. Problem is caused by change in prefetch where if all variables are requested, then no constraint is generated.  Fix is to always generate a constraint in prefetch.
  [NCF-308](https://bugtracking.unidata.ucar.edu/browse/NCF-308)

* Added a new option for cmake-based builds, `ENABLE_DOXYGEN_LATEX_OUTPUT`.  On those systems with `make` and `pdflatex`, setting this option **ON** will result in pdf versions of the documentation being built.  This feature is experimental.

* Bumped minimum CMake version to `2.8.9` from `2.8.8` as part of a larger pull request contributed by Nico Schlömer. [Pull Request #64](https://github.com/Unidata/netcdf-c/pull/64)

* Replaced the `NetCDF Library Architecture` image with an updated version from the 2012 NetCDF Workshop slides.

* Fix HDF4 files to support chunking.
  [NCF-272](https://bugtracking.unidata.ucar.edu/browse/NCF-272)

* NetCDF creates a `libnetcdf.settings` file after configuration now, similar to those generated by `HDF4` and `HDF5`.  It is installed into the same directory as the libraries. [NCF-303](https://bugtracking.unidata.ucar.edu/browse/NCF-303).


* Renamed `man4/` directory to `docs/` to make the purpose and contents clearer. See [man4 vs. docs #60](https://github.com/Unidata/netcdf-c/issues/60).

* Removed redundant variable `BUILD_DOCS` from the CMake configuration file.  See the issue at github: [#59](https://github.com/Unidata/netcdf-c/issues/59).

* Added missing documentation templates to `man4/Makefile.am`, to correct an issue when trying to build the local `Doxygen`-generated documentation. This issue was reported by Nico Schlömer and may be viewed on github.  [Releases miss Doxygen files #56](https://github.com/Unidata/netcdf-c/issues/56)

* When the NC_MPIPOSIX flag is given for parallel I/O access and the HDF5 library does not have the MPI-POSIX VFD configured in, the NC_MPIPOSIX flag is transparently aliased to the NC_MPIIO flag within the netCDF-4 library.

## 4.3.2 Released 2014-04-23

* As part of an ongoing project, the Doxygen-generated netcdf documentation has been reorganized.  The goal is to make the documentation easier to parse, and to eliminate redundant material.  This project is ongoing.

* The oc .dodsrc reader was improperly handling the user name and password entries. [NCF-299](https://bugtracking.unidata.ucar.edu/browse/NCF-299)

* CTestConfig.cmake has been made into a template so that users may easily specify the location of an alternative CDash-based Dashboard using the following two options:

	* `NC_TEST_DROP_SITE` - Specify an alternative Dashboard by URL or IP address.

	* `NC_CTEST_DROP_LOC_PREFIX` - Specify a prefix on the remote webserver relative to the root directory. This lets CTest accommodate dashboards that do not live at the top level of the web server.

* Return an error code on open instead of an assertion violation for truncated file.

* Documented limit on number of Groups per netCDF-4 file (32767).

### 4.3.2-rc2 Released 2014-04-15

* Cleaned up a number of CMake inconsistencies related to CMake usage, parallel builds.
* Now passing -Wl,--no-undefined to linker when appropriate.
* Corrected an issue preventing large file tests from running correctly under Windows.
* Misc Bug Fixes detected by static analysis.

### 4.3.2-rc1 Released 2014-03-20

* Pre-built Windows downloads will now be bundled with the latest (as of the time of this writing) versions of the various dependencies:
	* `hdf5: 1.8.12`
	* `zlib: 1.2.8`
	* `libcurl: 7.35.0`

* Added a separate flag to enable DAP AUTH tests. These tests are disabled by default.  The flags for autotools and CMAKE-based builds are (respectively):
	* --enable-dap-auth-tests
	* -DENABLE\_DAP\_AUTH\_TESTS

* Fixed small default chunk size for 1-dimensional record variables.  [NCF-211](https://bugtracking.unidata.ucar.edu/browse/NCF-211)

* Cleaned up type handling in netCDF-4 to fix bugs with fill-values.

* Corrected "BAIL" macros to avoid infinite loop when logging is disabled and an error occurs.

* Refactored how types are used for attributes, variables, and committed types, clarifying and categorizing fields in structs, and eliminating duplicated type information between variables and types they use.

* Made type structure shareable by committed datatypes and variables that use it.

* Handled string datatypes correctly, particularly for fill value attributes. Expanded testing for string fill values.

* Simplified iteration of objects in the file when it's opened, tracking fewer objects and using less memory.

* Enabled netCDF-4 bit-for-bit reproducibility for nccopy and other applications (thanks to Rimvydas Jasinskas and Quincey Koziol) by turning off HDF5 object creation, access, and modification time tracking.  [NCF-290](https://bugtracking.unidata.ucar.edu/browse/NCF-290)

* Addressed an issue where `cmake`-based builds would not properly create a `pkg-config` file. This file is now created properly by `cmake`.  [NCF-288](https://bugtracking.unidata.ucar.edu/browse/NCF-288)

* Addressed an issue related to old DAP servers. [NCF-287](https://bugtracking.unidata.ucar.edu/browse/NCF-287)

* Modified nc_{get/put}_vars to no longer use
  nc_get/put_varm. They now directly use nc_get/put_vara
  directly. This means that nc_get/put_vars now work
  properly for user defined types as well as atomic types.
  [NCF-228] (https://bugtracking.unidata.ucar.edu/browse/NCF-228)

## 4.3.1.1 Released 2014-02-05

This is a bug-fix-only release for version 4.3.1.

* Corrected a DAP issue reported by Jeff Whitaker related to non-conforming servers.

* Corrected an issue with DAP tests failing in a 64-bit Cygwin environment. [NCF-286](https://bugtracking.unidata.ucar.edu/browse/NCF-286)

## 4.3.1 Released 2014-01-16

* Add an extended format inquiry method to the netCDF API: nc\_inq\_format\_extended. NC\_HAVE\_INQ\_FORMAT\_EXTENDED is defined in netcdf.h [NCF-273]

[NCF-273]:https://bugtracking.unidata.ucar.edu/browse/NCF-273


### 4.3.1-rc6 Released 2013-12-19

* Fixed fill value handling for string types in nc4\_get\_vara().

* Corrected behavior of nc\_inq\_unlimdim and nv\_inq\_unlimdims to report dimids
  in same order as nc\_inq\_dimids.

* Addressed an issue reported by Jeff Whitaker regarding `nc_inq_nvars` returning an incorrect number of dimensions (this issue was introduced in 4.3.1-rc5).  Integrated a test contributed by Jeff Whitaker.

* A number of previously-disabled unit tests were reviewed and made active.


### 4.3.1-rc5 Released 2013-12-06

* When opening a netCDF-4 file, streamline the iteration over objects in the underlying HDF5 file.

* Fixed netCDF-4 failure when renaming a dimension and renaming a variable using that dimension, in either order. [NCF-177]

[NCF-177]:https://bugtracking.unidata.ucar.edu/browse/NCF-177

* When compiling with `hdf4` support, both autotools and cmake-based builds now properly look for the `libjpeg` dependency and will link against it when found (or complain if it's not).  Also added `ENABLE_HDF4_FILE_TESTS` option to CMake-based builds.

* Fixed bug in ncgen; it was not properly filling empty string constants ("") to be the proper length. [NCF-279]

[NCF-279]:https://bugtracking.unidata.ucar.edu/browse/NCF-279

* Fixed bug in ncgen where it was interpreting int64 constants
  as uint64 constants. [NCF-278]

[NCF-278]:https://bugtracking.unidata.ucar.edu/browse/NCF-278

* Fixed bug in handling Http Basic Authorization. The code was actually there but was not being executed. [NCF-277]

[NCF-277]:https://bugtracking.unidata.ucar.edu/browse/NCF-277

* Added hack to the DAP code to address a problem with the Columbia.edu server. That server does not serve up proper DAP2 DDS replies. The Dataset {...} name changes depending on if the request has certain kinds of constraints. [NCF-276]

[NCF-276]:https://bugtracking.unidata.ucar.edu/browse/NCF-276

* Fixed bugs with ncdump annotation of values, using -b or -f
  options. [NCF-275]

[NCF-275]:https://bugtracking.unidata.ucar.edu/browse/NCF-275


### 4.3.1-rc4 Released 2013-11-06

* Addressed an issue on Windows where `fstat` would report an incorrect file size on files > 4GB.  [NCF-219]


* Added better documentation about accessing ESG datasets.
  See https://docs.unidata.ucar.edu/netcdf-c/current/esg.html.

* Corrected an issue with CMake-based builds enabling HDF4 support where the HDF4 libraries were in a non-standard location.

* Fix bug introduced by [NCF-267] where octal constants above
'\177' were not recognized as proper octal constants. [NCF-271]

[NCF-271]:https://bugtracking.unidata.ucar.edu/browse/NCF-271

* Fixed an issue where the `netcdf.3` man page was not being installed by CMake-based builds. [Github](https://github.com/Unidata/netcdf-c/issues/3)



### 4.3.1-rc3 Released 2013-09-24

* Modify ncgen to support NUL characters in character array
  constants. [NCF-267]

[NCF-267]:https://bugtracking.unidata.ucar.edu/browse/NCF-267

* Modify ncgen to support disambiguating references to
  an enum constant in a data list. [NCF-265]

[NCF-265]:https://bugtracking.unidata.ucar.edu/browse/NCF-265

* Corrected bug in netCDF-4 dimension ID ordering assumptions, resulting in access that works locally but fails through DAP server. [NCF-166]

[NCF-166]:https://bugtracking.unidata.ucar.edu/browse/NCF-166

* Added a new configuration flag, `NC_USE_STATIC_CRT` for CMake-based Windows builds.  The default value is 'OFF'.  This will allow the user to define whether to use the shared CRT libraries (\\MD) or static CRT libraries (\\MT) in Visual Studio builds.

* Ensure netCDF-4 compiles with OpenMPI as an alternative to MPICH2. [NCF-160]

[NCF-160]:https://bugtracking.unidata.ucar.edu/browse/NCF-160

* Addressed issue with hanging Parallel netCDF-4 using HDF5 1.8.10. [NCF-240]

[NCF-240]:https://bugtracking.unidata.ucar.edu/browse/NCF-240

* Addressed issue with Large File Support on Windows, using both 32 and 64-bit builds. [NCF-219]

[NCF-219]:https://bugtracking.unidata.ucar.edu/browse/NCF-219

* Removed deprecated directories:
	* librpc/
	* udunits/
	* libcf/
	* libcdmr/

### 4.3.1-rc2 Released 2013-08-19

* Added `configure` and accompanying configuration files/templates to release repository.  **These will only be added to tagged releases on GitHub**.

* Integrated a fix by Quincey Koziol which addressed a variation of [NCF-250], *Fix issue of netCDF-4 parallel independent access with unlimited dimension hanging*.

[NCF-250]:https://bugtracking.unidata.ucar.edu/browse/NCF-250

* Integrated change contributed by Orion Poplawski which integrated GNUInstallDirs into the netCDF-C CMake system; this will permit systems that install into lib64 (such as Fedora) to `make install` without problem.

* Corrected an error with the CMake config files that resulted in the `netcdf.3` manpage not being built or installed.

### 4.3.1-rc1 Released 2013-08-09

* Migrated from the netCDF-C `subversion` repository to a publicly available GitHub repository available at https://github.com/Unidata/netCDF-C.  This repository may be checked out (cloned) with the following command:

	$ git clone https://github.com/Unidata/netCDF-C.git

* Note: in this release, it is necessary to generate the `configure` script and makefile templates using `autoreconf` in the root netCDF-C directory.:

	$ autoreconf -i -f

* Added `nc_rename_grp` to allow for group renaming in netCDF-4 files. [NCF-204]

[NCF-204]: https://bugtracking.unidata.ucar.edu/browse/NCF-204

* Added a `NC_HAVE_RENAME_GRP` macro to netcdf.h, [as per a request by Charlie Zender][cz1]. This will allow software compiling against netcdf to easily query whether or not nc\_rename\_grp() is available.

[cz1]: https://bugtracking.unidata.ucar.edu/browse/NCF-204

* Added Greg Sjaardema's contributed optimization for the nc4\_find\_dim\_len function in libsrc4/nc4internal.c. The patch eliminates several malloc/free calls that exist in the original coding.

* Added support for dynamic loading, to compliment the dynamic loading support introduced in hdf 1.8.11.  Dynamic loading support depends on libdl, and is enabled as follows: [NCF-258]
	* autotools-based builds: --enable-dynamic-loading
	* cmake-based builds: -DENABLE\_DYNAMIC\_LOADING=ON

[NCF-258]: https://bugtracking.unidata.ucar.edu/browse/NCF-258

* Fix issue of netCDF-4 parallel independent access with unlimited dimension hanging.  Extending the size of an unlimited dimension in HDF5 must be a collective operation, so now an error is returned if trying to extend in independent access mode. [NCF-250]

[NCF-250]: https://bugtracking.unidata.ucar.edu/browse/NCF-250

* Fixed bug with netCDF-4's inability to read HDF5 scalar numeric attributes. Also allow, in addition to zero length strings, a new NULL pointer as a string value. to improve interoperability with HDF5. This required a new CDL constant, 'NIL', that can be output from ncdump for such a string value in an HDF5 or netCDF-4 file. The ncgen utility was also modified to properly handle such NIL values for strings. [NCF-56]

[NCF-56]: https://bugtracking.unidata.ucar.edu/browse/NCF-56

* Parallel-build portability fixes, particularly for OpenMPI and gcc/gfortran-4.8.x on OSX.

* Fix contributed by Nath Gopalaswamy to large file problem reading netCDF classic or 64-bit offset files that have a UINT32_MAX flag for large last record size of a variable that has values larger than 1 byte.  This problem had previously been fixed for *writing* such data, but was only tested with an ncbyte variable.

* Fixed various minor documentation problems.

## 4.3.0 Released 2013-04-29

* fsync: Changed default in autotools config file; fsync must now be
explicitly enabled instead of explicitly disabled. [NCF-239]

[NCF-239]: https://bugtracking.unidata.ucar.edu/browse/NCF-239

* Fixed netCDF-4 bug where odometer code for libdap2 mishandled stride \> 1. Bug reported by Ansley Manke. [NCF-249]

[NCF-249]: https://bugtracking.unidata.ucar.edu/browse/NCF-249

* Fixed netCDF-4 bug so netCDF just ignores objects of HDF5 reference type in
the file, instead of rejecting the file. [NCF-29]

[NCF-29]: https://bugtracking.unidata.ucar.edu/browse/NCF-29

* Fixed netCDF-4 bug with particular order of creation of dimensions,
coordinate variables, and subgroups resulting in two dimensions with the
same dimension ID. [NCF-244]

[NCF-244]: https://bugtracking.unidata.ucar.edu/browse/NCF-244

* Fixed netCDF-4 bug with a multidimensional coordinate variable in a
subgroup getting the wrong dimension IDs for its dimensions. [NCF-247]

[NCF-247]: https://bugtracking.unidata.ucar.edu/browse/NCF-247

* Fixed bug with incorrect fixed-size variable offsets in header getting
written when schema changed for files created by PnetCDF Thanks
to Wei-keng Liao for developing and contributing the fix. [NCF-234]

[NCF-234]: https://bugtracking.unidata.ucar.edu/browse/NCF-234

* Fixed bug in handling old servers that do not do proper Grid to
Structure conversions. [NCF-232]

[NCF-232]: https://bugtracking.unidata.ucar.edu/browse/NCF-232

* Replaced the oc library with oc2.0

* Fix bug with nc\_get\_var1\_uint() not accepting unsigned ints larger
than 2\*\*31. [NCF-226]

[NCF-226]: https://bugtracking.unidata.ucar.edu/browse/NCF-226

* Fix to convert occurrences of '/' in DAP names to %2f. [NCF-223]

[NCF-223]: https://bugtracking.unidata.ucar.edu/browse/NCF-223

* Fix bug in netCDF-4 with scalar non-coordinate variables with same name
as dimensions. [NCF-222]

[NCF-222]: https://bugtracking.unidata.ucar.edu/browse/NCF-222

* Fix bug in which calling netCDF-4 functions in which behavior that
should not depend on order of calls sometimes produces the wrong
results. [NCF-217]

[NCF-217]: https://bugtracking.unidata.ucar.edu/browse/NCF-217

* Merged in nccopy additions from Martin van Driel to support -g and -v
options for specifying which groups or variables are to be copied.
[NCF-216]

[NCF-216]: https://bugtracking.unidata.ucar.edu/browse/NCF-216

* Merged in PnetCDF bugs fixes from Greg Sjaardema. [NCF-214]

[NCF-214]: https://bugtracking.unidata.ucar.edu/browse/NCF-214

* Modify ncgen so that if the incoming file has a special attribute, then
it is used to establish the special property of the netcdf file, but the
attribute is not included as a real attribute in the file. [NCF-213].

[NCF-213]: https://bugtracking.unidata.ucar.edu/browse/NCF-213

* Added library version info to the user-agent string so that the server
logs will be more informative. [NCF-210]

[NCF-210]: https://bugtracking.unidata.ucar.edu/browse/NCF-210

* Added work around for bad servers that sometimes sends DAP dataset with
duplicate field names. [NCF-208]

[NCF-208]: https://bugtracking.unidata.ucar.edu/browse/NCF-208

* Fixed bug with strided access for NC\_STRING type. [NCF-206]

[NCF-206]: https://bugtracking.unidata.ucar.edu/browse/NCF-206

* Prevented adding an invalid \_FillValue attribute to a variable (with
nonmatching type or multiple values), to avoid later error when any
record variable is extended. [NCF-190]

[NCF-190]: https://bugtracking.unidata.ucar.edu/browse/NCF-190

* Fix bug in which some uses of vlen within compounds causes HDF5 errors.
[NCF-155]

[NCF-155]: https://bugtracking.unidata.ucar.edu/browse/NCF-155

* Fixed ncdump bug in display of data values of variables that use
multiple unlimited dimensions. [NCF-144]

[NCF-144]: https://bugtracking.unidata.ucar.edu/browse/NCF-144

* Fix bug in which interspersing def\_var calls with put\_var calls can
lead to corrupt metadata in a netCDF file with groups and inherited
dimensions. [NCF-134]

[NCF-134]: https://bugtracking.unidata.ucar.edu/browse/NCF-134

* Building shared libraries works with DAP and netCDF4 functionality.
[NCF-205] [NCF-57]

[NCF-205]: https://bugtracking.unidata.ucar.edu/browse/NCF-205
[NCF-57]: https://bugtracking.unidata.ucar.edu/browse/NCF-57

* 32-and-64-bit builds are working under MinGW on Windows. [NCF-112]

[NCF-112]: https://bugtracking.unidata.ucar.edu/browse/NCF-112

* Config.h for Windows compiles are included in the build. [NCF-98]

[NCF-98]: https://bugtracking.unidata.ucar.edu/browse/NCF-98

* NetCDF-4 dependency on NC\_MAX\_DIMS has been removed. [NCF-71]

[NCF-71]: https://bugtracking.unidata.ucar.edu/browse/NCF-71

* 64-bit DLL's are produced on Windows. [NCF-65]

[NCF-65]: https://bugtracking.unidata.ucar.edu/browse/NCF-65

* DLL Packaging issues are resolved. [NCF-54]

[NCF-54]: https://bugtracking.unidata.ucar.edu/browse/NCF-54

* The CMake build system (with related ctest and cdash systems for
testing) has been integrated into netCDF-C. This allows for Visual
Studio-based builds in addition to gcc-based builds. This requires at
least CMake version 2.8.8. This replaces/supplements the cross-compiled
set of Visual-Studio compatible netCDF libraries introduced in netCDF
4.2.1-rc1.

## 4.2.1.1 Released 2012-08-03

* Patched libdap2/ncdap3.c to fix DAP performance bug remotely accessing large files (> 2GiB).

* Patched ncdump/dumplib.c to properly escape special characters in CDL output from ncdump for netCDF-4 string data.


### 4.2.1 Released 2012-07-18

* Added a specific NC\_MMAP mode flag to modify behavior of NC\_DISKLESS.

* Changed the file protections for NC\_DISKLESS created files to 0666
[NCF-182]

* Fixed ncdump to report error when an unsupported option is specified.
[NCF-180]

* Fixed documentation of CDL char constants in Users Guide and ncgen man
page.

* Fixed memory leak detected by valgrind in one of the HDF5 tests.

* Fixed problem with \#elif directives in posixio.c revealed by PGI
compilers.

### 4.2.1-rc1 Released 2012-06-18

* Ported static and shared libraries (DLL's) for both 32- and 64-bit
Windows, including support for DAP remote access, with netCDF-3 and
netCDF-4/HDF5 support enabled. The environment for this build is
MSYS/MinGW/MinGW64, but the resulting DLLs may be used with Visual
Studio. [NCF-112] [NCF-54] [NCF-57] [NCF-65]

* Implemented diskless files for all netCDF formats. For nc\_create(),
diskless operation performs all operations in memory and then optionally
persists the results to a file on close. For nc\_open(), but only for
netcdf classic files, diskless operation caches the file in-memory,
performs all operations on the memory resident version and then writes
all changes back to the original file on close.
[NCF-110][NCF-109][NCF-5]

* Added MMAP support. If diskless file support is enabled, then it is
possible to enable implementation of diskless files using the operating
system's MMAP facility (if available). The enabling flag is
"--enable-mmap". This is most useful when using nc\_open() and when only
parts of files, a single variable say, need to be read.

* Added configure flag for --disable-diskless.

* Added nccopy command-line options to exploit diskless files, resulting
in large speedups for some operations, for example converting unlimited
dimension to fixed size or rechunking files for faster access. Upgraded
doxygen and man-page documentation for ncdump and nccopy utilities,
including new -w option for diskless nccopy, with an example.

* Modified Makefile to allow for concurrent builds and to support builds
outside the source tree, e.g. 'mkdir build; cd build;
SOURCE-DIR/configure' where SOURCE-DIR is the top-level source
directory.

* Fixed some netCDF-4 bugs with handling strings in non-netCDF-4 HDF5
files. [NCF-150]

* Fixed bug using nccopy to compress with shuffling that doesn't compress
output variables unless they were already compressed in the input file.
[NCF-162]

* Fixed bug in 64-bit offset files with large records, when last record
variable requires more than 2\*\*32 bytes per record. [NCF-164]

* Fix bug in which passing a NULL path to nc\_open causes failure.
[NCF-173]

* Fixed ncgen bugs in parsing and handling opaque data.

* Fixed ncdump bug, not escaping characters special to CDL in enumeration
labels. [NCF-169]

* Fixed bug reading netCDF int into a C longlong or writing from longlong
to external int on 32-bit platforms with classic format files. The upper
32 bits of the longlong were not cleared on read or used on write.
[NCF-171]

* Resolved some erroneous returns of BADTYPE errors and RANGE errors due
to conflating C memory types with external netCDF types when accessing
classic or 64-bit offset files. [NCF-172]

* Fixed bug with ncdump -t interpreting unit attribute without base time
as a time unit. [NCF-175]

* Changed port for testing remote access test server to increase
reliability of tests.

* Modified ncio mechanism to support multiple ncio packages, so that it is
possible to have e.g. posixio and memio operating at the same time.

* Generation of documentation is disabled by default. Use --enable-doxygen
to generate. [NCF-168]

* Added description of configure flags to installation guide.

* Clarified documentation of arguments to nc**open() and nc**create() and
their default values.

* Fixed doxygen installation guide source file to preserve line breaks in
code and scripts. [NCF-174]

* Cleaned up a bunch of lint issues (unused variables, etc.) and some
similar problems reported by clang static analysis.

* Updated and fixed pkg-config source file netcdf.pc.in to work with
separated netCDF language-specific packages. Also fixed nc-config to
call nf-config, ncxx-config, and ncxx4-config for for backward
compatibility with use of nc-config in current Makefiles. [NCF-165]
[NCF-179]

## 4.2.0 2012-05-01

* Completely rebuilt the DAP constraint handling. This primarily affects
users who specify a DAP constraint as part of their URL. [NCF-120]

* Fixed cause of slow nccopy performance on file systems with many records
and large disk block size or many record variables, by accessing data a
record at a time instead of a variable at a time. [NCF-142]

* Performance improvement to DAP code to support fetching partial
variables into the cache; especially important when using nc\_get\_var()
API. A partial variable is one that has ranges attached to the
projection variables (e.g. x[1:10][20:21]) [NCF-157]

* Separate the Fortran and C++ libraries and release the C library and
ncdump/ncgen/nccopy without Fortran or C++. [NCF-24]

* Documentation mostly migrated to Doxygen, from Texinfo. [NCF-26]

* Properly convert vara start/count parameters to DAP [NCF-105][NCF-106]

* Fixed major wasted space from previous default variable chunk sizes
algorithm. [NCF-81]

* Fixed bug in nccopy, in which compression and chunking options were
ignored for netCDF-4 input files. [NCF-79]

* Fixed bug in ncgen in which large variables (more than 2**18 elements)
duplicates the first 2**18 values into subsequent chunks of data
[NCF-154].

* Applied Greg Sjaardema's nccopy bug fix, not compressing output
variables f they were not already using compression on the input file
when shuffle specified. [NCF-162]

* Fixed problem when a URL is provided that contains only a host name.
[NCF-103]

* Fixed behavior of ncgen flags so that -o =\> -lb and, in the absence of
any other markers, make the default be -k1 [NCF-158]

* Created a text INSTALL file for netCDF-4.2 release. [NCF-161]

* Fixed bug in ncgen for vlen arrays as fields of compound types where
datalists for those types was improperly interpreted [NCF-145] (but see
NCF-155).

* Improve use of chunk cache in nccopy utility, making it practical for
rechunking large files. [NCF-85]

* Fixed nccopy bug copying a netCDF-4 file with a chunksize for an
unlimited dimension that is larger than the associated dimension size.
[NCF-139]

* Fixed nccopy bug when rechunking a netCDF-4 file with a chunkspec option
that doesn't explicitly specify all dimensions. [NCF-140]

* Fixed bug in netCDF-4 files with non-coordinate variable with the same
name as a dimension. [NCF-141]

* Incorporated Wei Huang's fix for bug where netCDF-4 sometimes skips over
too many values before adding fill values to an in-memory buffer.
[NCF-143]

* Fixed ncgen bug with netCDF-4 variable-length constants (H/T to Lynton
Appel). [NCF-145]

* Incorporated Peter Cao's performance fixes using HDF5 link iterator for
any group with many variables or types. [NCF-148]

* Incorporated Constantine Khroulev's bug fix for invalid usage of
MPI\_Comm\_f2c in nc\_create\_par. [NCF-135]

* Fixed turning off fill values in HDF5 layers when NOFILL mode is set in
netCDF-4 API (thanks to Karen Schuchardt). [NCF-151]

* Fixed bug with scalar coordinate variables in netCDF-4 files, causing
failure with --enable-extra-tests [NCF-149]

* Cleaned up the definition and use of nulldup. [NCF-92][NCF-93][NCF-94]

* Fixed various '\#include' bugs. [NCF-91][NCF-96][NCF-127]

* v2 API functions modified to properly call the external API instead of
directly calling the netcdf-3 functions. [NCF-100]

* Fixed problem with 64-bit offset format where writing more than 2\*\*31
records resulted in erroneous NC\_EINVALCOORDS error. [NCF-101]

* Restored original functionality of ncgen so that a call with no flags,
only does the syntax check. [NCF-104]

* Corrected misc. test bugs [NCF-107]

* Modified ncdump to properly output various new types (ubyte, ushort,
uint, int64, and uint64). [NCF-111]

* Fixed incorrect link flag for szip in configure.ac [NCF-116]

* ncdump -t now properly parses ISO "T" separator in date-time strings.
[NCF-16]

* ncdump -t "human time" functionality now available for attributes and
bounds variables [NCF-70]

* In ncdump, add -g option to support selection of groups for which data
is displayed. [NCF-11]

* Now supports bluefire platform [NCF-52]

* ncdump now properly displays values of attributes of type NC\_USHORT as
signed shorts [NCF-82]

* Rename some code files so that there are no duplicate filenames.
[NCF-99]

* Demonstration of netCDF-4 Performance Improvement with KNMI Data
[NCF-113]

* Dimension size in classic model netCDF-4 files now allows larger sizes
than allowed for 64-bit offset classic files. [NCF-117]

* ncdump now reports correct error message when "-x" option specifying
NcML output is used on netCDF-4 enhanced model input. [NCF-129]

* Fixed bug causing infinite loop in ncdump -c of netCDF-4 file with
subgroup with variables using inherited dimensions. [NCF-136]

## 4.1.3 2011-06-17

* Replace use of --with-hdf5= and other such configure options that
violate conventions and causes build problems. Set environment variables
CPPFLAGS, LDFLAGS, and LD\_LIBRARY\_PATH instead, before running
configure script. [NCF-20]

* Detect from configure script when szlib is needed [NCF-21]

* Fix bug that can silently zero out portions of a file when writing data
in nofill mode beyond the end of a file, crossing disk-block boundaries
with region to be written while in-memory buffer is in a specific state.
This bug was observed disabling fill mode using Lustre (or other large
blksize file system) and writing data slices in reverse order on disk.
[NCF-22]

* Fix bug that prevents netCDF-4/HDF5 files created with netCDF-4.1.2 from
being read by earlier versions of netCDF or HDF5 versions before 1.8.7.
[NCF-23]

* Fix bug in configure that did not make the search for the xdr library
depend on --enable-dap. [NCF-41]

* Fix ncgen bug that did not use the value of a \_Format attribute in the
input CDL file to determine the kind of output file created, when not
specified by the -k command-line flag. [NCF-42]

* Fix ncgen bug, not properly handling unsigned longlong parsing. [NCF-43]

* Fix DAP client code to suppress variables with names such as "x.y",
which DAP protocol interprets as variable "y" inside container "x". Such
variables will be invisible when accessed through DAP client. [NCF-47]

* Define uint type for unsigned integer, if not otherwise available.
Symptom was compile error involving uint in putget.c. [NCF-49]

* Fix username+password handling in the DAP client code. [NCF-50]

* Add test for handling parallel I/O problem from f77 when user forgets to
turn on one of the two MPI flags. [NCF-60]

* Resolved "make check" problems when ifort compiler. Some "make install"
problems remain when using MPI and shared libraries. [NCF-61]

* Fix problem with f90\_def\_var not always handle deflate setting when
compiler was ifort. [NCF-67]

* Check that either MPIIO or MPIPOSIX flag is set when parallel create or
open is called. Also fix examples that didn't set at least one of these
flags. [NCF-68]

* Improve documentation on handling client-side certificates [NCF-48]

* Document that array arguments, except in varm functions, must point to
contiguous blocks of memory. [NCF-69]

* Get netCDF-4 tests working for DLLs generated with mingw. [NCF-6]

* Make changes necessary for upgrading to HDF5 1.8.7 [NCF-66]

### 4.1.3-rc1 2011-05-06

* Stop looking for xdr if --disable-dap is used.

* Don't try to run (some) fortran configure tests on machines with no
fortran.

* Allow nccopy to rechunk with chunksizes larger than current dimension
lengths.

* Initial implementation of CDMREMOTE is complete; needs comprehensive
testing.

### 4.1.3-beta1 2011-04-29

* Fixed szlib not linking bug.

* Fixed dreaded "nofill bug", lurking in netCDF classic since at least
1999. Writing more than a disk block's worth of data that crossed disk
block boundaries more than a disk block beyond the end of file in nofill
mode could zero out recently written earlier data that hadn't yet been
flushed to disk.

* Changed setting for H5Pset\_libver\_bounds to ensure that all netCDF-4
files can be read by HDF5 1.8.x.

* Merged libncdap3 and libncdap4 into new libdap2 library. The suffix dap2
now refers to the dap protocol. This is in prep for adding dap4 protocol
support.

* Took out --with-hdf5 and related options due to high cost of maintaining
this non-standard way of finding libraries.

## 4.1.2 2011-03-29

* Changes in build system to support building dlls on cygwin/mingw32.

* Changes to fix portability problems and get things running on all test
platforms.

* Some minor documentation fixes.

* Fixed opendap performance bug for nc\_get\_vars; required adding
nc\_get\_var{s,m} to the dispatch table.

* Now check for libz in configure.ac.

* Fixed some bugs and some performance problems with default chunksizes.

### 4.1.2-beta2 2011-01-11

* Add "-c" option to nccopy to specify chunk sizes used in output in terms
of list of dimension names.

* Rewrite netCDF-4 attribute put code for a large speedup when writing
lots of attributes.

* Fix nc-config --libs when static dependent libraries are not installed
in the same directory as netCDF libraries (thanks to Jeff Whitaker).

* Build shared libraries by default, requiring separate Fortran library.
Static libraries now built only with --disable-shared.

* Refactor of HDF5 file metadata scan for large speedup in opening files,
especially large files.

* Complete rewrite of the handling of character datalist constants. The
heuristics are documented in ncgen.1.

* Eliminate use of NC\_MAX\_DIMS and NC\_MAX\_VARS in ncdump and nccopy,
allocating memory as needed and reducing their memory footprint.

* Add documentation for new nc\_inq\_path() function.

* Use hashing to speedup lookups by name for files with lots of dimensions
and variables (thanks to Greg Sjaardema).

* Add options to nccopy to support uniform compression of variables in
output, shuffling, and fixing unlimited dimensions. Documented in
nccopy.1 man page and User's Guide.

### 4.1.2-beta1 2010-07-09

* Fix "ncdump -c" bug identifying coordinate variables in groups.

* Fix bug in libsrc/posixio.c when providing sizehint larger than default,
which then doesn't get used (thanks to Harald Anlauf).

* Fix netCDF-4 bug caused when doing enddef/redef and then defining
coordinate variable out of order.

* Fixed bug in man4 directory automake file which caused documentation to
be rebuilt after make clean.

* Turned off HDF5 caching when parallel I/O is in use because of its
memory use.

* Refactoring of netCDF code with dispatch layer to decide whether to call
netCDF classic, netCDF-4, or opendap version of a function.

* Refactoring of netCDF-4 memory internals to reduce memory use and end
dependence on NC\_MAX\_DIMS and NC\_MAX\_NAME.

* Modified constraint parser to be more compatible with a java version of
the parser.

* Modified ncgen to utilize iterators internally; should be no user
visible effect.

* Fixed two large-file bugs with using classic format or 64-bit offset
format and accessing multidimensional variables with more than 2\*\*32
values.

## 4.1.1 2010-04-01

* Fixed various build issues.

* Fixed various memory bugs.

* Fixed bug for netCDF-4 files with dimensions and coord vars written in
different orders, with data writes interspersed.

* Added test for HDF5-1.8.4 bug.

* Added new C++ API from Lynton Appel.

## 4.1 2010-01-30

* Much better memory leak checking with valgrind.

* Added per-variable chunk cache control for better performance. Use
nc\_set\_var\_chunk\_cache / nf\_set\_var\_chunk\_cache /
nf90\_set\_var\_chunk\_cache to set the per-variable cache.

* Automatically set per-variable chunk cache when opening a file, or
creating a variable, so that the cache is big enough for more than one
chunk. (Can be overridden by user). Settings may be changed with
configure options --max-default-chunk-size and
--default-chunks-in-cache.

* Better default chunks size. Now chunks are sized to fit inside the
DEFAULT\_CHUNK\_SIZE (settable at configure time with
--with-default-chunk-size= option.)

* Added nccopy utility for converting among netCDF format variants or to
copy data from DAP servers to netCDF files.

* The oc library has been modified to allow the occurrence of alias
definitions in the DAS, but they will be ignored.

* The old ncgen has been moved to ncgen3 and ncgen is now the new ncgen4.

* Modified --enable-remote-tests to be on by default.

* Fixed the nc\_get\_varm code as applied to DAP data sources.

* Added tests for nc-config.

* Many documentation fixes.

* Added capability to use the PnetCDF library to
perform parallel I/O on classic and 32-bit offset files. Use the
NC\_PNETCDF mode flag to get parallel I/O for non-netcdf-4 files.

* Added libcf library to netCDF distribution. Turn it on with configure
option --with-libcf.

* Added capability to read HDF4 files created with the SD (Scientific
Data) API.

* The DAP support was revised to closely mimic the original libnc-dap
support.

* Significantly revised the data handling mechanism in ncgen4 to more
closely mimic the output from the original ncgen.

* Added prototype NcML output capability to ncgen4. It is specified by the
-lcml flag.

* Added capability to read HDF5 files without dimension scales. This will
allow most existing HDF5 datasets to be read by netCDF-4.

* Fixed bug with endianness of default fill values for integer types when
variables are created with a non-native endianness and use the default
fill value.

* Significant refactoring of HDF5 type handling to improve performance and
handle complicated nesting of types in cross-platform cases.

* Added UDUNITS2 to the distribution. Use --with-udunits to build udunits
along with netcdf.

* Made changes suggested by HDF5 team to relax creation-order requirement
(for read-only cases) which allows HDF5 1.6.x files to be retrofitted
with dimension scales, and be readable to netCDF-4.

* Handle duplicate type names within different groups in ncdump. Fix group
path handling in absolute and relative variable names for "-v" option.

* Added nc-config shell script to help users build netCDF programs without
having to figure out all the compiler options they will need.

* Fixed ncdump -s bug with displaying special attributes for classic and
64-bit offset files.

* For writers, nc\_sync() now calls fsync() to flush data to disk sooner.

* The nc\_inq\_type() function now works for primitive types.

## 4.0.1 2009-03-26

* Added optional arguments to F90 API to nf90\_open/create,
nf90\_create\_var, and nf90\_inquire\_variable so that all netCDF-4
settings may be accomplished with optional arguments, instead of
separate function calls.

* Added control of HDF5 chunk cache to allow for user performance tuning.

* Added parallel example program in F90.

* Changed default chunking to better handle very large variables.

* Made contiguous the default for fixed size data sets with no filters.

* Fixed bug in nc\_inq\_ncid; now it returns NC\_ENOGRP if the named group
is not found.

* Fixed man pages for C and F77 so that netCDF-4 builds will result in man
pages that document new netCDF-4 functions.

* Added OPeNDAP support based on a new C-only implementation. This is
enabled using --enable-dap option and requires libcurl. The configure
script will attempt to locate libcurl, but if it fails, then its
location must be specified by the --with-curl option.

### 4.0.1-beta2 2008-12-26

* Changed chunksizes to size\_t from int.

* Fixed fill value problem from F77 API.

* Fixed problems in netcdf-4 files with multi-dimensional coordinate
variables.

* Fixed ncgen to properly handle CDL input that uses Windows line endings
("\r\n"), instead of getting a syntax error.

* Added "-s" option to ncdump to display performance characteristics of
netCDF-4 files as special virtual attributes, such as \_Chunking,
\_DeflateLevel, \_Format, and \_Endianness.

* Added "-t" option to ncdump to display times in human readable form as
strings. Added code to interpret "calendar" attribute according to CF
conventions, if present, in displaying human-readable times.

* Added experimental version of ncgen4 capable of generating netcdf-4 data
files and C code for creating them. In addition, it supports the special
attributes \_Format, etc.

* 4.0.1-beta1 2008-10-16

* Fixed Fortran 90 int64 problems.

* Rewrote HDF5 read/write code in accordance with performance advice from
Kent.

* Fixed memory leaks in gets/puts of HDF5 data.

* Fixed some broken tests for parallel I/O (i.e. MPI) builds.

* Fixed some cross-compile problems.

* Rewrote code which placed bogus errors on the HDF5 error stack, trying
to open non-existent attributes and variables. Now no HDF5 errors are
seen.

* Removed man subdirectory. Now man4 subdirectory is used for all builds.

* Changed build so that users with access to parallel make can use it.

* Added experimental support for accessing data through OPeNDAP servers
using the DAP protocol (use --enable-opendap to build it).

* Fixed ncdump bugs with array field members of compound type variables.
Fixed ncdump bug of assuming default fill value for data of type
unsigned byte.

## 4.0 2008-05-31

* Introduced the use of HDF5 as a storage layer, which allows use of
groups, user-defined types, multiple unlimited dimensions, compression,
data chunking, parallel I/O, and other features. See the netCDF Users
Guide for more information.

## 3.6.3 2008-05-31

* In ncdump and ncgen, added CDL support for UTF-8 encoding of characters
in names and for escaped special chars in names. Made sure UTF-8 names
are normalized using NFC rules before storing or comparing.

* Handle IEEE NaNs and infinities in a platform-independent way in ncdump
output.

* Added support for ARM representation of doubles, (thanks to Warren
Turkal).

* Fixed bug in C++ API creating 64-bit offset files. (See
https://docs.unidata.ucar.edu/netcdf-c/current/known_problems.html#cxx_64-bit).

* Fixed bug for variables larger than 4 GB. (See
https://docs.unidata.ucar.edu/netcdf-c/current/known_problems.html#large_vars_362).

* Changed the configure.ac to build either 3.6.x or 4.x build from the
same configure.ac.

* Build now checks gfortran version and handles it cleanly, also Portland
Group in Intel fortran, with various configurations.

* A Fortran netcdf.inc file is now created at build time, based on the
setting of --disable-v2.

* Documentation has been fixed in several places.

* Upgraded to automake 1.10, autoconf 2.62, and libtool 2.2.2.

* Includes missing Windows Visual Studio build files.

* Fixed missing include of config.h in a C++ test program.

* Fixed maintainer-clean in man directory.

* Fixed --enable-c-only and make check.

* Fixed behavior when opening a zero-length file.

* Many portability enhancements to build cleanly on various platforms.

* Turned on some old test programs which were not being used in the build.

## 3.6.2 2007-03-05

* Released.

### 3.6.2 beta6 2007-01-20

* Fine tuning of build system to properly handle cygwin, Mingw, and
strange configuration issues.

* Automake 1.10 has a problem with running our tests on MinGW, so I'm
switching back to automake 1.9.6 for this release.

### 3.6.2 beta5 2006-12-30

* Now netCDF configuration uses autoconf 2.61, and automake 1.10. (Thanks
to Ralf Wildenhues for the patches, and all the autotools help in
general!)

* Final major revision of netCDF tutorial before the 3.6.2 release.

* Now netCDF builds under MinGW, producing a windows DLL with the C and
F77 APIs. Use the --enable-shared --enable-dll --disable-cxx
--disable-f90 flags to configure. (C++ and F90 have never been built as
windows DLLs, but might be in a future release if there is user
interest). This has all been documented in the netCDF Porting and
Installation Guide.

* Now extreme numbers (i.e. those close to the limits of their type) can
be turned off in nc\_test/nf\_test, with --disable-extreme-numbers. It
is turned off automatically for Solaris i386 systems.

* Added --enable-c-only option to configure. This causes only the core
netCDF-3 C library to be built. It's the same as --disable-f77
--disable-cxx --disable-v2 --disable-utilities.

* Added --disable-utilities to turn off building and testing of
ncgen/ncdump.

* Fix a long-standing bug in nf90\_get\_att\_text() pointed out by Ryo
Furue, to make sure resulting string is blank-padded on return. This is
fixed in the Fortran-90 interface, but is impractical to fix in the
Fortran-77 interface implemented via cfortran.h.

* Now large file tests are run if --enable-large-file-tests is used in the
configure.

* For Cray users, the ffio module is used if the --enable-ffio option is
passed to configure.

* Unrolled loops in byte-swapping code used on little-endian platforms to
reduce loop overhead. This optimization resulted in a 22% speedup for
some applications accessing floats or ints (e.g. NCO utilities ncap and
ncbo) and a smaller speedup for shorts or doubles.

* Added "-k" option to ncdump and ncgen, for identifying and specifying
the kind of netCDF file, one of "classic", "64-bit-offset", "hdf5", or
"hdf5-nc3". Removed output of kind of netCDF file in CDL comment
produced by ncdump.

* Fixed bug of ncdump seg-faulting if invoked incorrectly with option like
"-c" or "-h" but no file name.

### 3.6.2 beta4 2006-08-15

* Changed F77/F90 man pages from netcdf.3f and netcdf.3f90 to
netcdf\_f77.3 and netcdf\_f90.3. Also fixed broken install of man pages.

* Changed configure script so that "-g -O2" is no longer set as CFLAGS,
CXXFLAGS, and FFLAGS by default if a GNU compiler is being used. Now
nothing is set.

* Changed configure script so that fortran flag is set in config.h.

* Updated Installation and Porting Guide, C++ Interface Guide, F77 and F90
Interface Guides.

* Build with static libraries by default.

* Added configure option --enable-separate-fortran, which causes the
fortran library to be built separately. This is turned on automatically
for shared libraries.

* Improved clarity of error messages.

* Changed configuration to get cygwin DLL and mingw DLL builds working,
for the C library only (i.e. no F77, F90, or C++ APIs).

* Changed type of ncbyte in C++ interface from unsigned char to signed
char, for consistency with C interface. The C++ documentation warned
this change would eventually occur.

* Changed the C++ interface to use only the netCDF-3 C interface instead
of the older netCDF-2 C interface. This has the added benefit that
on-the-fly numeric conversions are now supported using get methods, for
example you can get data of any type as double. When using --disable-v2
flag to configure, the C++ interface can now be built and installed.

### 3.6.2 beta3 2006-05-24

* Changed to use default prefix of /usr/local instead of package-based
prefix of previous releases of netCDF. Use the --prefix argument to the
configure script to override the default.

* Made separate fortran library file, instead of appending fortran library
functions to the C library file, if --enable-separate-fortran is used
during configure (it's turned on automatically if --enable-shared is
used). If uses, the fortran API users must link to *both* the C library
and the new fortran library, like this: -lnetcdff -lnetcdf

* Added netCDF examples in C, C++, F77, F90, and CDL. See the examples
subdirectory.

* Added the NetCDF Tutorial.

* Minor fixes to some of the netCDF documentation.

* Made it possible to build without V2 API using --disable-v2 from
configure.

* Switched to new build system, with automake and libtool. Now shared
libraries are built (as well as static ones) on platforms which support
it. For more information about shared libraries, see
https://docs.unidata.ucar.edu/netcdf-c/current/faq.html#shared_intro

* Fixed ncdump crash that happened when no arguments were used.

* Fixed for building with gfortran 4.1.0.

* Important fix for machines whose SIZEOF\_SIZE\_T != SIZEOF\_LONG, such
as NEC-SX, thanks to Stephen Leak.

* Fixed C++ on AIX platform.

* Fixed 64-bit builds on AIX platform.

* Removed bad assertion that could be triggered in rare cases when reading
a small file.

* Added comments in v1hpg.c to clarify purpose of each internal function.

* Make sure filesize is determined in nc\_close() *after* buffers get
flushed.

* Fix long-standing problem resulting in files up to 3 bytes longer than
necessary if there is exactly one record variable of type byte, char, or
short and if the number of values per record for that variable is not
divisible by 4 (or 2 in the case of short). Now the filesize determined
from header info by NC\_calcsize should be correct in all cases.

## 3.6.1 2006-01-31

* Updated installation manual for 3.6.1.

* Changed installation to try to provide correct compiler flags for
compiling in 64-bit mode on Sun, Irix, AIX, and HPUX. (HPUX doesn't work
for me, however). Now run configure with --enable-64bit to get a 64 bit
compile.

* Fixed long-standing bug that would cause small netCDF files to be padded
on the end with zero bytes to 4096 bytes when they were opened and
changed. Now small files should stay small after you change a value.

* Fixed bug in assertions in putget.c that would only be noticed if you
change the manifest constant NC\_MAX\_DIMS in netcdf.h to be different
from NC\_MAX\_VAR\_DIMS.

* Moved test ftest.F from fortran to nf\_test directory, and fixed bug in
ftest.F which caused it to return 0 even if tests failed (no tests were
failing, however). Also renamed some test output files to make things a
little clearer.

* If open for writing, pad with up to 3 extra zero bytes before close to
the correct canonical length, calculated from the header. Previously
files could be short due to not padding when writing in NOFILL mode.

* Doubled arbitrary limits on number of dimensions, variables, attributes,
and length of names.

* Change name of nc\_get\_format() to nc\_inq\_format(). Add analogous
interfaces for nf\_inq\_format(), nf90\_inquire(), and
NcFile::get\_format() to f77, f90, and C++ interfaces. Document new
function in texinfo files. Add minimal test to nc\_test, nf\_test.

### 3.6.1-beta3 2005-02-17

* Added function nc\_get\_format(int ncid, int\* formatp) that returns
either NC\_FORMAT\_CLASSIC or NC\_FORMAT\_64BIT for a CDF1 or CDF2 file,
respectively.

* Added test to nc\_test that detects whether format version was changed
after a file is reopened and define mode is entered.

* Correctly configure for Intel ifort Fortran compiler on Linux.

### 3.6.0-p1 2005-02-18

* Fixed bug that changes CDF2 files to CDF1 files if CDF2 file is reopened
for write access and either an attribute is changed or define mode is
entered.

### 3.6.1-beta2 2005-1-6

* Fixed absoft compile problem. Maybe.

### 3.6.1-beta1 2005-1-3

* Fixed Cygwin C++ problem.

* Fixed large file problem in MS Visual C++.NET environment.

* More information in installation and porting guide.

## 3.6.0 2004-12-16

* Added texinfo source for the documentation.

* Added large file tests to Windows directory in distribution.

* Modified win32 visual studio project files so that m4 is no longer
required to build netcdf under visual studio.

* Modified rules.make to use install instead of cp, fixing install problem
for cygwin users.

* Modified configure/install stuff to support HP-UX.

* Modified configure/install stuff to support G95.

* In the f90 interface, applied Arnaud Desitter's fixes to correct
mismatches between scalar and array arguments, eliminating (legitimate)
complaints by the NAGWare f95 compiler. Also fixed bugs introduced in
3.6.0-beta5 in the mapped array interfaces.

### 3.6.0-beta6 2004-10-05

* Fixed AIX 64-bit/largefile install problems.

* Removed FAQ section from netcdf.texi User's Guide, in deference to
online version that can be kept up to date more easily.

### 3.6.0-beta5 2004-10-04

* Fixed assertion violation on 64-bit platforms when size of last fixed
size variable exceeds 2\^32 - 1.

* Removed another restriction on file size by making record size (derived
from other sizes, not part of the format) an off\_t instead of a
size\_t, when an off\_t is larger than a size\_t. This permits records
to be *much* larger in either classic format or 64-bit-offset format.

* Incorporated patch from Mathis Rosenhauer to improve performance of
Fortran 90 interface for calls to nf90\_put\_var\_TYPE(),
nf90\_get\_var\_TYPE(), nf90\_put\_vara\_TYPE(), and
nf90\_get\_vara\_TYPE() functions by not emulating them with the
corresponding nf90\_put\_varm\_TYPE() and nf90\_get\_varm\_TYPE() calls.

* Added tests for invalid offsets in classic format when defining multiple
large variables.

* Improved installation ease. Have configure script use Large File Support
as a default, if available.

* Add "extra\_test" as a target for testing Large File Support.

### 3.6.0-beta3 2004-08-24

* Upgraded to recent autoconf, changed configure to (hopefully) improve
installation. Also added macros to deal with large file systems.

* Added nf\_set\_default\_format to Fortran interface.

* Added testing to the set\_default\_format functions to nc\_test and
nf\_test.

* Added documentation to the man page for set\_default\_format functions.

* Added two new error return codes to C, f77, and f90 interfaces for
invalid dimension size and for bad variable size. Made test for max
dimension size depend on whether 64-bit offsets used. Fixed bug with
dimension sizes between 2\^31 and 2\^32 (for byte variables).

* Fixed ncdump to properly print dimensions larger than 2\^31.

* Fixed ncgen to properly handle dimensions between 2\^31 and 2\^32.

### 3.6.0-beta2

* Added -v2 (version 2 format with 64-bit offsets) option to
ncgen, to specify that generated files or generated C/Fortran code
should create 64-bit offset files. Also added -x option to ncgen to
specify use of no-fill mode for fast creation of large files.

* Added function to set default create mode to C interface
(nc\_set\_default\_create).

* Added win32 directory, with NET subdirectory to hold .NET port of
netCDF. To use, open netcdf.sln with Visual Studio, and do a clean and
then a build of either the debug or release builds. Tests will be run as
part of the build process. VC++ with managed extensions is required
(i.e. VC++.NET).

* Added windows installer files to build windows binary installs.

### 3.6.0-beta1

* By incorporating Greg Sjaardema's patch, added support for
64-bit offset files, which remove many of the restrictions relating to
very large files (i.e. larger than 2 GB.) This introduces a new data
format for the first time since the original netCDF format was
introduced. Files in this new 64-bit offset format can't be read by
earlier versions of netCDF. Users should continue to use the netCDF
classic format unless they need to create very large files.

* The test suite, nc\_test, will now be run twice, once for netCDF classic
format testing, and once for 64-bit offset format testing.

* The implementation of the Fortran-77 interface has been adapted to
version 4.3 of Burkhard Burow's "cfortran.h".

### 3.6.0-alpha

* Added NEC SX specific optimization for NFILL tunable
parameter in libsrc/putget.c

Added support for the ifc Fortran-90 compiler creating files "netcdf.d"
and "typesizes.d" (instead of ".mod" files).

* Fixed access to iargc and getarg functions from Fortran-90 for NAG f90
compiler, contributed by Harald Anlauf.

## 3.5.1 2004-02-03

* Updated INSTALL.html for Mac OS X (Darwin).

* Made the installation of the netCDF Fortran-90 module file more robust
regarding the name of the file.

* Added support for eight-byte integers in Fortran90 interface.

* Increased advisory limits in C netcdf.h and Fortran netcdf.inc for
maximum number of dimensions, variables, and attributes.

* Changed C++ declarations "friend NcFile" to "friend class NcFile" in
cxx/netcdfcpp.h to conform to standard.

* Added Dan Schmitt's backward compatible extension to the C++ record
interface to work with arbitrary dimension slices.

* Added C++ documentation note that caller is responsible for deleting
pointer returned by Variable::values() method when no longer needed.

* Made C++ interface more standard; the result may not compile on some old
pre-standard C++ compilers.

* Fixed bug in ncgen when parsing values of a multidimensional char
variable that resulted in failure to pad a value with nulls on IRIX.

* Fixed ncdump bug adding extra quote to char variable data when using -fc
or -ff option.

* Fixed so compiling with -DNO\_NETCDF\_2 will work for building without
backward-compatibility netCDF-2 interfaces.

* Eliminated use of ftruncate(), because it fails on FAT32 file systems
under Linux.

* Initialized a pointer in putget.m4 (used to generate putget.c) that was
involved in uninitialized memory references when nc\_test is run under
Purify. Two users had reported seeing crashes resulting from this
problem in their applications.

* Reverted pointer initializations in putget.m4, after testing revealed
these caused a performance problem, resulting in many extra calls to
px\_pgin and px\_pgout when running nc\_test.

* Added checking of size of "dimids" vector in function
nf90\_inquire\_variable(...) and error-returning if it isn't
sufficiently capacious.

* Added variable index to ncvarget() and ncattinq() error messages and
attribute name to ncattinq() error message.

* Tweaked configure script to work with recent C++ compilers.

* Fixed a memory leak in C++ interface, making sure NcVar::cur\_rec[] gets
deleted in NcVar destructor.

* Reimplemented nc\_sync() fix of version 3.5.0 to eliminate performance
penalty when synchronization is unnecessary.

* Changed order of targets in Makefile to build Fortran interface last, as
a workaround for problem with make on AIX platforms.

## 3.5.0 2001-03-23

* Added Fortran 90 interface.

* Changed C macro TIMELEN in file cxx/nctst.cpp to TIMESTRINGLEN to avoid
clash with macro defined on AIX systems in /usr/include/time.h.

* Fixed miswriting of netCDF header when exiting define mode. Because the
header was always written correctly later, this was only a problem if
there was another reader of the netCDF file.

* Fixed explicit synchronizing between netCDF writer and readers via the
nc\_sync(), nf\_sync(), and ncsync() functions.

* Fixed a number of bugs related to attempts to support shrinking the
header in netCDF files when attributes are rewritten or deleted. Also
fixed the problem that nc\_\_endef() did not work as intended in
reserving extra space in the file header, since the extra space would be
compacted again on calling nc\_close().

* Fixed the "redef bug" that occurred when nc\_enddef() or nf\_enddef() is
called after nc\_redef() or nf\_redef(), the file is growing such that
the new beginning of a record variable is in the next "chunk", and the
size of at least one record variable exceeds the chunk size (see
netcdf.3 man page for a description of this tuning parameter and how to
set it). This bug resulted in corruption of some values in other
variables than the one being added.

* The "\*\*" tuning functions for the Fortran interface, nf\*\*create,
nf\*\*open, and nf\*\*enddef, are now documented in the Fortran interface
man pages.

* Add an 'uninstall' target to all the Makefiles. Dave Glowacki
<dglo@SSEC.WISC.EDU> 199810011851.MAA27335

* Added support for multiprocessing on Cray T3E. Hooks added by Glenn, but
the majority of the work was done at NERSC. Also includes changes to
ffio option specification. Patch rollup provided by R. K. Owen
<rkowen@Nersc.GOV>. The following functions are added to the public
interface. nc**create\_mp() nc**open\_mp() nc\_set\_base\_pe()
nc\_inq\_base\_pe()

* Fixed makefile URL for Win32 systems in INSTALL file.

* Made test for UNICOS system in the configure script case independent.

* Ported to the following systems: AIX 4.3 (both /bin/xlc and
/usr/vac/bin/xlc compilers) IRIX 6.5 IRIX64 6.5

* Changed the extension of C++ files from ".cc" to ".cpp". Renamed the C++
interface header file "netcdfcpp.h" instead of "netcdf.hh", changing
"netcdf.hh" to include "netcdfcpp.h" for backward compatibility.

* Treat "FreeBSD" systems the same as "BSD/OS" system w.r.t. Fortran and
"whatis" database.

* Corrected manual pages: corrected spelling of "enddef" (was "endef") and
ensured that the words "index" and "format" will be correctly printed.

* Updated support for Fortran-calling-C interface by updating
"fortran/cfortran.h" from version 3.9 to version 4.1. This new version
supports the Portland Group Fortran compiler (C macro "pgiFortran") and
the Absoft Pro Fortran compiler (C macro "AbsoftProFortran").

* Corrected use of non-integral-constant-expression in specifying size of
temporary arrays in file "libsrc/ncx\_cray.c".

* Added Compaq Alpha Linux workstation example to INSTALL file.

* Ported cfortran.h to Cygnus GNU Win32 C compiler (gcc for Windows).

* Fixed bug in ncdump using same CDL header name when called with multiple
files.

* Added new NULL data type NC\_NAT (Not A Type) to facilitate checking
whether a variable object has had its type defined yet, for example when
working with packed values.

* Fixed use of compile-time macro NO\_NETCDF\_2 so it really doesn't
include old netCDF-2 interfaces, as intended.

* Ported to MacOS X Public Beta (Darwin 1.2/PowerPC).

* Fixed C++ friend declarations to conform to C++ standard.

* Changed INSTALL file to INSTALL.html instead.

## 3.4 1998-03-09

* Fixed ncx\_cray.c to work on all CRAY systems, not just CRAY1. Reworked
USE\_IEG, which was incorrect. Reworked short support. Now USE\_IEG and
otherwise both pass t\_ncx.

* To better support parallel systems, static and malloc'ed scratch areas
which were shared in the library were eliminated. These were made
private and on the stack where possible. To support this, the macros
ALLOC\_ONSTACK and FREE\_ONSTACK are defined in onstack.h.

* The buffered i/o system implementation in posixio.c was reimplemented to
limit the number and size of read() or write() system calls and use
greater reliance on memory to memory copy. This saves a great deal of
wall clock time on slow (NFS) filesystems, especially during
nc\_endef().

* Added performance tuning "underbar underbar" interfaces nc**open(),
nc**create(), and nc\_\_enddef().

* The 'sizehint' contract between the higher layers and the ncio layer is
consistently enforced.

* The C++ interface has been updated so that the deprecated "nclong"
typedef should no longer be required, and casts to nclong no longer
necessary. Just use int or long as appropriate. nclong is still
supported for backwards compatibility.

* The ncdump utility now displays byte values as signed, even on platforms
where the type corresponding to a C char is unsigned (SGI, for example).
Also the ncdump and ncgen utilities have been updated to display and
accept byte attributes as signed numeric values (with a "b" suffix)
instead of using character constants.

* In libsrc/error.c:nc\_strerror(int), explain that NC\_EBADTYPE applies
to "\_FillValue type mismatch".

* Some changes to configure scripts (aclocal.m4), macros.make.in and
ncgen/Makefile to support NEC SUPER-UX 7.2.

* The "usage" messages of ncgen and ncdump include the string returned
from nc\_inq\_libvers().

* Corrected some casts in the library so that all phases of the arithmetic
computing file offsets occurs with "off\_t" type. This allows certain
larger netcdf files to be created and read on systems with larger
(64bit) off\_t.

* In ncgen, multidimensional character variables are now padded to the
length of last dimension, instead of just concatenating them. This
restores an undocumented but convenient feature of ncgen under netCDF-2.
Also, a syntax error is now reliably reported if the netcdf name is
omitted in CDL input.

* Fortran and C code generated by ncgen for netCDF components whose names
contain "-" characters will now compile and run correctly instead of
causing syntax errors.

* The library allows "." characters in names as well as "\_" and "-"
characters. A zero length name "" is explicitly not allowed. The ncgen
utility will now permit "." characters in CDL names as well.

* Memory leaks in the C++ interface NcVar::as\_\*() member functions and
NcFile::add\_var() member function are fixed. The documentation was
fixed where it indicated incorrectly that the library managed value
blocks that the user is actually responsible for deleting.

* he values of the version 2 Fortran error codes have been modified to
make the version 2 Fortran interface more backward compatible at the
source level.

* Added support for systems whose Fortran INTEGER*1 and INTEGER*2 types
are equivalent to the C "long" type but whose C "int" and "long" types
differ. An example of such a system is the NEC SX-4 with the "-ew"
option to the f90 compiler (sheesh, what a system!).

* Fixed Version 2 Fortran compatibility bug: NCVGTG, NCVGGC, NCVPTG, and
NCVPGC didn't work according to the Version 2 documentation if the
innermost mapping value (i.e. IMAP[1]) was zero (indicating that the
netCDF structure of the variable should be used).

## 3.3.1 1997-06-16

* One can now inquire about the number of attributes that a variable has
using the global variable ID.

* The FORTRAN interface should now work on more systems. In particular:

* It should now work with FORTRAN compilers whose "integer*1" datatype is
either a C "signed char", "short", or "int" and whose "integer*2"
datatype is either a C "short" or "int".

* It should now work with FORTRAN compilers that are extremely picky about
source code formatting (e.g. the NAG f90 compiler).

* The dependency on the non-POSIX utility m4(1) for generating the C and
FORTRAN manual pages has been eliminated.

* EXTERNAL statements have been added to the FORTRAN include-file
"netcdf.inc" to eliminate excessive warnings about "unused" variables
(which were actually functions) by some compilers (e.g. SunOS 4.1.3's
f77(1) version 1.x).

* Building the netCDF-3 package no longer requires the existence of the
Standard C macro RAND\_MAX.

* Fixed an ncdump bug resulting in ncdump reporting Attempt to convert
between text & numbers when \_FillValue attribute of a character
variable set to the empty string "".

* Made ncgen tests more stringent and fixed various bugs this uncovered.
These included bugs in handling byte attributes on platforms on which
char is unsigned, initializing scalar character variables in generated C
code under "-c" option, interspersing DATA statements with declaration
statements in generated Fortran code under "-f" option, handling empty
string as a value correctly in generated C and Fortran, and handling
escape characters in strings. The Fortran output under the "-f" option
was also made less obscure and more portable, using automatic conversion
with netCDF-3 interfaces instead of "BYTE", "INTEGER*1", or "INTEGER*2"
declarations.

* Fixed a C++ interface problem that prevented compiling the C++ library
with Digital's cxx compiler.

* Made ncgen "make test" report failure and stop if test resulted in a
failure of generated C or Fortran code.

* The file that you are now reading was created to contain a high-level
description of the evolution of the netCDF-3 package.

## 3.3 1997-05-15

* The production version of the netCDF-3 package was released.

* A comparison of the netCDF-2 and netCDF-3 releases can be found in the
file COMPATIBILITY.

*/
