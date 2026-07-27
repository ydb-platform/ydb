/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <cmath>
#include <limits>
#include <string>
#include <sstream>
#include <vector>
#include <fstream>
#include <set>

#include "init.h"
#include "global.h"
#include "utils.h"
#include "memory.h"
#include "ztimer.h"
#include "experiments.h"
#include "experimentconf.h"
#include "space.h"
#include "spacefactory.h"
#include "logging.h"
#include "methodfactory.h"

#include "meta_analysis.h"
#include "params.h"

#include "test_integr_util.h"
#include "testdataset.h"

using namespace similarity;

using std::set;
using std::multimap;
using std::vector;
using std::string;
using std::stringstream;

//#define MAX_THREAD_QTY 4
#define MAX_THREAD_QTY 1
#define TEST_SET_QTY   3
#define MAX_NUM_QUERY  700

#define INDEX_FILE_NAME "index.tmp" 

#define TEST_HNSW 1
#define TEST_SW_GRAPH 1
#define TEST_IR 1
#define TEST_NAPP 1
#define TEST_OTHER 1

vector<MethodTestCase>    vTestCaseDesc = {
#if (TEST_HNSW)
  // Make sure, it works with huge M
  MethodTestCase(DIST_TYPE_FLOAT, "cosinesimil_sparse", "sparse_5K.txt", "hnsw", true, "efConstruction=100,M=400", "ef=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.98, 0.9999, 0.0, 1, 1.3, 2.2),  
  MethodTestCase(DIST_TYPE_FLOAT, "cosinesimil_sparse", "sparse_5K.txt", "hnsw", true, "efConstruction=200,M=10", "ef=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.88, 0.96, 0.0, 1, 6, 12),  
  MethodTestCase(DIST_TYPE_FLOAT, "cosinesimil_sparse", "sparse_5K.txt", "hnsw", true, "efConstruction=200,M=10", "ef=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.88, 0.96, 0.0, 1, 6, 12),  
  MethodTestCase(DIST_TYPE_FLOAT, "angulardist_sparse", "sparse_5K.txt", "hnsw", true, "efConstruction=200,M=10", "ef=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.88, 0.96, 0.0, 1, 6, 12),  
  MethodTestCase(DIST_TYPE_FLOAT, "cosinesimil", "final8_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=1", "ef=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.96, 1, 0, 0.1, 40, 60),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=1", "ef=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.96, 1, 0, 0.1, 40, 60),



  // Now a few optimized versions
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=0", "ef=50",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 -1, -1, /* -1 means no testing for the improv. in # of dist computation, which cannot be measured for optimized indices */
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "cosinesimil", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=0", "ef=100",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 -1, -1, /* -1 means no testing for the improv. in # of dist computation, which cannot be measured for optimized indices */
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "negdotprod", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=0", "ef=200",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 -1, -1, /* -1 means no testing for the improv. in # of dist computation, which cannot be measured for optimized indices */
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "l1", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=0", "ef=200",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 -1, -1, /* -1 means no testing for the improv. in # of dist computation, which cannot be measured for optimized indices */
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "linf", "final128_10K.txt", "hnsw", true, "efConstruction=400,M=10,skip_optimized_index=0", "ef=400",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 -1, -1, /* -1 means no testing for the improv. in # of dist computation, which cannot be measured for optimized indices */
                 true /* recall only */),

  // ... and their non-optimized versions
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=1", "ef=50",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 22, 37,
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "cosinesimil", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=1", "ef=100",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 12, 25,
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "negdotprod", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=1", "ef=200",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 5, 15,
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "l1", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=1", "ef=200",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 7, 8,
                 true /* recall only */),
  MethodTestCase(DIST_TYPE_FLOAT, "linf", "final128_10K.txt", "hnsw", true, "efConstruction=200,M=10,skip_optimized_index=1", "ef=400",
                 10 /* KNN-10 */, 0 /* no range search */ , 0.98, 1, 0, 0.05,
                 3.5, 4.5,
                 true /* recall only */),
#endif

#if (TEST_SW_GRAPH)
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "sw-graph", true, "NN=10", "",
                1 /* KNN-1 */, 0 /* no range search */ , 0.9, 1.0, 0, 1.0, 36, 55),  
  MethodTestCase(DIST_TYPE_FLOAT, "cosinesimil_sparse_fast", "sparse_5K.txt", "sw-graph", true, "efConstruction=200,NN=10", "efSearch=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.88, 0.96, 0.0, 1, 5, 10),  
  MethodTestCase(DIST_TYPE_FLOAT, "angulardist_sparse_fast", "sparse_5K.txt", "sw-graph", true, "efConstruction=200,NN=10", "efSearch=50", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.88, 0.96, 0.0, 1, 5, 10),  
#endif


#if (TEST_IR)
  MethodTestCase(DIST_TYPE_FLOAT, "negdotprod_sparse_fast", "sparse_5K.txt", "simple_invindx", false, "", "", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.999, 1.0, 0.0, 0.001, 395, 510),  
  MethodTestCase(DIST_TYPE_FLOAT, "negdotprod_sparse_fast", "sparse_5K.txt", "simple_invindx", true, "", "", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.999, 1.0, 0.0, 0.001, 395, 510),  
#endif

#if (TEST_NAPP)
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "napp", true, "numPivot=8,numPivotIndex=8,chunkIndexSize=102", "numPivotSearch=8",
                1 /* KNN-1 */, 0 /* no range search */ , 0.999, 1.0, 0, 0.01, 0.99, 1.01),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "napp", true, "numPivot=8,numPivotIndex=8,chunkIndexSize=102", "numPivotSearch=8",
                1 /* KNN-1 */, 0 /* no range search */ , 0.999, 1.0, 0, 0.01, 0.99, 1.01),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "napp", true, "numPivot=32,numPivotIndex=8,chunkIndexSize=102", "numPivotSearch=8",
                1 /* KNN-1 */, 0 /* no range search */ , 0.6, 0.8, 2.0, 3.7, 20, 33),
#endif


#if (TEST_OTHER)

  // *************** Sequential search tests ********** //
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "seq_search", false, "", "",
                1 /* KNN-1 */, 0 /* no range search */ , 1.0, 1.0, 0, 0, 1, 1),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "seq_search", false, "", "",
                0 /* no-knn search */, 0.2 /* range 0.2 */ , 1.0, 1.0, 0, 0, 1, 1),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "seq_search", false, "multiThread=1,threadQty=4", "",
                1 /* KNN-1 */, 0 /* no range search */ , 1.0, 1.0, 0, 0, 1, 1),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "seq_search", false, "multiThread=1,threadQty=4", "",
                0 /* no-knn search */, 0.2 /* range 0.2 */ , 1.0, 1.0, 0, 0, 1, 1),  

  // *************** VP-tree tests ******************** //
  // knn
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10",  "",
                1 /* KNN-1 */, 0 /* no range search */ , 1.0, 1.0, 0.0, 0.0, 40, 80),  

  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", true /* test index reloading */, "chunkBucket=1,bucketSize=10",  "",
                1 /* KNN-1 */, 0 /* no range search */ , 1.0, 1.0, 0.0, 0.0, 40, 80),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", true /* test index reloading */, "chunkBucket=0,bucketSize=10" /* test without optimized buckets */,  "",
                1 /* KNN-1 */, 0 /* no range search */ , 1.0, 1.0, 0.0, 0.0, 40, 80),  


  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "alphaLeft=2,alphaRight=2", 
                1 /* KNN-1 */, 0 /* no range search */ , 0.93, 0.97, 0.03, 0.09, 120, 190),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final128_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "",
                1 /* KNN-1 */, 0 /* no range search */ , 1.0, 1.0, 0.0, 0.0, 1.5, 2.5),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final128_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "alphaLeft=2,alphaRight=2", 
                1 /* KNN-1 */, 0 /* no range search */ , 0.98, 1.0, 0.0, 0.02, 2.8, 5.5),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "",
                10 /* KNN-10 */, 0 /* no range search */ , 1.0, 1.0, 0.0, 0.0, 20, 30),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "alphaLeft=2,alphaRight=2", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.93, 0.96, 0.0, 0.02, 56, 80),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final128_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "",
                10 /* KNN-10 */, 0 /* no range search */ , 1.0, 1.0, 0.0, 0.0, 1.1, 1.6),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final128_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "alphaLeft=2,alphaRight=2", 
                10 /* KNN-10 */, 0 /* no range search */ , 0.98, 0.999, 0.0, 0.01, 1.5, 2.5),  
  // range
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "",
                0 /* no KNN */, 0.1 /* range search radius 0.1 */ , 1.0, 1.0, 0.0, 0.0, 23, 30),  
  MethodTestCase(DIST_TYPE_FLOAT, "l2", "final8_10K.txt", "vptree", false, "chunkBucket=1,bucketSize=10", "",
                0 /* no KNN */, 0.5 /* range search radius 0.5 */ , 1.0, 1.0, 0.0, 0.0, 2.4, 4),  


#endif
};


int main(int ac, char* av[]) {
  // This should be the first function called before
  string LogFile;
  if (ac == 2) LogFile = av[1];

  initLibrary(0, LogFile.empty() ? LIB_LOGSTDERR:LIB_LOGFILE, LogFile.c_str());

  WallClockTimer timer;
  timer.reset();

  set<string>           setDistType;
  set<string>           setSpaceType;
  set<string>           setDataSet;
  set<unsigned>         setKNN;
  set<float>            setRange;

  for (const auto& testCase: vTestCaseDesc) {
    setDistType.insert(testCase.distType_);
    setSpaceType.insert(testCase.spaceType_);
    setDataSet.insert(testCase.dataSet_);
    if (testCase.mKNN > 0)
      setKNN.insert(testCase.mKNN);
    if (testCase.mRange > 0)
      setRange.insert(testCase.mRange);
  };  

  size_t nTest = 0;
  size_t nFail = 0;

  try {
    for (int bTestReload = 0; bTestReload < 2; ++bTestReload) {
      cout << "Testing index reload: " << (bTestReload == 1) << endl;
      cout << "==================================================" << endl;
      /* 
       * 1. Let's iterate over all combinations of data sets,
       * distance, and space types. 
       * 2. For each combination, we select test cases 
       * with exactly same data set, distance and space type. 
       * 3. Create an array of arguments in the same format
       *    as used by the main benchmarking utility.
       * 4. Use a standard function to parse these arguments.
       */
      for (string dataSet  : setDataSet)
      for (string distType : setDistType)
      for (string spaceType: setSpaceType) {
        string dataFile = sampleDataPrefix + dataSet;

        for (unsigned K: setKNN) {
          vector<MethodTestCase>  vTestCases; 

          // Select appropriate test cases to share the same gold-standard data
          for (const auto& testCase: vTestCaseDesc) {
            if ((bTestReload == 0 || testCase.testReload_)  &&
                testCase.dataSet_ == dataSet &&
                testCase.distType_ == distType && 
                testCase.spaceType_ == spaceType &&
                testCase.mKNN  == K) {
              vTestCases.push_back(MethodTestCase(testCase));
            }
          }

          if (!vTestCases.empty())  { // Not all combinations of spaces, data sets, and search types are non-empty
            for (size_t threadQty = 1; threadQty <= MAX_THREAD_QTY; ++threadQty) {
              nTest += vTestCases.size();
              nFail += RunOneTest(vTestCases, 
                                  bTestReload == 1,
                                  INDEX_FILE_NAME,
                                  distType,
                                  spaceType,
                                  threadQty,
                                  TEST_SET_QTY,
                                  dataFile,
                                  "",
                                  0,
                                  MAX_NUM_QUERY,
                                  ConvertToString(K),
                                  0,
                                  ""
                                 );
            }
          }
        }

        for (float R: setRange) {
          vector<MethodTestCase>  vTestCases; 


          // Select appropriate test cases to share the same gold-standard data
          for (const auto& testCase: vTestCaseDesc) {
            if ((bTestReload == 0 || testCase.testReload_)  &&
                testCase.dataSet_ == dataSet &&
                testCase.distType_ == distType && 
                testCase.spaceType_ == spaceType &&
                testCase.mRange  == R) {
              vTestCases.push_back(MethodTestCase(testCase));
            }
          }

          if (!vTestCases.empty())  { // Not all combinations of spaces, data sets, and search types are non-empty
            for (size_t threadQty = 1; threadQty <= MAX_THREAD_QTY; ++threadQty) {
              nTest += vTestCases.size();
              nFail += RunOneTest(vTestCases, 
                                  bTestReload == 1,
                                  INDEX_FILE_NAME,
                                  distType,
                                  spaceType,
                                  threadQty,
                                  TEST_SET_QTY,
                                  dataFile,
                                  "",
                                  0,
                                  MAX_NUM_QUERY,
                                  "",
                                  0,
                                  ConvertToString(R)
                                 );
            }
          }

        }

      }

    }

  } catch (const std::exception& e) {
    cout << red << "Failure to test due to exception: " << e.what() << no_color << endl;
    ++nFail;
  } catch (...) {
    ++nFail;
    cout << red << "Failure to test due to unknown exception: " << no_color << endl;
  }

  timer.split();

  LOG(LIB_INFO) << "Time elapsed = " << timer.elapsed() / 1e6;
  LOG(LIB_INFO) << "Finished at " << LibGetCurrentTime();

  cout << endl << "==================================================" << endl;
  cout << (nFail ? "FAILURE" : "SUCCESS") << endl;
  cout << "Carried out: " << nTest << "  tests. Failed: " << nFail << " tests" << endl;

  return nFail ? 1:0;
}
