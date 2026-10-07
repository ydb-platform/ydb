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
#ifndef GOLD_STANDARD_H
#define GOLD_STANDARD_H

#include <vector>
#include <algorithm>
#include <iostream>
#include <memory>
#include <cmath>
#include <thread>

#include "object.h"
#include "query_creator.h"
#include "space.h"
#include "utils.h"
#include "ztimer.h"

#define SEQ_SEARCH_TIME        "SeqSearchTime"
#define SEQ_GS_QTY             "GoldStandQty"
#define GS_NOTE_FIELD          "Note"
#define GS_TEST_SET_ID         "TestSetId"
#define GS_THREAD_TEST_QTY     "ThreadTestQty"

namespace similarity {

using std::vector;
using std::ostream;
using std::istream;
using std::unique_ptr;
using std::thread;
using std::ref;


template <class dist_t>
struct ResultEntry {
  IdType      mId;
  LabelType   mLabel;
  dist_t      mDist;
  ResultEntry(IdType id = 0, LabelType label = 0, dist_t dist = 0) 
                 : mId(id), mLabel(label), mDist(dist) {}

  /*
   * Reads entry in the binary format (can't read if the data was written
   * on the machine with another type of endianness.)
   */
  void readBinary(istream& in) {
    in.read(reinterpret_cast<char*>(&mId),    sizeof mId);
    in.read(reinterpret_cast<char*>(&mLabel), sizeof mLabel);
    in.read(reinterpret_cast<char*>(&mDist),  sizeof mDist);
  }
  /*
   * Saves entry in the binary format, see the comment
   * on the endianness above.
   */
  void writeBinary(ostream& out) const {
    out.write(reinterpret_cast<const char*>(&mId),    sizeof mId);
    out.write(reinterpret_cast<const char*>(&mLabel), sizeof mLabel);
    out.write(reinterpret_cast<const char*>(&mDist),  sizeof mDist);
  }
  bool operator<(const ResultEntry& o) const {
    if (mDist != o.mDist) return mDist < o.mDist;
    return mId < o.mId;
  }
  bool operator==(const ResultEntry& o) const {
    return mId    == o.mId    && 
           mDist  == o.mDist  && 
           mLabel == o.mLabel;
  }
};

template <class dist_t>
ostream& operator<<(ostream& out, const ResultEntry<dist_t>& e) {
  return out << "[" << e.mId << " lab=" << e.mLabel << " dist=" << e.mDist << "]";
}

enum ClassResult {
  kClassUnknown,
  kClassCorrect,
  kClassWrong,
};

template <class dist_t>
class GoldStandard {
public:
  GoldStandard(){}
  GoldStandard(const typename similarity::Space<dist_t>& space,
              const ObjectVector& datapoints,
              typename similarity::Query<dist_t>* query,
              float maxKeepEntryCoeff
              ) {
    DoSeqSearch(space, datapoints, query);
    size_t maxKeepEntryQty =
        std::min((size_t)std::round(query->ResultSize()*maxKeepEntryCoeff),
                 SortedAllEntries_.size());

    if (maxKeepEntryQty != 0) {
      CHECK(maxKeepEntryQty <= SortedAllEntries_.size())
      vector<ResultEntry<dist_t>>         tmp(SortedAllEntries_.begin(),
                                              SortedAllEntries_.begin() + maxKeepEntryQty);
      /* 
       * The swap trick actually leads to memory being freed.
       * If we simply erase the extra entries, vector still keeps the memory!
       */
      SortedAllEntries_.swap(tmp);
    }
  }
  /*
   * See the endianness comment.
   */
  void Write(ostream& controlStream, ostream& binaryStream) const {

    WriteField(controlStream, SEQ_SEARCH_TIME, ConvertToString(SeqSearchTime_));
    WriteField(controlStream, SEQ_GS_QTY, ConvertToString(SortedAllEntries_.size()));
    for (size_t i = 0; i < SortedAllEntries_.size(); ++i) {
      SortedAllEntries_[i].writeBinary(binaryStream);
    }
  }

  void Read(istream& controlStream, istream& binaryStream) {
    string s;
    ReadField(controlStream, SEQ_SEARCH_TIME, s);
    ConvertFromString(s, SeqSearchTime_);
    ReadField(controlStream, SEQ_GS_QTY, s);
    size_t qty = 0;
    ConvertFromString(s, qty);
    SortedAllEntries_.resize(qty);
    for (size_t i = 0; i < qty; ++i)
      SortedAllEntries_[i].readBinary(binaryStream);
  }
  uint64_t GetSeqSearchTime()     const { return SeqSearchTime_; }

  /*
   * SortedAllEntries_ include all database entries sorted in the order
   * of increasing distance from the query.
   */
  const vector<ResultEntry<dist_t>>&   GetSortedEntries() const { return  SortedAllEntries_;}
private:
  void DoSeqSearch(const Space<dist_t>& space,
                   const ObjectVector&  datapoints,
                   typename similarity::Query<dist_t>* pQuery) {
    WallClockTimer  wtm;

    wtm.reset();

    SortedAllEntries_.resize(datapoints.size());
    const Object* pQueryObj = pQuery->QueryObject();

    for (size_t i = 0; i < datapoints.size(); ++i) {
      // Distance can be asymmetric, but the query is always on the right side
      SortedAllEntries_[i] = ResultEntry<dist_t>(datapoints[i]->id(),
                                                 datapoints[i]->label(),
                                                 space.IndexTimeDistance(datapoints[i], pQueryObj));
      pQuery->CheckAndAddToResult(SortedAllEntries_[i].mDist, datapoints[i]);
    }

    wtm.split();

    SeqSearchTime_ = wtm.elapsed();

    std::sort(SortedAllEntries_.begin(), SortedAllEntries_.end());
  }

  uint64_t                            SeqSearchTime_;

  vector<ResultEntry<dist_t>>         SortedAllEntries_;
};

template <typename dist_t, typename QueryCreatorType>
struct GoldStandardThreadParams {
  const ExperimentConfig<dist_t>&             config_;
  const QueryCreatorType&                     QueryCreator_;
  float                                       maxKeepEntryCoeff_;
  const unsigned                              ThreadQty_;
  const unsigned                              GoldStandPart_;
  vector<unique_ptr<GoldStandard<dist_t>>>&   vGoldStand_;
  GoldStandardThreadParams(const ExperimentConfig<dist_t>& config,
                           const QueryCreatorType& QueryCreator,
                           float maxKeepEntryCoeff,
                           unsigned ThreadQty,
                           unsigned GoldStandPart,
                           vector<unique_ptr<GoldStandard<dist_t>>>& vGoldStand) : config_(config),
                                                                                   QueryCreator_(QueryCreator),
                                                                                   maxKeepEntryCoeff_(maxKeepEntryCoeff),
                                                                                   ThreadQty_(ThreadQty),
                                                                                   GoldStandPart_(GoldStandPart),
                                                                                   vGoldStand_(vGoldStand) {}
};

template <typename dist_t, typename QueryCreatorType>
struct GoldStandardThread {
  void operator ()(GoldStandardThreadParams<dist_t,QueryCreatorType>& prm) {
    size_t numquery = prm.config_.GetQueryObjects().size();

    unsigned GoldStandPart = prm.GoldStandPart_;
    unsigned ThreadQty = prm.ThreadQty_;

    for (size_t q = 0; q < numquery; ++q) {
      if ((q % ThreadQty) == GoldStandPart) {
        unique_ptr<Query<dist_t>> query(prm.QueryCreator_(prm.config_.GetSpace(),
                                                          prm.config_.GetQueryObjects()[q]));

        prm.vGoldStand_[q].reset(new GoldStandard<dist_t>(prm.config_.GetSpace(),
                                                          prm.config_.GetDataObjects(),
                                                          query.get(),
                                                          prm.maxKeepEntryCoeff_));
      }
    }
  }
};

template <class dist_t>
class GoldStandardManager {
public:
  GoldStandardManager(const ExperimentConfig<dist_t>&  config) :
                      config_(config),
                      vvGoldStandardRange_(config_.GetRange().size()),
                      vvGoldStandardKNN_(config_.GetKNN().size()) {}
  // Both read and Compute can be called multiple times
  // if maxKeepEntryCoeff is non-zero, it defines how many GS entries (relative to the result set size) we memorize
  void Compute(size_t threadQty, float maxKeepEntryCoeff) {
    threadQty = std::max(size_t(1), threadQty);
    LOG(LIB_INFO) << "Computing gold standard data using " << threadQty << " threads, keeping " << maxKeepEntryCoeff<< "x entries compared to the result set size";;
    for (size_t i = 0; i < config_.GetRange().size(); ++i) {
      vvGoldStandardRange_[i].clear();
      const dist_t radius = config_.GetRange()[i];
      RangeCreator<dist_t>  cr(radius);
      procOneSet(cr, vvGoldStandardRange_[i], threadQty, maxKeepEntryCoeff);
    }
    for (size_t i = 0; i < config_.GetKNN().size(); ++i) {
      vvGoldStandardKNN_[i].clear();
      const size_t K = config_.GetKNN()[i];
      KNNCreator<dist_t>  cr(K, config_.GetEPS());
      procOneSet(cr, vvGoldStandardKNN_[i], threadQty, maxKeepEntryCoeff);
    }
  }
  void Read(istream& controlStream, istream& binaryStream,
            size_t queryQty,
            size_t& testSetId,
            size_t& savedThreadQty) {
    LOG(LIB_INFO) << "Reading gold standard data from cache";
    string s;
    ReadField(controlStream, GS_TEST_SET_ID, s);
    ConvertFromString(s, testSetId);
    ReadField(controlStream, GS_THREAD_TEST_QTY, savedThreadQty);
    for (size_t i = 0; i < vvGoldStandardRange_.size(); ++i) {
      ReadField(controlStream, GS_NOTE_FIELD, s);
      vvGoldStandardRange_[i].clear();
      readOneGS(controlStream, binaryStream,
                queryQty, vvGoldStandardRange_[i]);
    }
    for (size_t i = 0; i < vvGoldStandardKNN_.size(); ++i) {
      ReadField(controlStream, GS_NOTE_FIELD, s);
      vvGoldStandardKNN_[i].clear();
      readOneGS(controlStream, binaryStream,
                queryQty, vvGoldStandardKNN_[i]);
    }
  }
  void Write(ostream& controlStream, ostream& binaryStream,
             size_t testSetId, size_t threadTestQty) {
    WriteField(controlStream, GS_TEST_SET_ID, ConvertToString(testSetId));
    WriteField(controlStream,GS_THREAD_TEST_QTY, threadTestQty);
    // GS_NOTE_FIELD & GS_TEST_SET_ID are for informational purposes only
    for (size_t i = 0; i < vvGoldStandardRange_.size(); ++i) {
      WriteField(controlStream, GS_NOTE_FIELD,
                "range radius=" + ConvertToString(config_.GetRange()[i]));
      writeOneGS(controlStream, binaryStream,
                 vvGoldStandardRange_[i]);
    }
    for (size_t i = 0; i < vvGoldStandardKNN_.size(); ++i) {
      WriteField(controlStream, GS_NOTE_FIELD,
                "k=" + ConvertToString(config_.GetKNN()[i]) +
                "eps=" + ConvertToString(config_.GetEPS()));
      writeOneGS(controlStream, binaryStream,
                 vvGoldStandardKNN_[i]);
    }
  }
  const vector<unique_ptr<GoldStandard<dist_t>>> &GetRangeGS(size_t i) const {
    return vvGoldStandardRange_[i];
  }
  const vector<unique_ptr<GoldStandard<dist_t>>> &GetKNNGS(size_t i) const {
    return vvGoldStandardKNN_[i];
  }
private:
  const ExperimentConfig<dist_t>&         config_;
  vector<vector<unique_ptr<GoldStandard<dist_t>>>>    vvGoldStandardRange_;
  vector<vector<unique_ptr<GoldStandard<dist_t>>>>    vvGoldStandardKNN_;

  void writeOneGS(ostream& controlStream, ostream& binaryStream,
                  const vector<unique_ptr<GoldStandard<dist_t>>>& oneGS) {
    for(const unique_ptr<GoldStandard<dist_t>>& obj : oneGS) {
      obj->Write(controlStream, binaryStream);
    }
  }

  void readOneGS(istream& controlStream, istream& binaryStream,
                 size_t queryQty,
                 vector<unique_ptr<GoldStandard<dist_t>>>& oneGS) {
    oneGS.resize(queryQty);
    for (size_t k = 0; k < queryQty; ++k) {
      unique_ptr<GoldStandard<dist_t>>  gs(new GoldStandard<dist_t>());
      gs->Read(controlStream, binaryStream);
      oneGS[k].reset(gs.release());
    }
  }

  template <typename QueryCreatorType>
  void procOneSet(const QueryCreatorType&                   QueryCreator,
                  vector<unique_ptr<GoldStandard<dist_t>>>& vGoldStand,
                  size_t                                    threadQty,
                  float                                     maxKeepEntryCoeff) {
    size_t queryQty = config_.GetQueryObjects().size();
    vGoldStand.resize(queryQty);

    vector<unique_ptr<GoldStandardThreadParams<dist_t,QueryCreatorType>>> GoldStandParams(threadQty);

    for (unsigned GoldPart = 0; GoldPart < threadQty; ++GoldPart) {
      GoldStandParams[GoldPart].reset(
          new GoldStandardThreadParams<dist_t, QueryCreatorType>(config_,
                                                                 QueryCreator,
                                                                 maxKeepEntryCoeff,
                                                                 threadQty,
                                                                 GoldPart,
                                                                 vGoldStand));
    }

    if (threadQty == 1) {
      GoldStandardThread<dist_t, QueryCreatorType>()(*GoldStandParams[0]);
    } else {
      vector<thread> Threads(threadQty);

      for (unsigned GoldPart = 0; GoldPart < threadQty; ++GoldPart) {
        Threads[GoldPart] = thread(GoldStandardThread<dist_t,QueryCreatorType>(),ref(*GoldStandParams[GoldPart]));
      }

      for (unsigned GoldPart = 0; GoldPart < threadQty; ++GoldPart) {
        Threads[GoldPart].join();
      }
    }
  }
};


}

#endif
