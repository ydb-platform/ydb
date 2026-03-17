//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <fstream>
#include <string>
#include <vector>
#include "common.h"
#include "feature_index.h"
#include "freelist.h"
#include "lbfgs.h"
#include "learner_tagger.h"
#include "param.h"
#include "string_buffer.h"
#include "thread.h"
#include "utils.h"

namespace MeCab {
namespace {

#define DCONF(file) create_filename(dicdir, std::string(file)).c_str()

#ifdef MECAB_USE_THREAD
class learner_thread: public thread {
 public:
  unsigned short start_i;
  unsigned short thread_num;
  size_t size;
  size_t micro_p;
  size_t micro_r;
  size_t micro_c;
  size_t err;
  double f;
  EncoderLearnerTagger **x;
  std::vector<double> expected;
  void run() {
    micro_p = micro_r = micro_c = err = 0;
    f = 0.0;
    std::fill(expected.begin(), expected.end(), 0.0);
    for (size_t i = start_i; i < size; i += thread_num) {
      f += x[i]->gradient(&expected[0]);
      err += x[i]->eval(&micro_c, &micro_p, &micro_r);
    }
  }
};
#endif

class CRFLearner {
 public:
  static int run(Param *param) {
    const std::string dicdir = param->get<std::string>("dicdir");
    CHECK_DIE(param->load(DCONF(DICRC)))
        << "no such file or directory: " << DCONF(DICRC);

    const std::vector<std::string> &files = param->rest_args();
    if (files.size() != 2) {
      std::cout << "Usage: " <<
          param->program_name() << " corpus model" << std::endl;
      return -1;
    }

    const std::string ifile = files[0];
    const std::string model = files[1];
    const std::string old_model = param->get<std::string>("old-model");

    EncoderFeatureIndex feature_index;
    std::vector<double> expected;
    std::vector<double> observed;
    std::vector<double> alpha;
    std::vector<double> old_alpha;
    std::vector<EncoderLearnerTagger *> x;
    Tokenizer<LearnerNode, LearnerPath> tokenizer;
    Allocator<LearnerNode, LearnerPath> allocator;

    CHECK_DIE(tokenizer.open(*param)) << "cannot open tokenizer";
    CHECK_DIE(feature_index.open(*param)) << "cannot open feature index";

    if (!old_model.empty()) {
      std::cout << "Using previous model: " << old_model << std::endl;
      std::cout << "--cost --freq and --eta options are overwritten."
                << std::endl;
      CHECK_DIE(tokenizer.dictionary_info());
      const char *dic_charset = tokenizer.dictionary_info()->charset;
      feature_index.reopen(old_model.c_str(),
                           dic_charset, &old_alpha, param);
    }

    const double C = param->get<double>("cost");
    const double eta = param->get<double>("eta");
    const size_t eval_size = param->get<size_t>("eval-size");
    const size_t unk_eval_size = param->get<size_t>("unk-eval-size");
    const size_t thread_num = param->get<size_t>("thread");
    const size_t freq = param->get<size_t>("freq");

    CHECK_DIE(C > 0) << "cost parameter is out of range: " << C;
    CHECK_DIE(eta > 0) "eta is out of range: " << eta;
    CHECK_DIE(eval_size > 0) << "eval-size is out of range: " << eval_size;
    CHECK_DIE(unk_eval_size > 0) <<
        "unk-eval-size is out of range: " << unk_eval_size;
    CHECK_DIE(freq > 0) <<
        "freq is out of range: " << unk_eval_size;
    CHECK_DIE(thread_num > 0 && thread_num <= 512)
        << "# thread is invalid: " << thread_num;

    std::cout.setf(std::ios::fixed, std::ios::floatfield);
    std::cout.precision(5);

    std::cout << "reading corpus ..." << std::flush;

    std::ifstream ifs(WPATH(ifile.c_str()));
    CHECK_DIE(ifs) << "no such file or directory: " << ifile;

    while (ifs) {
      EncoderLearnerTagger *tagger = new EncoderLearnerTagger();

      CHECK_DIE(tagger->open(&tokenizer,
                             &allocator,
                             &feature_index,
                             eval_size,
                             unk_eval_size));

      CHECK_DIE(tagger->read(&ifs, &observed));

      if (!tagger->empty()) {
        x.push_back(tagger);
      } else {
        delete tagger;
      }

      if (x.size() % 100 == 0) {
        std::cout << x.size() << "... " << std::flush;
      }
    }

    feature_index.shrink(freq, &observed);
    feature_index.clearcache();

    const size_t psize = feature_index.size();
    observed.resize(psize);
    expected.resize(psize);
    alpha.resize(psize);
    old_alpha.resize(psize);
    alpha = old_alpha;

    feature_index.set_alpha(&alpha[0]);

    std::cout << std::endl;
    std::cout << "Number of sentences: " << x.size()  << std::endl;
    std::cout << "Number of features:  " << psize     << std::endl;
    std::cout << "eta:                 " << eta       << std::endl;
    std::cout << "freq:                " << freq      << std::endl;
    std::cout << "eval-size:           " << eval_size << std::endl;
    std::cout << "unk-eval-size:       " << unk_eval_size << std::endl;
#ifdef MECAB_USE_THREAD
    std::cout << "threads:             " << thread_num << std::endl;
#endif
    std::cout << "charset:             " <<
        tokenizer.dictionary_info()->charset << std::endl;
    std::cout << "C(sigma^2):          " << C          << std::endl
              << std::endl;

#ifdef MECAB_USE_THREAD
    std::vector<learner_thread> thread;
    if (thread_num > 1) {
      thread.resize(thread_num);
      for (size_t i = 0; i < thread_num; ++i) {
        thread[i].start_i = i;
        thread[i].size = x.size();
        thread[i].thread_num = thread_num;
        thread[i].x = &x[0];
        thread[i].expected.resize(expected.size());
      }
    }
#endif

    int converge = 0;
    double prev_obj = 0.0;
    LBFGS lbfgs;

    for (size_t itr = 0; ;  ++itr) {
      std::fill(expected.begin(), expected.end(), 0.0);
      double obj = 0.0;
      size_t err = 0;
      size_t micro_p = 0;
      size_t micro_r = 0;
      size_t micro_c = 0;

#ifdef MECAB_USE_THREAD
      if (thread_num > 1) {
        for (size_t i = 0; i < thread_num; ++i) {
          thread[i].start();
        }

        for (size_t i = 0; i < thread_num; ++i) {
          thread[i].join();
        }

        for (size_t i = 0; i < thread_num; ++i) {
          obj += thread[i].f;
          err += thread[i].err;
          micro_r += thread[i].micro_r;
          micro_p += thread[i].micro_p;
          micro_c += thread[i].micro_c;
          for (size_t k = 0; k < psize; ++k) {
            expected[k] += thread[i].expected[k];
          }
        }
      } else
#endif
      {
        for (size_t i = 0; i < x.size(); ++i) {
          obj += x[i]->gradient(&expected[0]);
          err += x[i]->eval(&micro_c, &micro_p, &micro_r);
        }
      }

      const double p = 1.0 * micro_c / micro_p;
      const double r = 1.0 * micro_c / micro_r;
      const double micro_f = 2 * p * r / (p + r);

      for (size_t i = 0; i < psize; ++i) {
        const double penalty = (alpha[i] - old_alpha[i]);
        obj += (penalty * penalty / (2.0 * C));
        expected[i] = expected[i] - observed[i] + penalty / C;
      }

      const double diff = (itr == 0 ? 1.0 :
                           std::fabs(1.0 * (prev_obj - obj)) / prev_obj);
      std::cout << "iter="    << itr
                << " err="    << 1.0 * err/x.size()
                << " F="      << micro_f
                << " target=" << obj
                << " diff="   << diff << std::endl;
      prev_obj = obj;

      if (diff < eta) {
        converge++;
      } else {
        converge = 0;
      }

      if (converge == 3) {
        break;  // 3 is ad-hoc
      }

      const int ret = lbfgs.optimize(psize,
                                     &alpha[0], obj,
                                     &expected[0], false, C);

      CHECK_DIE(ret >= 0) << "unexpected error in LBFGS routin";

      if (ret == 0) {
        break;
      }
    }

    std::cout << "\nDone! writing model file ... " << std::endl;

    std::ostringstream oss;

    oss << "eta: "  << eta   << std::endl;
    oss << "freq: " << freq  << std::endl;
    oss << "C: "    << C     << std::endl;
    oss.setf(std::ios::fixed, std::ios::floatfield);
    oss.precision(16);
    oss << "eval-size: " << eval_size << std::endl;
    oss << "unk-eval-size: " << unk_eval_size << std::endl;
    oss << "charset: " <<  tokenizer.dictionary_info()->charset << std::endl;

    const std::string header = oss.str();

    CHECK_DIE(feature_index.save(model.c_str(), header.c_str()))
        << "permission denied: " << model;

    return 0;
  }
};

class Learner {
 public:
  static bool run(int argc, char **argv) {
    static const MeCab::Option long_options[] = {
      { "dicdir",   'd',  ".",     "DIR",
        "set DIR as dicdir(default \".\" )" },
      { "old-model",   'M',  0,     "FILE",
        "set FILE as old CRF model file" },
      { "cost",     'c',  "1.0",   "FLOAT",
        "set FLOAT for cost C for constraints violatoin" },
      { "freq",     'f',  "1",     "INT",
        "set the frequency cut-off (default 1)" },
      { "eta",      'e',  "0.00005", "DIR",
        "set FLOAT for tolerance of termination criterion" },
      { "thread",   'p',  "1",     "INT",    "number of threads(default 1)" },
      { "version",  'v',  0,   0,  "show the version and exit"  },
      { "help",     'h',  0,   0,  "show this help and exit."      },
      { 0, 0, 0, 0 }
    };

    Param param;

    if (!param.open(argc, argv, long_options)) {
      std::cout << param.what() << "\n\n" <<  COPYRIGHT
                << "\ntry '--help' for more information." << std::endl;
      return -1;
    }

    if (!param.help_version()) {
      return 0;
    }

    return CRFLearner::run(&param);
  }
};
}
}

int mecab_cost_train(int argc, char **argv) {
  return MeCab::Learner::run(argc, argv);
}
