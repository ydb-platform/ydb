#ifndef __SRC_LIB_EVENTHANDLER_HPP__
#define __SRC_LIB_EVENTHANDLER_HPP__

#include <functional>
#include <vector>

template <typename T>  // T: void (*fncptr)(int, double)
class Eventhandler {
  std::vector<std::function<void(T)>> subscribers;

 public:
  void subscribe(std::function<void(T)> subscriber) {
    subscribers.push_back(subscriber);
  }

  void fire(T args) {
    for (std::function<void(T)> fun : subscribers) {
      fun(args);
    }
  }
};

#endif
