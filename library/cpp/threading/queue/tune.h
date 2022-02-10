#pragma once

/*
  Motivation: consider you have a template class with many parameters
  with default associations

  template <typename A = TDefA,
            typename B = TDefB,
            typename C = TDefC,
            typename D = TDefD>
  class TExample {
  };

  consider you would like to provide easy to use interface to tune all
  these parameters in position independed manner,
  In that case TTune would be helpful for you.

  How to use:
  First step: declare a struct with all default associations

  struct TDefaultTune {
      using TStructA = TDefA;
      using TStructB = TDefB;
      using TStructC = TDefC;
      using TStructD = TDefD;
  };

  Second step: declare helper names visible to a user

  DeclareTuneTypeParam(TTuneParamA, TStructA);
  DeclareTuneTypeParam(TTuneParamB, TStructB);
  DeclareTuneTypeParam(TTuneParamC, TStructC);
  DeclareTuneTypeParam(TTuneParamD, TStructD);

  Third step: declare TExample this way:

  template <typename...TParams>
  class TExample {
      using TMyParams = TTune<TDefaultTune, TParams...>;

      using TActualA = TMyParams::TStructA;
      using TActualB = TMyParams::TStructB;
      ...
  };

  TTune<TDefaultTune, TParams...> is a struct with the default parameteres
  taken from TDefaultTune and overridden from "TParams...".

  for example:  "TTune<TDefaultTune, TTuneParamC<TUserClass>>"
  will be virtually the same as:

  struct TTunedClass {
      using TStructA = TDefA;
      using TStructB = TDefB;
      using TStructC = TUserClass;
      using TStructD = TDefD;
  };

  From now on you can tune your TExample in the following manner:

  using TCustomClass =
      TExample <TTuneParamA<TUserStruct1>, TTuneParamD<TUserStruct2>>;

  You can also tweak constant expressions in your TDefaultTune.
  Consider you have:

  struct TDefaultTune {
      static constexpr ui32 MySize = 42;
  };

  declare an interface to modify the parameter this way:

  DeclareTuneValueParam(TStructSize, ui32, MySize);

  and tweak your class:

  using TTwiceBigger = TExample<TStructSize<84>>;

 */

#define DeclareTuneTypeParam(TParamName, InternalName) \
    template <typename TNewType>                       \
    struct TParamName {                                \
        template <typename TBase>                      \
        struct TApply: public TBase {                  \
            using InternalName = TNewType;             \
        };                                             \
    }

#define DeclareTuneValueParam(TParamName, TValueType, InternalName) \
    template <TValueType NewValue>                                  \
    struct TParamName {                                             \
        template <typename TBase>                                   \
        struct TApply: public TBase {                               \
            static constexpr TValueType InternalName = NewValue;    \
        };                                                          \
    }

#define DeclareTuneContainer(TParamName, InternalName)              \
    template <template <typename, typename...> class TNewContainer> \
    struct TParamName {                                             \
        template <typename TBase>                                   \
        struct TApply: public TBase {                               \
            template <typename TElem, typename... TRest>            \
            using InternalName = TNewContainer<TElem, TRest...>;    \
        };                                                          \
    }

namespace NTunePrivate {
    template <typename TBase, typename... TParams>
    struct TFold;

    template <typename TBase>
    struct TFold<TBase>: public TBase {
    };

    template <typename TBase, typename TFirstArg, typename... TRest>
    struct TFold<TBase, TFirstArg, TRest...>
       : public TFold<typename TFirstArg::template TApply<TBase>, TRest...> {
    };
}

template <typename TDefault, typename... TParams>
struct TTune: public NTunePrivate::TFold<TDefault, TParams...> {
};
