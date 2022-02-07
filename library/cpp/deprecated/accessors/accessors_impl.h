#pragma once

#include "memory_traits.h"

namespace NAccessors {
    namespace NPrivate {
        template <typename Ta>
        struct TMemoryAccessorBase {
            enum {
                SimpleMemory = TMemoryTraits<Ta>::SimpleMemory,
                ContinuousMemory = TMemoryTraits<Ta>::ContinuousMemory,
            };

            struct TBadAccessor;
        };

        template <typename Ta>
        struct TBegin: public TMemoryAccessorBase<Ta> {
            using TElementType = typename TMemoryTraits<Ta>::TElementType;

            template <typename Tb>
            struct TNoMemoryIndirectionBegin {
                static const TElementType* Get(const Tb& b) {
                    return (const TElementType*)&b;
                }
            };

            template <typename Tb>
            struct TIndirectMemoryRegionBegin {
                Y_HAS_MEMBER(Begin);
                Y_HAS_MEMBER(begin);

                template <typename Tc>
                struct TByBegin {
                    static const TElementType* Get(const Tc& b) {
                        return (const TElementType*)b.Begin();
                    }
                };

                template <typename Tc>
                struct TBybegin {
                    static const TElementType* Get(const Tc& b) {
                        return (const TElementType*)b.begin();
                    }
                };

                using TGet = std::conditional_t<THasBegin<Tb>::value, TByBegin<Tb>, TBybegin<Tb>>;

                static const TElementType* Get(const Tb& b) {
                    return TGet::Get(b);
                }
            };

            using TGet = std::conditional_t<
                TMemoryAccessorBase<Ta>::SimpleMemory,
                TNoMemoryIndirectionBegin<Ta>,
                std::conditional_t<
                    TMemoryAccessorBase<Ta>::ContinuousMemory,
                    TIndirectMemoryRegionBegin<Ta>,
                    typename TMemoryAccessorBase<Ta>::TBadAccessor>>;

            static const TElementType* Get(const Ta& b) {
                return TGet::Get(b);
            }
        };

        template <typename Ta>
        struct TEnd: public TMemoryAccessorBase<Ta> {
            using TElementType = typename TMemoryTraits<Ta>::TElementType;

            template <typename Tb>
            struct TNoMemoryIndirectionEnd {
                static const TElementType* Get(const Tb& b) {
                    return (const TElementType*)(&b + 1);
                }
            };

            template <typename Tb>
            struct TIndirectMemoryRegionEnd {
                Y_HAS_MEMBER(End);
                Y_HAS_MEMBER(end);

                template <typename Tc>
                struct TByEnd {
                    static const TElementType* Get(const Tc& b) {
                        return (const TElementType*)b.End();
                    }
                };

                template <typename Tc>
                struct TByend {
                    static const TElementType* Get(const Tc& b) {
                        return (const TElementType*)b.end();
                    }
                };

                using TGet = std::conditional_t<THasEnd<Tb>::value, TByEnd<Tb>, TByend<Tb>>;

                static const TElementType* Get(const Tb& b) {
                    return TGet::Get(b);
                }
            };

            using TGet = std::conditional_t<
                TMemoryAccessorBase<Ta>::SimpleMemory,
                TNoMemoryIndirectionEnd<Ta>,
                std::conditional_t<
                    TMemoryAccessorBase<Ta>::ContinuousMemory,
                    TIndirectMemoryRegionEnd<Ta>,
                    typename TMemoryAccessorBase<Ta>::TBadAccessor>>;

            static const TElementType* Get(const Ta& b) {
                return TGet::Get(b);
            }
        };

        template <typename Ta, bool Init>
        struct TClear: public TMemoryAccessorBase<Ta> {
            template <typename Tb>
            struct TNoMemoryIndirectionClear {
                static void Do(Tb& b) {
                    Zero(b);
                }
            };

            template <typename Tb>
            struct TIndirectMemoryRegionClear {
                Y_HAS_MEMBER(Clear);
                Y_HAS_MEMBER(clear);

                template <typename Tc>
                struct TByClear {
                    static void Do(Tc& b) {
                        b.Clear();
                    }
                };

                template <typename Tc>
                struct TByclear {
                    static void Do(Tc& b) {
                        b.clear();
                    }
                };

                template <typename Tc>
                struct TByNone {
                    static void Do(Tc& b) {
                        if (!Init)
                            b = Tc();
                    }
                };

                using TDo = std::conditional_t<
                    THasClear<Tb>::value,
                    TByClear<Tb>,
                    std::conditional_t<
                        THasclear<Tb>::value,
                        TByclear<Tb>,
                        TByNone<Tb>>>;

                static void Do(Tb& b) {
                    TDo::Do(b);
                }
            };

            using TDo = std::conditional_t<TMemoryAccessorBase<Ta>::SimpleMemory, TNoMemoryIndirectionClear<Ta>, TIndirectMemoryRegionClear<Ta>>;

            static void Do(Ta& b) {
                TDo::Do(b);
            }
        };

        template <typename Tb>
        struct TReserve {
            Y_HAS_MEMBER(Reserve);
            Y_HAS_MEMBER(reserve);

            template <typename Tc>
            struct TByReserve {
                static void Do(Tc& b, size_t sz) {
                    b.Reserve(sz);
                }
            };

            template <typename Tc>
            struct TByreserve {
                static void Do(Tc& b, size_t sz) {
                    b.reserve(sz);
                }
            };

            template <typename Tc>
            struct TByNone {
                static void Do(Tc&, size_t) {
                }
            };

            using TDo = std::conditional_t<
                THasReserve<Tb>::value,
                TByReserve<Tb>,
                std::conditional_t<
                    THasreserve<Tb>::value,
                    TByreserve<Tb>,
                    TByNone<Tb>>>;

            static void Do(Tb& b, size_t sz) {
                TDo::Do(b, sz);
            }
        };

        template <typename Tb>
        struct TResize {
            Y_HAS_MEMBER(Resize);
            Y_HAS_MEMBER(resize);

            template <typename Tc>
            struct TByResize {
                static void Do(Tc& b, size_t sz) {
                    b.Resize(sz);
                }
            };

            template <typename Tc>
            struct TByresize {
                static void Do(Tc& b, size_t sz) {
                    b.resize(sz);
                }
            };

            using TDo = std::conditional_t<THasResize<Tb>::value, TByResize<Tb>, TByresize<Tb>>;

            static void Do(Tb& b, size_t sz) {
                TDo::Do(b, sz);
            }
        };

        template <typename Tb>
        struct TAppend {
            Y_HAS_MEMBER(Append);
            Y_HAS_MEMBER(append);
            Y_HAS_MEMBER(push_back);

            template <typename Tc>
            struct TByAppend {
                using TElementType = typename TMemoryTraits<Tc>::TElementType;

                static void Do(Tc& b, const TElementType& val) {
                    b.Append(val);
                }
            };

            template <typename Tc>
            struct TByappend {
                using TElementType = typename TMemoryTraits<Tc>::TElementType;

                static void Do(Tc& b, const TElementType& val) {
                    b.append(val);
                }
            };

            template <typename Tc>
            struct TBypush_back {
                using TElementType = typename TMemoryTraits<Tc>::TElementType;

                static void Do(Tc& b, const TElementType& val) {
                    b.push_back(val);
                }
            };

            using TDo = std::conditional_t<
                THasAppend<Tb>::value,
                TByAppend<Tb>,
                std::conditional_t<
                    THasappend<Tb>::value,
                    TByappend<Tb>,
                    TBypush_back<Tb>>>;

            using TElementType = typename TMemoryTraits<Tb>::TElementType;

            static void Do(Tb& b, const TElementType& val) {
                TDo::Do(b, val);
            }
        };

        template <typename Tb>
        struct TAppendRegion {
            Y_HAS_MEMBER(Append);
            Y_HAS_MEMBER(append);
            Y_HAS_MEMBER(insert);

            template <typename Tc>
            struct TByAppend {
                using TElementType = typename TMemoryTraits<Tc>::TElementType;

                static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                    b.Append(beg, end);
                }
            };

            template <typename Tc>
            struct TByappend {
                using TElementType = typename TMemoryTraits<Tc>::TElementType;

                static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                    b.append(beg, end);
                }
            };

            template <typename Tc>
            struct TByinsert {
                using TElementType = typename TMemoryTraits<Tc>::TElementType;

                static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                    b.insert(b.end(), beg, end);
                }
            };

            template <typename Tc>
            struct TByNone {
                using TElementType = typename TMemoryTraits<Tc>::TElementType;

                static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                    for (const TElementType* it = beg; it != end; ++it)
                        TAppend<Tc>::Do(b, *it);
                }
            };

            using TDo = std::conditional_t<
                THasAppend<Tb>::value,
                TByAppend<Tb>,
                std::conditional_t<
                    THasappend<Tb>::value,
                    TByappend<Tb>,
                    std::conditional_t<
                        THasinsert<Tb>::value,
                        TByinsert<Tb>,
                        TByNone<Tb>>>>;

            using TElementType = typename TMemoryTraits<Tb>::TElementType;

            static void Do(Tb& b, const TElementType* beg, const TElementType* end) {
                TDo::Do(b, beg, end);
            }
        };

        template <typename Ta>
        struct TAssign: public TMemoryAccessorBase<Ta> {
            using TElementType = typename TMemoryTraits<Ta>::TElementType;

            template <typename Tb>
            struct TNoMemoryIndirectionAssign {
                static void Do(Tb& b, const TElementType* beg, const TElementType* end) {
                    if (sizeof(Tb) == sizeof(TElementType) && end - beg > 0) {
                        memcpy(&b, beg, sizeof(Tb));
                    } else if (end - beg > 0) {
                        memcpy(&b, beg, Min<size_t>((end - beg) * sizeof(TElementType), sizeof(Tb)));
                    } else {
                        Zero(b);
                    }
                }
            };

            template <typename Tb>
            struct TIndirectMemoryRegionAssign {
                Y_HAS_MEMBER(Assign);
                Y_HAS_MEMBER(assign);

                template <typename Tc>
                struct TByAssign {
                    static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                        b.Assign(beg, end);
                    }
                };

                template <typename Tc>
                struct TByassign {
                    static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                        b.assign(beg, end);
                    }
                };

                template <typename Tc>
                struct TByClearAppend {
                    static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                        TClear<Tc, false>::Do(b);
                        TAppendRegion<Tc>::Do(b, beg, end);
                    }
                };

                template <typename Tc>
                struct TByConstruction {
                    static void Do(Tc& b, const TElementType* beg, const TElementType* end) {
                        b = Tc(beg, end);
                    }
                };

                using TDo = std::conditional_t<
                    THasAssign<Tb>::value,
                    TByAssign<Tb>,
                    std::conditional_t<
                        THasassign<Tb>::value,
                        TByassign<Tb>,
                        std::conditional_t<
                            TMemoryTraits<Tb>::OwnsMemory,
                            TByClearAppend<Tb>,
                            TByConstruction<Tb>>>>;

                static void Do(Tb& b, const TElementType* beg, const TElementType* end) {
                    TDo::Do(b, beg, end);
                }
            };

            using TDo = std::conditional_t<TMemoryAccessorBase<Ta>::SimpleMemory, TNoMemoryIndirectionAssign<Ta>, TIndirectMemoryRegionAssign<Ta>>;

            static void Do(Ta& b, const TElementType* beg, const TElementType* end) {
                TDo::Do(b, beg, end);
            }
        };
    }
}
