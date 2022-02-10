#pragma once

#include <util/stream/str.h>

#include <tuple>
#include <utility>

namespace NKikimr {

    namespace NHelp {

        template<size_t ...> struct TIndexes { };

        template<size_t N, size_t ...Obtained>
        struct TGenIx : TGenIx<N - 1, N - 1, Obtained...> { };

        template<size_t ...Obtained>
        struct TGenIx<0, Obtained...>
        {
            using Type = TIndexes<Obtained...>;
        };

        template<typename TAr1, typename TAr2>
        static inline void IsArrSized(const TAr1&, const TAr2&)
        {
            constexpr auto nums = std::tuple_size<TAr1>::value;

            static_assert(nums == std::tuple_size<TAr1>::value, "");
        }
    }

    namespace NFmt {
        using TOut  = IOutputStream;

        template<typename Type>
        struct TPut {

            template<typename ...TArg>
            static void Do(TOut &out, const Type *ob, TArg&& ... args)
            {
                ob->Describe(out, std::forward<TArg>(args)...);
            }
        };

        template<typename Type, typename ...TArg>
        struct TDesc {
            TDesc(const Type *ob, TArg&& ...args)
                : Ob(ob), Args(std::forward<TArg>(args)...) { }

            inline TOut& Do(TOut &out) const noexcept
            {
                using Tups = std::tuple_size<std::tuple<TArg...>>;

                return Do(out, typename NHelp::TGenIx<Tups::value>::Type());
            }

        private:
            template<size_t ... Index>
            inline TOut& Do(TOut &out, NHelp::TIndexes<Index...>) const
            {
                if (Ob == nullptr) return out << "{nil}";

                NFmt::TPut<Type>::Do(out, Ob, std::get<Index>(Args)...);

                return out;
            }

            const Type              *Ob = nullptr;
            std::tuple<TArg...>     Args;
        };

        template<typename TIter>
        struct TArr {
            TArr(TIter begin, TIter end) : Begin(begin), End(end) { }

            TOut& Do(TOut &out) const noexcept
            {
                out << "{ ";

                for (auto it = Begin; it != End; ++it)
                    out << (it == Begin ? "" : ", ") << *it;

                return out << (Begin == End ? "}" : " }");
            }

        private:
            const TIter Begin, End;
        };

        template<typename Type, typename ...TArg>
        inline TDesc<Type, TArg...> If(const Type *ob, TArg&& ...args)
        {
            return { ob, std::forward<TArg>(args)... };
        }

        template<typename Type, typename ...TArg>
        inline TDesc<Type, TArg...> Do(const Type &ob, TArg&& ...args)
        {
            return { &ob, std::forward<TArg>(args)... };
        }

        template<typename Type, typename ...TArg>
        inline TString Ln(const Type &ob, TArg&& ...args) noexcept
        {
            TStringStream ss;

            TDesc<Type, TArg...>(&ob, std::forward<TArg>(args)...).Do(ss);

            return ss.Str();
        }

        template<typename Type,
            typename It = decltype(std::declval<const Type>().begin())>
        inline TArr<It> Arr(const Type &arr) noexcept
        {
            return { arr.begin(), arr.end() };
        }

        template<typename Type, typename ...TArg>
        inline TOut& operator<<(TOut &out, const TDesc<Type, TArg...> &wrap)
        {
            return wrap.Do(out);
        }

        template<typename Type>
        inline TOut& operator<<(TOut &out, const TArr<Type> &wrap)
        {
            return wrap.Do(out);
        }
    }
}
