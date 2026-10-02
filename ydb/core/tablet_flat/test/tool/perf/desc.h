#pragma once

#include "help.h"

#include <util/stream/str.h>

#include <tuple>
#include <utility>

namespace NKikiSched {

    namespace NFmt {
        using TOut  = IOutputStream&;

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

            inline TOut& Do(TOut &out) const
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
        inline TString Ln(const Type &ob, TArg&& ...args)
        {
            TStringStream ss;

            TDesc<Type, TArg...>(&ob, std::forward<TArg>(args)...).Do(ss);

            return ss.Str();
        }

        template<typename Type, typename ...TArg>
        inline TOut& operator<<(TOut &out, const TDesc<Type, TArg...> &wrap)
        {
            return wrap.Do(out);
        }
    }
}
