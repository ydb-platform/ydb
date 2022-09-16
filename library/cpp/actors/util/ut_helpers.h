#pragma once

// calls TCallback for all args permutations including (id, id)
template <class TCallback, class... TArgs>
void Permutate(TCallback&& fn, TArgs&&... args)
{
    auto forAll = [&](auto& arg){
        (fn(std::forward<decltype(arg)>(arg), std::forward<decltype(args)>(args)), ...);
    };

    (forAll(std::forward<decltype(args)>(args)), ...);
}
