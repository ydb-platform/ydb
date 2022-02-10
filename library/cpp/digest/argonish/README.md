Argonish
--------

Implementation of argon2 (i, d, id) algorithms with CPU dispatching. The list of features includes:
* C++14 interface
* constexpr and partial templates to get rid of useless branches (give +2% to performance)
* AVX2 implementation of Argon2 (allows to gain +30..40% to performance)
* Vectorized Blake2B implementation (including AVX2 version)
* OpenMP for multithreading in contrast to pthread in the reference implementation

Acknowledgements
----------------

This project uses some ideas and pieces of code from the following projects licensed under CC0:
* https://github.com/P-H-C/phc-winner-argon2
* https://github.com/BLAKE2/BLAKE2

I'm also thankful to the following people whose fruitful feedback improved the project:
* Igor Klevanets (cerevra@yandex-team.ru)

Benchmark results
-----------------

On my OS X 10.11, MacBook Pro (Early 2015, Core i5 2,7 GHz, 16 GB 1867 MHz DDR3) for `(Argon2d, 1, 2048, 1)` it gives:

```
---- REF ----
Num	| Count	| Time
0	| 4555	| 9.93424
1	| 4556	| 9.94529
---- SSE2 ---
Num	| Count	| Time
0	| 6606	| 9.93117
1	| 6594	| 9.93259
--- SSSE3 ---
Num	| Count	| Time
0	| 7393	| 9.93866
1	| 7392	| 9.94874
--- SSE41 ---
Num	| Count	| Time
0	| 7152	| 9.88648
1	| 7112	| 9.87276
-----AVX2----
Num	| Count	| Time
0	| 11120	| 9.9273
1	| 11138	| 9.94308
```

How to use
----------

```
#include <library/cpp/digest/argonish/argon2.h>
...
uint32_t tcost = 1;  	/* one pass */
uint32_t mcost = 32; 	/* in KB */
uint32_t threads = 1;	/* one thread version */
NArgonish::TArgon2Factory afactory;
THolder<NArgonish::IArgon2Base> argon2 = afactory.Create(NArgonish::EArgon2Type::Argon2d, tcost, mcost, threads);
argon2->Hash(input, insize, salt, saltsize, out, outlen);
...
#include <library/cpp/digest/argonish/blake2b.h>
...
NArgonish::TBlake2BFactory bfactory;
uint32_t outlen = 32;
THolder<NArgonish::IBlake2Base> blake2b = bfactory.Create(outlen);
blake2b->Update(in, inlen);
blake2b->Final(out, outlen);
```

How to add your own Argon2 configuration
----------------------------------------

Just modify the `internal/proxy/macro/proxy_macros.h` and add appropriate `ARGON2_INSTANCE_DECL` declaration.
