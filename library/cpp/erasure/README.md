# Erasure based codecs for arbitrary data

C++ wrapper for LRC and Reed-Solomon erasure codecs.
There are two backends for LRC: Jerasure(http://jerasure.org) and ISA-L(https://github.com/intel/isa-l). ISA-L is much faster - it condsiders different instrucion sets to optimize speed of encode and decode. The only limitations now are if you don't have SSE4.2 instruction set (then base variant is as slow as Jerasure) or if you run it on aarch64 architecture (however, 2.29 version will be going to support fast implementation). However, we still have to keep Jerasure because it is incompatible due to some optimization in it which affect data layout in coded blocks.
Also see https://wiki.yandex-team.ru/yt/userdoc/erasure/, https://wiki.yandex-team.ru/ignatijjkolesnichenko/yt/erasure/.

It is possible to use codecs LRC 2k-2-2 and Reed Solomon n-k for any data stream. All you need is to provide CodecTraits (see `codecs_ut.cpp` for examples). Note that ISA-L only supports WordSize equal to 8. If you use Jerasure codecs with bigger WordSize than `MaxWordSize` in `public.h`, codec is not guaranteed to be thread-safe.

You can use interface in `codec.h` or use the exact codec from `lrc_isa.h`, `lrc_jerasure.h` and `reed_solomon.h` if you like.
