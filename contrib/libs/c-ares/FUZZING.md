# Fuzzing Hints

## LibFuzzer

1. Set compiler that supports fuzzing, this is an example on MacOS using
   a homebrew-installed clang/llvm:
```
export CC="/opt/homebrew/Cellar/llvm/18.1.8/bin/clang"
export CXX="/opt/homebrew/Cellar/llvm/18.1.8/bin/clang++"
```

2. Compile c-ares with both ASAN and fuzzing support.  We want an optimized
   debug build so we will use `RelWithDebInfo`:
```
export CFLAGS="-fsanitize=address,fuzzer-no-link -DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION"
export CXXFLAGS="-fsanitize=address,fuzzer-no-link -DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION"
export LDFLAGS="-fsanitize=address,fuzzer-no-link"
mkdir buildfuzz
cd buildfuzz
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -G Ninja ..
ninja
```

3. Build the fuzz test itself linked against our fuzzing-enabled build:
```
${CC} -W -Wall -Og -fsanitize=address,fuzzer -I../include -I../src/lib -I. -o ares-test-fuzz ../test/ares-test-fuzz.c -L./lib -Wl,-rpath ./lib -lcares
${CC} -W -Wall -Og -fsanitize=address,fuzzer -I../include -I../src/lib -I. -o ares-test-fuzz-name ../test/ares-test-fuzz-name.c -L./lib -Wl,-rpath ./lib -lcares
```

4. Run the fuzzer, its better if you can provide seed input but it does pretty
   well on its own since it uses coverage data to determine how to proceed.
   You can play with other flags etc, like `-jobs=XX` for parallelism.  See
   https://llvm.org/docs/LibFuzzer.html
```
mkdir corpus
cp ../test/fuzzinput/* corpus
./ares-test-fuzz -max_len=65535 corpus
```
or
```
mkdir corpus
cp ../test/fuzznames/* corpus
./ares-test-fuzz-name -max_len=1024 corpus
```


## AFL

To fuzz using AFL, follow the
[AFL quick start guide](http://lcamtuf.coredump.cx/afl/QuickStartGuide.txt):

 - Download and build AFL.
 - Configure the c-ares library and test tool to use AFL's compiler wrappers:

   ```console
   % export CC=$AFLDIR/afl-gcc
   % ./configure --disable-shared && make
   % cd test && ./configure && make aresfuzz aresfuzzname
   ```

 - Run the AFL fuzzer against the starting corpus:

   ```console
   % mkdir fuzzoutput
   % $AFLDIR/afl-fuzz -i fuzzinput -o fuzzoutput -- ./aresfuzz  # OR
   % $AFLDIR/afl-fuzz -i fuzznames -o fuzzoutput -- ./aresfuzzname
   ```

## AFL Persistent Mode

If a recent version of Clang is available, AFL can use its built-in compiler
instrumentation; this configuration also allows the use of a (much) faster
persistent mode, where multiple fuzz inputs are run for each process invocation.

 - Download and build a recent AFL, and run `make` in the `llvm_mode`
   subdirectory to ensure that `afl-clang-fast` gets built.
 - Configure the c-ares library and test tool to use AFL's clang wrappers that
   use compiler instrumentation:

   ```console
   % export CC=$AFLDIR/afl-clang-fast
   % ./configure --disable-shared && make
   % cd test && ./configure && make aresfuzz
   ```

 - Run the AFL fuzzer (in persistent mode) against the starting corpus:

   ```console
   % mkdir fuzzoutput
   % $AFLDIR/afl-fuzz -i fuzzinput -o fuzzoutput -- ./aresfuzz
   ```
