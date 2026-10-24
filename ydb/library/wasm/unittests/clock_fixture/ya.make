DLL(clock_fixture)

BUILD_ONLY_IF(OS_EMSCRIPTEN)
NO_UTIL()

SRCS(main.cpp)

LDFLAGS(
    -Wl,--allow-undefined
    -Wl,--no-export-dynamic
    -Wl,--export=WallNow
    -Wl,--export=SteadyNow
    -Wl,--export=TimeNow
    -Wl,--export=ConstructorWall
    -Wl,--export=ConstructorSteady
    -Wl,--export=ConstructorTime
    -Wl,--export-if-defined=__wasm_call_ctors
)

END()
