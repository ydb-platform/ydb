python3_N folders here are mirrors of python3_small, adjusted for system python (Name Python3 -> SystemPython3_N, LDFLAGS(-lpython3.N))

They are supposed to be built with local python: `ya make -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON=yes -DOS_SDK=local -DPYTHON_BIN=python3.N -DPYTHON_CONFIG=python3.N-config python3.N`

One way to get all pythons on the same machine is `sudo add-apt-repository ppa:deadsnakes/ppa` and `sudo apt install python3.N-dev`

Use build_system_python_udfs.sh to build all python udfs with system pythons(local pythons)
