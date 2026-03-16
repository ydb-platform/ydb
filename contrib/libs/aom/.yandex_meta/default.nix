self: super: with self; {
  aom = stdenv.mkDerivation rec {
    pname = "aom";
    version = "3.13.1";

    src = fetchgit {
      url = "https://aomedia.googlesource.com/aom";
      rev = "refs/tags/v${version}";
      sha256 = "sha256-C6V2LxJo7VNA9Tb61zJKswnpczpoDj6O3a4J0Z5TZ0A=";
    };

    nativeBuildInputs = [
      cmake
      nasm
      perl
    ];

    cmakeFlags = [
      "-DBUILD_SHARED_LIBS=0"
      "-DCONFIG_MULTITHREAD=0"
      "-DENABLE_TESTS=0"
      # NB: uncomment this to re-generate platform dispatchers for arm64.h
      # "-DAOM_TARGET_CPU=aarch64"
    ];

    buildInputs = [
      libyuv
      svt-av1
    ];

  };
}
