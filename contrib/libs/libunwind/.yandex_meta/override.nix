pkgs: attrs: with pkgs; with attrs; rec {
  version = "21.1.0";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-4DLEZuhREHMl2t0f1iqvXSRSE5VBMVxd94Tj4m8Yf9s=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
