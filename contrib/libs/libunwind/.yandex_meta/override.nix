pkgs: attrs: with pkgs; with attrs; rec {
  version = "18.1.0-rc1";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-I9VUoDv+P/3C6R1I9VozGYJDjK4KBAkezVkpPMLjMuU=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
