pkgs: attrs: with pkgs; with attrs; rec {
  version = "22.1.2";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-z6YcxgDd3F3JwfU5Y/wMw5MK+ZPISI3KLwHwUaraTuw=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
