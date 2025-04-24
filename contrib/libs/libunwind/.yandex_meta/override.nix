pkgs: attrs: with pkgs; with attrs; rec {
  version = "20.1.2";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-t30Jh8ckp5qD6XDxtvnSaYiAWbEi6L6hAWh6tN8JjtY=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
