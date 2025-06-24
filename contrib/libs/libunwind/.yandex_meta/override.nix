pkgs: attrs: with pkgs; with attrs; rec {
  version = "20.1.6";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-PfCzECiCM+k0hHqEUSr1TSpnII5nqIxg+Z8ICjmMj0Y=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
