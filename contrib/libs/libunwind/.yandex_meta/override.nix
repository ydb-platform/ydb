pkgs: attrs: with pkgs; with attrs; rec {
  version = "21.1.4";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-jcMYtoa5Rsf+CxRSrqR9PMxnYAEfaoQZXHgYFpZucWs=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
