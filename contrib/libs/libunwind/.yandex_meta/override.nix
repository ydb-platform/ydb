pkgs: attrs: with pkgs; with attrs; rec {
  version = "21.1.1";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-IB9Z3bIMwfgw2W2Vxo89CmtCM9DfOyV2Ei64nqgHrgc=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
