pkgs: attrs: with pkgs; with attrs; rec {
  version = "21.1.3";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-zYoVXLfXY3CDbm0ZI0U1Mx5rM65ObhZg6VkU1YrGE0c=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
