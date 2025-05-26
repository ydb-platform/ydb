pkgs: attrs: with pkgs; with attrs; rec {
  version = "20.1.5";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-WKfY+VvAsZEEc0xYgF6+MsXDXZz7haMU6bxqmUpaHuQ=";
  };

  patches = [];

  sourceRoot = "source/libunwind";

  # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
  cmakeFlags = [
    "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"
  ];
}
