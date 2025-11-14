pkgs: attrs: with pkgs; with attrs; rec {
  version = "2025-11-05";

  src = fetchFromGitHub {
    owner = "google";
    repo = "re2";
    rev = "${version}";
    hash = "sha256-0J1HVk+eR7VN0ymucW9dNlT36j16XIfCzcs1EVyEIEU=";
  };

  buildInputs = [
    abseil-cpp
    gbenchmark
    gtest
  ];

  # revert weird macOS-specific setting from nixpkgs upstream
  cmakeBuildDir = "build";

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=ON"
    "-DRE2_BUILD_TESTING=ON"

    # re2 (as of 2023-06-02) unconditionally targets c++14,
    # we patch it in order to make it linkable against abseil-cpp
    # "-DCMAKE_CXX_STANDARD=17"
  ];

}
