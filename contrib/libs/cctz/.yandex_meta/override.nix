pkgs: attrs: with pkgs; with attrs; rec {
  version = "2021-03-11";

  src = fetchFromGitHub {
    owner = "google";
    repo = "cctz";
    rev = "583c52d1eaef159162790a1d4044940f5e0b201b";
    hash = "sha256-t5BWp24761mMBYWNBXBJXQZqiG76n8BYX3Rdo38jjX4=";
  };

  buildInputs = [
    gtest
    gbenchmark
  ];

  # FIXME: 
  # It looks like CMake build is not functional.
  # Understand and make it work.
  cmakeFlags = [
    "-DBUILD_TESTING=ON"
    "-DBUILD_TOOLS=OFF"
    "-DBUILD_EXAMPLES=OFF"
  ];
}
