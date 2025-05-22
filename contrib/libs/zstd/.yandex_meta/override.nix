pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.5.7";

  src = fetchFromGitHub {
    owner = "facebook";
    repo = "zstd";
    rev = "v${version}";
    hash = "sha256-tNFWIT9ydfozB8dWcmTMuZLCQmQudTFJIkSr0aG7S44=";
  };

  patches= [];

  cmakeFlags = [
    "-DZSTD_BUILD_SHARED=ON"
    "-DZSTD_BUILD_STATIC=OFF"
    "-DZSTD_BUILD_TESTS=OFF"
    "-DZSTD_LEGACY_LEVEL:INT=1"
    "-DZSTD_LEGACY_SUPPORT=ON"
    "-DZSTD_PROGRAMS_LINK_SHARED=ON"
  ];
}
