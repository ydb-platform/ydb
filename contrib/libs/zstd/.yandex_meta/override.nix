pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.5.6";

  src = fetchFromGitHub {
    owner = "facebook";
    repo = "zstd";
    rev = "v${version}";
    hash = "sha256-qcd92hQqVBjMT3hyntjcgk29o9wGQsg5Hg7HE5C0UNc=";
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
