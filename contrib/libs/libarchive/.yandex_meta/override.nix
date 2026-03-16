pkgs: attrs: with pkgs; rec {
  version = "3.8.5";

  src = fetchFromGitHub {
    owner = "libarchive";
    repo = "libarchive";
    rev = "v${version}";
    hash = "sha256-8UhumGjqW/AnRlx76bmvOV6I6Vpid8S5yVwg/AzIH0k=";
  };

  postPatch = "";

  # Use cmake instead of autotools
  nativeBuildInputs = [cmake ];

  buildInputs = [
    bzip2
    libb2
    lz4
    lzo
    openssl
    sharutils
    xz
    zlib
    zstd
  ];

  cmakeFlags = [
    "-DENABLE_ACL=OFF"
    "-DENABLE_ICONV=OFF"
    "-DENABLE_LIBXML2=OFF"
    "-DENABLE_XATTR=OFF"
    "-DHAVE_ACL_LIBACL_H=OFF"
    "-DHAVE_EXT2FS_EXT2_FS_H=OFF"
    "-DHAVE_SYS_ACL_H=OFF"
    "-DHAVE_BLAKE2_H=ON"
  ];
}
