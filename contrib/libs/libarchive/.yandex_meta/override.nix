pkgs: attrs: with pkgs; rec {
  version = "3.8.1";

  src = fetchFromGitHub {
    owner = "libarchive";
    repo = "libarchive";
    rev = "v${version}";
    hash = "sha256-KN5SvQ+/g/OOa+hntMX3D8p5IEWO0smke5WK+DwrOH0=";
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
