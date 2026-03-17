self: super: with self; rec {
  version = "1.9.2";

  src = fetchFromGitHub {
    owner = "libgit2";
    repo = "libgit2";
    rev = "v${version}";
    hash = "sha256-TCeEh8DpVoxpF/HkahxM3ONDjawAkIiMo6S7ogG3fLg=";
  };

  buildInputs = [
    libssh2
    llhttp
    openssl
    pcre
    zlib
  ];

  patches = [
    ./pr6658-openssl-includes.patch
  ];

  cmakeFlags = [
    "-DBUILD_TESTS=OFF"
    "-DREGEX_BACKEND=pcre"  # TODO: switch to pcre2
    "-DUSE_ICONV=ON"
    "-DUSE_HTTPS=ON"
    "-DUSE_HTTP_PARSER=llhttp"
    "-DUSE_NTLMCLIENT=OFF"
    "-DUSE_SSH=ON"
    "-DUSE_THREADS=ON"
  ];
}
