pkgs: attrs: with pkgs; rec {
  pname = "libssh2";
  version = "1.10.0";

  nativeBuildInputs = [ autoreconfHook ];
  buildInputs = [ pkg-config autoconf automake libtool openssl zlib ];

  src = fetchFromGitHub {
      owner = "libssh2";
      repo = "libssh2";
      rev = "libssh2-${version}";
      sha256 = "0iiwdnvzq7mw1h1frbsszzhhf259jvjmzbp15mkgdfypnhgh3ri5";
  };

  patches = [];
}
