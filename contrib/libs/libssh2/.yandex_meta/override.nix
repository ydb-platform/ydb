pkgs: attrs: with pkgs; rec {
  pname = "libssh2";
  version = "1.11.1";

  nativeBuildInputs = [ autoreconfHook ];
  buildInputs = [ pkg-config autoconf automake libtool openssl zlib ];

  src = fetchFromGitHub {
      owner = "libssh2";
      repo = "libssh2";
      rev = "libssh2-${version}";
      sha256 = "sha256-yz97oqqN+NJTDL/HPJe3niFynbR8QXHuuiKr+uuKJtw=";
  };

  patches = [];
}
