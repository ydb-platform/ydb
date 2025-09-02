pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.1.28";

  src = fetchFromGitHub {
    owner = "cyrusimap";
    repo = "cyrus-sasl";
    rev = "cyrus-sasl-${version}";
    hash = "sha256-0AiHAdcOwF7OKpIZwJ7j9E/KTmtk9qmgpvl8vkFk0oE=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  buildInputs = [
    openssl
    libxcrypt
    pam
  ];

  patches = [];

  configureFlags = [
    "--disable-shared"
    "--enable-static"
    "--with-saslauthd=/run/saslauthd"
    "--with-devrandom=/dev/urandom"
    "--with-openssl=${openssl.dev}"
  ];
}
