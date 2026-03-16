pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.3.0";

  src = fetchFromGitHub {
    owner = "redis";
    repo = "hiredis";
    rev = "v${version}";
    hash = "sha256-gbCLIz6nOpPbu0xbsxUVvr7XmvGdVWZQJWjpE76NIXY=";
  };

  patches = [];
  buildInputs = [
    openssl
  ];
}
