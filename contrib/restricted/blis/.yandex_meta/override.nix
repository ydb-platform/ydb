pkgs: attrs: with pkgs; rec {
  version = "0.8.1";

  src = fetchFromGitHub {
    owner = "flame";
    repo = "blis";
    rev = version;
    hash = "sha256-D5T/itq9zyD5TkeJ4Ae1vS4yEWU51omyJoIkKQ2NLhY=";
  };

  configureFlags = [
    "--enable-cblas"
    "--blas-int-size=32"
    "x86_64"
  ];
}
