pkgs: attrs: with pkgs; rec {
  version = "2.1.13";

  src = fetchFromGitHub {
    owner = "libevent";
    repo = "libevent";
    rev = "release-${version}-stable";
    sha256 = "sha256-JYA50pqzar4vWg5omjToS3tfLXjTGDRMNbdEb3sqyFk=";
  };

  nativeBuildInputs = [
    autoreconfHook
  ];

  buildInputs = [
    openssl
    zlib
  ];
}
