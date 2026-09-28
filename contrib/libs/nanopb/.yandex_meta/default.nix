self: super: with self; {
  nanopb = stdenv.mkDerivation rec {
    pname = "nanopb";
    version = "0.4.9.1";

    src = fetchFromGitHub {
      owner = "nanopb";
      repo = "nanopb";
      rev = "${version}";
      hash = "sha256-bMSZZaF8egAegi3enCM+DRyxOrPoWKAKybvWsrKZEDc=";
    };

    nativeBuildInputs = [cmake python3 protobuf];
  };
}
