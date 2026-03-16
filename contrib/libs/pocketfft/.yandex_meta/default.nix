self: super: with self; {
  pocketfft = stdenv.mkDerivation rec {
    name = "pocketfft";
    version = "2026-02-26";

    src = fetchFromGitHub {
      owner = "mreineck";
      repo = "pocketfft";
      rev = "8187407446316c3d16f15e5395dabd4b22f4fec7";
      sha256 = "sha256-qBaUJS/K6x0Kei+xpmtrb2d6S6oIRGVGDo9/hhqBa10=";
    };
  };
}