self: super: with self; {
  opentelemetry-proto = stdenv.mkDerivation rec {
    name = "opentelemetry-proto";
    version = "1.11.0";

    src = fetchFromGitHub {
      owner = "open-telemetry";
      repo = "opentelemetry-proto";
      rev = "v${version}";
      hash = "sha256-1s94AS5+bPD0/UKbI/Ox+nSZe4PGLkinUcItITVgEiQ=";
    };
  };
}
