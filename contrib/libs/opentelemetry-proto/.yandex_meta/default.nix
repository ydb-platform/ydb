self: super: with self; {
  opentelemetry-proto = stdenv.mkDerivation rec {
    name = "opentelemetry-proto";
    version = "1.8.0";

    src = fetchFromGitHub {
      owner = "open-telemetry";
      repo = "opentelemetry-proto";
      rev = "v${version}";
      hash = "sha256-5rNJDMjRFIOY/3j+PkAujbippBmxtAudU9busK0q8p0=";
    };
  };
}
