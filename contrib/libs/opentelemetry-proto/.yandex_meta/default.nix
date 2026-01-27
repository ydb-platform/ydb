self: super: with self; {
  opentelemetry-proto = stdenv.mkDerivation rec {
    name = "opentelemetry-proto";
    version = "1.9.0";

    src = fetchFromGitHub {
      owner = "open-telemetry";
      repo = "opentelemetry-proto";
      rev = "v${version}";
      hash = "sha256-5ZYu0HE0WRgf/TDmQ6oAwCJAcdnKtdHzDxU2DcmkBcg=";
    };
  };
}
