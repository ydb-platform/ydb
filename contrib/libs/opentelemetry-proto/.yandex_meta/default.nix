self: super: with self; {
  opentelemetry-proto = stdenv.mkDerivation rec {
    name = "opentelemetry-proto";
    version = "1.7.0";

    src = fetchFromGitHub {
      owner = "open-telemetry";
      repo = "opentelemetry-proto";
      rev = "v${version}";
      hash = "sha256-3SFf/7fStrglxcpwEya7hDp8Sr3wBG9OYyBoR78IUgs=";
    };
  };
}
