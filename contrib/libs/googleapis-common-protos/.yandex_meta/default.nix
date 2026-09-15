self: super: with self; {
  googleapis-common-protos = stdenv.mkDerivation rec {
    name = "googleapis-common-protos";
    version = "1.66.0";

    src = fetchFromGitHub {
      owner = "googleapis";
      repo = "python-api-common-protos";
      rev = "v${version}";
      hash = "sha256-OQkWSgxNJClbldwTOUU2evySTaRUN5ox5Lw+KytbZ7k=";
    };
  };
}
