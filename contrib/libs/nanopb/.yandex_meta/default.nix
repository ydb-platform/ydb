self: super: with self; {
  nanopb = stdenv.mkDerivation rec {
    pname = "nanopb";
    version = "0.4.9.2";

    src = fetchFromGitHub {
      owner = "nanopb";
      repo = "nanopb";
      rev = "${version}";
      hash = "sha256-kkwFhsGssr+Hl6yHCUuCQXNl1i2EZ5jFdF10+ekHK1w=";
    };

    nativeBuildInputs = [cmake python3 protobuf];
  };
}
