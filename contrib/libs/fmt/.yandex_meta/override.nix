pkgs: attrs: with pkgs; rec {
  version = "8.1.1";

  src = fetchFromGitHub {
    owner = "fmtlib";
    repo = "fmt";
    rev = version;
    hash = "sha256-leb2800CwdZMJRWF5b1Y9ocK0jXpOX/nwo95icDf308=";
  };

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=ON"

    # Disable master project setting to omit building tests
    "-DFMT_MASTER_PROJECT=OFF"
  ];
}
