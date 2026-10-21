self: super: with self; rec {
  version = "20260817.0";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-CEtLO4il9/jk+bFDDV0rXeX1OkirA0u6nxrSiWq0NPM=";
  };

  patches = [];
}
