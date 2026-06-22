self: super: with self; rec {
  version = "20260526.0";

  src = fetchFromGitHub {
    owner = "abseil";
    repo = "abseil-cpp";
    rev = version;
    hash = "sha256-O9ClnGm4WSTX3g1Q2VYTMhUtGG52XBwxzgHtWW9WSG0=";
  };

  patches = [];
}
