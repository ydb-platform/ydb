self: super: with self; rec {
  name = "fast_float";
  version = "8.2.3";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-cQxIzfVNMG0UPEUw/4GYcRzmfAcBJLcswO+gqQ8t6lw=";
  };
}
