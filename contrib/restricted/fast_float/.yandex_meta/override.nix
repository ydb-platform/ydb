self: super: with self; rec {
  name = "fast_float";
  version = "6.1.6";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-MEJMPQZZZhOFiKlPAKIi0zVzaJBvjAlbSyg3wLOQ1fg=";
  };
}
