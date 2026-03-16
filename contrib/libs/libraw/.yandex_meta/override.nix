self: super: with self; rec {
  pname = "libraw";
  version = "0.22.0";

  src = fetchFromGitHub {
    owner = "LibRaw";
    repo = "LibRaw";
    rev = "${version}";
    hash = "sha256-B2+LcdC6FqKryiu8t0wBifrESTAyz/+wDQhcGj7myhE=";
  };

  patches = [];
}
