self: super: with self; rec {
  pname = "xxHash";
  version = "0.8.2";

  src = fetchFromGitHub {
    owner = "Cyan4973";
    repo = "xxHash";
    rev = "v${version}";
    hash = "sha256-kofPs01jb189LUjYHHt+KxDifZQWl0Hm779711mvWtI=";
  };

  patches = [];
}
