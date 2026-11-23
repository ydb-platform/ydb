self: super: with self; rec {
  pname = "fribidi";
  version = "1.0.17";

  nativeBuildInputs = [ meson ninja pkg-config ];

  src = fetchFromGitHub {
    owner = "fribidi";
    repo = "fribidi";
    rev = "v${version}";
    hash = "sha256-U3wVnCFnII09NqRcKCworhlGh30WETJkIDJQqcfU5b4=";
  };
}
