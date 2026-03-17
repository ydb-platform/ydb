self: super: with self; rec {
  pname = "fribidi";
  version = "1.0.16";

  nativeBuildInputs = [ meson ninja pkg-config ];

  src = fetchFromGitHub {
    owner = "fribidi";
    repo = "fribidi";
    rev = "v${version}";
    hash = "sha256-VXDgyqpgjeNHnKfihWW0Oe1yzwIv1Bw8mrs8wLBJZgw=";
  };
}
