self: super: with self; rec {
  version = "20190702";

  src = fetchFromGitHub {
    owner = "P-H-C";
    repo = "phc-winner-argon2";
    rev = version;
    sha256 = "0p4ry9dn0mi9js0byijxdyiwx74p1nr8zj7wjpd1fjgqva4sk23i";
  };
}
